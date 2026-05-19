//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// LakekeeperNamespace is the Kubernetes namespace where per-org Lakekeeper
// instances live. The lakekeeper-operator and its CRD watch every namespace
// by default but we co-locate the CRs to keep RBAC tight.
const LakekeeperNamespace = "lakekeeper"

// lakekeeperGVR matches the operator at /Users/james/opt/ph/lakekeeper-operator.
var lakekeeperGVR = schema.GroupVersionResource{
	Group:    "lakekeeper.k8s.lakekeeper.io",
	Version:  "v1alpha1",
	Resource: "lakekeepers",
}

// LakekeeperK8sClient bundles the dynamic client (for the Lakekeeper CR) and
// the typed clientset (for the backing Secret) so the provisioner can drive
// both with one dependency. Construction mirrors DucklingClient.
type LakekeeperK8sClient struct {
	dynamic    dynamic.Interface
	kubernetes kubernetes.Interface
	namespace  string
}

// NewLakekeeperK8sClient builds a client from in-cluster config.
func NewLakekeeperK8sClient() (*LakekeeperK8sClient, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	return &LakekeeperK8sClient{dynamic: dc, kubernetes: kc, namespace: LakekeeperNamespace}, nil
}

// NewLakekeeperK8sClientWithClients is used by tests with fakes.
func NewLakekeeperK8sClientWithClients(dc dynamic.Interface, kc kubernetes.Interface, namespace string) *LakekeeperK8sClient {
	if namespace == "" {
		namespace = LakekeeperNamespace
	}
	return &LakekeeperK8sClient{dynamic: dc, kubernetes: kc, namespace: namespace}
}

// LakekeeperResourceName derives the K8s resource name (CR + Secret) for an
// org. Matches the ducklingName transform — strips hyphens so UUID-style
// org IDs land under the 63-char K8s name limit.
func LakekeeperResourceName(orgID string) string {
	return "lakekeeper-" + lakekeeperResourceSuffix(orgID)
}

func lakekeeperResourceSuffix(orgID string) string {
	// Inline to avoid coupling to ducklingName which lives in another file
	// under the same build tag.
	out := make([]byte, 0, len(orgID))
	for i := 0; i < len(orgID); i++ {
		if orgID[i] == '-' {
			continue
		}
		out = append(out, orgID[i])
	}
	return string(out)
}

// LakekeeperSecretData is the strongly-typed contents of the per-org Secret
// that the CR's *SecretRef fields point at. All values are required.
type LakekeeperSecretData struct {
	DBUser              string
	DBPassword          string
	EncryptionKey       string // 32-byte key used by Lakekeeper for at-rest secret encryption
	OAuth2ClientSecret  string // client_secret minted for the duckling
}

// SecretKey* are the keys inside the Secret. Stable contract with the CR.
const (
	SecretKeyDBUser             = "db-user"
	SecretKeyDBPassword         = "db-password"
	SecretKeyEncryptionKey      = "encryption-key"
	SecretKeyOAuth2ClientSecret = "oauth2-client-secret"
)

// EnsureSecret creates the per-org Secret in the lakekeeper namespace or
// updates it if it already exists. Update semantics: the four keys are
// replaced wholesale on every call — callers must pass the full desired
// state, not a delta.
func (c *LakekeeperK8sClient) EnsureSecret(ctx context.Context, orgID string, data LakekeeperSecretData) error {
	name := LakekeeperResourceName(orgID)
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.namespace,
			Labels: map[string]string{
				"app":                 "lakekeeper",
				"duckgres/active-org": orgID,
			},
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			SecretKeyDBUser:             data.DBUser,
			SecretKeyDBPassword:         data.DBPassword,
			SecretKeyEncryptionKey:      data.EncryptionKey,
			SecretKeyOAuth2ClientSecret: data.OAuth2ClientSecret,
		},
	}

	secrets := c.kubernetes.CoreV1().Secrets(c.namespace)
	_, err := secrets.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create secret %s: %w", name, err)
	}
	// Update path: fetch resourceVersion + replace.
	existing, getErr := secrets.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("get secret %s for update: %w", name, getErr)
	}
	desired.ResourceVersion = existing.ResourceVersion
	if _, err := secrets.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update secret %s: %w", name, err)
	}
	return nil
}

// LakekeeperCRSpec carries the inputs we need to render a Lakekeeper CR.
// One CR per org. PG connection points at the org's existing managed-warehouse
// Aurora cluster, with the lakekeeper_<orgid> database created by
// EnsureDatabase.
type LakekeeperCRSpec struct {
	OrgID      string
	Image      string // e.g. quay.io/lakekeeper/catalog:0.11.6
	Replicas   int32
	PGHost     string
	PGPort     int32
	PGDatabase string
	// SecretName is the K8s Secret holding db-user, db-password, encryption-key,
	// oauth2-client-secret. Typically LakekeeperResourceName(OrgID).
	SecretName string
	// BaseURI is the externally-visible URL clients (ducklings) use to reach
	// Lakekeeper. In-cluster: http://lakekeeper-<orgid>.lakekeeper.svc:8181
	BaseURI string
}

// EnsureCR creates the Lakekeeper CR for the given org or patches it to match
// spec if it already exists. We use Server-Side Apply for idempotency.
func (c *LakekeeperK8sClient) EnsureCR(ctx context.Context, spec LakekeeperCRSpec) error {
	if spec.Image == "" || spec.PGHost == "" || spec.PGDatabase == "" || spec.SecretName == "" {
		return fmt.Errorf("EnsureCR: missing required field in spec: %+v", spec)
	}
	if spec.Replicas == 0 {
		spec.Replicas = 1
	}
	if spec.PGPort == 0 {
		spec.PGPort = 5432
	}

	name := LakekeeperResourceName(spec.OrgID)
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "lakekeeper.k8s.lakekeeper.io/v1alpha1",
			"kind":       "Lakekeeper",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": c.namespace,
				"labels": map[string]interface{}{
					"app":                 "lakekeeper",
					"duckgres/active-org": spec.OrgID,
				},
			},
			"spec": map[string]interface{}{
				"image":    spec.Image,
				"replicas": int64(spec.Replicas),
				"database": map[string]interface{}{
					"type": "postgres",
					"postgres": map[string]interface{}{
						"host":     spec.PGHost,
						"port":     int64(spec.PGPort),
						"database": spec.PGDatabase,
						"userSecretRef": map[string]interface{}{
							"name": spec.SecretName,
							"key":  SecretKeyDBUser,
						},
						"passwordSecretRef": map[string]interface{}{
							"name": spec.SecretName,
							"key":  SecretKeyDBPassword,
						},
						"encryptionKeySecretRef": map[string]interface{}{
							"name": spec.SecretName,
							"key":  SecretKeyEncryptionKey,
						},
					},
				},
				"authorization": map[string]interface{}{
					"backend": "allowall",
				},
				"bootstrap": map[string]interface{}{
					"enabled": true,
				},
				"server": map[string]interface{}{
					"listenPort":           int64(8181),
					"enableDefaultProject": true,
					"baseURI":              spec.BaseURI,
				},
			},
		},
	}

	resource := c.dynamic.Resource(lakekeeperGVR).Namespace(c.namespace)
	_, err := resource.Create(ctx, cr, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create lakekeeper CR %s: %w", name, err)
	}
	// Update path. Fetch the existing CR to carry over resourceVersion.
	existing, getErr := resource.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("get lakekeeper CR %s for update: %w", name, getErr)
	}
	cr.SetResourceVersion(existing.GetResourceVersion())
	if _, err := resource.Update(ctx, cr, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update lakekeeper CR %s: %w", name, err)
	}
	return nil
}

// LakekeeperCRStatus is the subset of status fields the provisioner reads.
type LakekeeperCRStatus struct {
	Bootstrapped  bool
	ServerID      string
	ReadyReplicas int32
}

// GetCR fetches the current status of the Lakekeeper CR. Returns
// (nil, nil) if the CR does not exist yet, matching the "not provisioned"
// state the provisioner reconciler treats as a no-op.
func (c *LakekeeperK8sClient) GetCR(ctx context.Context, orgID string) (*LakekeeperCRStatus, error) {
	name := LakekeeperResourceName(orgID)
	cr, err := c.dynamic.Resource(lakekeeperGVR).Namespace(c.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get lakekeeper CR %s: %w", name, err)
	}
	st := &LakekeeperCRStatus{}
	status, _ := cr.Object["status"].(map[string]interface{})
	if status == nil {
		return st, nil
	}
	if v, _ := status["bootstrappedAt"].(string); v != "" {
		st.Bootstrapped = true
	}
	if v, _ := status["serverID"].(string); v != "" {
		st.ServerID = v
	}
	// readyReplicas may be int64 or float64 depending on the decoder path.
	switch v := status["readyReplicas"].(type) {
	case int64:
		st.ReadyReplicas = int32(v)
	case float64:
		st.ReadyReplicas = int32(v)
	}
	return st, nil
}
