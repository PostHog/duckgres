//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// k8sLabelValue matches the Kubernetes label-value grammar
// (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])? with ≤63 chars. We use it to
// validate org IDs before stamping them on labels, so a malformed ID
// surfaces as a clear error rather than an opaque API server rejection.
var k8sLabelValue = regexp.MustCompile(`^([A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?)$`)

func isValidOrgIDLabel(orgID string) bool {
	return len(orgID) > 0 && len(orgID) <= 63 && k8sLabelValue.MatchString(orgID)
}

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
//
// On a Get/Update resourceVersion conflict (concurrent recreator between
// our IsAlreadyExists branch and our Update), the returned error wraps the
// apierrors.IsConflict-detectable original so the reconciler can treat it
// as transient and requeue.
func (c *LakekeeperK8sClient) EnsureSecret(ctx context.Context, orgID string, data LakekeeperSecretData) error {
	if !isValidOrgIDLabel(orgID) {
		return fmt.Errorf("EnsureSecret: orgID %q is not a valid K8s label value", orgID)
	}
	name := LakekeeperResourceName(orgID)
	// Write to Data (bytes) rather than StringData (strings). The K8s API
	// server converts StringData → Data on write and clears StringData on
	// read, so production code that reads the Secret only ever sees Data
	// populated. The dynamic-fake clientset doesn't do that conversion, so
	// writing Data directly makes the fake's behavior match real K8s for
	// downstream readers like secretFromExisting.
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
		Data: map[string][]byte{
			SecretKeyDBUser:             []byte(data.DBUser),
			SecretKeyDBPassword:         []byte(data.DBPassword),
			SecretKeyEncryptionKey:      []byte(data.EncryptionKey),
			SecretKeyOAuth2ClientSecret: []byte(data.OAuth2ClientSecret),
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

// LakekeeperServiceAccountName is the per-org ServiceAccount the Lakekeeper
// Deployment + migration Job run under. It matches the CR/Secret resource
// name. The posthog-cloud-infra EKS Pod Identity association keys on this
// exact (namespace, name) pair to bind a per-org IAM role, so changing this
// convention requires a matching Terraform change.
func LakekeeperServiceAccountName(orgID string) string {
	return LakekeeperResourceName(orgID)
}

// EnsureServiceAccount creates the per-org ServiceAccount that the Lakekeeper
// workload runs under. One SA per org — in a single shared namespace — so each
// org's Lakekeeper can carry a distinct cloud identity (EKS Pod Identity)
// scoped to its own object store.
//
// The SA is intentionally bare: EKS Pod Identity binds the IAM role to the
// (namespace, serviceAccount) pair on the AWS side, so no IRSA role-arn
// annotation is needed here. On re-runs we leave an existing SA untouched
// rather than overwriting it, so any annotations added out-of-band (e.g. an
// IRSA role-arn, if a cluster uses IRSA instead of Pod Identity) survive.
func (c *LakekeeperK8sClient) EnsureServiceAccount(ctx context.Context, orgID string) error {
	if !isValidOrgIDLabel(orgID) {
		return fmt.Errorf("EnsureServiceAccount: orgID %q is not a valid K8s label value", orgID)
	}
	name := LakekeeperServiceAccountName(orgID)
	desired := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: c.namespace,
			Labels: map[string]string{
				"app":                 "lakekeeper",
				"duckgres/active-org": orgID,
			},
		},
	}
	sas := c.kubernetes.CoreV1().ServiceAccounts(c.namespace)
	_, err := sas.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil || apierrors.IsAlreadyExists(err) {
		return nil
	}
	return fmt.Errorf("create service account %s: %w", name, err)
}

// LakekeeperCRSpec carries the inputs we need to render a Lakekeeper CR.
// One CR per org. PG connection points at the org's existing managed-warehouse
// Aurora cluster, with the lakekeeper_<orgid> database created by
// EnsureDatabase.
type LakekeeperCRSpec struct {
	OrgID      string
	Image      string // e.g. quay.io/lakekeeper/catalog:v0.11.6
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
	// PGSSLMode is the Postgres SSL mode the Lakekeeper pod uses to connect.
	// Defaults to "require" (the operator's default). Set to "disable" for
	// local/dev where PG runs without TLS.
	PGSSLMode string

	// ServiceAccountName binds the Lakekeeper pod (and migration Job) to a
	// specific ServiceAccount via the operator's spec.serviceAccountName
	// field (a PostHog-fork addition). Empty falls back to the namespace
	// default. We set it to the per-org SA so each org's Lakekeeper carries
	// its own EKS Pod Identity for isolated object-store access.
	ServiceAccountName string

	// KubernetesAuthAudiences, when non-empty, enables the operator's
	// `authentication.kubernetes` mode with the given audiences. The
	// duckling's projected SA token must carry one of these audiences for
	// Lakekeeper to accept it. Empty disables kubernetes auth (Lakekeeper
	// runs in allowall mode behind NetworkPolicy — the PR1+PR2 deployment
	// shape).
	KubernetesAuthAudiences []string
}

// EnsureCR creates the Lakekeeper CR for the given org or patches it to match
// spec if it already exists.
//
// Like EnsureSecret, Update conflicts surface as apierrors.IsConflict-detectable
// errors so the reconciler can treat them as transient and requeue.
func (c *LakekeeperK8sClient) EnsureCR(ctx context.Context, spec LakekeeperCRSpec) error {
	if spec.OrgID == "" {
		return fmt.Errorf("EnsureCR: spec.OrgID is required")
	}
	if !isValidOrgIDLabel(spec.OrgID) {
		return fmt.Errorf("EnsureCR: orgID %q is not a valid K8s label value", spec.OrgID)
	}
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
					"postgres": func() map[string]interface{} {
						pg := map[string]interface{}{
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
						}
						if spec.PGSSLMode != "" {
							pg["sslMode"] = spec.PGSSLMode
						}
						return pg
					}(),
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
	// Bind the workload to the per-org ServiceAccount when set. Maps to the
	// operator's spec.serviceAccountName (PostHog-fork field); empty leaves it
	// unset so the operator falls back to the namespace default.
	if spec.ServiceAccountName != "" {
		cr.Object["spec"].(map[string]interface{})["serviceAccountName"] = spec.ServiceAccountName
	}
	// Optional: enable Kubernetes SA-token authentication. The operator
	// turns this into LAKEKEEPER__K8S_AUTH_ENABLED=true +
	// LAKEKEEPER__K8S_AUTH_AUDIENCES=<csv>, which makes Lakekeeper validate
	// incoming bearer tokens against the K8s TokenReview API.
	if len(spec.KubernetesAuthAudiences) > 0 {
		audiences := make([]interface{}, 0, len(spec.KubernetesAuthAudiences))
		for _, a := range spec.KubernetesAuthAudiences {
			audiences = append(audiences, a)
		}
		cr.Object["spec"].(map[string]interface{})["authentication"] = map[string]interface{}{
			"kubernetes": map[string]interface{}{
				"enabled":   true,
				"audiences": audiences,
			},
		}
	}

	resource := c.dynamic.Resource(lakekeeperGVR).Namespace(c.namespace)
	_, err := resource.Create(ctx, cr, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create lakekeeper CR %s: %w", name, err)
	}
	// Update path. Fetch the existing CR to carry over resourceVersion and
	// status. Real K8s separates status into a subresource, so an Update
	// against the main resource shouldn't touch status — but the fake
	// dynamic client doesn't honor that split, and the behaviour we want
	// either way is "spec drift correction never overwrites the operator's
	// status." So copy the existing status into the desired CR.
	existing, getErr := resource.Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("get lakekeeper CR %s for update: %w", name, getErr)
	}
	cr.SetResourceVersion(existing.GetResourceVersion())
	if status, ok := existing.Object["status"]; ok {
		cr.Object["status"] = status
	}
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
