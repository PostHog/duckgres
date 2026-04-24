//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var ducklingGVR = schema.GroupVersionResource{
	Group:    "k8s.posthog.com",
	Version:  "v1alpha1",
	Resource: "ducklings",
}

const ducklingNamespace = "ducklings"

// DucklingStatus holds the parsed status from a Duckling CR.
// The Duckling composition provisions AWS infrastructure (Aurora, S3, IAM)
// but not K8s workloads — those are managed by the duckgres Helm chart.
type DucklingStatus struct {
	MetadataStore struct {
		Type              string
		Endpoint          string
		PgBouncerEndpoint string
		Password          string
		User              string
		Database          string
	}
	DataStore struct {
		Type       string
		BucketName string
		S3Region   string
	}
	IAMRoleARN         string
	ReadyCondition     bool
	SyncedFalseMessage string
}

// DucklingClient wraps a Kubernetes dynamic client for Duckling CR operations.
type DucklingClient struct {
	client dynamic.Interface
}

// NewDucklingClient creates a DucklingClient using in-cluster config.
func NewDucklingClient() (*DucklingClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	dc, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	return &DucklingClient{client: dc}, nil
}

// NewDucklingClientWithDynamic creates a DucklingClient with a provided dynamic.Interface (for testing).
func NewDucklingClientWithDynamic(client dynamic.Interface) *DucklingClient {
	return &DucklingClient{client: client}
}

// ducklingName converts an org ID to a valid K8s/AWS resource name by stripping
// hyphens. This keeps names under the 63-char limit for RDS cluster identifiers
// when the org ID is a full UUID.
func ducklingName(orgID string) string {
	return strings.ReplaceAll(orgID, "-", "")
}

// CreateOptions carries per-org knobs that shape the generated Duckling CR.
type CreateOptions struct {
	MinACU           float64
	MaxACU           float64
	PgBouncerEnabled bool
}

// Create creates a Duckling CR for the given org.
func (d *DucklingClient) Create(ctx context.Context, orgID string, opts CreateOptions) error {
	name := ducklingName(orgID)
	metadataStore := map[string]interface{}{
		"type": "aurora",
		"aurora": map[string]interface{}{
			"minACU": opts.MinACU,
			"maxACU": opts.MaxACU,
		},
	}
	if opts.PgBouncerEnabled {
		metadataStore["pgbouncer"] = map[string]interface{}{
			"enabled": true,
		}
	}
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ducklingNamespace,
			},
			"spec": map[string]interface{}{
				"metadataStore": metadataStore,
				"dataStore": map[string]interface{}{
					"type": "s3bucket",
				},
			},
		},
	}

	_, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create duckling CR %q: %w", name, err)
	}
	return nil
}

// Get fetches the Duckling CR and parses its status.
func (d *DucklingClient) Get(ctx context.Context, orgID string) (*DucklingStatus, error) {
	name := ducklingName(orgID)
	cr, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get duckling CR %q: %w", name, err)
	}
	return parseDucklingStatus(cr)
}

// Delete removes the Duckling CR for the given org.
func (d *DucklingClient) Delete(ctx context.Context, orgID string) error {
	name := ducklingName(orgID)
	err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete duckling CR %q: %w", name, err)
	}
	return nil
}

// GetPgBouncerEnabled reads spec.metadataStore.pgbouncer.enabled from the
// Duckling CR. Missing blocks (composition at an older schema, CR never
// carried a pgbouncer section) are reported as false — same as an explicit
// opt-out — so the caller just needs to compare against the desired value.
func (d *DucklingClient) GetPgBouncerEnabled(ctx context.Context, orgID string) (bool, error) {
	name := ducklingName(orgID)
	cr, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("get duckling CR %q: %w", name, err)
	}
	spec, ok := cr.Object["spec"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	ms, ok := spec["metadataStore"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	pgb, ok := ms["pgbouncer"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	enabled, _ := pgb["enabled"].(bool)
	return enabled, nil
}

// SetPgBouncerEnabled patches spec.metadataStore.pgbouncer.enabled on the
// Duckling CR for the given org. Uses a JSON merge patch (RFC 7396) so the
// call is idempotent and only touches the pgbouncer block — sibling fields
// under metadataStore (aurora, type) are left untouched.
func (d *DucklingClient) SetPgBouncerEnabled(ctx context.Context, orgID string, enabled bool) error {
	name := ducklingName(orgID)
	patch, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"pgbouncer": map[string]interface{}{
					"enabled": enabled,
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal pgbouncer patch for %q: %w", name, err)
	}
	_, err = d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch duckling CR %q pgbouncer: %w", name, err)
	}
	return nil
}

func parseDucklingStatus(cr *unstructured.Unstructured) (*DucklingStatus, error) {
	status, ok := cr.Object["status"].(map[string]interface{})
	if !ok {
		return &DucklingStatus{}, nil
	}

	ds := &DucklingStatus{
		IAMRoleARN: getNestedString(status, "iamRoleArn"),
	}

	// Parse status.metadataStore
	if md, ok := status["metadataStore"].(map[string]interface{}); ok {
		ds.MetadataStore.Type = getNestedString(md, "type")
		ds.MetadataStore.Endpoint = getNestedString(md, "endpoint")
		ds.MetadataStore.PgBouncerEndpoint = getNestedString(md, "pgbouncerEndpoint")
		ds.MetadataStore.Password = getNestedString(md, "password")
		ds.MetadataStore.User = getNestedString(md, "user")
		ds.MetadataStore.Database = getNestedString(md, "database")
	}

	// Parse status.dataStore
	if store, ok := status["dataStore"].(map[string]interface{}); ok {
		ds.DataStore.Type = getNestedString(store, "type")
		ds.DataStore.BucketName = getNestedString(store, "bucketName")
		ds.DataStore.S3Region = getNestedString(store, "s3Region")
	}

	// Parse conditions
	conditions, _ := status["conditions"].([]interface{})
	for _, cond := range conditions {
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		condType := getNestedString(condMap, "type")
		condStatus := getNestedString(condMap, "status")

		switch condType {
		case "Ready":
			ds.ReadyCondition = condStatus == "True"
		case "Synced":
			if condStatus == "False" {
				ds.SyncedFalseMessage = getNestedString(condMap, "message")
			}
		}
	}

	return ds, nil
}

func getNestedString(obj map[string]interface{}, key string) string {
	v, _ := obj[key].(string)
	return v
}
