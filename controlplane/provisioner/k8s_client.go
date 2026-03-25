//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
		Type     string
		Endpoint string
		Password string
		User     string
		Database string
	}
	DataStore struct {
		Type       string
		BucketName string
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

// Create creates a Duckling CR for the given org.
func (d *DucklingClient) Create(ctx context.Context, orgID string, minACU, maxACU float64) error {
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      orgID,
				"namespace": ducklingNamespace,
			},
			"spec": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type": "aurora",
					"aurora": map[string]interface{}{
						"minACU": minACU,
						"maxACU": maxACU,
					},
				},
				"dataStore": map[string]interface{}{
					"type": "s3bucket",
				},
			},
		},
	}

	_, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create duckling CR %q: %w", orgID, err)
	}
	return nil
}

// Get fetches the Duckling CR and parses its status.
func (d *DucklingClient) Get(ctx context.Context, orgID string) (*DucklingStatus, error) {
	cr, err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, orgID, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get duckling CR %q: %w", orgID, err)
	}
	return parseDucklingStatus(cr)
}

// Delete removes the Duckling CR for the given org.
func (d *DucklingClient) Delete(ctx context.Context, orgID string) error {
	err := d.client.Resource(ducklingGVR).Namespace(ducklingNamespace).Delete(ctx, orgID, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete duckling CR %q: %w", orgID, err)
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
		ds.MetadataStore.Password = getNestedString(md, "password")
		ds.MetadataStore.User = getNestedString(md, "user")
		ds.MetadataStore.Database = getNestedString(md, "database")
	}

	// Parse status.dataStore
	if store, ok := status["dataStore"].(map[string]interface{}); ok {
		ds.DataStore.Type = getNestedString(store, "type")
		ds.DataStore.BucketName = getNestedString(store, "bucketName")
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
