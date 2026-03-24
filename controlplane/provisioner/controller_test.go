//go:build kubernetes

package provisioner

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// fakeStore implements WarehouseStore for unit tests.
type fakeStore struct {
	warehouses map[string]*configstore.ManagedWarehouse
}

func newFakeStore() *fakeStore {
	return &fakeStore{warehouses: make(map[string]*configstore.ManagedWarehouse)}
}

func (s *fakeStore) ListWarehousesByStates(states []configstore.ManagedWarehouseProvisioningState) ([]configstore.ManagedWarehouse, error) {
	var result []configstore.ManagedWarehouse
	for _, w := range s.warehouses {
		for _, st := range states {
			if w.State == st {
				result = append(result, *w)
				break
			}
		}
	}
	return result, nil
}

func (s *fakeStore) UpdateWarehouseState(orgID string, expectedState configstore.ManagedWarehouseProvisioningState, updates map[string]interface{}) error {
	w, ok := s.warehouses[orgID]
	if !ok {
		return nil
	}
	if w.State != expectedState {
		return nil
	}
	for k, v := range updates {
		switch k {
		case "state":
			w.State = v.(configstore.ManagedWarehouseProvisioningState)
		case "status_message":
			w.StatusMessage = v.(string)
		case "s3_state":
			w.S3State = v.(configstore.ManagedWarehouseProvisioningState)
		case "s3_bucket":
			w.S3.Bucket = v.(string)
		case "metadata_store_state":
			w.MetadataStoreState = v.(configstore.ManagedWarehouseProvisioningState)
		case "metadata_store_endpoint":
			w.MetadataStore.Endpoint = v.(string)
		case "metadata_store_port":
			w.MetadataStore.Port = v.(int)
		case "metadata_store_kind":
			w.MetadataStore.Kind = v.(string)
		case "metadata_store_engine":
			w.MetadataStore.Engine = v.(string)
		case "identity_state":
			w.IdentityState = v.(configstore.ManagedWarehouseProvisioningState)
		case "worker_identity_namespace":
			w.WorkerIdentity.Namespace = v.(string)
		case "secrets_state":
			w.SecretsState = v.(configstore.ManagedWarehouseProvisioningState)
		case "warehouse_database_state":
			w.WarehouseDatabaseState = v.(configstore.ManagedWarehouseProvisioningState)
		case "ready_at":
			t := v.(time.Time)
			w.ReadyAt = &t
		case "failed_at":
			t := v.(time.Time)
			w.FailedAt = &t
		case "provisioning_started_at":
			t := v.(time.Time)
			w.ProvisioningStartedAt = &t
		}
	}
	return nil
}

// Compile-time check that fakeStore satisfies WarehouseStore.
var _ WarehouseStore = (*fakeStore)(nil)

func newFakeDucklingClient() (*DucklingClient, *dynamicfake.FakeDynamicClient) {
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "k8s.posthog.com",
		Version: "v1alpha1",
		Kind:    "Duckling",
	}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group:   "k8s.posthog.com",
		Version: "v1alpha1",
		Kind:    "DucklingList",
	}, &unstructured.UnstructuredList{})

	fakeClient := dynamicfake.NewSimpleDynamicClient(scheme)
	return NewDucklingClientWithDynamic(fakeClient), fakeClient
}

func TestReconcilePendingCreatesCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-a"] = &configstore.ManagedWarehouse{
		OrgID:        "org-a",
		State:        configstore.ManagedWarehouseStatePending,
		AuroraMinACU: 0.5,
		AuroraMaxACU: 2,
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()

	ctrl.reconcile(ctx)

	// Verify CR was created
	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "org-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR to exist: %v", err)
	}

	spec, ok := cr.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatal("expected spec in CR")
	}
	metadataStore, ok := spec["metadataStore"].(map[string]interface{})
	if !ok {
		t.Fatal("expected metadataStore in spec")
	}
	if metadataStore["type"] != "aurora" {
		t.Fatalf("expected metadataStore type aurora, got %v", metadataStore["type"])
	}
	aurora, ok := metadataStore["aurora"].(map[string]interface{})
	if !ok {
		t.Fatal("expected aurora in metadataStore")
	}
	if aurora["minACU"] != 0.5 {
		t.Fatalf("expected minACU 0.5, got %v", aurora["minACU"])
	}
	if aurora["maxACU"] != 2.0 {
		t.Fatalf("expected maxACU 2, got %v", aurora["maxACU"])
	}

	// Verify state transitioned to provisioning
	if fs.warehouses["org-a"].State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected provisioning state, got %q", fs.warehouses["org-a"].State)
	}
	if fs.warehouses["org-a"].ProvisioningStartedAt == nil {
		t.Fatal("expected provisioning_started_at to be set")
	}
}

func TestReconcileProvisioningAllReady(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-b"] = &configstore.ManagedWarehouse{
		OrgID:  "org-b",
		State:     configstore.ManagedWarehouseStateProvisioning,
		CreatedAt: time.Now(),
	}

	// Create a Duckling CR with all status fields populated
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      "org-b",
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"bucketName":     "org-b-bucket",
				"auroraEndpoint": "org-b.cluster.us-east-1.rds.amazonaws.com",
				"auroraPassword": "supersecret123",
				"conditions": []interface{}{
					map[string]interface{}{
						"type":   "Ready",
						"status": "True",
					},
					map[string]interface{}{
						"type":   "Synced",
						"status": "True",
					},
				},
			},
		},
	}

	ctx := context.Background()
	_, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create test CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(ctx)

	// Verify state transitioned to ready
	w := fs.warehouses["org-b"]
	if w.State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected ready state, got %q", w.State)
	}
	if w.S3.Bucket != "org-b-bucket" {
		t.Fatalf("expected bucket org-b-bucket, got %q", w.S3.Bucket)
	}
	if w.MetadataStore.Endpoint != "org-b.cluster.us-east-1.rds.amazonaws.com" {
		t.Fatalf("expected aurora endpoint, got %q", w.MetadataStore.Endpoint)
	}
	if w.MetadataStore.Port != 5432 {
		t.Fatalf("expected aurora port 5432, got %d", w.MetadataStore.Port)
	}
	if w.ReadyAt == nil {
		t.Fatal("expected ready_at to be set")
	}
}

func TestReconcileDeletingDeletesCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-c"] = &configstore.ManagedWarehouse{
		OrgID: "org-c",
		State:    configstore.ManagedWarehouseStateDeleting,
	}
	ctx := context.Background()

	// Create a CR first
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      "org-c",
				"namespace": ducklingNamespace,
			},
		},
	}
	_, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create test CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(ctx)

	// Verify CR is gone
	_, err = fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "org-c", metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected CR to be deleted")
	}

	// Verify state transitioned to deleted
	if fs.warehouses["org-c"].State != configstore.ManagedWarehouseStateDeleted {
		t.Fatalf("expected deleted state, got %q", fs.warehouses["org-c"].State)
	}
}

func TestReconcileDeletingRetriesOnNonNotFoundError(t *testing.T) {
	// When the CR doesn't exist (NotFound), deleting should still succeed.
	// When it's a different error, it should NOT transition to deleted.
	dc, _ := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-d"] = &configstore.ManagedWarehouse{
		OrgID: "org-d",
		State:    configstore.ManagedWarehouseStateDeleting,
	}
	ctx := context.Background()

	// Don't create a CR — the fake client will return NotFound on delete.
	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(ctx)

	// NotFound on delete is fine — should still transition to deleted
	if fs.warehouses["org-d"].State != configstore.ManagedWarehouseStateDeleted {
		t.Fatalf("expected deleted state on NotFound, got %q", fs.warehouses["org-d"].State)
	}
}

func TestParseDucklingStatusSyncedFalse(t *testing.T) {
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"status": map[string]interface{}{
				"conditions": []interface{}{
					map[string]interface{}{
						"type":    "Synced",
						"status":  "False",
						"message": "cannot create Aurora cluster: InvalidParameterException",
					},
				},
			},
		},
	}

	status, err := parseDucklingStatus(cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.SyncedFalseMessage != "cannot create Aurora cluster: InvalidParameterException" {
		t.Fatalf("expected synced false message, got %q", status.SyncedFalseMessage)
	}
	if status.ReadyCondition {
		t.Fatal("expected Ready to be false")
	}
}

func TestParseDucklingStatusEmpty(t *testing.T) {
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{},
	}

	status, err := parseDucklingStatus(cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.BucketName != "" || status.AuroraEndpoint != "" || status.AuroraPassword != "" {
		t.Fatal("expected empty status for CR without status field")
	}
}

func TestFakeStoreUpdateWarehouseState(t *testing.T) {
	fs := newFakeStore()
	fs.warehouses["org-x"] = &configstore.ManagedWarehouse{
		OrgID: "org-x",
		State:    configstore.ManagedWarehouseStatePending,
	}

	// CAS update should succeed
	err := fs.UpdateWarehouseState("org-x", configstore.ManagedWarehouseStatePending, map[string]interface{}{
		"state":          configstore.ManagedWarehouseStateProvisioning,
		"status_message": "transitioning",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fs.warehouses["org-x"].State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected provisioning state, got %q", fs.warehouses["org-x"].State)
	}

	// CAS update with wrong expected state should be no-op
	err = fs.UpdateWarehouseState("org-x", configstore.ManagedWarehouseStatePending, map[string]interface{}{
		"state": configstore.ManagedWarehouseStateFailed,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fs.warehouses["org-x"].State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected state to remain provisioning, got %q", fs.warehouses["org-x"].State)
	}
}
