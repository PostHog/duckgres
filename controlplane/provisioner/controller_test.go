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
		case "worker_identity_iam_role_arn":
			w.WorkerIdentity.IAMRoleARN = v.(string)
		case "worker_identity_namespace":
			w.WorkerIdentity.Namespace = v.(string)
		case "metadata_store_username":
			w.MetadataStore.Username = v.(string)
		case "metadata_store_database_name":
			w.MetadataStore.DatabaseName = v.(string)
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
	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("org-a"), metav1.GetOptions{})
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

	// Default path has PgBouncer.Enabled=false — no pgbouncer block should appear.
	if _, present := metadataStore["pgbouncer"]; present {
		t.Fatalf("expected no pgbouncer block when disabled, got %v", metadataStore["pgbouncer"])
	}
}

func TestReconcilePendingEmitsPgBouncerBlock(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-pgb"] = &configstore.ManagedWarehouse{
		OrgID:        "org-pgb",
		State:        configstore.ManagedWarehouseStatePending,
		AuroraMinACU: 0.5,
		AuroraMaxACU: 4,
		PgBouncer:    configstore.ManagedWarehousePgBouncer{Enabled: true},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("org-pgb"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR to exist: %v", err)
	}
	spec := cr.Object["spec"].(map[string]interface{})
	metadataStore := spec["metadataStore"].(map[string]interface{})
	pgb, ok := metadataStore["pgbouncer"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected pgbouncer block in metadataStore, got %v", metadataStore)
	}
	if pgb["enabled"] != true {
		t.Fatalf("expected pgbouncer.enabled=true, got %v", pgb["enabled"])
	}
}

func TestReconcileReadyPatchesCRWhenPgBouncerFlippedOn(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-flip"] = &configstore.ManagedWarehouse{
		OrgID:     "org-flip",
		State:     configstore.ManagedWarehouseStateReady,
		PgBouncer: configstore.ManagedWarehousePgBouncer{Enabled: true},
	}
	// Seed a CR whose spec still reflects the pre-flip world (no pgbouncer block).
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      ducklingName("org-flip"),
			"namespace": ducklingNamespace,
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type": "aurora",
				"aurora": map[string]interface{}{
					"minACU": 0.5,
					"maxACU": 2.0,
				},
			},
		},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), ducklingName("org-flip"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	spec := got.Object["spec"].(map[string]interface{})
	ms := spec["metadataStore"].(map[string]interface{})
	pgb, ok := ms["pgbouncer"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected pgbouncer block after drift patch, got %v", ms)
	}
	if pgb["enabled"] != true {
		t.Fatalf("expected pgbouncer.enabled=true, got %v", pgb["enabled"])
	}
	// Merge-patch must not wipe sibling metadataStore fields.
	if ms["type"] != "aurora" {
		t.Fatalf("expected metadataStore.type preserved, got %v", ms["type"])
	}
	if _, ok := ms["aurora"].(map[string]interface{}); !ok {
		t.Fatalf("expected aurora block preserved, got %v", ms)
	}
}

func TestReconcileReadyPatchesCRWhenPgBouncerFlippedOff(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-off"] = &configstore.ManagedWarehouse{
		OrgID:     "org-off",
		State:     configstore.ManagedWarehouseStateReady,
		PgBouncer: configstore.ManagedWarehousePgBouncer{Enabled: false},
	}
	// Seed a CR that currently has pgbouncer enabled — expect it to be
	// patched back to false to match the config store.
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      ducklingName("org-off"),
			"namespace": ducklingNamespace,
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type": "aurora",
				"pgbouncer": map[string]interface{}{
					"enabled": true,
				},
			},
		},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), ducklingName("org-off"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	spec := got.Object["spec"].(map[string]interface{})
	ms := spec["metadataStore"].(map[string]interface{})
	pgb := ms["pgbouncer"].(map[string]interface{})
	if pgb["enabled"] != false {
		t.Fatalf("expected pgbouncer.enabled=false after drift patch, got %v", pgb["enabled"])
	}
}

func TestReconcileReadyNoDriftDoesNotPatch(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-sync"] = &configstore.ManagedWarehouse{
		OrgID:     "org-sync",
		State:     configstore.ManagedWarehouseStateReady,
		PgBouncer: configstore.ManagedWarehousePgBouncer{Enabled: true},
	}
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":            ducklingName("org-sync"),
			"namespace":       ducklingNamespace,
			"resourceVersion": "42",
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type":      "aurora",
				"pgbouncer": map[string]interface{}{"enabled": true},
			},
		},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	seededRV := cr.GetResourceVersion()

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), ducklingName("org-sync"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	if got.GetResourceVersion() != seededRV {
		t.Fatalf("expected no patch when in sync, resourceVersion went %q -> %q", seededRV, got.GetResourceVersion())
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
				"name":      ducklingName("org-b"),
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":     "aurora",
					"endpoint": "org-b.cluster.us-east-1.rds.amazonaws.com",
					"password": "supersecret123",
					"user":     "postgres",
					"database": "postgres",
				},
				"dataStore": map[string]interface{}{
					"type":       "s3bucket",
					"bucketName": "org-b-bucket",
				},
				"iamRoleArn": "arn:aws:iam::123456789012:role/duckling-org-b",
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

	// Verify state transitioned to ready with component states set
	w := fs.warehouses["org-b"]
	if w.State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected ready state, got %q", w.State)
	}
	if w.S3State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected s3_state ready, got %q", w.S3State)
	}
	if w.MetadataStoreState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected metadata_store_state ready, got %q", w.MetadataStoreState)
	}
	if w.SecretsState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected secrets_state ready, got %q", w.SecretsState)
	}
	if w.IdentityState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected identity_state ready, got %q", w.IdentityState)
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
				"name":      ducklingName("org-c"),
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
	_, err = fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("org-c"), metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected CR to be deleted")
		return
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

func TestParseDucklingStatusPgBouncerEndpoint(t *testing.T) {
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":              "aurora",
					"endpoint":          "posthog-duckling-foo.cluster-xyz.us-east-1.rds.amazonaws.com",
					"pgbouncerEndpoint": "pgbouncer-duckling-foo.ducklings.svc.cluster.local:6543",
					"user":              "postgres",
					"database":          "postgres",
					"password":          "s3cret",
				},
			},
		},
	}

	status, err := parseDucklingStatus(cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := status.MetadataStore.PgBouncerEndpoint; got != "pgbouncer-duckling-foo.ducklings.svc.cluster.local:6543" {
		t.Fatalf("PgBouncerEndpoint = %q, want pooler DNS", got)
	}
	if got := status.MetadataStore.Endpoint; got != "posthog-duckling-foo.cluster-xyz.us-east-1.rds.amazonaws.com" {
		t.Fatalf("Endpoint = %q, want Aurora DNS", got)
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
	if status.DataStore.BucketName != "" || status.MetadataStore.Endpoint != "" || status.MetadataStore.Password != "" {
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
