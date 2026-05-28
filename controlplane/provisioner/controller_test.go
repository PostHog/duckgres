//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
		return fmt.Errorf("warehouse %q: %w", orgID, configstore.ErrWarehouseStateMismatch)
	}
	if w.State != expectedState {
		return fmt.Errorf("warehouse %q expected %q got %q: %w", orgID, expectedState, w.State, configstore.ErrWarehouseStateMismatch)
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
		case "iceberg_table_bucket_arn":
			w.Iceberg.TableBucketArn = v.(string)
		case "iceberg_region":
			w.Iceberg.Region = v.(string)
		case "iceberg_namespace":
			w.Iceberg.Namespace = v.(string)
		case "iceberg_state":
			w.IcebergState = v.(configstore.ManagedWarehouseProvisioningState)
		case "iceberg_enabled":
			w.Iceberg.Enabled = v.(bool)
		case "iceberg_backend":
			w.Iceberg.Backend = v.(string)
		case "iceberg_lakekeeper_endpoint":
			w.Iceberg.LakekeeperEndpoint = v.(string)
		case "iceberg_lakekeeper_warehouse":
			w.Iceberg.LakekeeperWarehouse = v.(string)
		case "iceberg_lakekeeper_client_id":
			w.Iceberg.LakekeeperClientID = v.(string)
		case "iceberg_lakekeeper_oauth2_server_uri":
			w.Iceberg.LakekeeperOAuth2ServerURI = v.(string)
		case "iceberg_lakekeeper_client_credentials_namespace":
			w.Iceberg.LakekeeperClientCredentials.Namespace = v.(string)
		case "iceberg_lakekeeper_client_credentials_name":
			w.Iceberg.LakekeeperClientCredentials.Name = v.(string)
		case "iceberg_lakekeeper_client_credentials_key":
			w.Iceberg.LakekeeperClientCredentials.Key = v.(string)
		}
	}
	return nil
}

// UpdateIcebergConfig writes per-org Iceberg/Lakekeeper fields without a
// top-level state CAS. Mirrors the real configstore method's contract.
func (s *fakeStore) UpdateIcebergConfig(orgID string, updates map[string]interface{}) error {
	w, ok := s.warehouses[orgID]
	if !ok {
		return fmt.Errorf("warehouse %q: %w", orgID, configstore.ErrWarehouseNotFound)
	}
	for k, v := range updates {
		switch k {
		case "iceberg_enabled":
			w.Iceberg.Enabled = v.(bool)
		case "iceberg_backend":
			w.Iceberg.Backend = v.(string)
		case "iceberg_namespace":
			w.Iceberg.Namespace = v.(string)
		case "iceberg_region":
			w.Iceberg.Region = v.(string)
		case "iceberg_table_bucket_arn":
			w.Iceberg.TableBucketArn = v.(string)
		case "iceberg_state":
			w.IcebergState = v.(configstore.ManagedWarehouseProvisioningState)
		case "iceberg_lakekeeper_endpoint":
			w.Iceberg.LakekeeperEndpoint = v.(string)
		case "iceberg_lakekeeper_warehouse":
			w.Iceberg.LakekeeperWarehouse = v.(string)
		case "iceberg_lakekeeper_client_id":
			w.Iceberg.LakekeeperClientID = v.(string)
		case "iceberg_lakekeeper_oauth2_server_uri":
			w.Iceberg.LakekeeperOAuth2ServerURI = v.(string)
		case "iceberg_lakekeeper_client_credentials_namespace":
			w.Iceberg.LakekeeperClientCredentials.Namespace = v.(string)
		case "iceberg_lakekeeper_client_credentials_name":
			w.Iceberg.LakekeeperClientCredentials.Name = v.(string)
		case "iceberg_lakekeeper_client_credentials_key":
			w.Iceberg.LakekeeperClientCredentials.Key = v.(string)
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

func TestReconcileReadyPatchesCRWhenIcebergFlippedOn(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-iceberg-on"] = &configstore.ManagedWarehouse{
		OrgID:   "org-iceberg-on",
		State:   configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{Enabled: true},
	}
	// Seed a CR with no iceberg block — represents a warehouse that
	// existed before iceberg was opted into.
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      ducklingName("org-iceberg-on"),
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
			"dataStore": map[string]interface{}{"type": "s3bucket"},
		},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), ducklingName("org-iceberg-on"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	spec := got.Object["spec"].(map[string]interface{})
	iceberg, ok := spec["iceberg"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected iceberg block after drift patch, got spec=%v", spec)
	}
	if iceberg["enabled"] != true {
		t.Fatalf("expected iceberg.enabled=true, got %v", iceberg["enabled"])
	}
	// Merge-patch must not wipe sibling spec fields (metadataStore / dataStore).
	if _, ok := spec["metadataStore"].(map[string]interface{}); !ok {
		t.Fatalf("expected metadataStore preserved after iceberg patch")
	}
	if _, ok := spec["dataStore"].(map[string]interface{}); !ok {
		t.Fatalf("expected dataStore preserved after iceberg patch")
	}
}

func TestReconcileReadyPatchesCRWhenIcebergFlippedOff(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-iceberg-off"] = &configstore.ManagedWarehouse{
		OrgID:   "org-iceberg-off",
		State:   configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{Enabled: false},
	}
	// Seed a CR that currently has iceberg enabled — expect drift back to false.
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      ducklingName("org-iceberg-off"),
			"namespace": ducklingNamespace,
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{"type": "aurora"},
			"dataStore":     map[string]interface{}{"type": "s3bucket"},
			"iceberg":       map[string]interface{}{"enabled": true},
		},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), ducklingName("org-iceberg-off"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	spec := got.Object["spec"].(map[string]interface{})
	iceberg := spec["iceberg"].(map[string]interface{})
	if iceberg["enabled"] != false {
		t.Fatalf("expected iceberg.enabled=false after drift patch, got %v", iceberg["enabled"])
	}
}

func TestReconcileReadyPropagatesIcebergStatusToConfigstore(t *testing.T) {
	// Covers the late-iceberg-enable case: warehouse is already Ready,
	// then iceberg gets opted in via admin API. The Crossplane composition
	// eventually populates status.iceberg.tableBucketArn on the Duckling.
	// reconcileReady must copy that back to the configstore — otherwise
	// the worker activator's IcebergConfig.TableBucket stays empty and
	// AttachIcebergCatalog skips the attach.
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-iceberg-late"] = &configstore.ManagedWarehouse{
		OrgID: "org-iceberg-late",
		State: configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			// TableBucketArn / Region / Namespace are intentionally empty —
			// this is the configstore state right after an admin API PUT
			// that just flipped iceberg.enabled=true.
		},
	}
	const tableBucketArn = "arn:aws:s3tables:us-east-1:123456789012:bucket/org-iceberg-late-iceberg"
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      ducklingName("org-iceberg-late"),
			"namespace": ducklingNamespace,
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{"type": "aurora"},
			"dataStore":     map[string]interface{}{"type": "s3bucket"},
			"iceberg":       map[string]interface{}{"enabled": true},
		},
		"status": map[string]interface{}{
			"iceberg": map[string]interface{}{
				"tableBucketArn": tableBucketArn,
				"region":         "us-east-1",
				"namespaceName":  "main",
			},
		},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	got := fs.warehouses["org-iceberg-late"]
	if got.Iceberg.TableBucketArn != tableBucketArn {
		t.Fatalf("expected configstore iceberg.table_bucket_arn=%q, got %q", tableBucketArn, got.Iceberg.TableBucketArn)
	}
	if got.Iceberg.Region != "us-east-1" {
		t.Fatalf("expected iceberg.region=us-east-1, got %q", got.Iceberg.Region)
	}
	if got.Iceberg.Namespace != "main" {
		t.Fatalf("expected iceberg.namespace=main, got %q", got.Iceberg.Namespace)
	}
	if got.IcebergState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected iceberg_state=ready, got %q", got.IcebergState)
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
		OrgID:     "org-b",
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
	// Substitute a passing probe — the real one would try to dial the
	// duckling's pgbouncer service, which doesn't exist in this unit test.
	ctrl.SetProbe(func(context.Context, string, string, string, string, string) error { return nil })
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

// TestReconcileProvisioningProbeFailsKeepsProvisioning verifies the controller
// stays in the Provisioning state when the metadata-store probe fails — the
// case our load test surfaced where Aurora reports Available but its DNS
// hasn't propagated yet. Flipping to Ready in that window produces a flurry
// of "Worker activation failed" errors as workers try to connect through
// pgbouncer.
func TestReconcileProvisioningProbeFailsKeepsProvisioning(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-probe"] = &configstore.ManagedWarehouse{
		OrgID:     "org-probe",
		State:     configstore.ManagedWarehouseStateProvisioning,
		CreatedAt: time.Now(),
	}

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      ducklingName("org-probe"),
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":              "aurora",
					"endpoint":          "org-probe.cluster.us-east-1.rds.amazonaws.com",
					"pgbouncerEndpoint": "pgbouncer-duckling-org-probe.ducklings.svc.cluster.local:6543",
					"password":          "supersecret123",
					"user":              "postgres",
					"database":          "postgres",
				},
				"dataStore": map[string]interface{}{
					"type":       "s3bucket",
					"bucketName": "org-probe-bucket",
				},
				"iamRoleArn": "arn:aws:iam::123456789012:role/duckling-org-probe",
				"conditions": []interface{}{
					map[string]interface{}{"type": "Ready", "status": "True"},
					map[string]interface{}{"type": "Synced", "status": "True"},
				},
			},
		},
	}

	ctx := context.Background()
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create test CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.SetProbe(func(_ context.Context, endpoint, _, _, _, sslMode string) error {
		// PgBouncer is disabled by default (ManagedWarehousePgBouncer.Enabled
		// zero value), so the controller should probe the direct Aurora
		// endpoint with sslmode=require — not the pgbouncer Service.
		if endpoint != "org-probe.cluster.us-east-1.rds.amazonaws.com" {
			t.Errorf("expected direct endpoint, got %q", endpoint)
		}
		if sslMode != "require" {
			t.Errorf("expected sslmode=require for direct probe, got %q", sslMode)
		}
		return fmt.Errorf("dial tcp: no such host")
	})
	ctrl.reconcile(ctx)

	w := fs.warehouses["org-probe"]
	if w.State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected state to remain provisioning, got %q", w.State)
	}
	// Sub-states should still be promoted to ready — only the top-level state stays gated.
	if w.S3State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected s3_state ready, got %q", w.S3State)
	}
	if w.MetadataStoreState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected metadata_store_state ready, got %q", w.MetadataStoreState)
	}
	if w.StatusMessage == "" || !strings.Contains(w.StatusMessage, "reachability") {
		t.Fatalf("expected status_message to mention reachability, got %q", w.StatusMessage)
	}
	if w.ReadyAt != nil {
		t.Fatalf("expected ready_at to remain unset, got %v", w.ReadyAt)
	}

	// A subsequent reconcile with a passing probe should flip to ready.
	ctrl.SetProbe(func(context.Context, string, string, string, string, string) error { return nil })
	ctrl.reconcile(ctx)
	if fs.warehouses["org-probe"].State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected state ready after probe recovered, got %q", fs.warehouses["org-probe"].State)
	}
}

// TestReconcileProvisioningProbesPgBouncerWhenEnabled asserts that warehouses
// with pgbouncer.enabled=true have their end-to-end probe directed at the
// pgbouncer Service (sslmode=disable), not the direct Aurora endpoint —
// pgbouncer's resolver is the slow lookup that motivated the probe.
func TestReconcileProvisioningProbesPgBouncerWhenEnabled(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-pgb"] = &configstore.ManagedWarehouse{
		OrgID:     "org-pgb",
		State:     configstore.ManagedWarehouseStateProvisioning,
		PgBouncer: configstore.ManagedWarehousePgBouncer{Enabled: true},
		CreatedAt: time.Now(),
	}

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      ducklingName("org-pgb"),
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":              "aurora",
					"endpoint":          "org-pgb.cluster.us-east-1.rds.amazonaws.com",
					"pgbouncerEndpoint": "pgbouncer-duckling-org-pgb.ducklings.svc.cluster.local:6543",
					"password":          "supersecret123",
					"user":              "postgres",
					"database":          "postgres",
				},
				"dataStore":  map[string]interface{}{"type": "s3bucket", "bucketName": "org-pgb-bucket"},
				"iamRoleArn": "arn:aws:iam::123456789012:role/duckling-org-pgb",
				"conditions": []interface{}{
					map[string]interface{}{"type": "Ready", "status": "True"},
					map[string]interface{}{"type": "Synced", "status": "True"},
				},
			},
		},
	}

	ctx := context.Background()
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create test CR: %v", err)
	}

	var seenEndpoint, seenSSLMode string
	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.SetProbe(func(_ context.Context, endpoint, _, _, _, sslMode string) error {
		seenEndpoint, seenSSLMode = endpoint, sslMode
		return nil
	})
	ctrl.reconcile(ctx)

	if seenEndpoint != "pgbouncer-duckling-org-pgb.ducklings.svc.cluster.local:6543" {
		t.Errorf("expected probe to use pgbouncer endpoint, got %q", seenEndpoint)
	}
	if seenSSLMode != "disable" {
		t.Errorf("expected sslmode=disable for pgbouncer probe, got %q", seenSSLMode)
	}
	if fs.warehouses["org-pgb"].State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected state ready, got %q", fs.warehouses["org-pgb"].State)
	}
}

func TestReconcileDeletingDeletesCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-c"] = &configstore.ManagedWarehouse{
		OrgID: "org-c",
		State: configstore.ManagedWarehouseStateDeleting,
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
		State: configstore.ManagedWarehouseStateDeleting,
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
		State: configstore.ManagedWarehouseStatePending,
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

	// CAS update with wrong expected state should return ErrWarehouseStateMismatch
	// and leave the row unchanged.
	err = fs.UpdateWarehouseState("org-x", configstore.ManagedWarehouseStatePending, map[string]interface{}{
		"state": configstore.ManagedWarehouseStateFailed,
	})
	if !errors.Is(err, configstore.ErrWarehouseStateMismatch) {
		t.Fatalf("expected ErrWarehouseStateMismatch, got: %v", err)
	}
	if fs.warehouses["org-x"].State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected state to remain provisioning, got %q", fs.warehouses["org-x"].State)
	}
}

// --- cnpg-shard metadata store ---

// TestReconcilePendingCreatesCnpgShardCR verifies that a warehouse whose
// metadata-store kind is cnpg-shard produces a Duckling CR with
// metadataStore.type=cnpg-shard, no aurora/pgbouncer blocks, and the iceberg
// block enabled — the shape the composition expects for a Lakekeeper-backed
// shard tenant.
func TestReconcilePendingCreatesCnpgShardCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-cnpg"] = &configstore.ManagedWarehouse{
		OrgID: "org-cnpg",
		State: configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind: configstore.MetadataStoreKindCnpgShard,
		},
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendLakekeeper,
		},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("org-cnpg"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR to exist: %v", err)
	}
	spec := cr.Object["spec"].(map[string]interface{})
	metadataStore := spec["metadataStore"].(map[string]interface{})
	if metadataStore["type"] != configstore.MetadataStoreKindCnpgShard {
		t.Fatalf("metadataStore.type = %v, want cnpg-shard", metadataStore["type"])
	}
	if _, present := metadataStore["aurora"]; present {
		t.Errorf("cnpg-shard CR must not carry an aurora block, got %v", metadataStore["aurora"])
	}
	if _, present := metadataStore["pgbouncer"]; present {
		t.Errorf("cnpg-shard CR must not carry a pgbouncer block, got %v", metadataStore["pgbouncer"])
	}
	iceberg, ok := spec["iceberg"].(map[string]interface{})
	if !ok || iceberg["enabled"] != true {
		t.Errorf("expected iceberg.enabled=true on cnpg-shard CR, got %v", spec["iceberg"])
	}
	// DuckLake is emitted explicitly (false here — iceberg-only cnpg).
	ducklake, ok := spec["ducklake"].(map[string]interface{})
	if !ok || ducklake["enabled"] != false {
		t.Errorf("expected ducklake.enabled=false on iceberg-only cnpg-shard CR, got %v", spec["ducklake"])
	}
	if fs.warehouses["org-cnpg"].State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected provisioning state, got %q", fs.warehouses["org-cnpg"].State)
	}
}

// TestReconcilePendingCreatesDuckLakeOnlyCnpgCR verifies the decoupled combo:
// cnpg-shard with DuckLake on and Iceberg off → ducklake.enabled=true, no
// iceberg block.
func TestReconcilePendingCreatesDuckLakeOnlyCnpgCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-dlcnpg"] = &configstore.ManagedWarehouse{
		OrgID:         "org-dlcnpg",
		State:         configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{Kind: configstore.MetadataStoreKindCnpgShard},
		DuckLake:      configstore.ManagedWarehouseDuckLake{Enabled: true},
	}
	ctx := context.Background()
	NewControllerWithClient(fs, dc, time.Second).reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("org-dlcnpg"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR to exist: %v", err)
	}
	spec := cr.Object["spec"].(map[string]interface{})
	if dl, ok := spec["ducklake"].(map[string]interface{}); !ok || dl["enabled"] != true {
		t.Errorf("expected ducklake.enabled=true, got %v", spec["ducklake"])
	}
	if _, present := spec["iceberg"]; present {
		t.Errorf("ducklake-only cnpg CR must not carry an iceberg block, got %v", spec["iceberg"])
	}
}

// TestDucklingCreateCnpgShardRequiresCatalog verifies the Create guard: a
// cnpg-shard CR with neither DuckLake nor Iceberg has nothing to attach and is
// rejected.
func TestDucklingCreateCnpgShardRequiresCatalog(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()

	err := dc.Create(ctx, "no-catalog", CreateOptions{
		MetadataStoreType: configstore.MetadataStoreKindCnpgShard,
		IcebergEnabled:    false,
		DuckLakeEnabled:   false,
	})
	if err == nil {
		t.Fatal("expected error creating cnpg-shard CR with no catalog enabled")
	}
	if _, getErr := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("no-catalog"), metav1.GetOptions{}); getErr == nil {
		t.Error("CR should not have been created when validation failed")
	}
}

// TestDucklingCreateRejectsUnsupportedType verifies the control plane refuses
// to create metadata-store types it doesn't provision (notably "external").
func TestDucklingCreateRejectsUnsupportedType(t *testing.T) {
	dc, _ := newFakeDucklingClient()
	if err := dc.Create(context.Background(), "ext-org", CreateOptions{MetadataStoreType: "external"}); err == nil {
		t.Fatal("expected error creating CR with unsupported metadata store type 'external'")
	}
}

// TestReconcileProvisioningCnpgShardGatedOnIceberg verifies a cnpg-shard
// warehouse does not flip to Ready while iceberg_state is unset (the Lakekeeper
// catalog isn't up yet), then reaches Ready once iceberg_state is Ready. This
// is the gating that the reconcileProvisioning -> reconcileLakekeeper call
// satisfies in production; here we drive iceberg_state directly since no
// Lakekeeper provisioner is wired.
func TestReconcileProvisioningCnpgShardGatedOnIceberg(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-cs"] = &configstore.ManagedWarehouse{
		OrgID:     "org-cs",
		State:     configstore.ManagedWarehouseStateProvisioning,
		CreatedAt: time.Now(),
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind: configstore.MetadataStoreKindCnpgShard,
		},
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendLakekeeper,
		},
	}

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      ducklingName("org-cs"),
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":     configstore.MetadataStoreKindCnpgShard,
					"endpoint": "shard-001-pooler.cnpg-shards.svc.cluster.local",
					"password": "from-provider-sql",
					"user":     "lakekeeper_org_cs",
					"database": "lakekeeper_org_cs",
				},
				"dataStore": map[string]interface{}{
					"type":       "s3bucket",
					"bucketName": "org-cs-bucket",
				},
				"iamRoleArn": "arn:aws:iam::123456789012:role/duckling-org-cs",
				"conditions": []interface{}{
					map[string]interface{}{"type": "Ready", "status": "True"},
					map[string]interface{}{"type": "Synced", "status": "True"},
				},
			},
		},
	}
	ctx := context.Background()
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.SetProbe(func(context.Context, string, string, string, string, string) error { return nil })

	// First pass: infra is ready but iceberg_state is still pending (no
	// Lakekeeper provisioner wired), so the warehouse must stay in provisioning.
	ctrl.reconcile(ctx)
	w := fs.warehouses["org-cs"]
	if w.MetadataStoreState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected metadata_store_state ready, got %q", w.MetadataStoreState)
	}
	if w.State == configstore.ManagedWarehouseStateReady {
		t.Fatal("warehouse must not be Ready while iceberg (Lakekeeper) is unprovisioned")
	}

	// Simulate the Lakekeeper provisioner having completed.
	if err := fs.UpdateIcebergConfig("org-cs", map[string]interface{}{
		"iceberg_state":               configstore.ManagedWarehouseStateReady,
		"iceberg_lakekeeper_endpoint": "http://lakekeeper-org-cs.lakekeeper.svc/catalog",
	}); err != nil {
		t.Fatalf("update iceberg config: %v", err)
	}

	// Second pass: now all components incl. iceberg are ready -> Ready.
	ctrl.reconcile(ctx)
	if fs.warehouses["org-cs"].State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected ready state after iceberg ready, got %q", fs.warehouses["org-cs"].State)
	}
}

// --- external metadata store (iceberg+external and ducklake+external) ---

// TestReconcilePendingCreatesIcebergExternalCR verifies a warehouse with an
// external metadata store + iceberg produces a Duckling CR with
// metadataStore.type=external (carrying endpoint/passwordAwsSecret/user/
// database), an external dataStore reusing the named bucket, and iceberg on.
func TestReconcilePendingCreatesIcebergExternalCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-ext"] = &configstore.ManagedWarehouse{
		OrgID: "org-ext",
		State: configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:              configstore.MetadataStoreKindExternal,
			Endpoint:          "rds.example.us-east-1.rds.amazonaws.com",
			Username:          "postgres",
			DatabaseName:      "postgres",
			PasswordAWSSecret: "duckling-example-rds-password",
		},
		DataStore: configstore.ManagedWarehouseDataStore{
			Kind:       "external",
			BucketName: "posthog-duckling-example",
			Region:     "us-east-1",
		},
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendLakekeeper,
		},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("org-ext"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR to exist: %v", err)
	}
	spec := cr.Object["spec"].(map[string]interface{})
	ms := spec["metadataStore"].(map[string]interface{})
	if ms["type"] != configstore.MetadataStoreKindExternal {
		t.Fatalf("metadataStore.type = %v, want external", ms["type"])
	}
	ext, ok := ms["external"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadataStore.external block, got %v", ms)
	}
	if ext["endpoint"] != "rds.example.us-east-1.rds.amazonaws.com" || ext["passwordAwsSecret"] != "duckling-example-rds-password" {
		t.Errorf("external endpoint/secret wrong: %v", ext)
	}
	if ext["user"] != "postgres" || ext["database"] != "postgres" {
		t.Errorf("external user/database wrong: %v", ext)
	}
	ds := spec["dataStore"].(map[string]interface{})
	if ds["type"] != "external" {
		t.Fatalf("dataStore.type = %v, want external", ds["type"])
	}
	dsExt, ok := ds["external"].(map[string]interface{})
	if !ok || dsExt["bucketName"] != "posthog-duckling-example" || dsExt["region"] != "us-east-1" {
		t.Errorf("dataStore.external wrong: %v", ds["external"])
	}
	iceberg, ok := spec["iceberg"].(map[string]interface{})
	if !ok || iceberg["enabled"] != true {
		t.Errorf("expected iceberg.enabled=true, got %v", spec["iceberg"])
	}
}

// TestReconcilePendingCreatesDuckLakeExternalCR verifies external metadata
// WITHOUT iceberg yields a CR with no iceberg block (DuckLake-on-external).
func TestReconcilePendingCreatesDuckLakeExternalCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-dl"] = &configstore.ManagedWarehouse{
		OrgID: "org-dl",
		State: configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:              configstore.MetadataStoreKindExternal,
			Endpoint:          "rds.example.us-east-1.rds.amazonaws.com",
			PasswordAWSSecret: "duckling-example-rds-password",
		},
		DataStore: configstore.ManagedWarehouseDataStore{
			Kind: "external", BucketName: "posthog-duckling-example", Region: "us-east-1",
		},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("org-dl"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR to exist: %v", err)
	}
	spec := cr.Object["spec"].(map[string]interface{})
	if spec["metadataStore"].(map[string]interface{})["type"] != configstore.MetadataStoreKindExternal {
		t.Fatalf("metadataStore.type wrong: %v", spec["metadataStore"])
	}
	if _, present := spec["iceberg"]; present {
		t.Errorf("ducklake+external CR must not carry an iceberg block, got %v", spec["iceberg"])
	}
}

// TestDucklingCreateExternalRequiresFields verifies the Create guard: an
// external CR is rejected without endpoint + passwordAwsSecret.
func TestDucklingCreateExternalRequiresFields(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()
	if err := dc.Create(ctx, "ext-bad", CreateOptions{MetadataStoreType: configstore.MetadataStoreKindExternal}); err == nil {
		t.Fatal("expected error creating external CR without endpoint/passwordAwsSecret")
	}
	if _, getErr := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, ducklingName("ext-bad"), metav1.GetOptions{}); getErr == nil {
		t.Error("CR should not have been created when external validation failed")
	}
}

// TestDucklingCreateExternalDataStoreRequiresBucket verifies the dataStore
// guard: an external dataStore needs a bucket name.
func TestDucklingCreateExternalDataStoreRequiresBucket(t *testing.T) {
	dc, _ := newFakeDucklingClient()
	err := dc.Create(context.Background(), "ext-nobucket", CreateOptions{
		MetadataStoreType:         configstore.MetadataStoreKindExternal,
		ExternalEndpoint:          "h",
		ExternalPasswordAWSSecret: "s",
		DataStoreType:             "external",
	})
	if err == nil {
		t.Fatal("expected error: external dataStore without a bucket name")
	}
}

// TestDucklingGetFallsBackToLegacyName verifies the backward-compat path: a CR
// created before ducklingName preserved hyphens (i.e. named with hyphens
// stripped) is still found when looked up by its hyphenated org ID. Without
// this, switching ducklingName to keep hyphens would orphan existing prod
// ducklings whose org IDs contain hyphens (e.g. UUID-named tenants).
func TestDucklingGetFallsBackToLegacyName(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()

	org := "018d351a-9ff7-0000-eaff-4628875ad045"
	legacy := legacyDucklingName(org) // "018d351a9ff70000eaff4628875ad045"
	if ducklingName(org) == legacy {
		t.Fatal("test premise: hyphenated org id must differ from its legacy de-hyphenated name")
	}

	// Seed a CR under the legacy (de-hyphenated) name, as the old code would have.
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata":   map[string]interface{}{"name": legacy, "namespace": ducklingNamespace},
		"status":     map[string]interface{}{"iamRoleArn": "arn:aws:iam::123:role/duckling-" + legacy},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed legacy CR: %v", err)
	}

	st, err := dc.Get(ctx, org)
	if err != nil {
		t.Fatalf("Get must fall back to the legacy de-hyphenated name, got: %v", err)
	}
	if st.IAMRoleARN == "" {
		t.Error("expected to parse the legacy CR's status")
	}

	// And iceberg/pgbouncer reads + delete must resolve it too.
	if _, err := dc.GetIcebergEnabled(ctx, org); err != nil {
		t.Errorf("GetIcebergEnabled fallback: %v", err)
	}
	if err := dc.Delete(ctx, org); err != nil {
		t.Errorf("Delete fallback: %v", err)
	}
	if _, getErr := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, legacy, metav1.GetOptions{}); getErr == nil {
		t.Error("legacy CR should have been deleted via the fallback")
	}
}

// TestParseDucklingStatusDuckLakeEnabled locks in the present/absent contract
// for spec.ducklake.enabled: nil (legacy CR) lets the activator apply the
// type-based default; an explicit true/false is authoritative.
func TestParseDucklingStatusDuckLakeEnabled(t *testing.T) {
	mk := func(spec map[string]interface{}) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"spec":       spec,
			"status":     map[string]interface{}{},
		}}
	}
	ds, _ := parseDucklingStatus(mk(map[string]interface{}{}))
	if ds.DuckLakeEnabled != nil {
		t.Errorf("absent ducklake block → nil, got %v", *ds.DuckLakeEnabled)
	}
	ds, _ = parseDucklingStatus(mk(map[string]interface{}{"ducklake": map[string]interface{}{"enabled": true}}))
	if ds.DuckLakeEnabled == nil || !*ds.DuckLakeEnabled {
		t.Errorf("ducklake.enabled=true → *true, got %v", ds.DuckLakeEnabled)
	}
	ds, _ = parseDucklingStatus(mk(map[string]interface{}{"ducklake": map[string]interface{}{"enabled": false}}))
	if ds.DuckLakeEnabled == nil || *ds.DuckLakeEnabled {
		t.Errorf("ducklake.enabled=false → *false, got %v", ds.DuckLakeEnabled)
	}
}
