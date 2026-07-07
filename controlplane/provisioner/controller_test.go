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
	k8stesting "k8s.io/client-go/testing"

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
		DucklingName: "org-a",
		State:        configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:              configstore.MetadataStoreKindExternal,
			Endpoint:          "ext.example.internal",
			PasswordAWSSecret: "ext-secret",
		},
		DuckLake: configstore.ManagedWarehouseDuckLake{Enabled: true},
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
	if metadataStore["type"] != "external" {
		t.Fatalf("expected metadataStore type external, got %v", metadataStore["type"])
	}
	external, ok := metadataStore["external"].(map[string]interface{})
	if !ok {
		t.Fatal("expected external in metadataStore")
	}
	if external["endpoint"] != "ext.example.internal" {
		t.Fatalf("expected endpoint ext.example.internal, got %v", external["endpoint"])
	}
	if external["passwordAwsSecret"] != "ext-secret" {
		t.Fatalf("expected passwordAwsSecret ext-secret, got %v", external["passwordAwsSecret"])
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
		DucklingName: "org-pgb",
		State:        configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:              configstore.MetadataStoreKindExternal,
			Endpoint:          "ext.example.internal",
			PasswordAWSSecret: "ext-secret",
		},
		DuckLake:  configstore.ManagedWarehouseDuckLake{Enabled: true},
		PgBouncer: configstore.ManagedWarehousePgBouncer{Enabled: true},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "org-pgb", metav1.GetOptions{})
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
		OrgID:        "org-flip",
		DucklingName: "org-flip",
		State:        configstore.ManagedWarehouseStateReady,
		PgBouncer:    configstore.ManagedWarehousePgBouncer{Enabled: true},
	}
	// Seed a CR whose spec still reflects the pre-flip world (no pgbouncer block).
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      "org-flip",
			"namespace": ducklingNamespace,
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type": "external",
				"external": map[string]interface{}{
					"endpoint":          "ext.example.internal",
					"passwordAwsSecret": "ext-secret",
				},
			},
		},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), "org-flip", metav1.GetOptions{})
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
	if ms["type"] != "external" {
		t.Fatalf("expected metadataStore.type preserved, got %v", ms["type"])
	}
	if _, ok := ms["external"].(map[string]interface{}); !ok {
		t.Fatalf("expected external block preserved, got %v", ms)
	}
}

func TestReconcileReadyPatchesCRWhenPgBouncerFlippedOff(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-off"] = &configstore.ManagedWarehouse{
		OrgID:        "org-off",
		DucklingName: "org-off",
		State:        configstore.ManagedWarehouseStateReady,
		PgBouncer:    configstore.ManagedWarehousePgBouncer{Enabled: false},
	}
	// Seed a CR that currently has pgbouncer enabled — expect it to be
	// patched back to false to match the config store.
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      "org-off",
			"namespace": ducklingNamespace,
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type": "external",
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

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), "org-off", metav1.GetOptions{})
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
		OrgID:        "org-sync",
		DucklingName: "org-sync",
		State:        configstore.ManagedWarehouseStateReady,
		PgBouncer:    configstore.ManagedWarehousePgBouncer{Enabled: true},
	}
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":            "org-sync",
			"namespace":       ducklingNamespace,
			"resourceVersion": "42",
		},
		"spec": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type":      "external",
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

	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), "org-sync", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	if got.GetResourceVersion() != seededRV {
		t.Fatalf("expected no patch when in sync, resourceVersion went %q -> %q", seededRV, got.GetResourceVersion())
	}
}

// seedCnpgCR creates a cnpg-shard Duckling CR with the given spec override and
// status-pinned shard (either may be empty to omit the field).
func seedCnpgCR(t *testing.T, fakeK8s *dynamicfake.FakeDynamicClient, name, specShard, assignedShard string) {
	t.Helper()
	ms := map[string]interface{}{"type": "cnpg-shard"}
	if specShard != "" {
		ms["cnpgShard"] = specShard
	}
	obj := map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": ducklingNamespace,
		},
		"spec": map[string]interface{}{
			"metadataStore": ms,
		},
	}
	if assignedShard != "" {
		obj["status"] = map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type":          "cnpg-shard",
				"assignedShard": assignedShard,
			},
		}
	}
	cr := &unstructured.Unstructured{Object: obj}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
}

func cnpgWarehouse(org string) *configstore.ManagedWarehouse {
	return &configstore.ManagedWarehouse{
		OrgID:         org,
		DucklingName:  org,
		State:         configstore.ManagedWarehouseStateReady,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{Kind: configstore.MetadataStoreKindCnpgShard},
	}
}

func specCnpgShardOf(t *testing.T, fakeK8s *dynamicfake.FakeDynamicClient, name string) string {
	t.Helper()
	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	spec, _ := got.Object["spec"].(map[string]interface{})
	ms, _ := spec["metadataStore"].(map[string]interface{})
	shard, _ := ms["cnpgShard"].(string)
	return shard
}

// TestReconcileReadyBackfillsCnpgShard pins the derived-output → durable-input
// backfill: a ready cnpg-shard duckling with no spec override gets its own
// status-pinned assignedShard stamped into spec.metadataStore.cnpgShard, with
// sibling fields preserved.
func TestReconcileReadyBackfillsCnpgShard(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-bf"] = cnpgWarehouse("org-bf")
	seedCnpgCR(t, fakeK8s, "org-bf", "", "shard-001")

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	if got := specCnpgShardOf(t, fakeK8s, "org-bf"); got != "shard-001" {
		t.Fatalf("spec.metadataStore.cnpgShard = %q, want shard-001", got)
	}
	got, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(context.Background(), "org-bf", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-fetch CR: %v", err)
	}
	ms := got.Object["spec"].(map[string]interface{})["metadataStore"].(map[string]interface{})
	if ms["type"] != "cnpg-shard" {
		t.Fatalf("merge patch must preserve metadataStore.type, got %v", ms["type"])
	}
}

// TestReconcileCnpgShardSkips pins every no-op path: an existing spec value
// (an operator-set migration override pointing at a DIFFERENT shard) is never
// stomped, a not-yet-stamped pin defers, and non-cnpg metadata stores are
// ignored entirely.
func TestReconcileCnpgShardSkips(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()

	// Operator override present — must survive untouched.
	fs.warehouses["org-override"] = cnpgWarehouse("org-override")
	seedCnpgCR(t, fakeK8s, "org-override", "shard-002", "shard-001")

	// Pin not stamped yet (composition still rendering) — nothing to backfill.
	fs.warehouses["org-unpinned"] = cnpgWarehouse("org-unpinned")
	seedCnpgCR(t, fakeK8s, "org-unpinned", "", "")

	// External metadata store — no shard concept.
	fs.warehouses["org-ext"] = &configstore.ManagedWarehouse{
		OrgID:         "org-ext",
		DucklingName:  "org-ext",
		State:         configstore.ManagedWarehouseStateReady,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{Kind: configstore.MetadataStoreKindExternal},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	if got := specCnpgShardOf(t, fakeK8s, "org-override"); got != "shard-002" {
		t.Fatalf("operator override was stomped: cnpgShard = %q, want shard-002", got)
	}
	if got := specCnpgShardOf(t, fakeK8s, "org-unpinned"); got != "" {
		t.Fatalf("unpinned CR was patched: cnpgShard = %q, want empty", got)
	}
}

// TestReconcileCnpgShardLatchesOnPrunedPatch pins the XRD-drift guard: when
// the API server prunes the field (Duckling XRD predates it — simulated by a
// reactor that swallows the patch), the read-back mismatch latches the
// backfill off so it doesn't churn a patch per tick until the XRD ships.
func TestReconcileCnpgShardLatchesOnPrunedPatch(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-prune"] = cnpgWarehouse("org-prune")
	seedCnpgCR(t, fakeK8s, "org-prune", "", "shard-001")

	patches := 0
	fakeK8s.PrependReactor("patch", "ducklings", func(k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		// Swallow the patch: return the stored object unmodified, like an API
		// server whose XRD prunes the unknown field.
		obj, err := fakeK8s.Tracker().Get(ducklingGVR, ducklingNamespace, "org-prune")
		return true, obj, err
	})

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())
	if patches != 1 {
		t.Fatalf("first tick patches = %d, want 1", patches)
	}
	if !ctrl.cnpgShardFieldUnsupported {
		t.Fatal("pruned patch did not latch cnpgShardFieldUnsupported")
	}

	ctrl.reconcile(context.Background())
	if patches != 1 {
		t.Fatalf("latched backfill still patched: patches = %d, want 1", patches)
	}
}

func TestReconcileProvisioningAllReady(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-b"] = &configstore.ManagedWarehouse{
		OrgID:        "org-b",
		DucklingName: "org-b",
		State:        configstore.ManagedWarehouseStateProvisioning,
		CreatedAt:    time.Now(),
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
				"metadataStore": map[string]interface{}{
					"type":     "external",
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
// case our load test surfaced where the RDS reports Available but its DNS
// hasn't propagated yet. Flipping to Ready in that window produces a flurry
// of "Worker activation failed" errors as workers try to connect through
// pgbouncer.
func TestReconcileProvisioningProbeFailsKeepsProvisioning(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-probe"] = &configstore.ManagedWarehouse{
		OrgID:        "org-probe",
		DucklingName: "org-probe",
		State:        configstore.ManagedWarehouseStateProvisioning,
		CreatedAt:    time.Now(),
	}

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      "org-probe",
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":              "external",
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
		// zero value), so the controller should probe the direct RDS
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
// pgbouncer Service (sslmode=disable), not the direct RDS endpoint —
// pgbouncer's resolver is the slow lookup that motivated the probe.
func TestReconcileProvisioningProbesPgBouncerWhenEnabled(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-pgb"] = &configstore.ManagedWarehouse{
		OrgID:        "org-pgb",
		DucklingName: "org-pgb",
		State:        configstore.ManagedWarehouseStateProvisioning,
		PgBouncer:    configstore.ManagedWarehousePgBouncer{Enabled: true},
		CreatedAt:    time.Now(),
	}

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      "org-pgb",
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":              "external",
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
		OrgID:        "org-c",
		DucklingName: "org-c",
		State:        configstore.ManagedWarehouseStateDeleting,
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
		OrgID:        "org-d",
		DucklingName: "org-d",
		State:        configstore.ManagedWarehouseStateDeleting,
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
						"message": "cannot create metadata store: InvalidParameterException",
					},
				},
			},
		},
	}

	status, err := parseDucklingStatus(cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.SyncedFalseMessage != "cannot create metadata store: InvalidParameterException" {
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
					"type":              "external",
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
		t.Fatalf("Endpoint = %q, want RDS DNS", got)
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
		OrgID:        "org-x",
		DucklingName: "org-x",
		State:        configstore.ManagedWarehouseStatePending,
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
// metadataStore.type=cnpg-shard, no external/pgbouncer blocks, and
// ducklake.enabled=true — the shape the composition expects for a shard tenant.
func TestReconcilePendingCreatesCnpgShardCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-cnpg"] = &configstore.ManagedWarehouse{
		OrgID:        "org-cnpg",
		DucklingName: "org-cnpg",
		State:        configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind: configstore.MetadataStoreKindCnpgShard,
		},
		DuckLake: configstore.ManagedWarehouseDuckLake{Enabled: true},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "org-cnpg", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR to exist: %v", err)
	}
	spec := cr.Object["spec"].(map[string]interface{})
	metadataStore := spec["metadataStore"].(map[string]interface{})
	if metadataStore["type"] != configstore.MetadataStoreKindCnpgShard {
		t.Fatalf("metadataStore.type = %v, want cnpg-shard", metadataStore["type"])
	}
	if _, present := metadataStore["external"]; present {
		t.Errorf("cnpg-shard CR must not carry an external block, got %v", metadataStore["external"])
	}
	if _, present := metadataStore["pgbouncer"]; present {
		t.Errorf("cnpg-shard CR must not carry a pgbouncer block, got %v", metadataStore["pgbouncer"])
	}
	if dl, ok := spec["ducklake"].(map[string]interface{}); !ok || dl["enabled"] != true {
		t.Errorf("expected ducklake.enabled=true on cnpg-shard CR, got %v", spec["ducklake"])
	}
	// Iceberg support was removed: no CR may carry an iceberg block.
	if _, present := spec["iceberg"]; present {
		t.Errorf("CR must not carry an iceberg block, got %v", spec["iceberg"])
	}
	if fs.warehouses["org-cnpg"].State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatalf("expected provisioning state, got %q", fs.warehouses["org-cnpg"].State)
	}
}

// TestDucklingCreateCnpgShardRequiresDuckLake verifies the Create guard: a
// cnpg-shard CR without DuckLake has nothing to attach and is rejected.
func TestDucklingCreateCnpgShardRequiresDuckLake(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()

	err := dc.Create(ctx, "no-catalog", CreateOptions{
		MetadataStoreType: configstore.MetadataStoreKindCnpgShard,
		DuckLakeEnabled:   false,
	})
	if err == nil {
		t.Fatal("expected error creating cnpg-shard CR with no catalog enabled")
	}
	if !strings.Contains(err.Error(), "requires ducklake enabled") {
		t.Fatalf("error = %v, want 'requires ducklake enabled'", err)
	}
	if _, getErr := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "no-catalog", metav1.GetOptions{}); getErr == nil {
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

// TestReconcileProvisioningCnpgShardReadiness verifies a cnpg-shard warehouse
// flips to Ready once the Duckling infra is ready. (Readiness previously also
// gated on iceberg_state / Lakekeeper — that gate is gone with the Iceberg
// removal, so a single reconcile pass suffices.)
func TestReconcileProvisioningCnpgShardReadiness(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-cs"] = &configstore.ManagedWarehouse{
		OrgID:        "org-cs",
		DucklingName: "org-cs",
		State:        configstore.ManagedWarehouseStateProvisioning,
		CreatedAt:    time.Now(),
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind: configstore.MetadataStoreKindCnpgShard,
		},
		DuckLake: configstore.ManagedWarehouseDuckLake{Enabled: true},
	}

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "k8s.posthog.com/v1alpha1",
			"kind":       "Duckling",
			"metadata": map[string]interface{}{
				"name":      "org-cs",
				"namespace": ducklingNamespace,
			},
			"status": map[string]interface{}{
				"metadataStore": map[string]interface{}{
					"type":     configstore.MetadataStoreKindCnpgShard,
					"endpoint": "shard-001-pooler.cnpg-shards.svc.cluster.local",
					"password": "from-provider-sql",
					"user":     "duckgres_org_cs",
					"database": "duckgres_org_cs",
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

	ctrl.reconcile(ctx)
	w := fs.warehouses["org-cs"]
	if w.MetadataStoreState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected metadata_store_state ready, got %q", w.MetadataStoreState)
	}
	if w.State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected ready state once infra is ready, got %q", w.State)
	}
}

// --- external metadata store (ducklake+external) ---

// TestReconcilePendingCreatesDuckLakeExternalCR verifies a warehouse with an
// external metadata store produces a Duckling CR with metadataStore.type=external
// (carrying endpoint/passwordAwsSecret/user/database), an external dataStore
// reusing the named bucket, and ducklake on — with no iceberg block (Iceberg
// support was removed).
func TestReconcilePendingCreatesDuckLakeExternalCR(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-dl"] = &configstore.ManagedWarehouse{
		OrgID:        "org-dl",
		DucklingName: "org-dl",
		State:        configstore.ManagedWarehouseStatePending,
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
		DuckLake: configstore.ManagedWarehouseDuckLake{Enabled: true},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "org-dl", metav1.GetOptions{})
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
	if dl, ok := spec["ducklake"].(map[string]interface{}); !ok || dl["enabled"] != true {
		t.Errorf("expected ducklake.enabled=true, got %v", spec["ducklake"])
	}
	if _, present := spec["iceberg"]; present {
		t.Errorf("CR must not carry an iceberg block, got %v", spec["iceberg"])
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
	if _, getErr := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "ext-bad", metav1.GetOptions{}); getErr == nil {
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

// TestDucklingCreateUsesNameVerbatim locks in the naming contract: Create
// names the CR exactly the passed name — no lowercasing, hyphen-stripping, or
// any other derivation. The name comes from the warehouse row's duckling_name.
func TestDucklingCreateUsesNameVerbatim(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	ctx := context.Background()

	name := "018d351a-9ff7-0000-eaff-4628875ad045"
	if err := dc.Create(ctx, name, CreateOptions{
		MetadataStoreType: configstore.MetadataStoreKindCnpgShard,
		DuckLakeEnabled:   true,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	cr, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected CR named exactly %q: %v", name, err)
	}
	if cr.GetName() != name {
		t.Fatalf("CR name = %q, want %q verbatim", cr.GetName(), name)
	}

	// Lookups, mutation, and delete all take the same exact name.
	if _, err := dc.Get(ctx, name); err != nil {
		t.Errorf("Get by exact name: %v", err)
	}
	if _, err := dc.GetPgBouncerEnabled(ctx, name); err != nil {
		t.Errorf("GetPgBouncerEnabled by exact name: %v", err)
	}
	if err := dc.Delete(ctx, name); err != nil {
		t.Errorf("Delete by exact name: %v", err)
	}
	if _, getErr := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, name, metav1.GetOptions{}); getErr == nil {
		t.Error("CR should have been deleted")
	}
}

// TestReconcileUsesStoredDucklingName verifies the controller addresses the
// Duckling CR by the warehouse row's stored duckling_name — not anything
// derived from the org ID — across the create and delete flows.
func TestReconcileUsesStoredDucklingName(t *testing.T) {
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-stored"] = &configstore.ManagedWarehouse{
		OrgID:        "org-stored",
		DucklingName: "custom-cr-name",
		State:        configstore.ManagedWarehouseStatePending,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind: configstore.MetadataStoreKindCnpgShard,
		},
		DuckLake: configstore.ManagedWarehouseDuckLake{Enabled: true},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctx := context.Background()
	ctrl.reconcile(ctx)

	// The CR must be created under the stored name, not the org ID.
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "custom-cr-name", metav1.GetOptions{}); err != nil {
		t.Fatalf("expected CR under stored duckling_name: %v", err)
	}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "org-stored", metav1.GetOptions{}); err == nil {
		t.Fatal("no CR should exist under the org ID when duckling_name differs")
	}

	// Deletion resolves through the same stored name.
	fs.warehouses["org-stored"].State = configstore.ManagedWarehouseStateDeleting
	ctrl.reconcile(ctx)
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Get(ctx, "custom-cr-name", metav1.GetOptions{}); err == nil {
		t.Fatal("expected CR deleted via stored duckling_name")
	}
	if fs.warehouses["org-stored"].State != configstore.ManagedWarehouseStateDeleted {
		t.Fatalf("expected deleted state, got %q", fs.warehouses["org-stored"].State)
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
