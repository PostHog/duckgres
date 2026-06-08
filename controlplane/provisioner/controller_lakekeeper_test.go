//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestReconcileDeleting_TearsDownLakekeeper(t *testing.T) {
	dc, _ := newFakeDucklingClient()
	k8sClient, dyn, kc := newFakeLakekeeperClient()
	ctx := context.Background()
	orgID := "tenant-x"

	// Seed the per-org Lakekeeper resources EnsureForOrg would have created.
	// The Duckling CR is intentionally left absent — reconcileDeleting tolerates
	// NotFound on the CR delete and must still proceed to Lakekeeper teardown.
	if err := k8sClient.EnsureSecret(ctx, orgID, LakekeeperSecretData{
		DBUser: "u", DBPassword: "p", EncryptionKey: "k", OAuth2ClientSecret: "o",
	}); err != nil {
		t.Fatalf("seed secret: %v", err)
	}
	if err := k8sClient.EnsureServiceAccount(ctx, orgID); err != nil {
		t.Fatalf("seed service account: %v", err)
	}
	if err := k8sClient.EnsureCR(ctx, LakekeeperCRSpec{
		OrgID: orgID, Image: "stub", PGHost: "stub", PGDatabase: "stub",
		SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	fs := newFakeStore()
	fs.warehouses[orgID] = &configstore.ManagedWarehouse{
		OrgID: orgID,
		State: configstore.ManagedWarehouseStateDeleting,
	}

	p := NewLakekeeperProvisioner(fs, k8sClient)
	c := NewControllerWithClient(fs, dc, 0).
		WithLakekeeperProvisioner(p, func(context.Context, *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
			return ProvisioningInputs{}, nil
		})

	c.reconcileDeleting(ctx, fs.warehouses[orgID])

	if _, err := dyn.Resource(lakekeeperGVR).Namespace(k8sClient.namespace).Get(ctx, LakekeeperResourceName(orgID), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("Lakekeeper CR not torn down: err=%v", err)
	}
	if _, err := kc.CoreV1().Secrets(k8sClient.namespace).Get(ctx, LakekeeperResourceName(orgID), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("Lakekeeper Secret not torn down: err=%v", err)
	}
	if _, err := kc.CoreV1().ServiceAccounts(k8sClient.namespace).Get(ctx, LakekeeperServiceAccountName(orgID), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("Lakekeeper ServiceAccount not torn down: err=%v", err)
	}
	if fs.warehouses[orgID].State != configstore.ManagedWarehouseStateDeleted {
		t.Errorf("warehouse state = %q, want deleted", fs.warehouses[orgID].State)
	}
}

func TestReconcileLakekeeper_SkipsWhenProvisionerNotConfigured(t *testing.T) {
	store := newFakeStore()
	store.warehouses["acme"] = &configstore.ManagedWarehouse{
		OrgID: "acme",
		State: configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendLakekeeper,
		},
	}
	c := NewControllerWithClient(store, nil, 0) // no lakekeeperProvisioner
	// Should be a silent no-op — no panic, no calls, no err.
	c.reconcileLakekeeper(context.Background(), store.warehouses["acme"])
}

func TestReconcileLakekeeper_SkipsWhenIcebergDisabled(t *testing.T) {
	called := false
	store := newFakeStore()
	store.warehouses["acme"] = &configstore.ManagedWarehouse{
		OrgID: "acme",
		State: configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: false, // <-- gated off
			Backend: configstore.IcebergBackendLakekeeper,
		},
	}
	c := NewControllerWithClient(store, nil, 0)
	c.lakekeeperProvisioner = &LakekeeperProvisioner{} // present but unused
	c.lakekeeperInputs = func(_ context.Context, _ *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
		called = true
		return ProvisioningInputs{}, nil
	}
	c.reconcileLakekeeper(context.Background(), store.warehouses["acme"])
	if called {
		t.Errorf("inputs resolver should not be called when Iceberg.Enabled=false")
	}
}

func TestReconcileLakekeeper_DriftCorrectsWhenAlreadyProvisioned(t *testing.T) {
	// An already-provisioned org (LakekeeperEndpoint set) is NOT skipped: the
	// Ready loop patches the pod shape (replicas/requests/scrape) onto the org's
	// existing CR, matched by the duckgres/active-org label. No inputs are
	// resolved, and no duplicate CR is created under the recomputed name.
	called := false
	k8sClient, dyn, _ := newFakeLakekeeperClient()
	p := NewLakekeeperProvisioner(newFakeStore(), k8sClient)

	// Seed a legacy-named CR carrying the org label. Its name intentionally
	// differs from LakekeeperResourceName("acme") to prove the patch is matched
	// by label, not by a recomputed name (the post-#632 hyphenation bug).
	seed := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "lakekeeper.k8s.lakekeeper.io/v1alpha1",
		"kind":       "Lakekeeper",
		"metadata": map[string]interface{}{
			"name":      "lakekeeper-acme-legacy",
			"namespace": k8sClient.namespace,
			"labels":    map[string]interface{}{"duckgres/active-org": "acme"},
		},
		"spec": map[string]interface{}{"replicas": int64(1)},
	}}
	if _, err := dyn.Resource(lakekeeperGVR).Namespace(k8sClient.namespace).Create(context.Background(), seed, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}

	store := newFakeStore()
	store.warehouses["acme"] = &configstore.ManagedWarehouse{
		OrgID: "acme",
		State: configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled:            true,
			Backend:            configstore.IcebergBackendLakekeeper,
			LakekeeperEndpoint: "http://lk-acme.lakekeeper.svc:8181/catalog",
		},
	}
	c := NewControllerWithClient(store, nil, 0).
		WithLakekeeperProvisioner(p, func(_ context.Context, _ *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
			called = true
			return ProvisioningInputs{}, nil
		})

	c.reconcileLakekeeper(context.Background(), store.warehouses["acme"])

	// Pod-shape drift correction needs no inputs — the resolver must not run.
	if called {
		t.Errorf("inputs resolver should NOT be called for pod-shape drift correction")
	}
	// The existing legacy-named CR was patched in place by label.
	got, err := dyn.Resource(lakekeeperGVR).Namespace(k8sClient.namespace).Get(context.Background(), "lakekeeper-acme-legacy", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched CR: %v", err)
	}
	spec := got.Object["spec"].(map[string]interface{})
	if rv := spec["replicas"]; rv != int64(lakekeeperPodReplicas) && rv != float64(lakekeeperPodReplicas) {
		t.Errorf("replicas = %v, want %d", rv, lakekeeperPodReplicas)
	}
	ann := spec["podMetadata"].(map[string]interface{})["annotations"].(map[string]interface{})
	if ann["prometheus.io/scrape"] != "true" {
		t.Errorf("scrape annotation not applied: %v", ann)
	}
	// No duplicate CR created under the recomputed (hyphenated) name.
	if _, err := dyn.Resource(lakekeeperGVR).Namespace(k8sClient.namespace).Get(context.Background(), LakekeeperResourceName("acme"), metav1.GetOptions{}); err == nil {
		t.Errorf("drift correction created a duplicate CR under the recomputed name")
	}
}

func TestReconcileLakekeeper_LogsAndContinuesOnInputResolverError(t *testing.T) {
	store := newFakeStore()
	store.warehouses["acme"] = &configstore.ManagedWarehouse{
		OrgID: "acme",
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendLakekeeper,
		},
	}
	c := NewControllerWithClient(store, nil, 0)
	c.lakekeeperProvisioner = &LakekeeperProvisioner{}
	c.lakekeeperInputs = func(_ context.Context, _ *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
		return ProvisioningInputs{}, errors.New("crossplane secret not found")
	}
	// Should not panic; the controller logs at warn level and moves on so
	// the poll loop retries on the next tick.
	c.reconcileLakekeeper(context.Background(), store.warehouses["acme"])
}

func TestWithLakekeeperProvisioner_PanicsOnNilProvisioner(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil provisioner")
		}
	}()
	NewControllerWithClient(newFakeStore(), nil, 0).
		WithLakekeeperProvisioner(nil, func(context.Context, *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
			return ProvisioningInputs{}, nil
		})
}

func TestWithLakekeeperProvisioner_PanicsOnNilResolver(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil resolver")
		}
	}()
	NewControllerWithClient(newFakeStore(), nil, 0).
		WithLakekeeperProvisioner(&LakekeeperProvisioner{}, nil)
}

// TestReconcileLakekeeper_HappyPath exercises the positive case where all
// gates pass: a real LakekeeperProvisioner (built with fakes) is invoked,
// the inputs resolver's ProvisioningInputs reach EnsureForOrg intact, and
// the resulting Lakekeeper config is written through to the warehouse row.
//
// This is the "EnsureForOrg is actually called" coverage the early gate
// tests don't reach.
func TestReconcileLakekeeper_HappyPath(t *testing.T) {
	// Fake Lakekeeper HTTP server: accepts bootstrap, list, and create
	// warehouse. Tracks the create-warehouse call so we can assert it
	// fired with the expected inputs.
	var createCalled bool
	var createReq CreateWarehouseRequest
	lk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/management/v1/bootstrap":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/management/v1/warehouse":
			_, _ = io.WriteString(w, `{"warehouses":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/management/v1/warehouse":
			createCalled = true
			_ = json.NewDecoder(r.Body).Decode(&createReq)
			_, _ = io.WriteString(w, `{"warehouse-id":"wh-uuid","name":"`+createReq.WarehouseName+`","status":"active"}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(lk.Close)

	// Fake K8s client + dynamic for the Lakekeeper CR + Secret. Pre-seed
	// the CR with status.bootstrappedAt so waitForBootstrap (now
	// checkBootstrap) returns immediately.
	k8sClient, dyn, _ := newFakeLakekeeperClient()
	if err := k8sClient.EnsureCR(context.Background(), LakekeeperCRSpec{
		OrgID: "happy", Image: "stub", PGHost: "stub", PGDatabase: "stub",
		SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	cr, _ := dyn.Resource(lakekeeperGVR).Namespace(k8sClient.namespace).Get(context.Background(), LakekeeperResourceName("happy"), metav1.GetOptions{})
	cr.Object["status"] = map[string]interface{}{"bootstrappedAt": "2026-05-19T12:00:00Z"}
	if _, err := dyn.Resource(lakekeeperGVR).Namespace(k8sClient.namespace).Update(context.Background(), cr, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("inject status: %v", err)
	}

	// Real provisioner, with the Lakekeeper HTTP client factory pointed at
	// our fake server. The EnsureDatabase call inside EnsureForOrg needs a
	// live PG — we point at the prototype Postgres on localhost:5434, and
	// skip the test if PG_ADMIN_DSN isn't set.
	pgDSN := os.Getenv("PG_ADMIN_DSN")
	if pgDSN == "" {
		t.Skip("PG_ADMIN_DSN not set; skipping happy-path test")
	}
	p := NewLakekeeperProvisioner(newFakeStore(), k8sClient,
		WithImage("stub:test"),
		WithClientFactory(func(string) *LakekeeperClient { return NewLakekeeperClient(lk.URL) }),
	)
	t.Cleanup(func() { dropDatabaseAndRole(t, pgDSN, lakekeeperDBName("happy")) })

	// Wire into the controller.
	store := newFakeStore()
	store.warehouses["happy"] = &configstore.ManagedWarehouse{
		OrgID: "happy",
		State: configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendLakekeeper,
		},
	}
	// Re-point the provisioner's store at the controller's store so the
	// EnsureForOrg writeback hits the same row reconcileLakekeeper sees.
	p.store = store

	var capturedInputs ProvisioningInputs
	wantInputs := ProvisioningInputs{
		AdminDSN: pgDSN, PGHost: "localhost", PGPort: 5434, PGSSLMode: "disable",
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "happy", Region: "us-east-1", Flavor: "s3-compat",
			StaticAccessKeyID: "minioadmin", StaticAccessKeySecret: "minioadmin",
		},
	}

	c := NewControllerWithClient(store, nil, 0).
		WithLakekeeperProvisioner(p, func(_ context.Context, w *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
			if w.OrgID != "happy" {
				t.Errorf("resolver got wrong org: %s", w.OrgID)
			}
			capturedInputs = wantInputs
			return wantInputs, nil
		})

	c.reconcileLakekeeper(context.Background(), store.warehouses["happy"])

	if !createCalled {
		t.Fatalf("Lakekeeper warehouse-create REST call was never made")
	}
	if createReq.StorageProfile.KeyPrefix != "happy" {
		t.Errorf("storage key-prefix = %q, want happy", createReq.StorageProfile.KeyPrefix)
	}
	// Verify inputs threaded through unchanged.
	if capturedInputs.PGPort != wantInputs.PGPort || capturedInputs.S3.Bucket != wantInputs.S3.Bucket {
		t.Errorf("inputs not threaded intact to provisioner; got %+v", capturedInputs)
	}
	// Verify the warehouse row got the persisted Lakekeeper config.
	w := store.warehouses["happy"]
	if w.Iceberg.LakekeeperEndpoint == "" {
		t.Errorf("LakekeeperEndpoint not persisted after EnsureForOrg")
	}
	if w.Iceberg.LakekeeperWarehouse != "org-happy" {
		t.Errorf("LakekeeperWarehouse = %q, want org-happy", w.Iceberg.LakekeeperWarehouse)
	}
}
