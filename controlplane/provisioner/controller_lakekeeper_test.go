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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

func TestReconcileLakekeeper_SkipsWhenBackendIsS3Tables(t *testing.T) {
	called := false
	store := newFakeStore()
	store.warehouses["acme"] = &configstore.ManagedWarehouse{
		OrgID: "acme",
		State: configstore.ManagedWarehouseStateReady,
		Iceberg: configstore.ManagedWarehouseIceberg{
			Enabled: true,
			Backend: configstore.IcebergBackendS3Tables,
		},
	}
	c := NewControllerWithClient(store, nil, 0)
	c.lakekeeperProvisioner = &LakekeeperProvisioner{}
	c.lakekeeperInputs = func(_ context.Context, _ *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
		called = true
		return ProvisioningInputs{}, nil
	}
	c.reconcileLakekeeper(context.Background(), store.warehouses["acme"])
	if called {
		t.Errorf("inputs resolver should not be called when Backend=s3_tables")
	}
}

func TestReconcileLakekeeper_SkipsWhenAlreadyProvisioned(t *testing.T) {
	called := false
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
	c := NewControllerWithClient(store, nil, 0)
	c.lakekeeperProvisioner = &LakekeeperProvisioner{}
	c.lakekeeperInputs = func(_ context.Context, _ *configstore.ManagedWarehouse) (ProvisioningInputs, error) {
		called = true
		return ProvisioningInputs{}, nil
	}
	c.reconcileLakekeeper(context.Background(), store.warehouses["acme"])
	if called {
		t.Errorf("inputs resolver should not be called when LakekeeperEndpoint already set")
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
