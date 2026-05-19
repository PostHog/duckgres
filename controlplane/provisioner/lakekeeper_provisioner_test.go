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
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fakeLakekeeperServer is an httptest stand-in for Lakekeeper. Tracks calls
// and lets us assert the right requests landed.
type fakeLakekeeperServer struct {
	t          *testing.T
	srv        *httptest.Server
	warehouses []CreateWarehouseRequest
	bootstraps int
}

func newFakeLakekeeperServer(t *testing.T) *fakeLakekeeperServer {
	t.Helper()
	f := &fakeLakekeeperServer{t: t}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/management/v1/info":
			_, _ = io.WriteString(w, `{"version":"0.11.6","bootstrapped":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/management/v1/bootstrap":
			f.bootstraps++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/management/v1/warehouse":
			resp := listWarehousesResponse{Warehouses: nil}
			for i, req := range f.warehouses {
				resp.Warehouses = append(resp.Warehouses, Warehouse{
					Name: req.WarehouseName, WarehouseID: stableID(i),
				})
			}
			_ = json.NewEncoder(w).Encode(resp)
		case r.Method == http.MethodPost && r.URL.Path == "/management/v1/warehouse":
			var req CreateWarehouseRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			f.warehouses = append(f.warehouses, req)
			_ = json.NewEncoder(w).Encode(Warehouse{
				Name: req.WarehouseName, WarehouseID: stableID(len(f.warehouses) - 1),
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func stableID(i int) string {
	return "wh-" + string(rune('a'+i))
}

func newFakeProvisionerStore(orgID string, state configstore.ManagedWarehouseProvisioningState) *fakeStore {
	fs := newFakeStore()
	fs.warehouses[orgID] = &configstore.ManagedWarehouse{
		OrgID: orgID,
		State: state,
	}
	return fs
}

// markBootstrapped helps EnsureForOrg's wait-for-bootstrap step succeed under
// the fake dynamic client (which doesn't run the operator's reconciler).
func markBootstrapped(t *testing.T, c *LakekeeperK8sClient, orgID string) {
	t.Helper()
	name := LakekeeperResourceName(orgID)
	cr, err := c.dynamic.Resource(lakekeeperGVR).Namespace(c.namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR to mark bootstrapped: %v", err)
	}
	cr.Object["status"] = map[string]interface{}{
		"bootstrappedAt": "2026-05-19T12:00:00Z",
		"serverID":       "test-server",
	}
	if _, err := c.dynamic.Resource(lakekeeperGVR).Namespace(c.namespace).Update(context.Background(), cr, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("mark bootstrapped: %v", err)
	}
}

func TestEnsureForOrg_HappyPath_Fakes(t *testing.T) {
	// This test exercises everything EXCEPT the actual Postgres CREATE
	// DATABASE — that's covered by EnsureDatabase_AgainstLivePG. We point
	// AdminDSN at the local prototype Postgres if available, otherwise
	// skip the test.
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set; happy-path test needs a real Postgres for CREATE DATABASE")
	}

	c, _, _ := newFakeLakekeeperClient()
	fake := newFakeLakekeeperServer(t)
	store := newFakeProvisionerStore("acme", configstore.ManagedWarehouseStateProvisioning)
	p := NewLakekeeperProvisioner(store, c,
		WithImage("img:test"),
		WithBootstrapTimeout(2*time.Second),
		WithClientFactory(func(baseURL string) *LakekeeperClient {
			// Redirect "in-cluster" base URL to the fake test server.
			return NewLakekeeperClient(fake.srv.URL)
		}),
	)

	// We need the CR to exist before EnsureForOrg's wait-for-bootstrap step,
	// and we need it marked bootstrapped (the fake k8s client doesn't run
	// the operator). EnsureForOrg's k8s.EnsureCR call creates the CR; we
	// then need to inject status concurrently. Simpler: pre-create the CR
	// with status set, so EnsureForOrg's EnsureCR Update path executes
	// instead of Create.
	if err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
		OrgID: "acme", Image: "stub", PGHost: "stub", PGDatabase: "stub", SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	markBootstrapped(t, c, "acme")

	in := ProvisioningInputs{
		AdminDSN: dsn,
		PGHost:   "localhost",
		PGPort:   5434,
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "org-acme",
			Endpoint: "http://minio:9000", Region: "us-east-1", Flavor: "s3-compat",
			StaticAccessKeyID: "minioadmin", StaticAccessKeySecret: "minioadmin",
		},
	}
	t.Cleanup(func() {
		// Drop the lakekeeper_acme DB created by EnsureDatabase.
		dropDatabase(t, dsn, "lakekeeper_acme")
	})

	if err := p.EnsureForOrg(context.Background(), store.warehouses["acme"], in); err != nil {
		t.Fatalf("EnsureForOrg: %v", err)
	}

	// Warehouse row should now carry Lakekeeper config.
	w := store.warehouses["acme"]
	if !w.Iceberg.Enabled {
		t.Errorf("Iceberg.Enabled not flipped on")
	}
	if w.Iceberg.Backend != configstore.IcebergBackendLakekeeper {
		t.Errorf("Backend = %q, want lakekeeper", w.Iceberg.Backend)
	}
	if !strings.HasSuffix(w.Iceberg.LakekeeperEndpoint, "/catalog") {
		t.Errorf("Endpoint = %q, want /catalog suffix", w.Iceberg.LakekeeperEndpoint)
	}
	if w.Iceberg.LakekeeperWarehouse != "org-acme" {
		t.Errorf("Warehouse = %q, want org-acme", w.Iceberg.LakekeeperWarehouse)
	}
	if w.Iceberg.LakekeeperClientID != "duckling-acme" {
		t.Errorf("ClientID = %q, want duckling-acme", w.Iceberg.LakekeeperClientID)
	}
	if w.Iceberg.LakekeeperClientCredentials.Name != LakekeeperResourceName("acme") {
		t.Errorf("ClientCredentials.Name = %q, want %q", w.Iceberg.LakekeeperClientCredentials.Name, LakekeeperResourceName("acme"))
	}
	if w.IcebergState != configstore.ManagedWarehouseStateReady {
		t.Errorf("IcebergState = %q, want ready", w.IcebergState)
	}

	// Fake Lakekeeper should have received exactly one warehouse create.
	if len(fake.warehouses) != 1 {
		t.Fatalf("warehouse creates = %d, want 1", len(fake.warehouses))
	}
	got := fake.warehouses[0]
	if got.WarehouseName != "org-acme" {
		t.Errorf("WarehouseName = %q, want org-acme", got.WarehouseName)
	}
	if !got.StorageProfile.STSEnabled || got.StorageProfile.Flavor != "s3-compat" {
		t.Errorf("storage profile must be sts-enabled + s3-compat, got %+v", got.StorageProfile)
	}

	// Idempotency: a second call should be a no-op on writes.
	beforeCalls := len(fake.warehouses)
	if err := p.EnsureForOrg(context.Background(), w, in); err != nil {
		t.Fatalf("EnsureForOrg (second call): %v", err)
	}
	if len(fake.warehouses) != beforeCalls {
		t.Errorf("second EnsureForOrg created another warehouse (want idempotent)")
	}
}

func TestEnsureForOrg_BootstrapTimeoutReturnsTransient(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set")
	}
	c, _, _ := newFakeLakekeeperClient()
	store := newFakeProvisionerStore("acme2", configstore.ManagedWarehouseStateProvisioning)
	p := NewLakekeeperProvisioner(store, c,
		WithBootstrapTimeout(200*time.Millisecond), // short
	)
	t.Cleanup(func() { dropDatabase(t, dsn, "lakekeeper_acme2") })

	err := p.EnsureForOrg(context.Background(), store.warehouses["acme2"], ProvisioningInputs{
		AdminDSN: dsn, PGHost: "localhost", PGPort: 5434,
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "org-acme2", Region: "us-east-1", Flavor: "s3-compat",
			StaticAccessKeyID: "minioadmin", StaticAccessKeySecret: "minioadmin",
		},
	})
	if !errors.Is(err, ErrBootstrapPending) {
		t.Fatalf("expected ErrBootstrapPending, got: %v", err)
	}
}

func TestEnsureForOrg_RejectsInvalidInputs(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	store := newFakeProvisionerStore("acme", configstore.ManagedWarehouseStateProvisioning)
	p := NewLakekeeperProvisioner(store, c)

	cases := map[string]ProvisioningInputs{
		"missing AdminDSN":   {PGHost: "h", S3: S3StorageConfig{Bucket: "b", Region: "r", Flavor: "aws"}},
		"missing PGHost":     {AdminDSN: "d", S3: S3StorageConfig{Bucket: "b", Region: "r", Flavor: "aws"}},
		"missing S3 bucket":  {AdminDSN: "d", PGHost: "h", S3: S3StorageConfig{Region: "r", Flavor: "aws"}},
		"missing S3 region":  {AdminDSN: "d", PGHost: "h", S3: S3StorageConfig{Bucket: "b", Flavor: "aws"}},
		"missing S3 flavor":  {AdminDSN: "d", PGHost: "h", S3: S3StorageConfig{Bucket: "b", Region: "r"}},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if err := p.EnsureForOrg(context.Background(), store.warehouses["acme"], in); err == nil {
				t.Errorf("expected validation error for %s", name)
			}
		})
	}
}

// TestEnsureForOrg_StateMismatchIsBenign covers the case where the warehouse
// row transitions out of the expected state between when the caller fetched
// w and when EnsureForOrg's persist step runs. The Lakekeeper resources are
// provisioned; the persist step fails the CAS; EnsureForOrg returns nil so
// the next reconcile iteration can retry with the fresh state.
func TestEnsureForOrg_StateMismatchIsBenign(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set")
	}
	c, _, _ := newFakeLakekeeperClient()
	fake := newFakeLakekeeperServer(t)
	store := newFakeProvisionerStore("cas-test", configstore.ManagedWarehouseStateProvisioning)
	// Caller's view of the warehouse, with stale state.
	staleW := &configstore.ManagedWarehouse{
		OrgID: "cas-test",
		State: configstore.ManagedWarehouseStateProvisioning,
	}
	// Simulate concurrent transition: the actual row is now `ready`.
	store.warehouses["cas-test"].State = configstore.ManagedWarehouseStateReady

	if err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
		OrgID: "cas-test", Image: "stub", PGHost: "stub", PGDatabase: "stub", SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	markBootstrapped(t, c, "cas-test")

	p := NewLakekeeperProvisioner(store, c,
		WithBootstrapTimeout(2*time.Second),
		WithClientFactory(func(string) *LakekeeperClient { return NewLakekeeperClient(fake.srv.URL) }),
	)
	t.Cleanup(func() { dropDatabase(t, dsn, "lakekeeper_castest") })

	err := p.EnsureForOrg(context.Background(), staleW, ProvisioningInputs{
		AdminDSN: dsn, PGHost: "localhost", PGPort: 5434,
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "cas-test", Region: "us-east-1", Flavor: "s3-compat",
			StaticAccessKeyID: "minioadmin", StaticAccessKeySecret: "minioadmin",
		},
	})
	if err != nil {
		t.Fatalf("expected nil on benign CAS mismatch, got: %v", err)
	}
	// Lakekeeper-side write should have happened (warehouse created) even
	// though the row update was skipped.
	if len(fake.warehouses) != 1 {
		t.Errorf("warehouse create count = %d, want 1", len(fake.warehouses))
	}
}

func TestEnsureForOrg_RejectsInvalidOrgID(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	store := newFakeProvisionerStore("bad org", configstore.ManagedWarehouseStateProvisioning)
	p := NewLakekeeperProvisioner(store, c)
	err := p.EnsureForOrg(context.Background(), store.warehouses["bad org"], ProvisioningInputs{
		AdminDSN: "d", PGHost: "h",
		S3: S3StorageConfig{Bucket: "b", Region: "r", Flavor: "aws"},
	})
	if err == nil || !strings.Contains(err.Error(), "not a valid K8s label value") {
		t.Fatalf("expected label-value error, got: %v", err)
	}
}

// dropDatabase is a t.Cleanup helper that best-effort removes a DB.
func dropDatabase(t *testing.T, dsn, name string) {
	t.Helper()
	dropDSN := dsn // pgx accepts the same DSN; CREATE/DROP from any current DB.
	cleanupDB(t, dropDSN, name)
}
