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

// TestEnsureForOrg_PreProvisioned exercises the cnpg-shard path: the database
// and role were already created (by provider-sql on the shard), so EnsureForOrg
// must NOT run CREATE DATABASE / CREATE ROLE (it has no AdminDSN — a connection
// attempt would fail), and must put the supplied PG credentials into the
// Lakekeeper Secret verbatim. Unlike the happy-path test this needs no real
// Postgres, precisely because the DDL steps are skipped.
func TestEnsureForOrg_PreProvisioned(t *testing.T) {
	c, _, kube := newFakeLakekeeperClient()
	fake := newFakeLakekeeperServer(t)
	store := newFakeProvisionerStore("acme", configstore.ManagedWarehouseStateProvisioning)
	p := NewLakekeeperProvisioner(store, c,
		WithImage("img:test"),
		WithClientFactory(func(baseURL string) *LakekeeperClient {
			return NewLakekeeperClient(fake.srv.URL)
		}),
	)

	// Pre-create + bootstrap the CR (the fake k8s client doesn't run the operator).
	if err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
		OrgID: "acme", Image: "stub", PGHost: "stub", PGDatabase: "stub", SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	markBootstrapped(t, c, "acme")

	in := ProvisioningInputs{
		PGPreProvisioned: true,
		PGUser:           "lakekeeper_acme",
		PGPassword:       "from-provider-sql",
		PGDatabase:       "lakekeeper_acme",
		PGHost:           "shard-001-pooler.cnpg-shards.svc.cluster.local",
		PGPort:           5432,
		PGSSLMode:        "require",
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "lakekeeper",
			Region: "us-east-1", Flavor: "aws",
			RoleARN: "arn:aws:iam::123456789012:role/duckling-acme",
		},
	}

	// No PG_ADMIN_DSN, no real Postgres: succeeding proves the DDL steps were
	// skipped (a nil/blank AdminDSN connection would otherwise error).
	if err := p.EnsureForOrg(context.Background(), store.warehouses["acme"], in); err != nil {
		t.Fatalf("EnsureForOrg (pre-provisioned): %v", err)
	}

	// The Secret must carry the supplied DB creds verbatim (not generated).
	sec, err := kube.CoreV1().Secrets(c.namespace).Get(context.Background(), LakekeeperResourceName("acme"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get lakekeeper secret: %v", err)
	}
	if got := string(sec.Data[SecretKeyDBUser]); got != "lakekeeper_acme" {
		t.Errorf("db-user = %q, want lakekeeper_acme", got)
	}
	if got := string(sec.Data[SecretKeyDBPassword]); got != "from-provider-sql" {
		t.Errorf("db-password = %q, want from-provider-sql", got)
	}
	// Lakekeeper-internal secrets are still generated even in pre-provisioned mode.
	if len(sec.Data[SecretKeyEncryptionKey]) == 0 {
		t.Errorf("encryption-key should still be generated")
	}

	// Warehouse created + iceberg flipped on.
	w := store.warehouses["acme"]
	if !w.Iceberg.Enabled || w.Iceberg.Backend != configstore.IcebergBackendLakekeeper {
		t.Errorf("iceberg not configured: %+v", w.Iceberg)
	}
	if len(fake.warehouses) != 1 {
		t.Fatalf("warehouse creates = %d, want 1", len(fake.warehouses))
	}
}

// EnsureForOrg returns ErrBootstrapPending when the operator hasn't yet
// flipped status.bootstrappedAt — caller (the warehouse-reconcile loop)
// is responsible for requeueing without blocking other orgs.
func TestEnsureForOrg_NotBootstrappedReturnsTransient(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set")
	}
	c, _, _ := newFakeLakekeeperClient()
	store := newFakeProvisionerStore("acme2", configstore.ManagedWarehouseStateProvisioning)
	p := NewLakekeeperProvisioner(store, c)
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

// TestEnsureForOrg_PersistsAfterTopLevelStateMoved covers the case where
// the warehouse row's top-level state has already transitioned to Ready
// (e.g. the Duckling controller moved it ahead of us). The new persist
// path uses UpdateIcebergConfig which doesn't CAS on top-level state, so
// Lakekeeper config still lands.
func TestEnsureForOrg_PersistsAfterTopLevelStateMoved(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set")
	}
	c, _, _ := newFakeLakekeeperClient()
	fake := newFakeLakekeeperServer(t)
	store := newFakeProvisionerStore("ready-test", configstore.ManagedWarehouseStateProvisioning)
	// Simulate the Duckling state machine racing ahead.
	store.warehouses["ready-test"].State = configstore.ManagedWarehouseStateReady
	// Caller's snapshot of w is stale.
	staleW := &configstore.ManagedWarehouse{
		OrgID: "ready-test",
		State: configstore.ManagedWarehouseStateProvisioning,
	}

	if err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
		OrgID: "ready-test", Image: "stub", PGHost: "stub", PGDatabase: "stub", SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	markBootstrapped(t, c, "ready-test")

	p := NewLakekeeperProvisioner(store, c,
		WithClientFactory(func(string) *LakekeeperClient { return NewLakekeeperClient(fake.srv.URL) }),
	)
	t.Cleanup(func() { dropDatabase(t, dsn, "lakekeeper_readytest") })

	err := p.EnsureForOrg(context.Background(), staleW, ProvisioningInputs{
		AdminDSN: dsn, PGHost: "localhost", PGPort: 5434,
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "ready-test", Region: "us-east-1", Flavor: "s3-compat",
			StaticAccessKeyID: "minioadmin", StaticAccessKeySecret: "minioadmin",
		},
	})
	if err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}
	if w := store.warehouses["ready-test"]; w.Iceberg.LakekeeperEndpoint == "" {
		t.Errorf("expected LakekeeperEndpoint persisted even though top-level state was Ready")
	}
}

// TestEnsureForOrg_PersistsOAuth2URIWhenKubernetesAuthOn confirms that
// KubernetesAuthAudiences on the inputs flows through to:
//
//   - the Lakekeeper CR's spec.authentication.kubernetes block
//   - the warehouse row's LakekeeperOAuth2ServerURI (pointing at the
//     worker's local broker on 127.0.0.1)
//
// This is the wire-level handshake PR4 unlocks: the worker emits an
// OAuth2 secret + ATTACH because the URI is non-empty.
func TestEnsureForOrg_PersistsOAuth2URIWhenKubernetesAuthOn(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set")
	}
	c, _, _ := newFakeLakekeeperClient()
	fake := newFakeLakekeeperServer(t)
	store := newFakeProvisionerStore("oidc-org", configstore.ManagedWarehouseStateProvisioning)

	if err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
		OrgID: "oidc-org", Image: "stub", PGHost: "stub", PGDatabase: "stub",
		SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	markBootstrapped(t, c, "oidc-org")

	p := NewLakekeeperProvisioner(store, c,
		WithClientFactory(func(string) *LakekeeperClient { return NewLakekeeperClient(fake.srv.URL) }),
	)
	t.Cleanup(func() { dropDatabase(t, dsn, "lakekeeper_oidcorg") })

	err := p.EnsureForOrg(context.Background(), store.warehouses["oidc-org"], ProvisioningInputs{
		AdminDSN: dsn, PGHost: "localhost", PGPort: 5434, PGSSLMode: "disable",
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "oidc-org", Region: "us-east-1", Flavor: "s3-compat",
			StaticAccessKeyID: "minioadmin", StaticAccessKeySecret: "minioadmin",
		},
		KubernetesAuthAudiences: []string{"lakekeeper"},
	})
	if err != nil {
		t.Fatalf("EnsureForOrg: %v", err)
	}

	w := store.warehouses["oidc-org"]
	if w.Iceberg.LakekeeperOAuth2ServerURI == "" {
		t.Errorf("LakekeeperOAuth2ServerURI should be populated in OIDC mode")
	}
	if w.Iceberg.LakekeeperOAuth2ServerURI != "http://127.0.0.1:9876/token" {
		t.Errorf("OAUTH2_SERVER_URI = %q, want http://127.0.0.1:9876/token (worker-local broker)",
			w.Iceberg.LakekeeperOAuth2ServerURI)
	}

	// Cross-check that the CR's authentication.kubernetes block was set in
	// the SAME EnsureForOrg call. Without this read-back, the DB row and
	// the CR could drift — a future refactor that threads
	// KubernetesAuthAudiences into ProvisioningInputs but forgets to pass
	// it to LakekeeperCRSpec would leave Lakekeeper in allowall mode while
	// the worker tells DuckDB to POST to the broker. Lakekeeper would
	// then reject the token because k8s auth wasn't actually enabled.
	cr, err := c.dynamic.Resource(lakekeeperGVR).Namespace(c.namespace).
		Get(context.Background(), LakekeeperResourceName("oidc-org"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR for cross-check: %v", err)
	}
	specMap := cr.Object["spec"].(map[string]interface{})
	auth, ok := specMap["authentication"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec.authentication missing on CR — would be allowall in prod")
	}
	k8sAuth, ok := auth["kubernetes"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec.authentication.kubernetes missing on CR")
	}
	if k8sAuth["enabled"] != true {
		t.Errorf("spec.authentication.kubernetes.enabled = %v, want true", k8sAuth["enabled"])
	}
	auds, ok := k8sAuth["audiences"].([]interface{})
	if !ok || len(auds) != 1 || auds[0] != "lakekeeper" {
		t.Errorf("audiences = %v, want [lakekeeper]", k8sAuth["audiences"])
	}
}

func TestEnsureForOrg_OAuth2URIEmptyInAllowallMode(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set")
	}
	c, _, _ := newFakeLakekeeperClient()
	fake := newFakeLakekeeperServer(t)
	store := newFakeProvisionerStore("allowall-org", configstore.ManagedWarehouseStateProvisioning)

	if err := c.EnsureCR(context.Background(), LakekeeperCRSpec{
		OrgID: "allowall-org", Image: "stub", PGHost: "stub", PGDatabase: "stub",
		SecretName: "stub", BaseURI: "http://stub",
	}); err != nil {
		t.Fatalf("seed CR: %v", err)
	}
	markBootstrapped(t, c, "allowall-org")

	p := NewLakekeeperProvisioner(store, c,
		WithClientFactory(func(string) *LakekeeperClient { return NewLakekeeperClient(fake.srv.URL) }),
	)
	t.Cleanup(func() { dropDatabase(t, dsn, "lakekeeper_allowallorg") })

	// KubernetesAuthAudiences left empty → allowall mode.
	err := p.EnsureForOrg(context.Background(), store.warehouses["allowall-org"], ProvisioningInputs{
		AdminDSN: dsn, PGHost: "localhost", PGPort: 5434, PGSSLMode: "disable",
		S3: S3StorageConfig{
			Bucket: "warehouse", KeyPrefix: "allowall-org", Region: "us-east-1", Flavor: "s3-compat",
			StaticAccessKeyID: "minioadmin", StaticAccessKeySecret: "minioadmin",
		},
	})
	if err != nil {
		t.Fatalf("EnsureForOrg: %v", err)
	}
	if w := store.warehouses["allowall-org"]; w.Iceberg.LakekeeperOAuth2ServerURI != "" {
		t.Errorf("OAUTH2_SERVER_URI = %q, want empty (allowall mode)", w.Iceberg.LakekeeperOAuth2ServerURI)
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

// TestDeleteForOrg_SkipsPGCleanupForCnpgShard guards the contract that the
// cnpg-shard path leaves PG cleanup to the Crossplane composition. The
// composition's [Delete] managementPolicy on the cnpg-tenant-role and
// cnpg-tenant-database resources owns role+DB teardown there; the
// duckgres provisioner doesn't have the AdminDSN to do it itself
// anyway. The k8s teardown still runs.
func TestDeleteForOrg_SkipsPGCleanupForCnpgShard(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	p := NewLakekeeperProvisioner(newFakeStore(), c)
	// AdminDSN empty + PGPreProvisioned=true: DropDatabase/DropRole must
	// not be invoked. With a bogus DSN they'd surface as connection
	// errors; a nil-DSN attempt would panic deep in pgx. The fact that
	// this returns nil with no DSN configured is the assertion.
	err := p.DeleteForOrg(context.Background(), "acme", ProvisioningInputs{
		PGPreProvisioned: true,
	})
	if err != nil {
		t.Fatalf("DeleteForOrg with PGPreProvisioned should succeed without DSN, got: %v", err)
	}
}

// TestDeleteForOrg_SkipsPGCleanupWithoutAdminDSN covers the dev/orbstack
// path where the lakekeeper provisioner never had an AdminDSN in the
// first place (no env, no Duckling CR). PG cleanup must be skipped
// rather than failing the teardown.
func TestDeleteForOrg_SkipsPGCleanupWithoutAdminDSN(t *testing.T) {
	c, _, _ := newFakeLakekeeperClient()
	p := NewLakekeeperProvisioner(newFakeStore(), c)
	err := p.DeleteForOrg(context.Background(), "acme", ProvisioningInputs{})
	if err != nil {
		t.Fatalf("DeleteForOrg with empty inputs should succeed, got: %v", err)
	}
}
