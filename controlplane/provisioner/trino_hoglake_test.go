//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"github.com/posthog/duckgres/controlplane/configstore"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestTrinoHoglakeBootstrap(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		conflict, wrongPath, unsupported bool
	}{
		{name: "create and repeat"}, {name: "concurrent create", conflict: true}, {name: "wrong path", wrongPath: true}, {name: "old server", unsupported: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exists := tc.wrongPath || tc.unsupported
			ns := false
			creates := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/catalogs":
					creates++
					var req map[string]string
					_ = json.NewDecoder(r.Body).Decode(&req)
					if req["name"] != "tenant-a" || req["data_path"] != "s3://example-bucket/trino/tenant-a/" {
						t.Errorf("unexpected create body: %v", req)
					}
					exists = true
					if tc.conflict {
						w.WriteHeader(http.StatusConflict)
					} else {
						w.WriteHeader(http.StatusCreated)
					}
				case "GET /v1/catalogs/tenant-a":
					if !exists {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					path := "s3://example-bucket/trino/tenant-a/"
					if tc.wrongPath {
						path = "s3://example-bucket/other/"
					}
					caps := []string{"atomic-table-creation-v1"}
					if tc.unsupported {
						caps = nil
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"name": "tenant-a", "data_path": path, "capabilities": caps})
				case "GET /v1/catalogs/tenant-a/namespaces/main":
					if !ns {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					_, _ = w.Write([]byte(`{"name":"main"}`))
				case "POST /v1/catalogs/tenant-a/namespaces":
					ns = true
					w.WriteHeader(http.StatusCreated)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			cfg := TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
			err := cfg.ensure(context.Background(), "tenant-a", "tenant-a")
			if tc.wrongPath || tc.unsupported {
				if err == nil {
					t.Fatal("unsafe catalog accepted")
				}
				if creates != 0 || ns {
					t.Fatal("modified existing unsafe catalog")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = cfg.ensure(context.Background(), "tenant-a", "tenant-a"); err != nil {
				t.Fatal(err)
			}
			if creates != 1 || !ns {
				t.Fatalf("creates=%d namespace=%v", creates, ns)
			}
		})
	}
}

func TestTrinoHoglakeValidation(t *testing.T) {
	for _, path := range []string{"s3://bucket/", "s3://bucket/trino/../", "s3://bucket/trino//", "s3://bucket/trino/%2e/", "https://example.com/prefix/"} {
		cfg := TrinoManagedHoglakeConfig{URI: "http://hoglake.example", DataPath: path, Namespace: "main"}
		if cfg.Validate() == nil {
			t.Errorf("accepted unsafe path %q", path)
		}
	}
	cfg := TrinoManagedHoglakeConfig{URI: "http://hoglake.example", DataPath: "s3://example-bucket/trino/", Namespace: "main"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, org := range []string{"..", "../other", "a/b", "a%2fb", ""} {
		if _, err := cfg.catalogPath(org); err == nil {
			t.Errorf("accepted unsafe org %q", org)
		}
	}
}

// The inventory observes the actual connector and can model catalog drift.
type hoglakeTestCatalog struct {
	*fakeCatalogClient
	connector string
}

func (c *hoglakeTestCatalog) CatalogConnectors(context.Context) (map[string]string, error) {
	result := map[string]string{}
	for _, name := range c.existing {
		result[name] = c.connector
		if props := c.created[name]; props != nil {
			result[name] = props["connector.name"]
		}
	}
	return result, nil
}

func TestTrinoHoglakeManagedReconcile(t *testing.T) {
	for _, tc := range []struct {
		name, connector        string
		missingWorker, badPath bool
	}{
		{name: "bootstrap and reconcile without metadata password"},
		{name: "existing ducklake preserved", connector: "ducklake"},
		{name: "wrong Hoglake path", connector: "hoglake", badPath: true},
		{name: "missing worker", missingWorker: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			org := configstore.TrinoEnabledOrg{OrgID: "tenant-a", DatabaseName: "tenant_a", Tier: "free", CellID: testCellID, RootPasswordHash: "$2a$10$hash", Backend: configstore.TrinoBackendHoglake}
			warehouse := readyWarehouse(org.OrgID)
			warehouse.DucklingName = "warehouse-a"
			h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{org}, map[string]*configstore.ManagedWarehouse{org.OrgID: warehouse})
			h.ducklings[org.OrgID].MetadataStore.Password = ""
			h.ducklings[org.OrgID].ReadyCondition = true
			h.provisioner.hoglakeDucklings = h.provisioner.ducklings
			h.provisioner.ducklings = func(context.Context, string) (*DucklingStatus, error) {
				t.Fatal("Hoglake resolved metadata credentials")
				return nil, nil
			}
			name := TrinoCatalogName(org.TrinoPrincipal())
			client := &hoglakeTestCatalog{fakeCatalogClient: h.catalog, connector: tc.connector}
			h.provisioner.catalog = client
			if tc.connector != "" {
				h.catalog.existing = []string{name}
			}
			if tc.missingWorker {
				h.catalog.nodes = []TrinoNode{{ID: "coordinator", State: "active", Coordinator: true}}
			}
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				switch r.URL.Path {
				case "/v1/catalogs/tenant-a":
					path := "s3://example-bucket/trino/warehouse-a/"
					if tc.badPath {
						path = "s3://example-bucket/other/"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"name": "tenant-a", "data_path": path, "capabilities": []string{"atomic-table-creation-v1"}})
				case "/v1/catalogs/tenant-a/namespaces/main":
					_, _ = w.Write([]byte(`{"name":"main"}`))
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			h.provisioner.managedHoglake = &TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
			err := h.provisioner.Reconcile(context.Background())
			if tc.connector == "ducklake" || tc.badPath {
				if err == nil {
					t.Fatal("unsafe existing catalog accepted")
				}
				if len(h.catalog.created) != 0 || len(h.catalog.dropped) != 0 {
					t.Fatal("existing catalog mutated")
				}
				if tc.connector == "ducklake" && requests != 0 {
					t.Fatal("touched Hoglake for an existing DuckLake catalog")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(h.tenantSecret(t)) != 0 {
				t.Fatal("Hoglake projected a DuckLake metadata password")
			}
			props := h.catalog.created[name]
			if props["s3.auth-type"] != "IAM_ROLE" || props["s3.iam-role"] != h.ducklings[org.OrgID].IAMRoleARN || props["s3.region"] == "" || props["hoglake.catalog"] != org.OrgID || props[trinoDuckLakePasswordFileProperty] != "" {
				t.Fatalf("incorrect Hoglake properties %v", props)
			}
			projection, e := h.provisioner.reconcileTenantSecrets(context.Background(), []configstore.TrinoEnabledOrg{org})
			if e != nil {
				t.Fatal(e)
			}
			outcomes, e := h.provisioner.reconcileBoundedBackend(context.Background(), []configstore.TrinoEnabledOrg{org}, projection, client)
			if e != nil {
				t.Fatal(e)
			}
			if outcomes[org.OrgID].Pending != tc.missingWorker {
				t.Fatalf("unexpected readiness: %+v", outcomes)
			}
			before := requests
			if err = h.provisioner.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if requests <= before {
				t.Fatal("existing Hoglake metadata was not revalidated")
			}
		})
	}
}

func TestTrinoHoglakeHTTPRejectsRedirectAndCancellation(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	cfg := TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
	if err := cfg.ensure(context.Background(), "tenant-a", "warehouse-a"); err == nil {
		t.Fatal("accepted redirect")
	}
	if redirected {
		t.Fatal("followed redirect")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cfg.ensure(ctx, "tenant-a", "warehouse-a"); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestTrinoHoglakeLegacyOptionCannotOverridePersistedDuckLake(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(strconv.FormatBool(explicit), func(t *testing.T) {
			opts := baseTestOpts()
			opts.HoglakeURI = "http://benchmark.example:8080"
			opts.ExplicitAssignmentOnly = explicit
			p, err := NewTrinoProvisioner(opts)
			if err != nil {
				t.Fatal(err)
			}
			props := p.buildCatalogProperties("tenant-a", readyWarehouse("tenant-a"), readyDuckling("tenant-a"))
			if props["connector.name"] != "ducklake" {
				t.Fatal("deprecated global switch overrode persisted DuckLake backend")
			}
		})
	}
}

func TestTrinoHoglakePersistedBackendsPreserveExistingClients(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(strconv.FormatBool(explicit), func(t *testing.T) {
			old := configstore.TrinoEnabledOrg{OrgID: "old-client", DatabaseName: "old_client", CellID: testCellID, RootPasswordHash: "$2a$10$example", Backend: configstore.TrinoBackendDuckLake}
			fresh := configstore.TrinoEnabledOrg{OrgID: "new-client", DatabaseName: "new_client", CellID: testCellID, RootPasswordHash: "$2a$10$example", Backend: configstore.TrinoBackendHoglake}
			cold := configstore.TrinoEnabledOrg{OrgID: "existing-client", DatabaseName: "existing_client", CellID: testCellID, RootPasswordHash: "$2a$10$example", Backend: configstore.TrinoBackendDuckLake}
			warehouse := readyWarehouse(fresh.OrgID)
			warehouse.DucklingName = "new-warehouse"
			h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{old}, map[string]*configstore.ManagedWarehouse{old.OrgID: readyWarehouse(old.OrgID), cold.OrgID: readyWarehouse(cold.OrgID), fresh.OrgID: warehouse})
			h.provisioner.explicitAssignmentOnly = explicit
			if err := h.provisioner.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			oldName := TrinoCatalogName(old.TrinoPrincipal())
			oldProps := maps.Clone(h.catalog.created[oldName])
			client := &hoglakeTestCatalog{fakeCatalogClient: h.catalog, connector: "ducklake"}
			h.provisioner.catalog = client
			h.store.orgs = append(h.store.orgs, fresh, cold)
			h.ducklings[cold.OrgID] = readyDuckling(cold.OrgID)
			h.ducklings[fresh.OrgID] = readyDuckling(fresh.OrgID)
			h.ducklings[fresh.OrgID].ReadyCondition = true
			h.ducklings[fresh.OrgID].MetadataStore.Password = ""
			h.provisioner.hoglakeDucklings = h.provisioner.ducklings
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/catalogs/new-client":
					_ = json.NewEncoder(w).Encode(map[string]any{"name": "new-client", "data_path": "s3://example-bucket/trino/new-warehouse/", "capabilities": []string{"atomic-table-creation-v1"}})
				case "/v1/catalogs/new-client/namespaces/main":
					_, _ = w.Write([]byte(`{"name":"main"}`))
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			h.provisioner.managedHoglake = &TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
			for i := 0; i < 2; i++ {
				if err := h.provisioner.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if len(h.catalog.dropped) != 0 || !maps.Equal(oldProps, h.catalog.created[oldName]) {
				t.Fatal("existing DuckLake registration changed")
			}
			if got := h.catalog.created[TrinoCatalogName(cold.TrinoPrincipal())]["connector.name"]; got != "ducklake" {
				t.Fatalf("recreated existing client's backend = %s", got)
			}
			if got := h.catalog.created[TrinoCatalogName(fresh.TrinoPrincipal())]["connector.name"]; got != "hoglake" {
				t.Fatalf("new client's backend = %s", got)
			}
			secrets := h.tenantSecret(t)
			if string(secrets[old.OrgID]) != "pw-"+old.OrgID || string(secrets[cold.OrgID]) != "pw-"+cold.OrgID || len(secrets[fresh.OrgID]) != 0 {
				t.Fatal("metadata credentials did not follow persisted backends")
			}
		})
	}
}
