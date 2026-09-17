//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type countedHoglakeInventory struct {
	*hoglakeTestCatalog
	reads            int
	failRead         int
	missingOnRefresh bool
}

func (c *countedHoglakeInventory) CatalogConnectors(ctx context.Context) (map[string]string, error) {
	c.reads++
	if c.reads == c.failRead {
		return nil, errors.New("inventory unavailable")
	}
	if c.missingOnRefresh && c.reads > 1 {
		return map[string]string{}, nil
	}
	return c.hoglakeTestCatalog.CatalogConnectors(ctx)
}

func TestTrinoHoglakeInventoryIsBatchedPerBackend(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", existing), func(t *testing.T) {
			var orgs []configstore.TrinoEnabledOrg
			warehouses := map[string]*configstore.ManagedWarehouse{}
			for i := 0; i < 12; i++ {
				id := fmt.Sprintf("tenant-%d", i)
				orgs = append(orgs, configstore.TrinoEnabledOrg{OrgID: id, DatabaseName: fmt.Sprintf("tenant_%d", i), CellID: testCellID, RootPasswordHash: "$2a$10$example", Backend: configstore.TrinoBackendHoglake})
				warehouses[id] = readyWarehouse(id)
				warehouses[id].DucklingName = id
			}
			h := newTestTrinoProvisioner(t, orgs, warehouses)
			client := &countedHoglakeInventory{hoglakeTestCatalog: &hoglakeTestCatalog{fakeCatalogClient: h.catalog, connector: "hoglake"}}
			h.provisioner.catalog = client
			second := &countedHoglakeInventory{hoglakeTestCatalog: &hoglakeTestCatalog{fakeCatalogClient: &fakeCatalogClient{}, connector: "hoglake"}}
			h.provisioner.additionalCatalogs = []TrinoCatalogClient{second}
			h.provisioner.hoglakeDucklings = h.provisioner.ducklings
			for _, org := range orgs {
				h.ducklings[org.OrgID].ReadyCondition = true
				h.ducklings[org.OrgID].MetadataStore.Password = ""
				if existing {
					h.catalog.existing = append(h.catalog.existing, TrinoCatalogName(org.TrinoPrincipal()))
					second.existing = append(second.existing, TrinoCatalogName(org.TrinoPrincipal()))
				}
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation: %s", r.Method)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				if len(parts) == 3 {
					_ = json.NewEncoder(w).Encode(map[string]any{"name": parts[2], "data_path": "s3://example-bucket/trino/" + parts[2] + "/", "capabilities": []string{"atomic-table-creation-v1"}})
					return
				}
				_, _ = w.Write([]byte(`{"name":"main"}`))
			}))
			defer srv.Close()
			h.provisioner.managedHoglake = &TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
			if err := h.provisioner.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := 1
			if !existing {
				want = 2
			}
			if client.reads != want || second.reads != want {
				t.Fatalf("inventory reads=%d/%d want=%d per backend for %d tenants", client.reads, second.reads, want, len(orgs))
			}
			client.reads = 0
			second.reads = 0
			if err := h.provisioner.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if client.reads != 1 || second.reads != 1 {
				t.Fatalf("steady-state inventory reads=%d/%d want=1 per backend", client.reads, second.reads)
			}
		})
	}
}

func TestTrinoHoglakeInventoryFailurePreventsAdmission(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failRead int
		missing  bool
	}{
		{"initial read", 1, false}, {"refresh read", 2, false}, {"missing after create", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, base, _, _ := newHoglakeStateHarness(t)
			client := &countedHoglakeInventory{hoglakeTestCatalog: base, failRead: tc.failRead, missingOnRefresh: tc.missing}
			outcomes, err := h.provisioner.reconcileBackendCatalogs(context.Background(), h.store.orgs, tenantSecretProjection{
				projected: map[string]bool{"tenant-a": true}, statuses: h.ducklings,
			}, client)
			if err == nil || outcomes["tenant-a"].Err == nil || outcomes["tenant-a"].Created || outcomes["tenant-a"].Existed {
				t.Fatalf("unverified catalog admitted: outcomes=%+v err=%v", outcomes, err)
			}
			if tc.failRead == 1 && len(base.created) != 0 {
				t.Fatal("created catalog without inventory")
			}
		})
	}
}

type countedHoglakeTarget struct{ *countedHoglakeInventory }

func (c *countedHoglakeTarget) CatalogStates(context.Context) (map[string]string, error) {
	states := map[string]string{}
	for _, name := range c.existing {
		states[name] = "OPERATIONAL"
	}
	return states, nil
}

func TestTrinoHoglakeTargetInventoryIsBatched(t *testing.T) {
	h, lifecycle, _ := managedHarness(t)
	h.store.orgs = nil
	projected := map[string]bool{}
	target := &countedHoglakeTarget{&countedHoglakeInventory{hoglakeTestCatalog: &hoglakeTestCatalog{fakeCatalogClient: &fakeCatalogClient{}, connector: "hoglake"}}}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("tenant-%d", i)
		org := configstore.TrinoEnabledOrg{OrgID: id, DatabaseName: fmt.Sprintf("tenant_%d", i), Backend: configstore.TrinoBackendHoglake, HoglakeInitialized: true}
		h.store.orgs = append(h.store.orgs, org)
		projected[id] = true
		warehouse := readyWarehouse(id)
		warehouse.DucklingName = id
		h.warehouses.rows[id] = warehouse
		target.existing = append(target.existing, TrinoCatalogName(org.TrinoPrincipal()))
	}
	lifecycle.admitted = h.store.orgs
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("certification mutated metadata")
			w.WriteHeader(500)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) == 3 {
			_ = json.NewEncoder(w).Encode(map[string]any{"name": parts[2], "data_path": "s3://example-bucket/trino/" + parts[2] + "/", "capabilities": []string{"atomic-table-creation-v1"}})
		} else {
			_, _ = w.Write([]byte(`{"name":"main"}`))
		}
	}))
	defer srv.Close()
	h.provisioner.managedHoglake = &TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
	h.provisioner.managed.Target = func(context.Context, *configstore.TrinoCellFreeze) (*TrinoManagedBackend, error) {
		return &TrinoManagedBackend{Name: "green", Catalog: target}, nil
	}
	if err := h.provisioner.prepareManagedTarget(context.Background(), configstore.TrinoCellLease{}, &configstore.TrinoCellFreeze{Stable: true, TargetBackend: "green"}, tenantSecretProjection{projected: projected}); err != nil {
		t.Fatal(err)
	}
	if target.reads != 1 || lifecycle.certificate == nil {
		t.Fatalf("reads=%d certificate=%v", target.reads, lifecycle.certificate)
	}
}
