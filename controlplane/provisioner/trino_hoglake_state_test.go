//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type hoglakeMarkerStore struct {
	*fakeTrinoStore
	markErr           error
	commitBeforeError bool
}

func (s *hoglakeMarkerStore) MarkTrinoHoglakeInitialized(ctx context.Context, id string) error {
	if s.markErr != nil {
		if s.commitBeforeError {
			if err := s.fakeTrinoStore.MarkTrinoHoglakeInitialized(ctx, id); err != nil {
				return err
			}
		}
		return s.markErr
	}
	return s.fakeTrinoStore.MarkTrinoHoglakeInitialized(ctx, id)
}

type hoglakeMetadataFixture struct {
	catalog, namespace bool
	posts              int
}

func (f *hoglakeMetadataFixture) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		f.posts++
		if r.URL.Path == "/v1/catalogs" {
			f.catalog = true
		} else {
			f.namespace = true
		}
		w.WriteHeader(http.StatusCreated)
		return
	}
	if r.URL.Path == "/v1/catalogs/tenant-a" {
		if !f.catalog {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "tenant-a", "data_path": "s3://example-bucket/trino/warehouse-a/", "capabilities": []string{"atomic-table-creation-v1"}})
		return
	}
	if !f.namespace {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_, _ = w.Write([]byte(`{"name":"main"}`))
}

func newHoglakeStateHarness(t *testing.T) (*testProvisionerHarness, *hoglakeTestCatalog, *hoglakeMetadataFixture, *hoglakeMarkerStore) {
	t.Helper()
	org := configstore.TrinoEnabledOrg{OrgID: "tenant-a", DatabaseName: "tenant_a", Backend: configstore.TrinoBackendHoglake}
	warehouse := readyWarehouse(org.OrgID)
	warehouse.DucklingName = "warehouse-a"
	h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{org}, map[string]*configstore.ManagedWarehouse{org.OrgID: warehouse})
	fixture := &hoglakeMetadataFixture{}
	srv := httptest.NewServer(http.HandlerFunc(fixture.serve))
	t.Cleanup(srv.Close)
	h.provisioner.managedHoglake = &TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
	marker := &hoglakeMarkerStore{fakeTrinoStore: h.store}
	h.provisioner.store = marker
	client := &hoglakeTestCatalog{fakeCatalogClient: h.catalog, connector: "hoglake"}
	return h, client, fixture, marker
}

func TestTrinoHoglakeInitializedResourcesNeverRecreated(t *testing.T) {
	for _, missing := range []string{"catalog", "namespace"} {
		for _, registrationExists := range []bool{false, true} {
			t.Run(missing+"/"+map[bool]string{false: "after disable", true: "registered"}[registrationExists], func(t *testing.T) {
				h, client, fixture, _ := newHoglakeStateHarness(t)
				if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err != nil {
					t.Fatal(err)
				}
				if !h.store.orgs[0].HoglakeInitialized {
					t.Fatal("successful bootstrap was not persisted")
				}
				posts := fixture.posts
				if missing == "catalog" {
					fixture.catalog = false
				} else {
					fixture.namespace = false
				}
				if !registrationExists {
					h.catalog.existing = nil
					h.catalog.created = nil
				}
				if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], registrationExists); err == nil {
					t.Fatal("lost metadata was silently accepted")
				}
				if fixture.posts != posts {
					t.Fatal("lost initialized resource was recreated")
				}
			})
		}
	}
}

func TestTrinoHoglakeMarkerFailureRetriesWithoutAdmitting(t *testing.T) {
	h, client, fixture, marker := newHoglakeStateHarness(t)
	marker.markErr = errors.New("database unavailable")
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err == nil {
		t.Fatal("marker persistence failure was ignored")
	}
	if len(h.catalog.created) != 0 {
		t.Fatal("Trino registration preceded durable initialization")
	}
	posts := fixture.posts
	marker.markErr = nil
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err != nil {
		t.Fatal(err)
	}
	if fixture.posts != posts || !h.store.orgs[0].HoglakeInitialized {
		t.Fatal("retry did not adopt verified initial resources idempotently")
	}
}

func TestTrinoHoglakeFailedRegistrationStillProtectsMetadata(t *testing.T) {
	h, client, fixture, _ := newHoglakeStateHarness(t)
	h.catalog.createErr = errors.New("registration response lost")
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err == nil {
		t.Fatal("missing registration error")
	}
	if !h.store.orgs[0].HoglakeInitialized {
		t.Fatal("initialization lost after registration failure")
	}
	posts := fixture.posts
	fixture.catalog = false
	h.catalog.createErr = nil
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err == nil || fixture.posts != posts {
		t.Fatal("failed registration permitted metadata recreation")
	}
}

type hoglakeTargetCatalog struct{ *hoglakeTestCatalog }

func (c *hoglakeTargetCatalog) CatalogStates(context.Context) (map[string]string, error) {
	return map[string]string{"org_tenant_a": "OPERATIONAL"}, nil
}

func TestTrinoHoglakeRolloutCertificationNeverBootstraps(t *testing.T) {
	for _, missing := range []string{"catalog", "namespace"} {
		t.Run(missing, func(t *testing.T) {
			h, lifecycle, _ := managedHarness(t)
			org := configstore.TrinoEnabledOrg{OrgID: "tenant-a", DatabaseName: "tenant_a", Backend: configstore.TrinoBackendHoglake, HoglakeInitialized: true}
			h.store.orgs = []configstore.TrinoEnabledOrg{org}
			lifecycle.admitted = []configstore.TrinoEnabledOrg{org}
			warehouse := readyWarehouse(org.OrgID)
			warehouse.DucklingName = "warehouse-a"
			h.warehouses.rows[org.OrgID] = warehouse
			fixture := &hoglakeMetadataFixture{catalog: missing != "catalog", namespace: missing != "namespace"}
			srv := httptest.NewServer(http.HandlerFunc(fixture.serve))
			defer srv.Close()
			h.provisioner.managedHoglake = &TrinoManagedHoglakeConfig{URI: srv.URL, DataPath: "s3://example-bucket/trino/", Namespace: "main"}
			target := &hoglakeTargetCatalog{&hoglakeTestCatalog{fakeCatalogClient: &fakeCatalogClient{existing: []string{"org_tenant_a"}}, connector: "hoglake"}}
			h.provisioner.managed.Target = func(context.Context, *configstore.TrinoCellFreeze) (*TrinoManagedBackend, error) {
				return &TrinoManagedBackend{Name: "green", Catalog: target}, nil
			}
			err := h.provisioner.prepareManagedTarget(context.Background(), configstore.TrinoCellLease{}, &configstore.TrinoCellFreeze{Stable: true, TargetBackend: "green"}, tenantSecretProjection{projected: map[string]bool{org.OrgID: true}})
			if err == nil || lifecycle.certificate != nil || fixture.posts != 0 {
				t.Fatal("rollout certification recreated or accepted lost metadata")
			}
		})
	}
}

func (s *fakeTrinoStore) GetTrinoHoglakeInitialized(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, org := range s.orgs {
		if org.OrgID == id {
			return org.HoglakeInitialized, nil
		}
	}
	return false, errors.New("tenant missing")
}
func (s *fakeTrinoStore) MarkTrinoHoglakeInitialized(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.orgs {
		if s.orgs[i].OrgID == id {
			s.orgs[i].HoglakeInitialized = true
			return nil
		}
	}
	return errors.New("tenant missing")
}

func TestTrinoHoglakeUncertainRemoteCreateIsAdoptedBeforeRegistration(t *testing.T) {
	h, client, fixture, _ := newHoglakeStateHarness(t)
	lost := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/catalogs" && !lost {
			lost = true
			fixture.catalog = true
			fixture.posts++
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		fixture.serve(w, r)
	}))
	defer srv.Close()
	h.provisioner.managedHoglake.URI = srv.URL
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err == nil {
		t.Fatal("lost create response unexpectedly succeeded")
	}
	if len(h.catalog.created) != 0 || h.store.orgs[0].HoglakeInitialized {
		t.Fatal("uncertain bootstrap admitted tenant")
	}
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err != nil {
		t.Fatal(err)
	}
	if fixture.posts != 2 || !h.store.orgs[0].HoglakeInitialized {
		t.Fatal("uncertain catalog creation was not recovered idempotently")
	}
}

func TestTrinoHoglakeAmbiguousMarkerCommitDoesNotReinitialize(t *testing.T) {
	h, client, fixture, marker := newHoglakeStateHarness(t)
	marker.markErr = errors.New("commit response lost")
	marker.commitBeforeError = true
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err == nil {
		t.Fatal("missing commit error")
	}
	if len(h.catalog.created) != 0 || !h.store.orgs[0].HoglakeInitialized {
		t.Fatal("ambiguous commit handling is unsafe")
	}
	marker.markErr = nil
	fixture.catalog = false
	posts := fixture.posts
	if err := h.provisioner.reconcileHoglakeCatalog(context.Background(), client, "org_tenant_a", "tenant-a", h.ducklings["tenant-a"], false); err == nil || fixture.posts != posts {
		t.Fatal("retry recreated metadata after committed marker")
	}
}
