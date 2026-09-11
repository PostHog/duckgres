//go:build kubernetes

package admin

import (
	"net/http"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoFleetSelectsAuthoritativeOwner(t *testing.T) {
	legacy := &fakeTrinoCoordinator{}
	current := &fakeTrinoCoordinator{queries: []TrinoQuery{{Principal: "tenant"}}}
	store := &fakeTrinoOrgStore{
		orgs: []configstore.TrinoEnabledOrg{{OrgID: "tenant", DatabaseName: "tenant", CellID: "registered:cell-001"}},
		rows: map[string]*configstore.ManagedWarehouseTrino{"tenant": {OrgID: "tenant", Enabled: true, TrinoCellID: "registered:cell-001", State: configstore.ManagedWarehouseStateReady}},
	}
	api := NewTrinoFleetAPI([]TrinoCell{{ID: "legacy", StoredID: "cell-001"}, {ID: "cell-001", StoredID: "registered:cell-001", ClientURL: "https://gateway.example.com"}}, []TrinoCoordinatorClient{legacy, current}, store, nil)
	r := trinoTestRouter(api, RoleAdmin)
	code, data := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/queries?cell=cell-001", "")
	if code != http.StatusOK || data["cell"].(map[string]any)["id"] != "cell-001" || legacy.queryCalls.Load() != 0 || current.queryCalls.Load() != 1 {
		t.Fatalf("wrong coordinator: %d %#v", code, data)
	}
	code, data = doTrinoJSON(t, r, http.MethodGet, "/api/v1/orgs/tenant/trino?cell=legacy", "")
	if code != http.StatusOK || data["cell"].(map[string]any)["id"] != "cell-001" {
		t.Fatalf("org must resolve its stored owner: %d %#v", code, data)
	}
	status := data["status"].(map[string]any)
	if status["connection"].(map[string]any)["host"] != "gateway.example.com" {
		t.Fatalf("wrong connection: %#v", status)
	}
	for _, cell := range []string{"legacy", "cell-001"} {
		code, data = doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/orgs?cell="+cell, "")
		want := 0
		if cell == "cell-001" {
			want = 1
		}
		if code != http.StatusOK || len(data["orgs"].([]any)) != want {
			t.Fatalf("org listing crossed cell boundary: %d %#v", code, data)
		}
	}
	for _, path := range []string{"/api/v1/trino/queries", "/api/v1/trino/status", "/api/v1/trino/nodes", "/api/v1/trino/orgs", "/api/v1/trino/queries/query"} {
		code, _ = doTrinoJSON(t, r, http.MethodGet, path+"?cell=unknown", "")
		if code != http.StatusNotFound {
			t.Errorf("%s unknown cell returned %d", path, code)
		}
	}
	store.rows["tenant"].TrinoCellID = "unregistered"
	code, data = doTrinoJSON(t, r, http.MethodGet, "/api/v1/orgs/tenant/trino", "")
	if code != http.StatusConflict || data["status"] != nil {
		t.Fatalf("unknown stored owner must fail closed: %d %#v", code, data)
	}
}

func TestTrinoFleetRejectsAmbiguousConfiguration(t *testing.T) {
	for _, cells := range [][]TrinoCell{
		{{ID: "legacy", StoredID: "same"}, {ID: "cell-001", StoredID: "same"}},
		{{ID: "legacy", StoredID: "first"}, {ID: "legacy", StoredID: "second"}},
		{{ID: ""}, {ID: "cell-001"}},
	} {
		if api := NewTrinoFleetAPI(cells, []TrinoCoordinatorClient{&fakeTrinoCoordinator{}, &fakeTrinoCoordinator{}}, &fakeTrinoOrgStore{}, nil); api != nil {
			t.Fatalf("ambiguous fleet accepted: %+v", cells)
		}
	}
}

type selectingTrinoStore struct {
	fakeTrinoOrgStore
	selected string
}

func (s *selectingTrinoStore) SelectTrinoCell(_, cell string) error {
	s.selected = cell
	return nil
}

func TestTrinoFleetSelectionIsAdminOnly(t *testing.T) {
	store := &selectingTrinoStore{}
	api := NewTrinoFleetAPI([]TrinoCell{{ID: "legacy", StoredID: "cell-001"}, {ID: "cell-001", StoredID: "registered:cell-001"}}, []TrinoCoordinatorClient{&fakeTrinoCoordinator{}, &fakeTrinoCoordinator{}}, store, nil)
	for _, role := range []Role{RoleViewer, RoleAdmin} {
		code, _ := doTrinoJSON(t, trinoTestRouter(api, role), http.MethodPut, "/api/v1/orgs/tenant/trino/cell", `{"cell":"cell-001"}`)
		if role == RoleViewer && (code != http.StatusForbidden || store.selected != "") {
			t.Fatalf("viewer changed assignment: %d", code)
		}
		if role == RoleAdmin && (code != http.StatusOK || store.selected != "registered:cell-001") {
			t.Fatalf("admin assignment: %d %q", code, store.selected)
		}
	}
	store.selected = ""
	code, _ := doTrinoJSON(t, trinoTestRouter(api, RoleAdmin), http.MethodPut, "/api/v1/orgs/tenant/trino/cell", `{"cell":"unknown"}`)
	if code != http.StatusBadRequest || store.selected != "" {
		t.Fatalf("unknown cell selected: %d", code)
	}
}
