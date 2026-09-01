//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// --- fakes ---

type fakeTrinoCoordinator struct {
	queries     []TrinoQuery
	queriesErr  error
	queryByID   map[string]TrinoQuery
	queryErr    error
	nodes       []TrinoNode
	nodeSource  string
	nodesErr    error
	info        *TrinoServerInfo
	infoErr     error
	killErr     error
	killed      []string
	killReasons []string
	// queryCalls counts Queries() invocations so cache behaviour is
	// observable.
	queryCalls atomic.Int32
}

func (f *fakeTrinoCoordinator) Queries(context.Context) ([]TrinoQuery, error) {
	f.queryCalls.Add(1)
	return f.queries, f.queriesErr
}

func (f *fakeTrinoCoordinator) Query(_ context.Context, id string) (*TrinoQuery, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	q, ok := f.queryByID[id]
	if !ok {
		return nil, fmt.Errorf("query %s: %w", id, errTrinoNotFound)
	}
	return &q, nil
}

func (f *fakeTrinoCoordinator) KillQuery(_ context.Context, id, message string) error {
	if f.killErr != nil {
		return f.killErr
	}
	f.killed = append(f.killed, id)
	f.killReasons = append(f.killReasons, message)
	return nil
}

func (f *fakeTrinoCoordinator) Nodes(context.Context) (TrinoNodeInventory, error) {
	// Default to the failure detector so the existing cases keep describing
	// an AIRLIFT_DISCOVERY cell; the ANNOUNCE cases set nodeSource.
	source := f.nodeSource
	if source == "" {
		source = TrinoNodeSourceFailureDetector
	}
	return TrinoNodeInventory{Source: source, Nodes: f.nodes}, f.nodesErr
}

func (f *fakeTrinoCoordinator) ServerInfo(context.Context) (*TrinoServerInfo, error) {
	return f.info, f.infoErr
}

type fakeTrinoOrgStore struct {
	orgs    []configstore.TrinoEnabledOrg
	listErr error
	rows    map[string]*configstore.ManagedWarehouseTrino
	rowErr  error
}

func (f *fakeTrinoOrgStore) ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error) {
	return f.orgs, f.listErr
}

func (f *fakeTrinoOrgStore) GetManagedWarehouseTrino(orgID string) (*configstore.ManagedWarehouseTrino, error) {
	if f.rowErr != nil {
		return nil, f.rowErr
	}
	return f.rows[orgID], nil
}

// trinoTestRouter mounts the Trino routes with a fixed identity so the
// admin-only kill path is exercisable.
func trinoTestRouter(api *TrinoAPI, role Role) *gin.Engine {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	e.Use(func(c *gin.Context) {
		c.Set(ctxIdentityKey, &Identity{Email: "operator@posthog.com", Role: role, Source: "test"})
		c.Next()
	})
	registerTrinoAPI(e.Group("/api/v1"), api)
	return e
}

func twoOrgTrinoStore() *fakeTrinoOrgStore {
	return &fakeTrinoOrgStore{
		orgs: []configstore.TrinoEnabledOrg{
			{OrgID: "org-a", DatabaseName: "db_a", Tier: "free", CellID: "cell-test", State: configstore.ManagedWarehouseStateReady, RootPasswordHash: "$2a$10$secrethash"},
			{OrgID: "org-b", DatabaseName: "db_b", Tier: "scale", CellID: "cell-test", State: configstore.ManagedWarehouseStatePending, RootPasswordHash: "$2a$10$othersecret"},
		},
	}
}

func testTrinoAPI(t *testing.T, coord *fakeTrinoCoordinator, store *fakeTrinoOrgStore) *TrinoAPI {
	t.Helper()
	api := NewTrinoAPI(TrinoCell{ID: "cell-test", CoordinatorURL: "https://coordinator.invalid"}, coord, store, nil)
	if api == nil {
		t.Fatal("NewTrinoAPI returned nil for a fully-wired cell")
	}
	return api
}

func doTrinoJSON(t *testing.T, r *gin.Engine, method, path, body string) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	r.ServeHTTP(w, req)
	var out map[string]any
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: decode body %q: %v", method, path, w.Body.String(), err)
		}
	}
	return w.Code, out
}

// --------------------------------------------------------------------------
// Org annotation.
// --------------------------------------------------------------------------

// TestQueriesAreAnnotatedWithTheOwningOrg is the join that makes the live
// view usable: Trino only knows the principal (the org's database_name),
// and the mapping back to a duckgres org id lives in the config store. If
// it were left to the SPA, every consumer would need the org table.
func TestQueriesAreAnnotatedWithTheOwningOrg(t *testing.T) {
	coord := &fakeTrinoCoordinator{queries: []TrinoQuery{
		{QueryID: "q1", State: "RUNNING", Principal: "db_a", ElapsedMS: 100},
		{QueryID: "q2", State: "RUNNING", Principal: "db_b", ElapsedMS: 200},
		// A query from a principal that is not a tenant: the provisioner's
		// own reconcile DDL. It must appear with an EMPTY org rather than
		// be silently attributed to someone.
		{QueryID: "q3", State: "FINISHED", Principal: "__admin_provisioner", Source: "duckgres-provisioner"},
	}}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/queries", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	got := map[string]string{}
	for _, raw := range body["queries"].([]any) {
		q := raw.(map[string]any)
		got[q["query_id"].(string)] = q["org"].(string)
	}
	want := map[string]string{"q1": "org-a", "q2": "org-b", "q3": ""}
	for id, wantOrg := range want {
		if got[id] != wantOrg {
			t.Errorf("query %s: org = %q, want %q", id, got[id], wantOrg)
		}
	}
}

// TestQueriesNeverLeakTheRootPasswordHash: TrinoEnabledOrg carries each
// org's root bcrypt hash, because the provisioner projects it into
// password.db. Nothing on this surface may serialize it.
func TestTrinoPayloadsNeverLeakThePasswordHash(t *testing.T) {
	coord := &fakeTrinoCoordinator{
		queries: []TrinoQuery{{QueryID: "q1", State: "RUNNING", Principal: "db_a"}},
		info:    &TrinoServerInfo{Version: "484"},
	}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleAdmin)

	for _, path := range []string{"/api/v1/trino/queries", "/api/v1/trino/orgs", "/api/v1/trino/status", "/api/v1/orgs/org-a/trino"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if strings.Contains(w.Body.String(), "$2a$10$") {
			t.Errorf("%s leaked a bcrypt hash: %s", path, w.Body.String())
		}
	}
}

// --------------------------------------------------------------------------
// Filtering + ordering.
// --------------------------------------------------------------------------

func TestQueriesFiltering(t *testing.T) {
	coord := &fakeTrinoCoordinator{queries: []TrinoQuery{
		{QueryID: "run-a", State: "RUNNING", Principal: "db_a", ElapsedMS: 500},
		{QueryID: "queued-a", State: "QUEUED", Principal: "db_a", ElapsedMS: 10},
		{QueryID: "done-a", State: "FINISHED", Principal: "db_a", ElapsedMS: 9000},
		{QueryID: "run-b", State: "RUNNING", Principal: "db_b", ElapsedMS: 100},
	}}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	ids := func(path string) []string {
		_, body := doTrinoJSON(t, r, http.MethodGet, path, "")
		var out []string
		for _, raw := range body["queries"].([]any) {
			out = append(out, raw.(map[string]any)["query_id"].(string))
		}
		return out
	}

	// Ordered longest-running first: that is what an operator is hunting.
	if got := ids("/api/v1/trino/queries"); len(got) != 4 || got[0] != "done-a" || got[1] != "run-a" {
		t.Errorf("unfiltered order = %v, want longest-elapsed first", got)
	}
	// active=1 drops finished queries — the live view's default.
	got := ids("/api/v1/trino/queries?active=1")
	for _, id := range got {
		if id == "done-a" {
			t.Errorf("active=1 returned a FINISHED query: %v", got)
		}
	}
	if len(got) != 3 {
		t.Errorf("active=1 returned %v, want the 3 running/queued queries", got)
	}
	if got := ids("/api/v1/trino/queries?org=org-b"); len(got) != 1 || got[0] != "run-b" {
		t.Errorf("org filter = %v, want [run-b]", got)
	}
	// State matching is case-insensitive on the way in; Trino's own names
	// are upper-case.
	if got := ids("/api/v1/trino/queries?state=queued"); len(got) != 1 || got[0] != "queued-a" {
		t.Errorf("state filter = %v, want [queued-a]", got)
	}
}

// TestActiveFilterKeepsEveryNonTerminalState is the regression guard for a
// filter that used to allowlist {RUNNING, QUEUED}. Trino has nine query
// states and only FINISHED and FAILED are terminal, so that allowlist hid
// five in-flight states — including PLANNING, which on a DuckLake-backed
// cell means "waiting on the tenant's metadata Postgres" and is the single
// most likely thing an operator opens this page to find.
func TestActiveFilterKeepsEveryNonTerminalState(t *testing.T) {
	inFlight := []string{
		"QUEUED", "WAITING_FOR_RESOURCES", "DISPATCHING",
		"PLANNING", "STARTING", "RUNNING", "FINISHING",
	}
	var queries []TrinoQuery
	for i, st := range inFlight {
		queries = append(queries, TrinoQuery{QueryID: st, State: st, Principal: "db_a", ElapsedMS: int64(i)})
	}
	queries = append(queries,
		TrinoQuery{QueryID: "FINISHED", State: "FINISHED", Principal: "db_a"},
		TrinoQuery{QueryID: "FAILED", State: "FAILED", Principal: "db_a"},
	)
	r := trinoTestRouter(testTrinoAPI(t, &fakeTrinoCoordinator{queries: queries}, twoOrgTrinoStore()), RoleViewer)

	_, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/queries?active=1", "")
	got := map[string]bool{}
	for _, raw := range body["queries"].([]any) {
		got[raw.(map[string]any)["query_id"].(string)] = true
	}
	for _, st := range inFlight {
		if !got[st] {
			t.Errorf("active=1 dropped %s; it is not a terminal state and the query is still killable", st)
		}
	}
	for _, st := range []string{"FINISHED", "FAILED"} {
		if got[st] {
			t.Errorf("active=1 kept terminal state %s", st)
		}
	}
}

// --------------------------------------------------------------------------
// Degradation.
// --------------------------------------------------------------------------

// TestStatusReportsProvisioningWhenTheCoordinatorIsDown is the distinction
// that matters during an incident: "the cell is unreachable" and "these
// tenants never provisioned" have different fixes, and the provisioning
// half comes from the config store, so it must still render.
func TestStatusReportsProvisioningWhenTheCoordinatorIsDown(t *testing.T) {
	coord := &fakeTrinoCoordinator{
		queriesErr: errors.New("dial tcp: connection refused"),
		infoErr:    errors.New("dial tcp: connection refused"),
		nodesErr:   errors.New("dial tcp: connection refused"),
	}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/status", "")
	if code != http.StatusOK {
		t.Fatalf("status must still answer 200 when the cell is down, got %d", code)
	}
	if body["available"] != false {
		t.Error("available must be false when the coordinator cannot be read")
	}
	if s, _ := body["error"].(string); s == "" {
		t.Error("an unavailable cell must carry the reason")
	}
	if got := body["total_orgs"]; got != float64(2) {
		t.Errorf("total_orgs = %v, want 2 — provisioning state comes from the config store, not the coordinator", got)
	}
	states := body["orgs_by_state"].(map[string]any)
	if states["ready"] != float64(1) || states["pending"] != float64(1) {
		t.Errorf("orgs_by_state = %v, want one ready and one pending", states)
	}
}

// TestStatusStaysAvailableWhenTheCellDoesNotServeNodes is the bug this
// distinction exists for. Trino binds NodeResource only under
// discovery.type=AIRLIFT_DISCOVERY, and these cells run the default
// ANNOUNCE, so /v1/node answers 404 on a perfectly healthy coordinator.
// Before this, that 404 set available=false and the console reported a
// working cell as one that never answered.
func TestStatusStaysAvailableWhenTheCellDoesNotServeNodes(t *testing.T) {
	coord := &fakeTrinoCoordinator{
		info:     &TrinoServerInfo{Version: "484", Environment: "production"},
		queries:  []TrinoQuery{{QueryID: "q1", State: "RUNNING", Principal: "db_a"}},
		nodesErr: fmt.Errorf("GET /v1/node: %w", errTrinoEndpointUnavailable),
	}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/status", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if body["available"] != true {
		t.Errorf("a cell that does not serve /v1/node is still available, body: %v", body)
	}
	if s, _ := body["error"].(string); s != "" {
		t.Errorf("a missing endpoint is not a cell error, got %q", s)
	}
	if body["node_stats"] != false {
		t.Error("node_stats must be false so the console shows 'not reported' rather than zero nodes")
	}
	// The rest of the cell must still be reported.
	states := body["queries_by_state"].(map[string]any)
	if states["RUNNING"] != float64(1) {
		t.Errorf("queries must still be counted, got %v", states)
	}
}

func TestStatusCountsQueriesAndNodes(t *testing.T) {
	coord := &fakeTrinoCoordinator{
		info: &TrinoServerInfo{Version: "484", Environment: "production", UptimeMS: 3600000},
		queries: []TrinoQuery{
			{QueryID: "q1", State: "RUNNING", Principal: "db_a"},
			{QueryID: "q2", State: "RUNNING", Principal: "db_a", FullyBlocked: true},
			{QueryID: "q3", State: "QUEUED", Principal: "db_b"},
		},
		nodes: []TrinoNode{{URI: "http://a"}, {URI: "http://b", Failed: true}},
	}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/status", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if body["available"] != true {
		t.Fatalf("expected an available cell, body: %v", body)
	}
	states := body["queries_by_state"].(map[string]any)
	if states["RUNNING"] != float64(2) || states["QUEUED"] != float64(1) {
		t.Errorf("queries_by_state = %v", states)
	}
	// A fully-blocked running query is the "waiting on the metadata store
	// or S3" signature, counted separately from merely running.
	if body["blocked_queries"] != float64(1) {
		t.Errorf("blocked_queries = %v, want 1", body["blocked_queries"])
	}
	if body["nodes"] != float64(2) || body["failed_nodes"] != float64(1) {
		t.Errorf("nodes = %v, failed = %v; want 2 and 1", body["nodes"], body["failed_nodes"])
	}
	if body["cell"].(map[string]any)["id"] != "cell-test" {
		t.Errorf("cell id missing from the status payload: %v", body["cell"])
	}
}

// TestOrgsRenderWhenTheCoordinatorIsDown: the provisioning view is the
// point of /trino/orgs, so a dead coordinator must cost only the live
// counts.
func TestOrgsRenderWhenTheCoordinatorIsDown(t *testing.T) {
	coord := &fakeTrinoCoordinator{queriesErr: errors.New("connection refused")}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/orgs", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if body["available"] != false {
		t.Error("available must be false so zero live counts are not read as an idle cell")
	}
	orgs := body["orgs"].([]any)
	if len(orgs) != 2 {
		t.Fatalf("expected both orgs, got %d", len(orgs))
	}
	first := orgs[0].(map[string]any)
	if first["org"] != "org-a" || first["catalog"] != "org_db_a" || first["principal"] != "db_a" {
		t.Errorf("unexpected org row: %v", first)
	}
	if first["trino_catalog_name"] != "org_db_a" {
		t.Errorf("trino_catalog_name = %v, want org_db_a", first["trino_catalog_name"])
	}
}

// --------------------------------------------------------------------------
// Kill.
// --------------------------------------------------------------------------

func TestKillQueryRequiresAdmin(t *testing.T) {
	coord := &fakeTrinoCoordinator{queryByID: map[string]TrinoQuery{"q1": {QueryID: "q1", Principal: "db_a"}}}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, _ := doTrinoJSON(t, r, http.MethodPost, "/api/v1/trino/queries/q1/kill", `{"reason":"noisy"}`)
	if code != http.StatusForbidden {
		t.Fatalf("viewer kill: expected 403, got %d", code)
	}
	if len(coord.killed) != 0 {
		t.Error("a viewer's kill reached the coordinator")
	}
}

// TestKillQueryDeliversAReason: the reason reaches the TENANT as the
// query's failure message, so they learn why their query died instead of
// seeing an unexplained cancellation.
func TestKillQueryDeliversAReason(t *testing.T) {
	coord := &fakeTrinoCoordinator{queryByID: map[string]TrinoQuery{"q1": {QueryID: "q1", Principal: "db_a"}}}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleAdmin)

	code, body := doTrinoJSON(t, r, http.MethodPost, "/api/v1/trino/queries/q1/kill", `{"reason":"scanning 40TB"}`)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%v)", code, body)
	}
	if len(coord.killed) != 1 || coord.killed[0] != "q1" {
		t.Fatalf("killed = %v, want [q1]", coord.killed)
	}
	if coord.killReasons[0] != "scanning 40TB" {
		t.Errorf("reason = %q, want the operator's text", coord.killReasons[0])
	}
	// The response names the org whose query was killed, resolved before
	// the kill while the coordinator still knew the query.
	if body["org"] != "org-a" {
		t.Errorf("org = %v, want org-a", body["org"])
	}

	// A kill with no body still works and carries a default explanation —
	// an unexplained failure is worse than a generic one.
	_, _ = doTrinoJSON(t, r, http.MethodPost, "/api/v1/trino/queries/q1/kill", "")
	if len(coord.killReasons) != 2 || coord.killReasons[1] == "" {
		t.Errorf("reasons = %v, want a default reason for the bodyless kill", coord.killReasons)
	}
}

func TestKillQueryUnknownQueryIsNotFound(t *testing.T) {
	coord := &fakeTrinoCoordinator{
		queryByID: map[string]TrinoQuery{},
		killErr:   fmt.Errorf("gone: %w", errTrinoNotFound),
	}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleAdmin)

	code, _ := doTrinoJSON(t, r, http.MethodPost, "/api/v1/trino/queries/nope/kill", "")
	if code != http.StatusNotFound {
		t.Fatalf("expected 404 for an unknown query, got %d", code)
	}
}

// --------------------------------------------------------------------------
// Query detail.
// --------------------------------------------------------------------------

func TestQueryDetailAgedOutIsNotFound(t *testing.T) {
	coord := &fakeTrinoCoordinator{queryByID: map[string]TrinoQuery{}}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, _ := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/queries/old", "")
	if code != http.StatusNotFound {
		t.Fatalf("a query the coordinator has dropped must 404, got %d", code)
	}
}

// --------------------------------------------------------------------------
// Org detail.
// --------------------------------------------------------------------------

// TestOrgDetailSurfacesTheReconcileOutcome is the whole point of the org
// tab: state, status message and timestamps live on
// duckgres_managed_warehouse_trino and were surfaced nowhere, so a failed
// Trino provision was silent.
func TestOrgDetailSurfacesTheReconcileOutcome(t *testing.T) {
	failedAt := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	store := twoOrgTrinoStore()
	store.rows = map[string]*configstore.ManagedWarehouseTrino{
		"org-b": {
			OrgID:         "org-b",
			Enabled:       true,
			Tier:          "scale",
			TrinoCellID:   "cell-test",
			State:         configstore.ManagedWarehouseStateFailed,
			StatusMessage: "catalog reconcile failed: duckling has published no credential",
			FailedAt:      &failedAt,
		},
	}
	coord := &fakeTrinoCoordinator{queries: []TrinoQuery{
		{QueryID: "q1", State: "RUNNING", Principal: "db_b"},
		{QueryID: "q2", State: "QUEUED", Principal: "db_b"},
		{QueryID: "q3", State: "RUNNING", Principal: "db_a"},
	}}
	r := trinoTestRouter(testTrinoAPI(t, coord, store), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/orgs/org-b/trino", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if body["enabled"] != true {
		t.Fatalf("expected enabled, got %v", body)
	}
	st := body["status"].(map[string]any)
	if st["trino_catalog_name"] != "org_db_b" {
		t.Errorf("trino_catalog_name = %v, want org_db_b", st["trino_catalog_name"])
	}
	if st["state"] != "failed" {
		t.Errorf("state = %v, want failed", st["state"])
	}
	if !strings.Contains(st["status_message"].(string), "duckling has published no credential") {
		t.Errorf("status_message = %v; the reconcile detail is the actionable part", st["status_message"])
	}
	if st["failed_at"] == nil {
		t.Error("failed_at must be surfaced")
	}
	if _, ok := st["connection"]; ok {
		t.Error("a failed Trino target must not expose connection details")
	}
	// Live counts are scoped to this org only.
	if st["running_queries"] != float64(1) || st["queued_queries"] != float64(1) {
		t.Errorf("live counts = %v/%v, want 1 running and 1 queued for org-b", st["running_queries"], st["queued_queries"])
	}
}

func TestReadyOrgDetailReturnsTenantClientConnection(t *testing.T) {
	store := twoOrgTrinoStore()
	store.rows = map[string]*configstore.ManagedWarehouseTrino{
		"org-a": {
			OrgID:       "org-a",
			Enabled:     true,
			TrinoCellID: "cell-test",
			State:       configstore.ManagedWarehouseStateReady,
		},
	}
	api := NewTrinoAPI(
		TrinoCell{
			ID:             "cell-test",
			CoordinatorURL: "https://coordinator.invalid:8443",
			TLSServerName:  "coordinator.example.com",
			ClientURL:      "https://trino.example.com:443",
		},
		&fakeTrinoCoordinator{},
		store,
		nil,
	)
	if api == nil {
		t.Fatal("NewTrinoAPI returned nil for a fully-wired cell")
	}
	r := trinoTestRouter(api, RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/orgs/org-a/trino", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	status := body["status"].(map[string]any)
	connection := status["connection"].(map[string]any)
	if connection["host"] != "trino.example.com" {
		t.Errorf("connection host = %v, want trino.example.com", connection["host"])
	}
	if connection["port"] != float64(443) {
		t.Errorf("connection port = %v, want 443", connection["port"])
	}
	if connection["username"] != "db_a" {
		t.Errorf("connection username = %v, want db_a", connection["username"])
	}
	if _, ok := connection["password"]; ok {
		t.Error("the control plane must not return the tenant password")
	}
}

// TestOrgDetailNotEnabledIsNotAnError: most orgs have no Trino row, and
// the org page renders a "not enabled" state rather than a failure.
func TestOrgDetailNotEnabledIsNotAnError(t *testing.T) {
	store := twoOrgTrinoStore()
	store.rows = map[string]*configstore.ManagedWarehouseTrino{}
	r := trinoTestRouter(testTrinoAPI(t, &fakeTrinoCoordinator{}, store), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/orgs/org-z/trino", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if body["enabled"] != false {
		t.Errorf("expected enabled=false for an org with no Trino row, got %v", body)
	}
}

// --------------------------------------------------------------------------
// Caching.
// --------------------------------------------------------------------------

// TestQueryListIsCachedAcrossPolls: the console polls from every open tab,
// and `/v1/query` walks every query the coordinator holds. Collapsing those
// polls is what keeps operator load off the scheduler during an incident —
// exactly when the most tabs are open.
func TestQueryListIsCachedAcrossPolls(t *testing.T) {
	coord := &fakeTrinoCoordinator{queries: []TrinoQuery{{QueryID: "q1", State: "RUNNING", Principal: "db_a"}}}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	for range 5 {
		if code, _ := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/queries", ""); code != http.StatusOK {
			t.Fatalf("expected 200, got %d", code)
		}
	}
	if got := coord.queryCalls.Load(); got != 1 {
		t.Errorf("coordinator was polled %d times for 5 console polls; the cache should collapse them to 1", got)
	}
}

// TestCacheServesStaleRatherThanQueueing: a slow coordinator must not turn
// N polling tabs into N stuck requests. Readers during an in-flight refresh
// take the previous value.
func TestCacheServesStaleRatherThanQueueing(t *testing.T) {
	cache := newTrinoCache[int](time.Millisecond)
	ctx := context.Background()

	if v, err := cache.get(ctx, func(context.Context) (int, error) { return 1, nil }); v != 1 || err != nil {
		t.Fatalf("first get = %v, %v", v, err)
	}
	time.Sleep(2 * time.Millisecond) // let it go stale

	release := make(chan struct{})
	entered := make(chan struct{})
	done := make(chan int)
	go func() {
		v, _ := cache.get(ctx, func(context.Context) (int, error) {
			close(entered)
			<-release
			return 2, nil
		})
		done <- v
	}()
	<-entered

	// While that refresh is parked, a second reader gets the old value
	// immediately rather than blocking on the slow fetch.
	got := make(chan int, 1)
	go func() {
		v, _ := cache.get(ctx, func(context.Context) (int, error) { return 99, nil })
		got <- v
	}()
	select {
	case v := <-got:
		if v != 1 {
			t.Errorf("concurrent reader got %d, want the cached 1", v)
		}
	case <-time.After(time.Second):
		t.Fatal("a reader queued behind an in-flight refresh; it must serve the stale value instead")
	}

	close(release)
	if v := <-done; v != 2 {
		t.Errorf("refresh returned %d, want 2", v)
	}
}

// --------------------------------------------------------------------------
// Wiring.
// --------------------------------------------------------------------------

// TestTrinoRoutesUnregisteredWithoutACell mirrors how the rest of Extras
// treats a capability the deployment does not have: no routes, not broken
// routes.
func TestTrinoRoutesUnregisteredWithoutACell(t *testing.T) {
	if api := NewTrinoAPI(TrinoCell{}, nil, twoOrgTrinoStore(), nil); api != nil {
		t.Error("NewTrinoAPI must return nil without a coordinator client")
	}
	if api := NewTrinoAPI(TrinoCell{}, &fakeTrinoCoordinator{}, nil, nil); api != nil {
		t.Error("NewTrinoAPI must return nil without an org store")
	}

	gin.SetMode(gin.TestMode)
	e := gin.New()
	registerTrinoAPI(e.Group("/api/v1"), nil)
	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/trino/status", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 with no Trino cell wired, got %d", w.Code)
	}
}

// A cell on Trino's default discovery.type reports membership through
// /v1/announce and no health at all. The status payload must name that
// source, and must not turn "nothing was measured" into "nothing is wrong":
// FailedNodes stays 0 because the field is meaningless, and the SPA keys off
// node_source rather than reading that 0 as a healthy fleet.
func TestStatusNamesTheAnnounceInventoryAndClaimsNoHealth(t *testing.T) {
	coord := &fakeTrinoCoordinator{
		info:       &TrinoServerInfo{Version: "476", Environment: "mw"},
		nodeSource: TrinoNodeSourceAnnounce,
		// Failed is set to prove the handler ignores it for this source
		// rather than merely relying on the client to zero it.
		nodes: []TrinoNode{{URI: "http://a"}, {URI: "http://b", Failed: true}},
	}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/status", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if body["available"] != true || body["node_stats"] != true {
		t.Fatalf("an announce-only cell is still available and still reports nodes, body: %v", body)
	}
	if body["node_source"] != TrinoNodeSourceAnnounce {
		t.Errorf("node_source = %v, want %q", body["node_source"], TrinoNodeSourceAnnounce)
	}
	if body["nodes"] != float64(2) {
		t.Errorf("nodes = %v, want 2", body["nodes"])
	}
	if body["failed_nodes"] != float64(0) {
		t.Errorf("failed_nodes = %v; the announce inventory measures no health, so it must not be counted", body["failed_nodes"])
	}
}

// The nodes route tells the SPA which inventory answered, so it can render
// membership-only rows instead of zero-filled health columns.
func TestNodesRouteReportsItsSource(t *testing.T) {
	coord := &fakeTrinoCoordinator{
		nodeSource: TrinoNodeSourceAnnounce,
		nodes:      []TrinoNode{{URI: "http://a"}},
	}
	r := trinoTestRouter(testTrinoAPI(t, coord, twoOrgTrinoStore()), RoleViewer)

	code, body := doTrinoJSON(t, r, http.MethodGet, "/api/v1/trino/nodes", "")
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if body["source"] != TrinoNodeSourceAnnounce {
		t.Errorf("source = %v, want %q", body["source"], TrinoNodeSourceAnnounce)
	}
	if got := body["nodes"].([]any); len(got) != 1 {
		t.Errorf("nodes = %v, want 1 entry", got)
	}
}
