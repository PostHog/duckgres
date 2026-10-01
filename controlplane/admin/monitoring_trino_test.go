//go:build kubernetes

package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

const trinoMonitoringTestCell = "registered:cell-a"

func trinoMonitoringRouter(store trinoMonitoringStore, api *TrinoAPI, metrics *MetricsProxy, identitySource string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxIdentityKey, &Identity{Role: RoleAdmin, Source: identitySource})
		c.Next()
	})
	registerTrinoMonitoringAPI(r.Group("/api/v1"), store, api, metrics)
	return r
}

func trinoMonitoringFleet(coordinator TrinoCoordinatorClient, orgs TrinoOrgStore) *TrinoAPI {
	return NewTrinoFleetAPI(
		[]TrinoCell{{ID: "cell-a", StoredID: trinoMonitoringTestCell}},
		[]TrinoCoordinatorClient{coordinator},
		orgs,
		nil,
	)
}

func trinoMonitoringOrgStore(readyAt time.Time) *fakeTrinoOrgStore {
	return &fakeTrinoOrgStore{
		orgs: []configstore.TrinoEnabledOrg{
			{
				OrgID: "org-a", DatabaseName: "tenantdb", Tier: "growth", CellID: trinoMonitoringTestCell,
				State: configstore.ManagedWarehouseStateReady, RootPasswordHash: "$2a$10$roothash",
				Users: []configstore.TrinoOrgUser{{Username: "analyst", PasswordHash: "$2a$10$analysthash"}},
			},
			{
				OrgID: "org-b", DatabaseName: "otherdb", Tier: "free", CellID: trinoMonitoringTestCell,
				State: configstore.ManagedWarehouseStateReady, RootPasswordHash: "$2a$10$otherhash",
			},
		},
		rows: map[string]*configstore.ManagedWarehouseTrino{
			"org-a": {
				OrgID: "org-a", Enabled: true, Tier: "growth", TrinoCellID: trinoMonitoringTestCell,
				State: configstore.ManagedWarehouseStateReady, ReadyAt: &readyAt,
				StatusMessage: "sensitive reconcile detail",
			},
		},
	}
}

func getTrinoMonitoringSnapshot(t *testing.T, r *gin.Engine, orgID string) (trinoMonitoringSnapshotResponse, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orgs/"+orgID+"/monitoring/trino/snapshot", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got trinoMonitoringSnapshotResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got, rec.Body.String()
}

func TestTrinoMonitoringSnapshotIsOrgScopedMaskedAndCounted(t *testing.T) {
	readyAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	created := time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)
	half := 50.0
	coordinator := &fakeTrinoCoordinator{queries: []TrinoQuery{
		{QueryID: "q-running", State: "RUNNING", Principal: "tenantdb", Source: "dbt", ResourceGroup: "root.tenants.growth.tenantdb",
			Query: "SELECT * FROM t WHERE email = 'person@example.com'", Created: created, ElapsedMS: 5000, QueuedMS: 20, CPUMS: 900,
			PhysicalInputBytes: 100, PeakMemoryBytes: 64, ProcessedInputRows: 7, ProgressPercentage: &half},
		{QueryID: "q-queued", State: "QUEUED", Principal: "tenantdb.analyst", ElapsedMS: 9000},
		{QueryID: "q-planning", State: "PLANNING", Principal: "tenantdb", ElapsedMS: 100},
		{QueryID: "q-blocked", State: "RUNNING", FullyBlocked: true, Principal: "tenantdb", ElapsedMS: 7000, PhysicalInputBytes: 50},
		{QueryID: "q-finished", State: "FINISHED", Principal: "tenantdb", ElapsedMS: 99999},
		{QueryID: "q-other-org", State: "RUNNING", Principal: "otherdb", Query: "SELECT 'other-org-secret'", ElapsedMS: 99999},
	}}
	r := trinoMonitoringRouter(
		&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")},
		trinoMonitoringFleet(coordinator, trinoMonitoringOrgStore(readyAt)),
		nil, "internal-secret",
	)

	got, body := getTrinoMonitoringSnapshot(t, r, "org-a")

	if got.SchemaVersion != 1 || got.OrgID != "org-a" || !got.Available {
		t.Fatalf("envelope = %+v", got)
	}
	if got.Trino.State != "ready" || got.Trino.ReadyAt == nil || !got.Trino.ReadyAt.Equal(readyAt) {
		t.Fatalf("trino = %+v, want ready at %s", got.Trino, readyAt)
	}
	if got.Limits != (trinoMonitoringLimits{MaxRunningQueries: 10, MaxQueuedQueries: 50}) {
		t.Fatalf("limits = %+v, want the growth tier's 10 / 50", got.Limits)
	}
	wantTotals := trinoMonitoringTotals{InFlight: 4, Running: 3, Queued: 1, Blocked: 1, LongestRunningMS: 9000, PhysicalInputBytes: 150}
	if got.Totals != wantTotals {
		t.Fatalf("totals = %+v, want %+v", got.Totals, wantTotals)
	}

	var order, users []string
	for _, q := range got.Queries {
		order = append(order, q.QueryID)
		users = append(users, q.User)
	}
	if strings.Join(order, ",") != "q-queued,q-blocked,q-running,q-planning" {
		t.Fatalf("query order = %v, want longest-running first and only this org's in-flight queries", order)
	}
	if strings.Join(users, ",") != "analyst,root,root,root" {
		t.Fatalf("users = %v, want duckgres usernames", users)
	}
	running := got.Queries[2]
	if running.State != "running" || running.Query != "SELECT * FROM t WHERE email = ?" || running.Source != "dbt" {
		t.Fatalf("running row = %+v", running)
	}
	if running.CreatedAt == nil || !running.CreatedAt.Equal(created) || running.ProgressPercentage == nil || *running.ProgressPercentage != 50 {
		t.Fatalf("running row timing = %+v", running)
	}
	if !got.Queries[1].Blocked || got.Queries[0].CreatedAt != nil || got.QueriesTruncated {
		t.Fatalf("blocked flag, null created_at or truncation is wrong: %+v", got)
	}

	for _, forbidden := range []string{
		"person@example.com", "other-org-secret", "tenantdb", "otherdb", "cell-a",
		"root.tenants", "sensitive reconcile detail", "principal", "resource_group", "$2a$10$",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("snapshot leaks %q: %s", forbidden, body)
		}
	}
}

func TestTrinoMonitoringSnapshotReportsLifecycleAndAvailability(t *testing.T) {
	readyAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		api           func() *TrinoAPI
		wantState     string
		wantAvailable bool
		wantLimits    trinoMonitoringLimits
	}{
		{"deployment without a Trino cell", func() *TrinoAPI { return nil }, "not_enabled", false, trinoMonitoringLimits{}},
		{"org without a Trino row", func() *TrinoAPI {
			store := trinoMonitoringOrgStore(readyAt)
			delete(store.rows, "org-a")
			return trinoMonitoringFleet(&fakeTrinoCoordinator{}, store)
		}, "not_enabled", false, trinoMonitoringLimits{}},
		{"disabled Trino row", func() *TrinoAPI {
			store := trinoMonitoringOrgStore(readyAt)
			store.rows["org-a"].Enabled = false
			return trinoMonitoringFleet(&fakeTrinoCoordinator{}, store)
		}, "not_enabled", false, trinoMonitoringLimits{}},
		{"enabled but not assigned to a configured cell", func() *TrinoAPI {
			store := trinoMonitoringOrgStore(readyAt)
			store.rows["org-a"].TrinoCellID = "registered:retired-cell"
			store.rows["org-a"].State = ""
			return trinoMonitoringFleet(&fakeTrinoCoordinator{}, store)
		}, "pending", false, trinoMonitoringLimits{MaxRunningQueries: 10, MaxQueuedQueries: 50}},
		{"coordinator unreachable", func() *TrinoAPI {
			return trinoMonitoringFleet(&fakeTrinoCoordinator{queriesErr: errors.New("connection refused")}, trinoMonitoringOrgStore(readyAt))
		}, "ready", false, trinoMonitoringLimits{MaxRunningQueries: 10, MaxQueuedQueries: 50}},
		{"org list unreadable", func() *TrinoAPI {
			store := trinoMonitoringOrgStore(readyAt)
			store.listErr = errors.New("config store down")
			return trinoMonitoringFleet(&fakeTrinoCoordinator{queries: []TrinoQuery{{QueryID: "q", State: "RUNNING", Principal: "tenantdb"}}}, store)
		}, "ready", false, trinoMonitoringLimits{MaxRunningQueries: 10, MaxQueuedQueries: 50}},
		{"idle and reachable", func() *TrinoAPI {
			return trinoMonitoringFleet(&fakeTrinoCoordinator{}, trinoMonitoringOrgStore(readyAt))
		}, "ready", true, trinoMonitoringLimits{MaxRunningQueries: 10, MaxQueuedQueries: 50}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := trinoMonitoringRouter(&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")}, tc.api(), nil, "internal-secret")

			got, body := getTrinoMonitoringSnapshot(t, r, "org-a")

			if got.Trino.State != tc.wantState || got.Available != tc.wantAvailable || got.Limits != tc.wantLimits {
				t.Fatalf("state/available/limits = %q/%v/%+v, want %q/%v/%+v",
					got.Trino.State, got.Available, got.Limits, tc.wantState, tc.wantAvailable, tc.wantLimits)
			}
			if got.Totals != (trinoMonitoringTotals{}) {
				t.Fatalf("totals = %+v, want zero", got.Totals)
			}
			if !strings.Contains(body, `"queries":[]`) {
				t.Fatalf("queries must serialize as an empty list, not null: %s", body)
			}
		})
	}
}

func TestTrinoMonitoringSnapshotCapsTheQueryList(t *testing.T) {
	queries := make([]TrinoQuery, 0, 205)
	for i := 0; i < 205; i++ {
		queries = append(queries, TrinoQuery{QueryID: fmt.Sprintf("q-%03d", i), State: "RUNNING", Principal: "tenantdb", ElapsedMS: int64(i), PhysicalInputBytes: 1})
	}
	r := trinoMonitoringRouter(
		&fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")},
		trinoMonitoringFleet(&fakeTrinoCoordinator{queries: queries}, trinoMonitoringOrgStore(time.Now())),
		nil, "internal-secret",
	)

	got, _ := getTrinoMonitoringSnapshot(t, r, "org-a")

	if len(got.Queries) != 200 || !got.QueriesTruncated || got.Queries[0].QueryID != "q-204" {
		t.Fatalf("list = %d rows, truncated %v, first %q; want 200, true, q-204", len(got.Queries), got.QueriesTruncated, got.Queries[0].QueryID)
	}
	if got.Totals.InFlight != 205 || got.Totals.PhysicalInputBytes != 205 || got.Totals.LongestRunningMS != 204 {
		t.Fatalf("totals = %+v, want them computed over all 205 queries", got.Totals)
	}
}

func TestTrinoMonitoringRejectsSSOAndUnknownWarehouses(t *testing.T) {
	api := trinoMonitoringFleet(&fakeTrinoCoordinator{}, trinoMonitoringOrgStore(time.Now()))
	store := &fakeMonitoringStore{snapshot: monitoringTestSnapshot("org-a")}

	rec := httptest.NewRecorder()
	trinoMonitoringRouter(store, api, nil, "sso").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-a/monitoring/trino/snapshot", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("SSO status = %d, want 403", rec.Code)
	}

	rec = httptest.NewRecorder()
	trinoMonitoringRouter(store, api, nil, "internal-secret").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/orgs/org-zzz/monitoring/trino/snapshot", nil))
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), monitoringWarehouseNotFoundCode) {
		t.Errorf("unknown warehouse = %d %s, want 404 with the stable code", rec.Code, rec.Body.String())
	}
}

func TestTrinoInFlightStatePartitionsActiveQueries(t *testing.T) {
	cases := []struct {
		query     TrinoQuery
		wantState string
		wantOK    bool
	}{
		{TrinoQuery{State: "QUEUED"}, TrinoInFlightQueued, true},
		{TrinoQuery{State: "RUNNING"}, TrinoInFlightRunning, true},
		{TrinoQuery{State: "RUNNING", FullyBlocked: true}, TrinoInFlightBlocked, true},
		{TrinoQuery{State: "PLANNING"}, TrinoInFlightOther, true},
		{TrinoQuery{State: "WAITING_FOR_RESOURCES"}, TrinoInFlightOther, true},
		{TrinoQuery{State: "FINISHING", FullyBlocked: true}, TrinoInFlightOther, true},
		{TrinoQuery{State: "FINISHED"}, "", false},
		{TrinoQuery{State: "FAILED"}, "", false},
	}
	for _, tc := range cases {
		state, ok := TrinoInFlightState(tc.query)
		if state != tc.wantState || ok != tc.wantOK {
			t.Errorf("TrinoInFlightState(%+v) = %q, %v; want %q, %v", tc.query, state, ok, tc.wantState, tc.wantOK)
		}
	}
}
