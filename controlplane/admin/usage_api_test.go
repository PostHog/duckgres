//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// fakeMonthlyUsageStore is an in-memory usageStore for handler tests.
type fakeMonthlyUsageStore struct {
	compute   []configstore.MonthlyComputeUsageRow
	storage   []configstore.MonthlyStorageUsageRow
	cursor    time.Time
	hasCursor bool
	err       error

	dailyCompute  []configstore.DailyComputeUsageRow
	dailyStorage  []configstore.DailyStorageUsageRow
	lastDailyOrg  string
	lastDailyFrom time.Time
}

func (s *fakeMonthlyUsageStore) AggregateComputeUsageMonthly(from time.Time) ([]configstore.MonthlyComputeUsageRow, error) {
	return s.compute, s.err
}
func (s *fakeMonthlyUsageStore) AggregateStorageUsageMonthly(from time.Time) ([]configstore.MonthlyStorageUsageRow, error) {
	return s.storage, s.err
}
func (s *fakeMonthlyUsageStore) ComputeBillingCursor() (time.Time, bool, error) {
	return s.cursor, s.hasCursor, nil
}

// setupUsageRouter mounts the monthly-usage route exactly as production does
// (registerUsageAPI, which self-gates with RequireAdmin), with an injected
// identity standing in for AuthMiddleware. role="" simulates an
// unauthenticated caller (no identity in context).
func setupUsageRouter(store usageStore, role Role) *gin.Engine {
	return setupUsageRouterForRegion(store, role, "us-east-1")
}

func setupUsageRouterForRegion(store usageStore, role Role, awsRegion string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if role != "" {
		r.Use(func(c *gin.Context) {
			c.Set(ctxIdentityKey, &Identity{Email: "op@posthog.com", Role: role, Source: "sso"})
		})
	}
	registerUsageAPI(r.Group("/api/v1"), store, awsRegion)
	return r
}

// setupUsageRouterAt mounts the route with a controllable clock (window
// tests); caller is an admin.
func setupUsageRouterAt(store usageStore, now func() time.Time) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxIdentityKey, &Identity{Email: "op@posthog.com", Role: RoleAdmin, Source: "sso"})
	})
	h := &usageAPIHandler{store: store, now: now, awsRegion: "us-east-1"}
	r.GET("/api/v1/usage/monthly", RequireAdmin(), h.getMonthlyUsage)
	r.GET("/api/v1/orgs/:id/usage/daily", RequireAdmin(), h.getDailyUsage)
	return r
}

func TestMonthlyUsageReportsDeploymentPricingRegion(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		awsRegion   string
		wantStatus  int
		wantPricing string
	}{
		{name: "US production and development", awsRegion: "us-east-1", wantStatus: http.StatusOK, wantPricing: "US"},
		{name: "EU deployment", awsRegion: "eu-central-1", wantStatus: http.StatusOK, wantPricing: "EU"},
		{name: "missing region", awsRegion: "", wantStatus: http.StatusServiceUnavailable},
		{name: "unsupported region", awsRegion: "ap-southeast-1", wantStatus: http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := setupUsageRouterForRegion(&fakeMonthlyUsageStore{}, RoleAdmin, tt.awsRegion)
			code, body := usageRequest(t, r, "/api/v1/usage/monthly")
			if code != tt.wantStatus {
				t.Fatalf("status = %d, want %d: %v", code, tt.wantStatus, body)
			}
			if code == http.StatusOK && (body["aws_region"] != tt.awsRegion || body["customer_pricing_region"] != tt.wantPricing) {
				t.Fatalf("deployment pricing fields = %v, want aws_region=%q customer_pricing_region=%q", body, tt.awsRegion, tt.wantPricing)
			}
		})
	}
}

// Usage data is cost data: the billing pull API gates the raw families behind
// RequireAdmin, and this monthly aggregate is no less sensitive — it exposes
// every org's per-team spend. Viewers (any @posthog.com SSO login) must NOT
// read it; unauthenticated callers get 401.
func TestMonthlyUsageRequiresAdmin(t *testing.T) {
	store := &fakeMonthlyUsageStore{}

	r := setupUsageRouter(store, RoleViewer)
	code, _ := usageRequest(t, r, "/api/v1/usage/monthly")
	if code != http.StatusForbidden {
		t.Fatalf("viewer GET /usage/monthly = %d, want 403", code)
	}

	r = setupUsageRouter(store, "")
	code, _ = usageRequest(t, r, "/api/v1/usage/monthly")
	if code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET /usage/monthly = %d, want 401", code)
	}

	r = setupUsageRouter(store, RoleAdmin)
	code, _ = usageRequest(t, r, "/api/v1/usage/monthly")
	if code != http.StatusOK {
		t.Fatalf("admin GET /usage/monthly = %d, want 200", code)
	}
}

func usageRequest(t *testing.T, r *gin.Engine, path string) (int, map[string]interface{}) {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	r.ServeHTTP(w, req)
	var body map[string]interface{}
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("response not JSON: %v (%s)", err, w.Body.String())
		}
	}
	return w.Code, body
}

func strptr(s string) *string { return &s }

// The handler merges the compute and storage families on (month, org, team):
// a key present in both lands in ONE row with both metric sets; a key in only
// one family still appears with the other zeroed.
func TestMonthlyUsageMergesFamilies(t *testing.T) {
	store := &fakeMonthlyUsageStore{
		compute: []configstore.MonthlyComputeUsageRow{
			{Month: "2026-08", OrgID: "acme", TeamID: 5, SchemaName: strptr("team_5"), CPUSeconds: 120, MemorySeconds: 240},
			{Month: "2026-08", OrgID: "globex", TeamID: 7, CPUSeconds: 60, MemorySeconds: 60},
		},
		storage: []configstore.MonthlyStorageUsageRow{
			{Month: "2026-08", OrgID: "acme", TeamID: 5, SchemaName: strptr("team_5"), GiBSeconds: "10800"},
			{Month: "2026-07", OrgID: "acme", TeamID: 5, SchemaName: strptr("team_5"), GiBSeconds: "3600"},
		},
		cursor:    time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
		hasCursor: true,
	}
	r := setupUsageRouter(store, RoleAdmin)

	code, body := usageRequest(t, r, "/api/v1/usage/monthly")
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	rows, ok := body["rows"].([]interface{})
	if !ok {
		t.Fatalf("rows missing/not array: %v", body)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 merged rows, got %d: %v", len(rows), rows)
	}
	// Sorted newest month first.
	first := rows[0].(map[string]interface{})
	if first["month"] != "2026-08" {
		t.Fatalf("rows not sorted month-desc: %v", rows)
	}
	// The acme/5 August row carries BOTH families.
	var acmeAug map[string]interface{}
	for _, ri := range rows {
		m := ri.(map[string]interface{})
		if m["org_id"] == "acme" && m["month"] == "2026-08" {
			acmeAug = m
		}
	}
	if acmeAug == nil {
		t.Fatalf("merged acme 2026-08 row missing: %v", rows)
	}
	if acmeAug["cpu_seconds"].(float64) != 120 || acmeAug["memory_seconds"].(float64) != 240 {
		t.Fatalf("compute side lost in merge: %v", acmeAug)
	}
	if acmeAug["gib_seconds"].(float64) != 10800 {
		t.Fatalf("storage side lost in merge: %v", acmeAug)
	}
	if acmeAug["schema_name"] != "team_5" {
		t.Fatalf("schema name not carried: %v", acmeAug)
	}
	// Storage-only row (July) appears with zeroed compute.
	var july map[string]interface{}
	for _, ri := range rows {
		m := ri.(map[string]interface{})
		if m["month"] == "2026-07" {
			july = m
		}
	}
	if july == nil || july["cpu_seconds"].(float64) != 0 || july["gib_seconds"].(float64) != 3600 {
		t.Fatalf("storage-only row wrong: %v", july)
	}
	// The ack cursor is surfaced so the UI can caveat the retention window.
	if body["watermark_low"] == nil {
		t.Fatalf("watermark_low missing: %v", body)
	}
}

func TestMonthlyUsageWindowFromMonthsParam(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) }
	store := &fakeMonthlyUsageStore{}

	var gotFrom time.Time
	spy := &spyUsageStore{fakeMonthlyUsageStore: store, onFrom: func(f time.Time) { gotFrom = f }}
	r := setupUsageRouterAt(spy, now)

	// months=3 starting mid-August → window opens 2026-06-01.
	code, _ := usageRequest(t, r, "/api/v1/usage/monthly?months=3")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if want := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC); !gotFrom.Equal(want) {
		t.Fatalf("from = %s, want %s", gotFrom, want)
	}
}

type spyUsageStore struct {
	*fakeMonthlyUsageStore
	onFrom func(time.Time)
}

func (s *spyUsageStore) AggregateComputeUsageMonthly(from time.Time) ([]configstore.MonthlyComputeUsageRow, error) {
	s.onFrom(from)
	return s.fakeMonthlyUsageStore.AggregateComputeUsageMonthly(from)
}

func TestMonthlyUsageRejectsBadMonthsParam(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) }
	r := setupUsageRouterAt(&fakeMonthlyUsageStore{}, now)
	for _, bad := range []string{"0", "-2", "abc", "999"} {
		code, body := usageRequest(t, r, "/api/v1/usage/monthly?months="+bad)
		if code != http.StatusBadRequest {
			t.Fatalf("months=%s: status %d, want 400 (%v)", bad, code, body)
		}
	}
}

func TestMonthlyUsageStoreErrorIs500(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) }
	r := setupUsageRouterAt(&fakeMonthlyUsageStore{err: errFakeUsageStore}, now)
	code, _ := usageRequest(t, r, "/api/v1/usage/monthly")
	if code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", code)
	}
}

var errFakeUsageStore = errFakeUsageStoreT{}

type errFakeUsageStoreT struct{}

func (errFakeUsageStoreT) Error() string { return "fake store error" }

// ---- daily per-org usage (org detail page charts) ----

func (s *fakeMonthlyUsageStore) AggregateComputeUsageDaily(orgID string, from time.Time) ([]configstore.DailyComputeUsageRow, error) {
	s.lastDailyOrg = orgID
	s.lastDailyFrom = from
	return s.dailyCompute, s.err
}
func (s *fakeMonthlyUsageStore) AggregateStorageUsageDaily(orgID string, from time.Time) ([]configstore.DailyStorageUsageRow, error) {
	return s.dailyStorage, s.err
}

// The daily handler merges both families per (date, team) and passes the
// :id path segment through as the org scope — the SQL is what keeps one
// org's usage away from another's, so pin that the handler never swaps it.
func TestDailyUsageScopesToOrgAndMergesFamilies(t *testing.T) {
	store := &fakeMonthlyUsageStore{
		dailyCompute: []configstore.DailyComputeUsageRow{
			{Date: "2026-08-13", TeamID: 5, SchemaName: strptr("team_5"), CPUSeconds: 600, MemorySeconds: 1200},
			{Date: "2026-08-13", TeamID: 6, CPUSeconds: 60, MemorySeconds: 60},
		},
		dailyStorage: []configstore.DailyStorageUsageRow{
			{Date: "2026-08-13", TeamID: 5, SchemaName: strptr("team_5"), GiBSeconds: "3600"},
			{Date: "2026-08-14", TeamID: 5, SchemaName: strptr("team_5"), GiBSeconds: "7200"},
		},
	}
	r := setupUsageRouter(store, RoleAdmin)

	code, body := usageRequest(t, r, "/api/v1/orgs/acme/usage/daily?days=7")
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	if store.lastDailyOrg != "acme" {
		t.Fatalf("handler queried org %q, want acme — org scope must come from the path", store.lastDailyOrg)
	}
	if body["org_id"] != "acme" {
		t.Fatalf("org_id echo wrong: %v", body)
	}
	rows, ok := body["rows"].([]interface{})
	if !ok || len(rows) != 3 {
		t.Fatalf("want 3 merged rows, got %v", body["rows"])
	}
	// The 2026-08-13 team_5 row carries both families.
	var d13 map[string]interface{}
	for _, ri := range rows {
		m := ri.(map[string]interface{})
		if m["date"] == "2026-08-13" && m["team_id"].(float64) == 5 {
			d13 = m
		}
	}
	if d13 == nil || d13["cpu_seconds"].(float64) != 600 || d13["gib_seconds"].(float64) != 3600 {
		t.Fatalf("merged 08-13/team5 row wrong: %v", d13)
	}
	// Storage-only day still appears.
	var d14 map[string]interface{}
	for _, ri := range rows {
		m := ri.(map[string]interface{})
		if m["date"] == "2026-08-14" {
			d14 = m
		}
	}
	if d14 == nil || d14["cpu_seconds"].(float64) != 0 || d14["gib_seconds"].(float64) != 7200 {
		t.Fatalf("storage-only day row wrong: %v", d14)
	}
	// Retention transparency travels on this endpoint too.
	if _, ok := body["watermark_low"]; !ok {
		t.Fatalf("watermark_low missing: %v", body)
	}
}

func TestDailyUsageWindowAndValidation(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) }
	store := &fakeMonthlyUsageStore{}
	r := setupUsageRouterAt(store, now)

	// days=7 on Aug-15 (any time of day) → window opens at the START of the
	// UTC day 6 days back, 2026-08-09T00:00Z. Truncating to the day boundary
	// is load-bearing: without it, days=1 opens the window at the current
	// SECOND and excludes every bucket written earlier today (the mw-dev e2e
	// caught exactly that against the real stack).
	code, _ := usageRequest(t, r, "/api/v1/orgs/acme/usage/daily?days=7")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if want := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC); !store.lastDailyFrom.Equal(want) {
		t.Fatalf("from = %s, want %s (start of UTC day)", store.lastDailyFrom, want)
	}

	// days=1 must cover ALL of today, not just the current second.
	code, _ = usageRequest(t, r, "/api/v1/orgs/acme/usage/daily?days=1")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if want := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC); !store.lastDailyFrom.Equal(want) {
		t.Fatalf("days=1: from = %s, want %s (start of today UTC)", store.lastDailyFrom, want)
	}

	for _, bad := range []string{"0", "-1", "abc", "32"} {
		code, _ := usageRequest(t, r, "/api/v1/orgs/acme/usage/daily?days="+bad)
		if code != http.StatusBadRequest {
			t.Fatalf("days=%s: status %d, want 400", bad, code)
		}
	}
}

func TestDailyUsageRequiresAdmin(t *testing.T) {
	store := &fakeMonthlyUsageStore{}
	for _, tc := range []struct {
		role Role
		want int
	}{
		{"", http.StatusUnauthorized},
		{RoleViewer, http.StatusForbidden},
		{RoleAdmin, http.StatusOK},
	} {
		r := setupUsageRouter(store, tc.role)
		code, _ := usageRequest(t, r, "/api/v1/orgs/acme/usage/daily")
		if code != tc.want {
			t.Fatalf("role %q: status %d, want %d", tc.role, code, tc.want)
		}
	}
}
