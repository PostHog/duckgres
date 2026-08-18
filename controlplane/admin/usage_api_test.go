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

// fakeMonthlyUsageStore is an in-memory monthlyUsageStore for handler tests.
type fakeMonthlyUsageStore struct {
	compute   []configstore.MonthlyComputeUsageRow
	storage   []configstore.MonthlyStorageUsageRow
	cursor    time.Time
	hasCursor bool
	err       error
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

func setupUsageRouter(store monthlyUsageStore, now func() time.Time) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := &usageAPIHandler{store: store, now: now}
	r.GET("/api/v1/usage/monthly", h.getMonthlyUsage)
	return r
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
	now := func() time.Time { return time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) }
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
	r := setupUsageRouter(store, now)

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
	r := setupUsageRouter(spy, now)

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
	r := setupUsageRouter(&fakeMonthlyUsageStore{}, now)
	for _, bad := range []string{"0", "-2", "abc", "999"} {
		code, body := usageRequest(t, r, "/api/v1/usage/monthly?months="+bad)
		if code != http.StatusBadRequest {
			t.Fatalf("months=%s: status %d, want 400 (%v)", bad, code, body)
		}
	}
}

func TestMonthlyUsageStoreErrorIs500(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) }
	r := setupUsageRouter(&fakeMonthlyUsageStore{err: errFakeUsageStore}, now)
	code, _ := usageRequest(t, r, "/api/v1/usage/monthly")
	if code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", code)
	}
}

var errFakeUsageStore = errFakeUsageStoreT{}

type errFakeUsageStoreT struct{}

func (errFakeUsageStoreT) Error() string { return "fake store error" }
