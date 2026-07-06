package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// fakeBillingStore implements billingUsageStore + computeGCStore in memory.
type fakeBillingStore struct {
	mu       sync.Mutex
	cursor   time.Time
	hasAck   bool
	rows     []configstore.ComputeUsageRow
	aggLow   time.Time
	aggHigh  time.Time
	ackedTo  time.Time
	deleted  int64
	gcCutoff time.Time
	gcDrop   int64
	failWith error
}

func (f *fakeBillingStore) AggregateComputeUsage(low, high time.Time) ([]configstore.ComputeUsageRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return nil, f.failWith
	}
	f.aggLow, f.aggHigh = low, high
	return f.rows, nil
}

func (f *fakeBillingStore) ComputeBillingCursor() (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return time.Time{}, false, f.failWith
	}
	return f.cursor, f.hasAck, nil
}

func (f *fakeBillingStore) AckComputeUsage(high time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return 0, f.failWith
	}
	if high.After(f.cursor) {
		f.cursor, f.hasAck = high, true
	}
	f.ackedTo = high
	return f.deleted, nil
}

func (f *fakeBillingStore) GCComputeUsage(olderThan time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return 0, f.failWith
	}
	f.gcCutoff = olderThan
	return f.gcDrop, nil
}

func newBillingTestRouter(store *fakeBillingStore, now time.Time) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := &billingAPIHandler{store: store, now: func() time.Time { return now }}
	grp := r.Group("/api/v1")
	grp.GET("/billing/usage", h.getUsage)
	grp.POST("/billing/ack", h.postAck)
	return r
}

// TestRegisterBillingAPIGatesBothRoutes exercises the real registration used
// by multitenant.go: both routes are mounted and BOTH pass through the
// admin-gating middleware (a viewer/billing-secret mixup must never reach the
// ack mutation or raw usage).
func TestRegisterBillingAPIGatesBothRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	gateHits := 0
	deny := func(c *gin.Context) {
		gateHits++
		c.AbortWithStatus(http.StatusForbidden)
	}
	registerBillingAPI(r.Group("/api/v1"), &fakeBillingStore{}, deny)

	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/api/v1/billing/usage", nil),
		httptest.NewRequest(http.MethodPost, "/api/v1/billing/ack", bytes.NewReader([]byte(`{"watermark_high":"2026-07-01T12:39:00Z"}`))),
	} {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s %s = %d, want 403 from the admin gate", req.Method, req.URL.Path, rec.Code)
		}
	}
	if gateHits != 2 {
		t.Fatalf("admin gate ran %d times, want 2 (every billing route must be gated)", gateHits)
	}
}

type usageResponse struct {
	WatermarkLow  time.Time                     `json:"watermark_low"`
	WatermarkHigh time.Time                     `json:"watermark_high"`
	Usage         []configstore.ComputeUsageRow `json:"usage"`
}

func TestBillingUsageWindowAndRows(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 40, 47, 0, time.UTC)
	cursor := time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC)
	store := &fakeBillingStore{
		cursor: cursor, hasAck: true,
		rows: []configstore.ComputeUsageRow{{
			Date: "2026-07-01", OrgID: "org_abc", TeamID: "12345", QuerySource: "standard",
			CPU: "8", MemGiB: "16", CPUSeconds: 4800, MemorySeconds: 9600,
		}},
	}
	router := newBillingTestRouter(store, now)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/billing/usage", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var resp usageResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.WatermarkLow.Equal(cursor) {
		t.Fatalf("watermark_low = %v, want cursor %v", resp.WatermarkLow, cursor)
	}
	// Latest closed bucket at 12:40:47 with 60s width + 30s grace:
	// 12:40:47 − 90s = 12:39:17 → truncated to 12:39:00.
	wantHigh := time.Date(2026, 7, 1, 12, 39, 0, 0, time.UTC)
	if !resp.WatermarkHigh.Equal(wantHigh) {
		t.Fatalf("watermark_high = %v, want %v", resp.WatermarkHigh, wantHigh)
	}
	// The aggregate window must be exactly (cursor, high].
	if !store.aggLow.Equal(cursor) || !store.aggHigh.Equal(wantHigh) {
		t.Fatalf("aggregate window = (%v, %v], want (%v, %v]", store.aggLow, store.aggHigh, cursor, wantHigh)
	}
	if len(resp.Usage) != 1 || resp.Usage[0].OrgID != "org_abc" || resp.Usage[0].CPUSeconds != 4800 {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	// The exact-decimal sizes must serialize as unquoted JSON numbers.
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"cpu":8`)) || !bytes.Contains(rec.Body.Bytes(), []byte(`"mem_gib":16`)) {
		t.Fatalf("cpu/mem_gib not serialized as JSON numbers: %s", rec.Body.String())
	}
}

func TestBillingUsageNeverAckedServesEverything(t *testing.T) {
	// No cursor row yet → the window starts at the zero time (serves all
	// buffered history).
	now := time.Date(2026, 7, 1, 12, 40, 47, 0, time.UTC)
	store := &fakeBillingStore{}
	router := newBillingTestRouter(store, now)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/billing/usage", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if !store.aggLow.IsZero() {
		t.Fatalf("window low = %v, want zero time when never acked", store.aggLow)
	}
}

func TestBillingUsageEmptyWindowWhenCursorCurrent(t *testing.T) {
	// Cursor already at the latest closed bucket → nothing to serve; the
	// response must be a valid empty window with high == low, not an error.
	now := time.Date(2026, 7, 1, 12, 40, 47, 0, time.UTC)
	cursor := time.Date(2026, 7, 1, 12, 39, 0, 0, time.UTC)
	store := &fakeBillingStore{cursor: cursor, hasAck: true}
	router := newBillingTestRouter(store, now)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/billing/usage", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var resp usageResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.WatermarkHigh.Equal(resp.WatermarkLow) || !resp.WatermarkLow.Equal(cursor) {
		t.Fatalf("want empty window at cursor, got (%v, %v]", resp.WatermarkLow, resp.WatermarkHigh)
	}
	if len(resp.Usage) != 0 {
		t.Fatalf("usage = %+v, want empty", resp.Usage)
	}
	if !store.aggLow.IsZero() || !store.aggHigh.IsZero() {
		t.Fatal("aggregate must not run for an empty window")
	}
}

func TestBillingAckAdvancesAndIsBounded(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 40, 47, 0, time.UTC)
	store := &fakeBillingStore{deleted: 7}
	router := newBillingTestRouter(store, now)

	// A valid ack (≤ latest closed bucket) advances and reports deletions.
	body := []byte(`{"watermark_high":"2026-07-01T12:39:00Z"}`)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/ack", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if want := time.Date(2026, 7, 1, 12, 39, 0, 0, time.UTC); !store.ackedTo.Equal(want) {
		t.Fatalf("acked = %v, want %v", store.ackedTo, want)
	}
	var resp struct {
		Deleted int64 `json:"deleted"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Deleted != 7 {
		t.Fatalf("deleted = %d (err=%v), want 7", resp.Deleted, err)
	}

	// An ack beyond the latest closed bucket must be rejected — it could
	// delete buckets that were never served.
	store.ackedTo = time.Time{}
	body = []byte(`{"watermark_high":"2026-07-01T12:40:00Z"}`)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/ack", bytes.NewReader(body)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("future ack status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if !store.ackedTo.IsZero() {
		t.Fatal("future ack must not reach the store")
	}

	// Garbage body → 400.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/ack", bytes.NewReader([]byte(`{}`))))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty ack status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
}

func TestBillingStoreErrorsSurfaceAs500(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 40, 47, 0, time.UTC)
	store := &fakeBillingStore{failWith: errors.New("db down")}
	router := newBillingTestRouter(store, now)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/billing/usage", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("usage status = %d, want 500", rec.Code)
	}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/ack", bytes.NewReader([]byte(`{"watermark_high":"2026-07-01T12:39:00Z"}`))))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("ack status = %d, want 500", rec.Code)
	}
}

func TestComputeUsageGCRunsAndStops(t *testing.T) {
	store := &fakeBillingStore{gcDrop: 3}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { runComputeUsageGC(ctx, store); close(done) }()
	// The first tick runs immediately; wait for it, then cancel.
	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		got := store.gcCutoff
		store.mu.Unlock()
		if !got.IsZero() {
			if d := time.Since(got); d < computeUsageRetention-time.Minute || d > computeUsageRetention+time.Minute {
				t.Fatalf("gc cutoff %v not ~retention (30d) ago", got)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("gc first tick never ran")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("gc loop did not stop on cancel")
	}
}
