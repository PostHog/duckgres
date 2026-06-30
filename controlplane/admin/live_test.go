//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func liveTestRouter(live LiveInfo, fetcher PeerFetcher) *gin.Engine {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	registerLiveAPI(e.Group("/api/v1"), live, fetcher)
	return e
}

func TestQueryDetailRoute(t *testing.T) {
	live := &fakeLiveInfo{detail: &QueryDetail{
		Org:      "acme",
		User:     "bob",
		PID:      42,
		WorkerID: 7,
		State:    "active",
		Query:    "SELECT 1",
	}}
	r := liveTestRouter(live, nil)

	// Hit (addressed by cluster-unique worker id): returns the detail JSON.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/by-worker/7", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var got QueryDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.WorkerID != 7 || got.Query != "SELECT 1" || got.Org != "acme" {
		t.Fatalf("unexpected body: %+v", got)
	}

	// Miss (no fetcher): worker not owned by this replica → 404.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/by-worker/99", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown worker, got %d", w.Code)
	}

	// Bad worker id → 400.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/by-worker/notanint", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-numeric worker id, got %d", w.Code)
	}
}

// TestQueryDetailFansOutAcrossCPs proves /queries/by-worker/:wid scatter-gathers:
// when the serving replica doesn't own the worker, it fetches peers and returns
// the one owner's body; and a scope=local (peer) call never fans out.
func TestQueryDetailFansOutAcrossCPs(t *testing.T) {
	local := &fakeLiveInfo{} // owns nothing
	peerBody, _ := json.Marshal(QueryDetail{Org: "b", PID: 77, WorkerID: 20, Query: "SELECT 2"})
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/queries/by-worker/20": {peerBody}}}
	r := liveTestRouter(local, fetcher)

	// Default scope: not owned locally → fan out → peer's detail returned.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/by-worker/20", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from peer fan-out, got %d (%s)", w.Code, w.Body.String())
	}
	var got QueryDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.WorkerID != 20 || got.Query != "SELECT 2" || got.Org != "b" {
		t.Fatalf("unexpected fanned-out body: %+v", got)
	}

	// scope=local: recursion guard — no fan-out even with a fetcher present.
	before := fetcher.callCount()
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/by-worker/20?scope=local", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 under scope=local, got %d", w.Code)
	}
	if fetcher.callCount() != before {
		t.Fatalf("scope=local must not fan out, but FetchPeers was called (%d → %d)", before, fetcher.callCount())
	}
}

// TestQueryDetailWorkerIDDistinguishesCollidingPIDs is the regression for the
// addressing bug: PIDs are per-org (every stack starts at 1000), so two orgs can
// share a pid. Addressing detail by the cluster-unique worker id must return the
// RIGHT query even when two live queries share a pid.
func TestQueryDetailWorkerIDDistinguishesCollidingPIDs(t *testing.T) {
	// Local replica owns worker 11 (org-A, pid 1001). A peer owns worker 22
	// (org-B, SAME pid 1001) — only the worker id disambiguates them.
	local := &fakeLiveInfo{detail: &QueryDetail{Org: "a", PID: 1001, WorkerID: 11, Query: "SELECT 'a'"}}
	peerBody, _ := json.Marshal(QueryDetail{Org: "b", PID: 1001, WorkerID: 22, Query: "SELECT 'b'"})
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/queries/by-worker/22": {peerBody}}}
	r := liveTestRouter(local, fetcher)

	// Worker 11 → org-A's query, locally (no fan-out needed).
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/by-worker/11", nil))
	var a QueryDetail
	_ = json.Unmarshal(w.Body.Bytes(), &a)
	if w.Code != http.StatusOK || a.Org != "a" || a.WorkerID != 11 || a.Query != "SELECT 'a'" {
		t.Fatalf("worker 11 should resolve to org-a's query, got %d %+v", w.Code, a)
	}

	// Worker 22 (same pid 1001, different org) → org-B's query via fan-out, NOT
	// the local org-A query that happens to share the pid.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/by-worker/22", nil))
	var b QueryDetail
	_ = json.Unmarshal(w.Body.Bytes(), &b)
	if w.Code != http.StatusOK || b.Org != "b" || b.WorkerID != 22 || b.Query != "SELECT 'b'" {
		t.Fatalf("worker 22 should resolve to org-b's query despite the shared pid, got %d %+v", w.Code, b)
	}
}
