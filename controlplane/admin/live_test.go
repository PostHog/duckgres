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

	// Hit: returns the detail JSON.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/42", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", w.Code, w.Body.String())
	}
	var got QueryDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.PID != 42 || got.Query != "SELECT 1" || got.Org != "acme" {
		t.Fatalf("unexpected body: %+v", got)
	}

	// Miss (no fetcher): pid not owned by this replica → 404.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/99", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown pid, got %d", w.Code)
	}

	// Bad pid → 400.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/notanint", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-numeric pid, got %d", w.Code)
	}
}

// TestQueryDetailFansOutAcrossCPs proves /queries/:pid scatter-gathers: when the
// serving replica doesn't own the pid, it fetches peers and returns the one
// owner's body; and a scope=local (peer) call never fans out (recursion guard).
func TestQueryDetailFansOutAcrossCPs(t *testing.T) {
	// Local replica owns nothing; the detail lives on a peer at pid 77.
	local := &fakeLiveInfo{}
	peerBody, _ := json.Marshal(QueryDetail{Org: "b", PID: 77, WorkerID: 20, Query: "SELECT 2"})
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/queries/77": {peerBody}}}
	r := liveTestRouter(local, fetcher)

	// Default scope: not owned locally → fan out → peer's detail returned.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/77", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from peer fan-out, got %d (%s)", w.Code, w.Body.String())
	}
	var got QueryDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.PID != 77 || got.Query != "SELECT 2" || got.Org != "b" {
		t.Fatalf("unexpected fanned-out body: %+v", got)
	}

	// scope=local: recursion guard — no fan-out even though a fetcher exists, so
	// a pid this replica doesn't own is a clean 404 (a peer answering us).
	before := fetcher.calls
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/queries/77?scope=local", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 under scope=local, got %d", w.Code)
	}
	if fetcher.calls != before {
		t.Fatalf("scope=local must not fan out, but FetchPeers was called (%d → %d)", before, fetcher.calls)
	}
}
