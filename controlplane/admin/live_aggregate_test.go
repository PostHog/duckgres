//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
)

// fakeLiveInfo returns a fixed local view.
type fakeLiveInfo struct {
	queries  []QueryStatus
	sessions []SessionStatus
	orgStats []OrgStatus
	detail   *QueryDetail // local detail for QueryDetailForPID (nil = not owned here)
}

func (f *fakeLiveInfo) RunningQueries() []QueryStatus { return f.queries }
func (f *fakeLiveInfo) QueryDetailForPID(pid int32) (QueryDetail, bool) {
	if f.detail != nil && f.detail.PID == pid {
		return *f.detail, true
	}
	return QueryDetail{}, false
}
func (f *fakeLiveInfo) WorkerFleet() ([]FleetStat, error)            { return nil, nil }
func (f *fakeLiveInfo) ControlPlaneInstances() ([]CPInstance, error) { return nil, nil }
func (f *fakeLiveInfo) KillSession(int32) error                      { return nil }

func (f *fakeLiveInfo) AllOrgStats() []OrgStatus            { return f.orgStats }
func (f *fakeLiveInfo) AllWorkerStatuses() []WorkerStatus   { return nil }
func (f *fakeLiveInfo) AllSessionStatuses() []SessionStatus { return f.sessions }

// fakePeerFetcher returns canned peer bodies and counts how many times it ran
// (to prove ?scope=local does NOT fan out).
type fakePeerFetcher struct {
	byPath map[string][][]byte
	calls  int32
}

func (f *fakePeerFetcher) FetchPeers(_ context.Context, path string) ([][]byte, int) {
	atomic.AddInt32(&f.calls, 1)
	b := f.byPath[path]
	return b, len(b)
}

func TestQueriesAggregateAcrossCPs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &fakeLiveInfo{queries: []QueryStatus{{Org: "a", PID: 1, WorkerID: 10}}}
	peerBody, _ := json.Marshal(map[string]any{"queries": []QueryStatus{{Org: "b", PID: 2, WorkerID: 20}}})
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/queries": {peerBody}}}

	r := gin.New()
	registerLiveAPI(r.Group("/api/v1"), local, fetcher)

	// Default: merged (local + 1 peer).
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/queries", nil))
	var got struct {
		Queries    []QueryStatus `json:"queries"`
		Responders int           `json:"cp_responders"`
		Total      int           `json:"cp_total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Queries) != 2 {
		t.Fatalf("merged queries = %d, want 2 (local + peer): %+v", len(got.Queries), got.Queries)
	}
	if got.Responders != 2 || got.Total != 2 {
		t.Fatalf("coverage = %d/%d, want 2/2", got.Responders, got.Total)
	}

	// scope=local: local only, fetcher NOT called.
	fetcher.calls = 0
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/queries?scope=local", nil))
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if len(got.Queries) != 1 {
		t.Fatalf("scope=local queries = %d, want 1 (local only)", len(got.Queries))
	}
	if atomic.LoadInt32(&fetcher.calls) != 0 {
		t.Fatalf("scope=local must NOT fan out, but fetcher ran %d times", fetcher.calls)
	}
}

func TestSessionsAggregateAcrossCPs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &fakeLiveInfo{sessions: []SessionStatus{{PID: 1, Org: "a", WorkerID: 10}}}
	peerBody, _ := json.Marshal([]SessionStatus{{PID: 2, Org: "b", WorkerID: 20}})
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/sessions": {peerBody}}}

	r := gin.New()
	registerAPIWithStore(r.Group("/api/v1"), nil, local, fetcher)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil))
	var sessions []SessionStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &sessions); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("merged sessions = %d, want 2 (local + peer): %+v", len(sessions), sessions)
	}

	// scope=local: local only.
	fetcher.calls = 0
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sessions?scope=local", nil))
	_ = json.Unmarshal(rec.Body.Bytes(), &sessions)
	if len(sessions) != 1 || atomic.LoadInt32(&fetcher.calls) != 0 {
		t.Fatalf("scope=local sessions = %d (want 1), fetcher calls = %d (want 0)", len(sessions), fetcher.calls)
	}
}

func TestPeerFetcherInterfaceSatisfied(t *testing.T) {
	var _ PeerFetcher = (*fakePeerFetcher)(nil)
}

// /status merges per-org stats across CPs: active_sessions summed, orgs unioned.
func TestStatusAggregateAcrossCPs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &fakeLiveInfo{orgStats: []OrgStatus{{Name: "a", ActiveSessions: 1, MaxWorkers: 5}}}
	peerBody, _ := json.Marshal(ClusterStatus{Orgs: []OrgStatus{
		{Name: "a", ActiveSessions: 1, MaxWorkers: 5},
		{Name: "b", ActiveSessions: 2, MaxWorkers: 0},
	}})
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/status": {peerBody}}}

	r := gin.New()
	registerAPIWithStore(r.Group("/api/v1"), nil, local, fetcher)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))

	var cs ClusterStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &cs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cs.TotalSessions != 4 { // a:1+1, b:2
		t.Fatalf("total_sessions = %d, want 4", cs.TotalSessions)
	}
	if cs.TotalOrgs != 2 {
		t.Fatalf("total_orgs = %d, want 2 (a,b)", cs.TotalOrgs)
	}
	var aSessions int
	for _, o := range cs.Orgs {
		if o.Name == "a" {
			aSessions = o.ActiveSessions
		}
	}
	if aSessions != 2 {
		t.Fatalf("org a active_sessions = %d, want 2 (summed across CPs)", aSessions)
	}
}

// A peer that returns garbage is skipped, not allowed to corrupt the result.
func TestAggregateSkipsBadPeer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &fakeLiveInfo{sessions: []SessionStatus{{PID: 1, WorkerID: 10}}}
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{
		"/api/v1/sessions": {[]byte("not json"), []byte(`{"unexpected":true}`)},
	}}
	r := gin.New()
	registerAPIWithStore(r.Group("/api/v1"), nil, local, fetcher)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil))

	var sessions []SessionStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &sessions); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("bad peers must be skipped; got %d sessions, want 1 (local only)", len(sessions))
	}
}

// A worker that appears on both local and a peer (handover window / self-loop)
// is deduped by worker id — the merge is idempotent.
func TestAggregateDedupesByWorker(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &fakeLiveInfo{sessions: []SessionStatus{{PID: 1, WorkerID: 10, Org: "a"}}}
	peerBody, _ := json.Marshal([]SessionStatus{{PID: 99, WorkerID: 10, Org: "a"}}) // same worker
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/sessions": {peerBody}}}
	r := gin.New()
	registerAPIWithStore(r.Group("/api/v1"), nil, local, fetcher)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil))

	var sessions []SessionStatus
	_ = json.Unmarshal(rec.Body.Bytes(), &sessions)
	if len(sessions) != 1 {
		t.Fatalf("duplicate worker not deduped; got %d sessions, want 1", len(sessions))
	}
}
