//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// fakeLiveInfo returns a fixed local view.
type fakeLiveInfo struct {
	queries  []QueryStatus
	sessions []SessionStatus
	orgStats []OrgStatus
	detail   *QueryDetail // local detail for QueryDetailForWorkerID (nil = not owned here)

	killedPerUser int            // returned by KillUserSessions (local kill count)
	killUserCalls []killUserCall // recorded (org, user) of each KillUserSessions
}

type killUserCall struct{ org, user string }

func (f *fakeLiveInfo) RunningQueries() []QueryStatus { return f.queries }
func (f *fakeLiveInfo) QueryDetailForWorkerID(wid int) (QueryDetail, bool) {
	if f.detail != nil && f.detail.WorkerID == wid {
		return *f.detail, true
	}
	return QueryDetail{}, false
}
func (f *fakeLiveInfo) WorkerFleet() ([]FleetStat, error)            { return nil, nil }
func (f *fakeLiveInfo) ControlPlaneInstances() ([]CPInstance, error) { return nil, nil }
func (f *fakeLiveInfo) KillSession(int32) error                      { return nil }
func (f *fakeLiveInfo) KillUserSessions(org, user string) int {
	f.killUserCalls = append(f.killUserCalls, killUserCall{org, user})
	return f.killedPerUser
}

func (f *fakeLiveInfo) AllOrgStats() []OrgStatus            { return f.orgStats }
func (f *fakeLiveInfo) AllWorkerStatuses() []WorkerStatus   { return nil }
func (f *fakeLiveInfo) AllSessionStatuses() []SessionStatus { return f.sessions }

// fakeUserAdmin records disable/enable + reload calls for the kill-switch tests.
type fakeUserAdmin struct {
	mu       sync.Mutex
	disabled map[string]bool // "org/user" -> disabled
	reloads  int
	notFound bool // when true, SetOrgUserDisabled returns ErrOrgUserNotFound
}

func (f *fakeUserAdmin) SetOrgUserDisabled(org, user string, disabled bool) error {
	if f.notFound {
		return configstore.ErrOrgUserNotFound
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.disabled == nil {
		f.disabled = map[string]bool{}
	}
	f.disabled[org+"/"+user] = disabled
	return nil
}

func (f *fakeUserAdmin) ReloadSnapshot() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reloads++
	return nil
}

func (f *fakeUserAdmin) isDisabled(org, user string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.disabled[org+"/"+user]
}

// fakePeerFetcher returns canned peer bodies and counts how many times it ran
// (to prove ?scope=local does NOT fan out). It serves GET (byPath) and POST
// (postByPath) fan-outs separately so a test can assert which verb was used.
type fakePeerFetcher struct {
	byPath     map[string][][]byte
	postByPath map[string][][]byte
	calls      int32
	postCalls  int32
	mu         sync.Mutex
	postPaths  []string
}

func (f *fakePeerFetcher) FetchPeers(_ context.Context, path string) ([][]byte, int) {
	atomic.AddInt32(&f.calls, 1)
	b := f.byPath[path]
	return b, len(b)
}

func (f *fakePeerFetcher) PostPeers(_ context.Context, path string) ([][]byte, int) {
	atomic.AddInt32(&f.postCalls, 1)
	f.mu.Lock()
	f.postPaths = append(f.postPaths, path)
	f.mu.Unlock()
	b := f.postByPath[path]
	return b, len(b)
}

func (f *fakePeerFetcher) callCount() int32     { return atomic.LoadInt32(&f.calls) }
func (f *fakePeerFetcher) postCallCount() int32 { return atomic.LoadInt32(&f.postCalls) }

func TestQueriesAggregateAcrossCPs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	local := &fakeLiveInfo{queries: []QueryStatus{{Org: "a", PID: 1, WorkerID: 10}}}
	peerBody, _ := json.Marshal(map[string]any{"queries": []QueryStatus{{Org: "b", PID: 2, WorkerID: 20}}})
	fetcher := &fakePeerFetcher{byPath: map[string][][]byte{"/api/v1/queries": {peerBody}}}

	r := gin.New()
	registerLiveAPI(r.Group("/api/v1"), local, fetcher, nil)

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
