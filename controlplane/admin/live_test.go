//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// fakeLive implements LiveInfo for route tests.
type fakeLive struct {
	detail   QueryDetail
	detailOK bool
}

func (f *fakeLive) RunningQueries() []QueryStatus { return nil }
func (f *fakeLive) QueryDetailForPID(pid int32) (QueryDetail, bool) {
	if !f.detailOK || pid != f.detail.PID {
		return QueryDetail{}, false
	}
	return f.detail, true
}
func (f *fakeLive) WorkerFleet() ([]FleetStat, error)            { return nil, nil }
func (f *fakeLive) ControlPlaneInstances() ([]CPInstance, error) { return nil, nil }
func (f *fakeLive) KillSession(pid int32) error                  { return nil }

func liveTestRouter(live LiveInfo) *gin.Engine {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	registerLiveAPI(e.Group("/"), live)
	return e
}

func TestQueryDetailRoute(t *testing.T) {
	live := &fakeLive{
		detailOK: true,
		detail: QueryDetail{
			Org:      "acme",
			User:     "bob",
			PID:      42,
			WorkerID: 7,
			State:    "active",
			Query:    "SELECT 1",
		},
	}
	r := liveTestRouter(live)

	// Hit: returns the detail JSON.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/queries/42", nil))
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

	// Miss: pid not owned by this replica → 404.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/queries/99", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown pid, got %d", w.Code)
	}

	// Bad pid → 400.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/queries/notanint", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-numeric pid, got %d", w.Code)
	}
}
