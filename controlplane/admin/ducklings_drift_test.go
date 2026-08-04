//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeDriftStore struct {
	warehouses []configstore.ManagedWarehouse
}

func (s *fakeDriftStore) ListWarehouses() ([]configstore.ManagedWarehouse, error) {
	out := make([]configstore.ManagedWarehouse, len(s.warehouses))
	copy(out, s.warehouses)
	return out, nil
}

type fakeCRStatus struct {
	present bool
	ready   bool
}

type fakeDucklingChecker struct {
	statuses map[string]fakeCRStatus
	names    []string
	checked  []string
}

func (c *fakeDucklingChecker) CRStatus(_ context.Context, name string) (present, ready bool, err error) {
	c.checked = append(c.checked, name)
	status := c.statuses[name]
	return status.present, status.ready, nil
}

func (c *fakeDucklingChecker) ListCRNames(_ context.Context) ([]string, error) {
	out := make([]string, len(c.names))
	copy(out, c.names)
	return out, nil
}

type driftResponse struct {
	Available bool         `json:"available"`
	Checked   int          `json:"checked"`
	Entries   []driftEntry `json:"entries"`
}

func runDriftCheck(t *testing.T, warehouses []configstore.ManagedWarehouse, checker *fakeDucklingChecker) driftResponse {
	t.Helper()

	gin.SetMode(gin.TestMode)
	h := &driftHandler{
		store:   &fakeDriftStore{warehouses: warehouses},
		checker: checker,
	}
	r := gin.New()
	r.GET("/drift", h.findDrift)

	req := httptest.NewRequest(http.MethodGet, "/drift", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp driftResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v\nbody: %s", err, rec.Body.String())
	}
	return resp
}

func TestDucklingsDriftIgnoresDeletedWarehouseWithMissingCR(t *testing.T) {
	checker := &fakeDucklingChecker{statuses: map[string]fakeCRStatus{}}
	resp := runDriftCheck(t, []configstore.ManagedWarehouse{
		{
			OrgID:         "org-deleted",
			DucklingName:  "org-deleted-duckling",
			State:         configstore.ManagedWarehouseStateDeleted,
			StatusMessage: "Resources deleted",
		},
	}, checker)

	if len(resp.Entries) != 0 {
		t.Fatalf("expected no drift entries for deleted warehouse with missing CR, got %+v", resp.Entries)
	}
	if len(checker.checked) != 0 {
		t.Fatalf("deleted warehouse should not be checked directly, checked %v", checker.checked)
	}
}

func TestDucklingsDriftReportsLiveCRForDeletedWarehouseAsOrphan(t *testing.T) {
	const ducklingName = "org-deleted-duckling"
	resp := runDriftCheck(t, []configstore.ManagedWarehouse{
		{
			OrgID:        "org-deleted",
			DucklingName: ducklingName,
			State:        configstore.ManagedWarehouseStateDeleted,
		},
	}, &fakeDucklingChecker{
		statuses: map[string]fakeCRStatus{},
		names:    []string{ducklingName},
	})

	if len(resp.Entries) != 1 {
		t.Fatalf("expected one orphan entry, got %+v", resp.Entries)
	}
	entry := resp.Entries[0]
	if entry.Issue != "orphan" {
		t.Fatalf("issue = %q, want orphan; entry=%+v", entry.Issue, entry)
	}
	if entry.Org != "" {
		t.Fatalf("orphan org = %q, want empty", entry.Org)
	}
	if entry.DucklingName != ducklingName {
		t.Fatalf("duckling_name = %q, want %q", entry.DucklingName, ducklingName)
	}
}

func TestDucklingsDriftReportsReadyWarehouseWithMissingCR(t *testing.T) {
	const ducklingName = "org-ready-duckling"
	resp := runDriftCheck(t, []configstore.ManagedWarehouse{
		{
			OrgID:        "org-ready",
			DucklingName: ducklingName,
			State:        configstore.ManagedWarehouseStateReady,
		},
	}, &fakeDucklingChecker{statuses: map[string]fakeCRStatus{}})

	if len(resp.Entries) != 1 {
		t.Fatalf("expected one missing entry, got %+v", resp.Entries)
	}
	entry := resp.Entries[0]
	if entry.Issue != "missing" {
		t.Fatalf("issue = %q, want missing; entry=%+v", entry.Issue, entry)
	}
	if entry.Org != "org-ready" {
		t.Fatalf("org = %q, want org-ready", entry.Org)
	}
	if entry.DucklingName != ducklingName {
		t.Fatalf("duckling_name = %q, want %q", entry.DucklingName, ducklingName)
	}
}
