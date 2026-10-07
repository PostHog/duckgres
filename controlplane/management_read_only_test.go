//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type fakeHandover struct {
	owner string
	err   error
	reads int
}

func (f *fakeHandover) lookup(context.Context) (string, error) {
	f.reads++
	return f.owner, f.err
}

func newTestWriteGate(f *fakeHandover, startupOwner string, override *bool) (*managementWriteGate, *time.Time) {
	now := time.Unix(1_700_000_000, 0)
	g := &managementWriteGate{
		override:  override,
		lookup:    f.lookup,
		ttl:       managementReadOnlyTTL,
		now:       func() time.Time { return now },
		owner:     startupOwner,
		checkedAt: now,
	}
	return g, &now
}

func TestManagementWriteGateFollowsTheMarkerWithinOneTTL(t *testing.T) {
	f := &fakeHandover{}
	g, now := newTestWriteGate(f, "", nil)
	if owner := g.readOnlyOwner(); owner != "" || f.reads != 0 {
		t.Fatalf("startup answer must be reused within the TTL: owner=%q reads=%d", owner, f.reads)
	}

	f.owner = "hogtower"
	*now = now.Add(managementReadOnlyTTL - time.Second)
	if owner := g.readOnlyOwner(); owner != "" || f.reads != 0 {
		t.Fatalf("the marker must not be re-read before the TTL: owner=%q reads=%d", owner, f.reads)
	}
	*now = now.Add(time.Second)
	if owner := g.readOnlyOwner(); owner != "hogtower" || f.reads != 1 {
		t.Fatalf("a recorded hand-over must take effect after one TTL: owner=%q reads=%d", owner, f.reads)
	}

	// A transient store error keeps the previous answer instead of
	// reopening writes, and is not retried before another TTL.
	f.owner, f.err = "", errors.New("connection refused")
	*now = now.Add(managementReadOnlyTTL)
	if owner := g.readOnlyOwner(); owner != "hogtower" {
		t.Fatalf("a failed read must keep read-only, got owner=%q", owner)
	}
	if g.readOnlyOwner(); f.reads != 2 {
		t.Fatalf("a failed read must not be retried within the TTL, reads=%d", f.reads)
	}

	// Deleting the marker hands the HTTP surface back without a restart.
	f.err = nil
	*now = now.Add(managementReadOnlyTTL)
	if owner := g.readOnlyOwner(); owner != "" {
		t.Fatalf("a removed hand-over must reopen writes, got owner=%q", owner)
	}
}

func TestManagementWriteGateOverride(t *testing.T) {
	yes, no := true, false

	f := &fakeHandover{}
	g, _ := newTestWriteGate(f, "", &yes)
	if owner := g.readOnlyOwner(); owner != defaultManagementOwner {
		t.Fatalf("forced read-only without a marker: owner=%q", owner)
	}
	g, _ = newTestWriteGate(f, "hogtower-eu", &yes)
	if owner := g.readOnlyOwner(); owner != "hogtower-eu" {
		t.Fatalf("forced read-only must name the recorded owner: owner=%q", owner)
	}

	g, now := newTestWriteGate(&fakeHandover{owner: "hogtower"}, "hogtower", &no)
	*now = now.Add(managementReadOnlyTTL)
	if owner := g.readOnlyOwner(); owner != "" {
		t.Fatalf("forced writable must ignore the marker: owner=%q", owner)
	}

	for value, want := range map[string]*bool{"": nil, "true": &yes, "0": &no, "maybe": nil} {
		t.Setenv(envManagementReadOnly, value)
		got := managementReadOnlyOverride()
		if (got == nil) != (want == nil) || (got != nil && *got != *want) {
			t.Errorf("%s=%q: got %v, want %v", envManagementReadOnly, value, got, want)
		}
	}
}

// TestManagementWriteGateRouteMatrix pins which routes of the authenticated
// /api/v1 group answer 409 after the hand-over. It mirrors every non-GET
// route registered there today (admin, provisioning, extras, Trino fleet,
// reshard, billing) plus a sample of reads. New write routes are refused by
// default; add them here when deciding otherwise.
func TestManagementWriteGateRouteMatrix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		method, route, path string
		open                bool
	}{
		// Provisioning API.
		{"POST", "/orgs/:id/provision", "/orgs/o/provision", false},
		{"POST", "/orgs/:id/deprovision", "/orgs/o/deprovision", false},
		{"POST", "/orgs/:id/reset-password", "/orgs/o/reset-password", false},
		{"POST", "/orgs/:id/trino", "/orgs/o/trino", false},
		{"DELETE", "/orgs/:id/trino", "/orgs/o/trino", false},
		{"POST", "/orgs/:id/teams", "/orgs/o/teams", false},
		{"DELETE", "/orgs/:id/teams/:team_id", "/orgs/o/teams/1", false},
		{"POST", "/orgs/:id/service-credentials", "/orgs/o/service-credentials", false},
		{"POST", "/orgs/:id/service-credentials/refresh", "/orgs/o/service-credentials/refresh", false},
		// Admin API.
		{"POST", "/orgs", "/orgs", false},
		{"PUT", "/orgs/:id", "/orgs/o", false},
		{"DELETE", "/orgs/:id", "/orgs/o", false},
		{"PUT", "/orgs/:id/warehouse", "/orgs/o/warehouse", false},
		{"PATCH", "/orgs/:id/warehouse/pinning", "/orgs/o/warehouse/pinning", false},
		{"POST", "/teams", "/teams", false},
		{"PUT", "/orgs/:id/teams/:team_id", "/orgs/o/teams/1", false},
		{"PUT", "/orgs/:id/teams/:team_id/project-reader", "/orgs/o/teams/1/project-reader", false},
		{"PUT", "/orgs/:id/teams/:team_id/project-user", "/orgs/o/teams/1/project-user", false},
		{"POST", "/users", "/users", false},
		{"PUT", "/orgs/:id/users/:username", "/orgs/o/users/u", false},
		{"DELETE", "/orgs/:id/users/:username", "/orgs/o/users/u", false},
		{"DELETE", "/orgs/:id/service-grants/:credential_id", "/orgs/o/service-grants/c", false},
		{"POST", "/operators", "/operators", false},
		{"DELETE", "/operators/:email", "/operators/a@b", false},
		{"POST", "/orgs/:id/users/:username/disable", "/orgs/o/users/u/disable", false},
		{"POST", "/orgs/:id/users/:username/enable", "/orgs/o/users/u/enable", false},
		{"POST", "/orgs/:id/users/:username/disable", "/orgs/o/users/u/disable?scope=local", true},
		{"POST", "/orgs/:id/users/:username/enable", "/orgs/o/users/u/enable?scope=local", true},
		// Trino placement and pool recovery (hogtower owns the pool).
		{"PUT", "/orgs/:id/trino/cell", "/orgs/o/trino/cell", false},
		{"POST", "/orgs/:id/trino/cell/move", "/orgs/o/trino/cell/move", false},
		{"POST", "/trino/instances/:id/recovery", "/trino/instances/i/recovery", false},
		// Runtime and duckgres-owned writes stay open.
		{"POST", "/internal/reload-snapshot", "/internal/reload-snapshot", true},
		{"POST", "/billing/ack", "/billing/ack", true},
		{"POST", "/orgs/:id/impersonate/query", "/orgs/o/impersonate/query", true},
		{"POST", "/sessions/:pid/cancel", "/sessions/1/cancel", true},
		{"POST", "/sessions/by-worker/:wid/cancel", "/sessions/by-worker/1/cancel", true},
		{"POST", "/orgs/:id/users/:username/kill", "/orgs/o/users/u/kill", true},
		{"POST", "/trino/queries/:id/kill", "/trino/queries/q/kill", true},
		{"DELETE", "/orgs/:id/users/:username/secrets/:name", "/orgs/o/users/u/secrets/s", true},
		{"POST", "/orgs/:id/reshard", "/orgs/o/reshard", true},
		{"POST", "/reshards/:opid/cancel", "/reshards/r/cancel", true},
		// Reads, including monitoring and status.
		{"GET", "/orgs", "/orgs", true},
		{"GET", "/orgs/:id/warehouse/status", "/orgs/o/warehouse/status", true},
		{"GET", "/orgs/:id/monitoring/snapshot", "/orgs/o/monitoring/snapshot", true},
		{"GET", "/teams", "/teams", true},
		{"HEAD", "/orgs", "/orgs", true},
	}

	for _, readOnly := range []bool{false, true} {
		f := &fakeHandover{}
		startup := ""
		if readOnly {
			startup = "hogtower"
		}
		g, _ := newTestWriteGate(f, startup, nil)
		engine := gin.New()
		api := engine.Group("/api/v1", g.Middleware())
		registered := map[string]bool{}
		for _, tc := range cases {
			key := tc.method + " " + tc.route
			if registered[key] {
				continue
			}
			registered[key] = true
			api.Handle(tc.method, tc.route, func(c *gin.Context) { c.Status(http.StatusNoContent) })
		}
		for _, tc := range cases {
			rec := httptest.NewRecorder()
			engine.ServeHTTP(rec, httptest.NewRequest(tc.method, "/api/v1"+tc.path, nil))
			want := http.StatusNoContent
			if readOnly && !tc.open {
				want = http.StatusConflict
			}
			if rec.Code != want {
				t.Errorf("read_only=%v %s %s: status %d, want %d", readOnly, tc.method, tc.path, rec.Code, want)
				continue
			}
			if want != http.StatusConflict {
				continue
			}
			var body struct {
				Error     string `json:"error"`
				ManagedBy string `json:"managed_by"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("409 body is not JSON: %v %q", err, rec.Body.String())
			}
			if body.Error != "managed by hogtower: this warehouse control plane is read-only" || body.ManagedBy != "hogtower" {
				t.Errorf("%s %s: unexpected 409 body %q", tc.method, tc.path, rec.Body.String())
			}
		}
		if f.reads != 0 {
			t.Errorf("read_only=%v: the startup answer must serve requests within the TTL, reads=%d", readOnly, f.reads)
		}
	}

	// Every allow-listed route must be in the matrix, so the allow-list
	// cannot silently drift from the routes it is meant to describe.
	inMatrix := map[string]bool{}
	for _, tc := range cases {
		inMatrix[tc.method+" /api/v1"+tc.route] = true
	}
	for key := range managementOperationalWrites {
		if !inMatrix[key] {
			t.Errorf("allow-listed %q is missing from the route matrix", key)
		}
	}
	for key := range managementLocalScopeWrites {
		if !inMatrix[key] || !strings.HasPrefix(key, "POST ") {
			t.Errorf("local-scope %q is missing from the route matrix", key)
		}
	}
}
