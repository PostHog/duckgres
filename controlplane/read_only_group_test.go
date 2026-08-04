//go:build kubernetes

package controlplane

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

// stubProvisioningStore satisfies provisioning.Store with zero values so
// registerReadOnlyGroup's REAL routes can be exercised for auth behavior.
type stubProvisioningStore struct{}

func (stubProvisioningStore) GetManagedWarehouse(string) (*configstore.ManagedWarehouse, error) {
	return nil, nil
}
func (stubProvisioningStore) GetOrg(string) (*configstore.Org, error)       { return nil, nil }
func (stubProvisioningStore) Provision(provisioning.ProvisionRequest) error { return nil }
func (stubProvisioningStore) CreatePendingWarehouse(string, string, *configstore.ManagedWarehouse) error {
	return nil
}
func (stubProvisioningStore) CreateOrgUser(string, string, string) error         { return nil }
func (stubProvisioningStore) UpdateOrgUserPassword(string, string, string) error { return nil }
func (stubProvisioningStore) SetWarehouseDeleting(string, configstore.ManagedWarehouseProvisioningState) error {
	return nil
}
func (stubProvisioningStore) IsDatabaseNameAvailable(string) (bool, error) { return true, nil }
func (stubProvisioningStore) ListOrgTeams(string) ([]configstore.OrgTeam, error) {
	return nil, nil
}
func (stubProvisioningStore) UpsertOrgTeam(string, configstore.OrgTeamUpsert) (*configstore.OrgTeam, error) {
	return nil, nil
}
func (stubProvisioningStore) DeleteOrgTeam(string, int64) error {
	return nil
}
func (stubProvisioningStore) ListWarehousesByStates([]configstore.ManagedWarehouseProvisioningState) ([]configstore.ManagedWarehouse, error) {
	return nil, nil
}
func (stubProvisioningStore) ListOrgTeamsByOrgIDs([]string) ([]configstore.OrgTeam, error) {
	return nil, nil
}
func (stubProvisioningStore) LatestConfigChange() (time.Time, error) { return time.Time{}, nil }

// TestReadOnlyGroupTopology pins the surface the discovery credential can
// reach, against the REAL wiring multitenant.go uses (registerReadOnlyGroup
// is the only mount point for discovery routes). Two assertions:
//
//  1. The group registers EXACTLY the two discovery GETs — a new route
//     added to the group shows up here and forces a deliberate decision.
//  2. The auth matrix on those real routes: discovery and admin tokens
//     pass, junk and empty fail. (Cross-surface rejection — discovery
//     token on admin routes — is pinned in admin/dashboard_test.go; a
//     duplicate mount of a discovery path on the admin group would panic
//     at startup, so drift in that direction is fail-loud already.)
func TestReadOnlyGroupTopology(t *testing.T) {
	gin.SetMode(gin.TestMode)
	adminTokens := admin.NewTokenSet("admin-secret", nil)
	readOnlyTokens := admin.NewTokenSet("read-only-secret", nil)

	engine := gin.New()
	registerReadOnlyGroup(engine, readOnlyTokens, adminTokens, stubProvisioningStore{})

	var got []string
	for _, r := range engine.Routes() {
		got = append(got, r.Method+" "+r.Path)
	}
	sort.Strings(got)
	want := []string{
		"GET /api/v1/warehouse-team-ids",
		"GET /api/v1/warehouses",
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("discovery group routes = %v, want exactly %v — a new route on this group extends what the discovery credential can reach; move it or update this test deliberately", got, want)
	}

	serve := func(path, secret string) int {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if secret != "" {
			req.Header.Set("X-Duckgres-Internal-Secret", secret)
		}
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)
		return rec.Code
	}
	for _, path := range []string{"/api/v1/warehouses", "/api/v1/warehouse-team-ids"} {
		if code := serve(path, "read-only-secret"); code != http.StatusOK {
			t.Errorf("%s with discovery token: %d, want 200", path, code)
		}
		if code := serve(path, "admin-secret"); code != http.StatusOK {
			t.Errorf("%s with admin token: %d, want 200", path, code)
		}
		if code := serve(path, "junk"); code != http.StatusUnauthorized {
			t.Errorf("%s with junk token: %d, want 401", path, code)
		}
		if code := serve(path, ""); code != http.StatusUnauthorized {
			t.Errorf("%s with no token: %d, want 401", path, code)
		}
	}
}

func TestValidateDistinctReadOnlySecret(t *testing.T) {
	cases := []struct {
		name               string
		discovery          string
		discoveryFallbacks []string
		internal           string
		internalFallbacks  []string
		wantErr            bool
	}{
		{"distinct values ok", "d", nil, "i", nil, false},
		{"unset discovery ok", "", nil, "i", []string{"i-old"}, false},
		{"discovery equals internal", "same", nil, "same", nil, true},
		{"discovery equals internal fallback", "i-old", nil, "i", []string{"i-old"}, true},
		{"discovery fallback equals internal", "d", []string{"i"}, "i", nil, true},
		{"discovery fallback equals internal fallback", "d", []string{"i-old"}, "i", []string{"i-old"}, true},
		{"empty strings never collide", "", []string{""}, "", nil, false},
	}
	for _, tc := range cases {
		err := validateDistinctReadOnlySecret(tc.discovery, tc.discoveryFallbacks, tc.internal, tc.internalFallbacks)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err = %v, wantErr = %v", tc.name, err, tc.wantErr)
		}
	}
}
