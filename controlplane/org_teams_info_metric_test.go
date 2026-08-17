//go:build kubernetes

package controlplane

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type stubSnapshotter struct{ snap *configstore.Snapshot }

func (s *stubSnapshotter) Snapshot() *configstore.Snapshot { return s.snap }

func TestOrgTeamsInfoCollector(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"org-deleting": {
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-deleting",
					State:        configstore.ManagedWarehouseStateDeleting,
				},
			},
			"org-failed": {
				Teams: []configstore.OrgTeamConfig{
					{TeamID: 12, SchemaName: "team_12", CreatedAt: t0},
				},
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-failed",
					State:        configstore.ManagedWarehouseStateFailed,
				},
			},
			"org-pending": {
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-pending",
					State:        configstore.ManagedWarehouseStatePending,
				},
			},
			"org-provisioning": {
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-provisioning",
					State:        configstore.ManagedWarehouseStateProvisioning,
				},
			},
			"org-resharding": {
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-resharding",
					State:        configstore.ManagedWarehouseStateResharding,
				},
			},
			"org-unknown": {
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-unknown",
					State:        configstore.ManagedWarehouseProvisioningState("unexpected"),
				},
			},
			// Legacy tenant: duckling name is the org id with hyphens
			// stripped — the case that makes the label non-derivable.
			"4dc8564d-bd82-1065-2f40-97f7c50f67cf": {
				Teams: []configstore.OrgTeamConfig{
					{TeamID: 2, SchemaName: "team_2", CreatedAt: t0},
				},
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "4dc8564dbd8210652f4097f7c50f67cf",
					State:        configstore.ManagedWarehouseStateReady,
				},
			},
			// Multi-team org: teams metric emits one series per team; org
			// metric emits ONE row carrying the OLDEST team (created
			// earlier wins even with a higher id — OldestTeam ordering).
			"org-multi": {
				Teams: []configstore.OrgTeamConfig{
					{TeamID: 10, SchemaName: "team_10", CreatedAt: t0.Add(time.Hour)},
					{TeamID: 11, SchemaName: "team_11", CreatedAt: t0},
				},
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-multi",
					State:        configstore.ManagedWarehouseStateReady,
				},
			},
			// No warehouse row: mapping still emitted, empty duckling.
			"org-legacy": {
				Teams: []configstore.OrgTeamConfig{
					{TeamID: 7, SchemaName: "team_7", CreatedAt: t0},
				},
			},
			// Deprovisioned: the warehouse row lingers at state=deleted
			// until an admin purge — must NOT point at torn-down infra.
			"org-deleted": {
				Teams: []configstore.OrgTeamConfig{
					{TeamID: 9, SchemaName: "team_9", CreatedAt: t0},
				},
				Warehouse: &configstore.ManagedWarehouseConfig{
					DucklingName: "org-deleted",
					State:        configstore.ManagedWarehouseStateDeleted,
				},
			},
		},
	}

	want := `# HELP duckgres_org_info Org/duckling identity mapping, one series per org with the oldest team as representative (constant 1). Safe one-side for group_left joins
# TYPE duckgres_org_info gauge
duckgres_org_info{duckling="",org="org-deleted",schema_name="team_9",team_id="9"} 1
duckgres_org_info{duckling="",org="org-legacy",schema_name="team_7",team_id="7"} 1
duckgres_org_info{duckling="4dc8564dbd8210652f4097f7c50f67cf",org="4dc8564d-bd82-1065-2f40-97f7c50f67cf",schema_name="team_2",team_id="2"} 1
duckgres_org_info{duckling="org-failed",org="org-failed",schema_name="team_12",team_id="12"} 1
duckgres_org_info{duckling="org-multi",org="org-multi",schema_name="team_11",team_id="11"} 1
# HELP duckgres_org_teams_info Org/team/duckling identity mapping, one series per team (constant 1). Join key material for dashboards — see org_teams_info_metric.go for the safe join shapes
# TYPE duckgres_org_teams_info gauge
duckgres_org_teams_info{duckling="",org="org-deleted",schema_name="team_9",team_id="9"} 1
duckgres_org_teams_info{duckling="",org="org-legacy",schema_name="team_7",team_id="7"} 1
duckgres_org_teams_info{duckling="4dc8564dbd8210652f4097f7c50f67cf",org="4dc8564d-bd82-1065-2f40-97f7c50f67cf",schema_name="team_2",team_id="2"} 1
duckgres_org_teams_info{duckling="org-failed",org="org-failed",schema_name="team_12",team_id="12"} 1
duckgres_org_teams_info{duckling="org-multi",org="org-multi",schema_name="team_10",team_id="10"} 1
duckgres_org_teams_info{duckling="org-multi",org="org-multi",schema_name="team_11",team_id="11"} 1
# HELP duckgres_managed_warehouse_state Managed warehouse lifecycle state, one series per live warehouse (constant 1)
# TYPE duckgres_managed_warehouse_state gauge
duckgres_managed_warehouse_state{duckling="4dc8564dbd8210652f4097f7c50f67cf",org="4dc8564d-bd82-1065-2f40-97f7c50f67cf",state="ready"} 1
duckgres_managed_warehouse_state{duckling="org-deleting",org="org-deleting",state="deleting"} 1
duckgres_managed_warehouse_state{duckling="org-failed",org="org-failed",state="failed"} 1
duckgres_managed_warehouse_state{duckling="org-multi",org="org-multi",state="ready"} 1
duckgres_managed_warehouse_state{duckling="org-pending",org="org-pending",state="pending"} 1
duckgres_managed_warehouse_state{duckling="org-provisioning",org="org-provisioning",state="provisioning"} 1
duckgres_managed_warehouse_state{duckling="org-resharding",org="org-resharding",state="resharding"} 1
duckgres_managed_warehouse_state{duckling="org-unknown",org="org-unknown",state="unknown"} 1
`
	c := newOrgTeamsInfoCollector(&stubSnapshotter{snap: snap})
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

func TestOrgTeamsInfoCollectorNilSnapshot(t *testing.T) {
	// Pre-first-load scrape (startup race): no series, no panic.
	c := newOrgTeamsInfoCollector(&stubSnapshotter{snap: nil})
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Fatalf("nil snapshot must emit nothing, got %d series", n)
	}
}
