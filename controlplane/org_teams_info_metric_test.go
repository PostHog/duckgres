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
duckgres_org_info{duckling="org-multi",org="org-multi",schema_name="team_11",team_id="11"} 1
# HELP duckgres_org_teams_info Org/team/duckling identity mapping, one series per team (constant 1). Join key material for dashboards — see org_teams_info_metric.go for the safe join shapes
# TYPE duckgres_org_teams_info gauge
duckgres_org_teams_info{duckling="",org="org-deleted",schema_name="team_9",team_id="9"} 1
duckgres_org_teams_info{duckling="",org="org-legacy",schema_name="team_7",team_id="7"} 1
duckgres_org_teams_info{duckling="4dc8564dbd8210652f4097f7c50f67cf",org="4dc8564d-bd82-1065-2f40-97f7c50f67cf",schema_name="team_2",team_id="2"} 1
duckgres_org_teams_info{duckling="org-multi",org="org-multi",schema_name="team_10",team_id="10"} 1
duckgres_org_teams_info{duckling="org-multi",org="org-multi",schema_name="team_11",team_id="11"} 1
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
