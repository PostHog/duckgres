//go:build kubernetes

package controlplane

import (
	"errors"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Org ↔ team ↔ duckling identity mapping as info-style metrics (constant
// value 1), so dashboards can render human meaning onto resources that only
// carry duckling names — e.g. the composition-stamped maintenance CronJobs
// (cronjob = "<duckling>-compaction").
//
// TWO shapes, because PromQL join direction dictates which is usable where
// (both verified against the real engine — a multi-team org on the "one"
// side of group_left is a hard many-to-many error that poisons the whole
// panel, and NOT expressible around with topk):
//
//   - duckgres_org_info: ONE series per org, team_id/schema_name from the
//     org's oldest team (OldestTeam — the same representative-team semantics
//     usage buckets stamp). Safe as the "one" side of a kube-left join:
//
//     max by (duckling) (label_replace(
//       kube_cronjob_status_last_successful_time{cronjob=~".+-compaction"},
//       "duckling", "$1", "cronjob", "(.+)-compaction"))
//     * on (duckling) group_left(team_id, schema_name)
//       duckgres_org_info{duckling!=""}
//
//   - duckgres_org_teams_info: one series per (org, team) — the complete,
//     truthful mapping. NEVER put it on the "one" side of a duckling join;
//     use it as the many/left side instead:
//
//     duckgres_org_teams_info{duckling!=""}
//     * on (duckling) group_left() max by (duckling) (label_replace(...))
//
// Every duckling-keyed join MUST filter {duckling!=""} on the info side and
// anchor the kube side's regex (cronjob=~".+-compaction"): label_replace
// keeps non-matching series with duckling="", which otherwise cross-matches
// the warehouse-less orgs' rows — silently attributing an unrelated
// cronjob's value to them.
//
// Labels use `org` per the repo's per-org metric convention
// (duckgres_org_sessions_active etc.), so `on (org)` decoration of existing
// metrics needs no label_replace shim. The duckling label is the k8s join
// key and comes from the warehouse row's DucklingName — NOT derivable from
// org id (legacy tenants stripped uuid hyphens; newer ones keep them).
// Orgs without a live warehouse (none, or state=deleted — the row lingers
// after deprovision until an admin purge) emit duckling="" so the org/team
// mapping stays complete without pointing at torn-down infra.
//
// Read from the in-memory config snapshot at scrape time: zero database
// access, always as fresh as the snapshot poller; series for removed
// orgs/teams vanish on the next scrape by construction. Duplicate label
// sets (which would fail the ENTIRE /metrics scrape) are structurally
// impossible: Orgs is keyed by org id and (org_id, team_id) is the
// duckgres_org_teams primary key.

type orgTeamsSnapshotter interface {
	Snapshot() *configstore.Snapshot
}

type orgTeamsInfoCollector struct {
	store    orgTeamsSnapshotter
	teamDesc *prometheus.Desc
	orgDesc  *prometheus.Desc
}

func newOrgTeamsInfoCollector(store orgTeamsSnapshotter) *orgTeamsInfoCollector {
	labels := []string{"org", "duckling", "team_id", "schema_name"}
	return &orgTeamsInfoCollector{
		store: store,
		teamDesc: prometheus.NewDesc(
			"duckgres_org_teams_info",
			"Org/team/duckling identity mapping, one series per team (constant 1). Join key material for dashboards — see org_teams_info_metric.go for the safe join shapes",
			labels, nil,
		),
		orgDesc: prometheus.NewDesc(
			"duckgres_org_info",
			"Org/duckling identity mapping, one series per org with the oldest team as representative (constant 1). Safe one-side for group_left joins",
			labels, nil,
		),
	}
}

func (c *orgTeamsInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.teamDesc
	ch <- c.orgDesc
}

func (c *orgTeamsInfoCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.store.Snapshot()
	if snap == nil {
		return
	}
	for orgID, org := range snap.Orgs {
		if org == nil {
			continue
		}
		duckling := ""
		if org.Warehouse != nil && org.Warehouse.State != configstore.ManagedWarehouseStateDeleted {
			duckling = org.Warehouse.DucklingName
		}
		for _, t := range org.Teams {
			ch <- prometheus.MustNewConstMetric(
				c.teamDesc, prometheus.GaugeValue, 1,
				orgID, duckling, strconv.FormatInt(t.TeamID, 10), t.SchemaName,
			)
		}
		if oldest := org.OldestTeam(); oldest != nil {
			ch <- prometheus.MustNewConstMetric(
				c.orgDesc, prometheus.GaugeValue, 1,
				orgID, duckling, strconv.FormatInt(oldest.TeamID, 10), oldest.SchemaName,
			)
		}
	}
}

// registerOrgTeamsInfoMetric registers the collector on the default registry.
// First registration wins: an AlreadyRegistered error is ignored (defensive
// only — SetupMultiTenant has a single call site per process), anything else
// is a programming error and panics like promauto would.
func registerOrgTeamsInfoMetric(store orgTeamsSnapshotter) {
	if err := prometheus.Register(newOrgTeamsInfoCollector(store)); err != nil {
		are := prometheus.AlreadyRegisteredError{}
		if !errors.As(err, &are) {
			panic(err)
		}
	}
}
