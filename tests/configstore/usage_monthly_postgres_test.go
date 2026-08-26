//go:build linux || darwin

package configstore_test

import (
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// The admin console's monthly usage page aggregates the SAME billing buffer the
// pull API serves serves, but grouped by UTC month per (org, team) instead of per
// day per full key. These tests pin that aggregation against the real
// migrated schema: worker-size and query_source splits of the same month must
// sum into one row, months must not bleed into each other, and the team
// schema name must join through when the team row exists.
func TestAggregateComputeUsageMonthlyPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	seedOrg(t, store, "globex")
	if err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('acme', 5, 'team_5', TRUE, now(), now())`).Error; err != nil {
		t.Fatalf("seed team: %v", err)
	}

	month := func(y int, m time.Month, day int) time.Time {
		return time.Date(y, m, day, 12, 0, 0, 0, time.UTC)
	}
	// Two keys (query sources) in August for acme/team 5 — must sum into ONE
	// monthly row. A second acme team-less key (team 0 — defensive) stays
	// separate. July and a different org must not leak in.
	deltas := []configstore.ComputeUsageDelta{
		{OrgID: "acme", TeamID: 5, QuerySource: "standard", Millicores: 2000, MiB: 8192, BucketStart: month(2026, 8, 3), CPUSeconds: 600, MemorySeconds: 1200},
		{OrgID: "acme", TeamID: 5, QuerySource: "endpoints", Millicores: 2000, MiB: 8192, BucketStart: month(2026, 8, 20), CPUSeconds: 300, MemorySeconds: 300},
		{OrgID: "acme", TeamID: 5, QuerySource: "standard", Millicores: 4000, MiB: 16384, BucketStart: month(2026, 8, 21), CPUSeconds: 100, MemorySeconds: 200},
		{OrgID: "acme", TeamID: 0, QuerySource: "standard", Millicores: 2000, MiB: 8192, BucketStart: month(2026, 8, 4), CPUSeconds: 50, MemorySeconds: 50},
		{OrgID: "acme", TeamID: 5, QuerySource: "standard", Millicores: 2000, MiB: 8192, BucketStart: month(2026, 7, 31), CPUSeconds: 900, MemorySeconds: 900},
		{OrgID: "globex", TeamID: 7, QuerySource: "standard", Millicores: 2000, MiB: 8192, BucketStart: month(2026, 8, 4), CPUSeconds: 70, MemorySeconds: 80},
	}
	if err := store.FlushComputeUsage(deltas); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Window opens at Aug 1: July is out.
	rows, err := store.AggregateComputeUsageMonthly(month(2026, 8, 1))
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 monthly rows, got %d: %+v", len(rows), rows)
	}
	byKey := map[[2]interface{}]configstore.MonthlyComputeUsageRow{}
	for _, r := range rows {
		if r.Month != "2026-08" {
			t.Fatalf("row outside window/month: %+v", r)
		}
		byKey[[2]interface{}{r.OrgID, r.TeamID}] = r
	}
	acme5, ok := byKey[[2]interface{}{"acme", int64(5)}]
	if !ok {
		t.Fatalf("missing acme/5 row: %+v", rows)
	}
	if acme5.CPUSeconds != 1000 || acme5.MemorySeconds != 1700 {
		t.Fatalf("acme/5 not summed across keys: %+v", acme5)
	}
	if acme5.SchemaName == nil || *acme5.SchemaName != "team_5" {
		t.Fatalf("acme/5 schema_name not joined: %+v", acme5)
	}
	acme0, ok := byKey[[2]interface{}{"acme", int64(0)}]
	if !ok || acme0.CPUSeconds != 50 {
		t.Fatalf("acme/0 row wrong: %+v (rows %+v)", acme0, rows)
	}
	if acme0.SchemaName != nil {
		t.Fatalf("team 0 must not join a schema name: %+v", acme0)
	}
	if g, ok := byKey[[2]interface{}{"globex", int64(7)}]; !ok || g.CPUSeconds != 70 || g.MemorySeconds != 80 {
		t.Fatalf("globex row wrong: %+v", g)
	}

	// A window covering July picks the July row up.
	rows, err = store.AggregateComputeUsageMonthly(month(2026, 7, 1))
	if err != nil {
		t.Fatalf("aggregate wide window: %v", err)
	}
	var sawJuly bool
	for _, r := range rows {
		if r.Month == "2026-07" {
			sawJuly = true
			if r.OrgID != "acme" || r.CPUSeconds != 900 {
				t.Fatalf("july row wrong: %+v", r)
			}
		}
	}
	if !sawJuly {
		t.Fatalf("july row missing from wide window: %+v", rows)
	}
}

func TestAggregateStorageUsageMonthlyPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	if err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('acme', 5, 'team_5', TRUE, now(), now())`).Error; err != nil {
		t.Fatalf("seed team: %v", err)
	}

	month := func(y int, m time.Month, day int) time.Time {
		return time.Date(y, m, day, 12, 0, 0, 0, time.UTC)
	}
	// Two August samples for acme/5 + one July sample. 1 GiB held for one hour
	// = 2^30 * 3600 byte-seconds = 3865470566400.
	const gibHour = int64(1) << 30 * 3600
	for _, s := range []struct {
		bucket time.Time
		bytes  int64
	}{
		{month(2026, 8, 3), gibHour},
		{month(2026, 8, 10), 2 * gibHour},
		{month(2026, 7, 15), gibHour},
	} {
		if err := store.UpsertStorageSample("acme", 5, s.bucket, s.bytes); err != nil {
			t.Fatalf("upsert sample: %v", err)
		}
	}

	rows, err := store.AggregateStorageUsageMonthly(month(2026, 8, 1))
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 monthly row, got %d: %+v", len(rows), rows)
	}
	r := rows[0]
	if r.Month != "2026-08" || r.OrgID != "acme" || r.TeamID != 5 {
		t.Fatalf("row identity wrong: %+v", r)
	}
	if r.SchemaName == nil || *r.SchemaName != "team_5" {
		t.Fatalf("schema_name not joined: %+v", r)
	}
	// 3 GiB-hours = 10800 GiB-seconds exactly.
	if r.GiBSeconds.String() != "10800" {
		t.Fatalf("gib_seconds = %s, want 10800", r.GiBSeconds)
	}
}
