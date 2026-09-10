//go:build linux || darwin

package configstore_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// The org detail page's usage charts aggregate the retained usage ledger as the
// monthly view, but per UTC day and SCOPED TO ONE ORG. These tests pin the
// scoping (no cross-org bleed), the day split, the per-team split, and the
// window cutoff against the real migrated schema.
func TestAggregateScanUsageDailyPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	seedOrg(t, store, "globex")
	if err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('acme', 5, 'team_5', TRUE, now(), now()),
		       ('acme', 6, 'team_6', TRUE, now(), now())`).Error; err != nil {
		t.Fatalf("seed teams: %v", err)
	}

	day := func(n int) time.Time { return time.Date(2026, 8, n, 12, 0, 0, 0, time.UTC) }
	deltas := []scanUsageFixture{
		// Same day, same team, two query outcomes → one summed day row.
		{OrgID: "acme", TeamID: 5, State: "FINISHED", CompletedAt: day(3), BytesScanned: 600},
		{OrgID: "acme", TeamID: 5, State: "FAILED", CompletedAt: day(3), BytesScanned: 60},
		// Different team same day → separate row.
		{OrgID: "acme", TeamID: 6, State: "FINISHED", CompletedAt: day(3), BytesScanned: 30},
		// Different day, same team → separate row.
		{OrgID: "acme", TeamID: 5, State: "FINISHED", CompletedAt: day(4), BytesScanned: 900},
		// Another org entirely → must NOT appear in acme's result.
		{OrgID: "globex", TeamID: 9, State: "FINISHED", CompletedAt: day(3), BytesScanned: 5000},
	}
	seedScanUsage(t, store, deltas)

	rows, err := store.AggregateScanUsageDaily("acme", day(1))
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 daily rows for acme (globex must not leak), got %d: %+v", len(rows), rows)
	}
	type key struct {
		date string
		team int64
	}
	byKey := map[key]configstore.DailyScanUsageRow{}
	for _, r := range rows {
		byKey[key{r.Date, r.TeamID}] = r
	}
	d3t5, ok := byKey[key{"2026-08-03", 5}]
	if !ok || d3t5.BytesScanned.String() != "660" {
		t.Fatalf("day3/team5 row wrong (outcomes not summed): %+v", d3t5)
	}
	if d3t5.SchemaName == nil || *d3t5.SchemaName != "team_5" {
		t.Fatalf("day3/team5 schema not joined: %+v", d3t5)
	}
	if r, ok := byKey[key{"2026-08-03", 6}]; !ok || r.BytesScanned.String() != "30" {
		t.Fatalf("day3/team6 row wrong: %+v", r)
	}
	if r, ok := byKey[key{"2026-08-04", 5}]; !ok || r.BytesScanned.String() != "900" {
		t.Fatalf("day4/team5 row wrong: %+v", r)
	}

	// Window cutoff: from day(4) drops the day-3 rows.
	rows, err = store.AggregateScanUsageDaily("acme", day(4))
	if err != nil {
		t.Fatalf("aggregate narrow window: %v", err)
	}
	if len(rows) != 1 || rows[0].Date != "2026-08-04" {
		t.Fatalf("window cutoff wrong: %+v", rows)
	}

	// An org with no usage gets an empty result, not an error.
	rows, err = store.AggregateScanUsageDaily("no-such-org", day(1))
	if err != nil || len(rows) != 0 {
		t.Fatalf("unknown org: rows=%+v err=%v", rows, err)
	}
}

func TestAggregateStorageUsageDailyPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	seedOrg(t, store, "globex")
	if err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('acme', 5, 'team_5', TRUE, now(), now())`).Error; err != nil {
		t.Fatalf("seed team: %v", err)
	}

	day := func(n int) time.Time { return time.Date(2026, 8, n, 12, 0, 0, 0, time.UTC) }
	const gibHour = int64(1) << 30 * 3600
	for _, s := range []struct {
		org    string
		bucket time.Time
		bytes  int64
	}{
		{"acme", day(3), gibHour},
		{"acme", day(3), gibHour}, // same-day second sample must sum
		{"acme", day(4), gibHour},
		{"globex", day(3), 10 * gibHour},
	} {
		team := int64(5)
		if s.org == "globex" {
			team = 9
		}
		if err := store.UpsertStorageSample(s.org, team, s.bucket, s.bytes); err != nil {
			t.Fatalf("upsert sample: %v", err)
		}
	}

	rows, err := store.AggregateStorageUsageDaily("acme", day(1))
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 daily rows for acme, got %d: %+v", len(rows), rows)
	}
	if rows[0].Date != "2026-08-03" || rows[0].GiBSeconds.String() != "7200" {
		t.Fatalf("day3 row wrong (samples not summed): %+v", rows[0])
	}
	if rows[0].SchemaName == nil || *rows[0].SchemaName != "team_5" {
		t.Fatalf("schema not joined: %+v", rows[0])
	}
	if rows[1].Date != "2026-08-04" || rows[1].GiBSeconds.String() != "3600" {
		t.Fatalf("day4 row wrong: %+v", rows[1])
	}
}

// Insert through the real migrated schema to test aggregation independently of attribution.
type scanUsageFixture struct {
	OrgID        string
	TeamID       int64
	State        string
	ErrorCode    string
	CompletedAt  time.Time
	BytesScanned int64
}

func seedScanUsage(t *testing.T, store *configstore.ConfigStore, records []scanUsageFixture) {
	t.Helper()
	for i, record := range records {
		err := store.DB().Exec(`INSERT INTO duckgres_trino_query_usage
   (cluster_id,query_id,principal,org_id,team_id,source,state,error_code,completed_at,
    physical_input_bytes,processed_input_bytes,statistics_complete,trino_version)
   VALUES ('test', ?, ?, ?, ?, '', ?, ?, ?, ?, 0, true, 'test')`,
			fmt.Sprintf("query-%d", i), record.OrgID, record.OrgID, record.TeamID, record.State, record.ErrorCode,
			record.CompletedAt, record.BytesScanned).Error
		if err != nil {
			t.Fatalf("seed query usage: %v", err)
		}
	}
}
func TestAggregateScanUsageRetainsLargeFailedQueryTotalsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	seedScanUsage(t, store, []scanUsageFixture{
		{OrgID: "retired-org", State: "FAILED", CompletedAt: from, BytesScanned: 9223372036854775807},
		{OrgID: "retired-org", State: "FAILED", ErrorCode: "USER_CANCELED", CompletedAt: from, BytesScanned: 9223372036854775807},
	})
	daily, err := store.AggregateScanUsageDaily("retired-org", from)
	if err != nil || len(daily) != 1 || daily[0].BytesScanned.String() != "18446744073709551614" {
		t.Fatalf("daily history or precision: %+v, %v", daily, err)
	}
	monthly, err := store.AggregateScanUsageMonthly(from)
	if err != nil || len(monthly) != 1 || monthly[0].BytesScanned.String() != "18446744073709551614" {
		t.Fatalf("monthly history or precision: %+v, %v", monthly, err)
	}
}
