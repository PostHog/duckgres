package duckdbservice

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCommitStatsExporterDelta(t *testing.T) {
	e := newCommitStatsExporter()

	steps := []struct {
		name     string
		catalog  string
		stat     string
		value    int64
		expected int64
	}{
		{"first observation counts in full", "lake_a", "attempts", 5, 5},
		{"monotonic increase yields diff", "lake_a", "attempts", 12, 7},
		{"unchanged yields zero", "lake_a", "attempts", 12, 0},
		{"decrease treats current value as delta", "lake_a", "attempts", 3, 3},
		{"recovers after decrease", "lake_a", "attempts", 10, 7},
		{"negative current value yields zero and keeps baseline", "lake_a", "attempts", -4, 0},
		{"post-glitch value counts only the increase over the kept baseline", "lake_a", "attempts", 11, 1},
		{"stats tracked independently", "lake_a", "successes", 4, 4},
		{"catalogs tracked independently", "lake_b", "attempts", 9, 9},
	}

	for _, step := range steps {
		if got := e.delta(step.catalog, step.stat, step.value); got != step.expected {
			t.Errorf("%s: delta(%q, %q, %d) = %d, want %d",
				step.name, step.catalog, step.stat, step.value, got, step.expected)
		}
	}
}

func TestCommitStatCounterMapping(t *testing.T) {
	tests := []struct {
		stat    string
		counter *prometheus.CounterVec
		cause   string
		ok      bool
	}{
		{"attempts", ducklakeCommitAttemptsTotal, "", true},
		{"successes", ducklakeCommitSuccessesTotal, "", true},
		{"retries_exhausted", ducklakeCommitRetriesExhaustedTotal, "", true},
		{"nonretryable_errors", ducklakeCommitNonretryableErrorsTotal, "", true},
		{"backoff_ms", ducklakeCommitBackoffMsTotal, "", true},
		{"total_commit_ms", ducklakeCommitDurationMsTotal, "", true},
		{"conflicts.primary_key", ducklakeCommitConflictsTotal, "primary_key", true},
		{"conflicts.unique", ducklakeCommitConflictsTotal, "unique", true},
		{"conflicts.conflict", ducklakeCommitConflictsTotal, "conflict", true},
		{"conflicts.concurrent", ducklakeCommitConflictsTotal, "concurrent", true},
		// Forward compatibility: a new conflict cause from a newer extension
		// is exported under its own cause label rather than dropped.
		{"conflicts.future_cause", ducklakeCommitConflictsTotal, "future_cause", true},
		{"conflicts.", nil, "", false},
		{"unknown_stat", nil, "", false},
		{"", nil, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.stat, func(t *testing.T) {
			counter, cause, ok := commitStatCounter(tt.stat)
			if ok != tt.ok {
				t.Fatalf("commitStatCounter(%q) ok = %v, want %v", tt.stat, ok, tt.ok)
			}
			if counter != tt.counter {
				t.Errorf("commitStatCounter(%q) returned unexpected counter", tt.stat)
			}
			if cause != tt.cause {
				t.Errorf("commitStatCounter(%q) cause = %q, want %q", tt.stat, cause, tt.cause)
			}
		})
	}
}

func TestCommitStatsExporterRecord(t *testing.T) {
	// Package-level counters live in the default registry, so this test uses
	// a catalog label no other test touches and asserts absolute values.
	const catalog = "record_test_lake"
	e := newCommitStatsExporter()

	e.record([]commitStatRow{
		{Catalog: catalog, Stat: "attempts", Value: 10},
		{Catalog: catalog, Stat: "successes", Value: 8},
		{Catalog: catalog, Stat: "total_commit_ms", Value: 1500},
		{Catalog: catalog, Stat: "conflicts.primary_key", Value: 2},
		{Catalog: catalog, Stat: "not_a_known_stat", Value: 99},
	})
	e.record([]commitStatRow{
		{Catalog: catalog, Stat: "attempts", Value: 14},
		{Catalog: catalog, Stat: "successes", Value: 8},
		{Catalog: catalog, Stat: "total_commit_ms", Value: 400}, // went DOWN → current value is the delta
		{Catalog: catalog, Stat: "conflicts.primary_key", Value: 3},
	})

	assertCounter := func(name string, c prometheus.Counter, want float64) {
		t.Helper()
		if got := testutil.ToFloat64(c); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	assertCounter("attempts", ducklakeCommitAttemptsTotal.WithLabelValues(catalog), 14)
	assertCounter("successes", ducklakeCommitSuccessesTotal.WithLabelValues(catalog), 8)
	assertCounter("duration_ms", ducklakeCommitDurationMsTotal.WithLabelValues(catalog), 1900)
	assertCounter("conflicts.primary_key", ducklakeCommitConflictsTotal.WithLabelValues(catalog, "primary_key"), 3)
}

func TestProbeCommitStatsFunction(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stock DuckDB (like every currently-released ducklake extension binary)
	// does not provide ducklake_commit_stats().
	available, err := probeCommitStatsFunction(ctx, db)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if available {
		t.Fatal("probe reported ducklake_commit_stats() available on stock DuckDB")
	}

	// A table macro of the same name is indistinguishable from the extension's
	// table function for both the probe and the scrape query.
	if _, err := db.ExecContext(ctx, `CREATE MACRO ducklake_commit_stats() AS TABLE
		SELECT * FROM (VALUES
			('ducklake', 'attempts', 7::BIGINT),
			('ducklake', 'conflicts.concurrent', 1::BIGINT)
		) AS t(catalog, stat, value)`); err != nil {
		t.Fatalf("create macro: %v", err)
	}

	available, err = probeCommitStatsFunction(ctx, db)
	if err != nil {
		t.Fatalf("probe after macro: %v", err)
	}
	if !available {
		t.Fatal("probe did not find ducklake_commit_stats()")
	}

	rows, err := queryCommitStats(ctx, db)
	if err != nil {
		t.Fatalf("query commit stats: %v", err)
	}
	want := []commitStatRow{
		{Catalog: "ducklake", Stat: "attempts", Value: 7},
		{Catalog: "ducklake", Stat: "conflicts.concurrent", Value: 1},
	}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(rows), len(want), rows)
	}
	for i, w := range want {
		if rows[i] != w {
			t.Errorf("row %d = %+v, want %+v", i, rows[i], w)
		}
	}
}

func TestScrapeCommitStatsDegradesWhenFunctionAbsent(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	p := &SessionPool{controlDB: db}
	e := newCommitStatsExporter()

	queries := 0
	e.query = func(ctx context.Context, db *sql.DB) ([]commitStatRow, error) {
		queries++
		return nil, nil
	}
	probes := 0
	realProbe := e.probe
	e.probe = func(ctx context.Context, db *sql.DB) (bool, error) {
		probes++
		return realProbe(ctx, db)
	}

	p.scrapeCommitStats(e)
	p.scrapeCommitStats(e)

	if probes != 1 {
		t.Errorf("probe ran %d times, want 1 (once per engine)", probes)
	}
	if queries != 0 {
		t.Errorf("query ran %d times, want 0 when the function is absent", queries)
	}
}

func TestScrapeCommitStatsPollsWhenFunctionPresent(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	p := &SessionPool{controlDB: db}
	e := newCommitStatsExporter()
	e.probe = func(ctx context.Context, db *sql.DB) (bool, error) { return true, nil }
	e.query = func(ctx context.Context, db *sql.DB) ([]commitStatRow, error) {
		return []commitStatRow{{Catalog: "scrape_test_lake", Stat: "attempts", Value: 3}}, nil
	}

	p.scrapeCommitStats(e)
	p.scrapeCommitStats(e)

	got := testutil.ToFloat64(ducklakeCommitAttemptsTotal.WithLabelValues("scrape_test_lake"))
	if got != 3 {
		t.Errorf("attempts counter = %v, want 3 (cumulative value unchanged across polls)", got)
	}
}

func TestScrapeCommitStatsSkipsWithoutDB(t *testing.T) {
	p := &SessionPool{}
	e := newCommitStatsExporter()
	e.probe = func(ctx context.Context, db *sql.DB) (bool, error) {
		t.Fatal("probe must not run without a DB")
		return false, nil
	}
	p.scrapeCommitStats(e)
}
