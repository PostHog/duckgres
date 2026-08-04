package duckdbservice

// Polls the posthog ducklake extension's ducklake_commit_stats() table
// function and re-exports its counters as the
// duckgres_worker_ducklake_commit_* Prometheus metrics (metrics.go).
//
// The function contract (shared with the extension):
//   - Signature: ducklake_commit_stats() — no arguments.
//   - Columns: catalog VARCHAR, stat VARCHAR, value BIGINT.
//   - One row per (catalog, stat); values are process-global, cumulative, and
//     monotonic for the life of the process.
//   - Stat names: attempts, successes, retries_exhausted, nonretryable_errors,
//     backoff_ms, total_commit_ms, and conflicts.<cause> with cause one of
//     primary_key, unique, conflict, concurrent (an unknown cause from a newer
//     extension is exported under its own cause label rather than dropped).
//
// The function only exists in ducklake extension builds that expose the
// commit retry loop counters — no currently-released extension binary does —
// so availability is probed once per DuckDB engine and, when absent, polling
// is disabled with a single info log. Everything degrades gracefully with the
// pinned extension.

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	commitStatsInterval      = 30 * time.Second
	commitStatsScrapeTimeout = 5 * time.Second

	commitStatsProbeQuery = "SELECT 1 FROM duckdb_functions() WHERE function_name = 'ducklake_commit_stats' LIMIT 1"
	commitStatsQuery      = "SELECT catalog, stat, value FROM ducklake_commit_stats()"

	commitStatsConflictPrefix = "conflicts."
)

// commitStatRow is one (catalog, stat, value) row from ducklake_commit_stats().
type commitStatRow struct {
	Catalog string
	Stat    string
	Value   int64
}

type commitStatKey struct {
	catalog string
	stat    string
}

// commitStatsExporter converts the cumulative counters reported by
// ducklake_commit_stats() into Prometheus counter increments by tracking the
// last-seen value per (catalog, stat). It also caches the per-engine
// capability probe verdict. Only ever touched from the commitStatsLoop
// goroutine, so it needs no locking. The probe and query indirections default
// to the real SQL implementations; tests inject stubs.
type commitStatsExporter struct {
	lastSeen map[commitStatKey]int64

	// probedDB is the *sql.DB the capability probe last delivered a verdict
	// for; available is that verdict. A different DB (engine replaced) is
	// re-probed once.
	probedDB  *sql.DB
	available bool

	probe func(ctx context.Context, db *sql.DB) (bool, error)
	query func(ctx context.Context, db *sql.DB) ([]commitStatRow, error)
}

func newCommitStatsExporter() *commitStatsExporter {
	return &commitStatsExporter{
		lastSeen: make(map[commitStatKey]int64),
		probe:    probeCommitStatsFunction,
		query:    queryCommitStats,
	}
}

// delta returns the Prometheus counter increment for a newly observed
// cumulative value, updating the last-seen state. The values are monotonic
// per process and we poll the same process, so a decrease should never
// happen — but be defensive: a value below the last-seen one is treated as a
// fresh baseline and the current value is the delta. A negative delta is never
// returned (prometheus Counter.Add panics on negative values).
func (e *commitStatsExporter) delta(catalog, stat string, value int64) int64 {
	key := commitStatKey{catalog: catalog, stat: stat}
	if value < 0 {
		// Contract violation — emit nothing and keep the existing baseline
		// untouched: resetting it would make the next valid cumulative value
		// count in full, double-exporting everything already recorded.
		return 0
	}
	last := e.lastSeen[key]
	e.lastSeen[key] = value
	if delta := value - last; delta >= 0 {
		return delta
	}
	return value
}

// record applies one poll's rows to the Prometheus counters. Unknown stat
// names are ignored (a newer extension may add stats this build doesn't map).
// A zero delta still touches the series so every known series exists from the
// first successful poll onward.
func (e *commitStatsExporter) record(rows []commitStatRow) {
	for _, row := range rows {
		counter, cause, ok := commitStatCounter(row.Stat)
		if !ok {
			continue
		}
		delta := float64(e.delta(row.Catalog, row.Stat, row.Value))
		if cause != "" {
			counter.WithLabelValues(row.Catalog, cause).Add(delta)
		} else {
			counter.WithLabelValues(row.Catalog).Add(delta)
		}
	}
}

// commitStatCounter maps a ducklake_commit_stats() stat name to its Prometheus
// counter. For conflicts.* stats the second return is the cause label value;
// it is empty for all other stats. ok is false for stat names this build does
// not know how to export.
func commitStatCounter(stat string) (counter *prometheus.CounterVec, cause string, ok bool) {
	if c, found := strings.CutPrefix(stat, commitStatsConflictPrefix); found {
		if c == "" {
			return nil, "", false
		}
		return ducklakeCommitConflictsTotal, c, true
	}
	switch stat {
	case "attempts":
		return ducklakeCommitAttemptsTotal, "", true
	case "successes":
		return ducklakeCommitSuccessesTotal, "", true
	case "retries_exhausted":
		return ducklakeCommitRetriesExhaustedTotal, "", true
	case "nonretryable_errors":
		return ducklakeCommitNonretryableErrorsTotal, "", true
	case "backoff_ms":
		return ducklakeCommitBackoffMsTotal, "", true
	case "total_commit_ms":
		return ducklakeCommitDurationMsTotal, "", true
	}
	return nil, "", false
}

// ensureAvailable answers whether ducklake_commit_stats() exists on this
// engine, probing at most once per *sql.DB. Absence is the common case today
// (no released ducklake extension binary provides the function), so it is
// logged once at info and polling stays disabled for that engine. A probe
// error records no verdict — the next tick retries.
func (e *commitStatsExporter) ensureAvailable(ctx context.Context, db *sql.DB) bool {
	if db == e.probedDB {
		return e.available
	}
	available, err := e.probe(ctx, db)
	if err != nil {
		slog.Debug("DuckLake commit-stats capability probe failed; will retry.", "error", err)
		return false
	}
	e.probedDB = db
	e.available = available
	if available {
		slog.Info("ducklake_commit_stats() detected; exporting DuckLake commit-loop metrics.")
	} else {
		slog.Info("ducklake_commit_stats() not provided by the loaded ducklake extension; DuckLake commit-loop metrics disabled.")
	}
	return available
}

func probeCommitStatsFunction(ctx context.Context, db *sql.DB) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, commitStatsProbeQuery).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func queryCommitStats(ctx context.Context, db *sql.DB) ([]commitStatRow, error) {
	rows, err := db.QueryContext(ctx, commitStatsQuery)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []commitStatRow
	for rows.Next() {
		var row commitStatRow
		if err := rows.Scan(&row.Catalog, &row.Stat, &row.Value); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// commitStatsLoop periodically polls ducklake_commit_stats() and re-exports
// the counters as Prometheus metrics. Same lifecycle as metadataMetricsLoop:
// started by NewDuckDBService, stops on pool close, and every tick is
// best-effort — a failure is logged and the next tick retries; the loop never
// holds pool locks across a query and never blocks user queries.
func (p *SessionPool) commitStatsLoop() {
	exporter := newCommitStatsExporter()
	ticker := time.NewTicker(commitStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.scrapeCommitStats(exporter)
		case <-p.stopCh:
			return
		}
	}
}

// scrapeCommitStats runs one poll. Prefer controlDB (side connection sharing
// the same DuckDB instance with its own pool) so a long-running user query on
// the main DB does not queue the scrape; fall back to the activation DB only
// when controlDB has not been wired (test paths). Unlike scrapeMetadataMetrics
// this does not require an activation: the counters are process-global to the
// ducklake extension and already carry the catalog name.
func (p *SessionPool) scrapeCommitStats(e *commitStatsExporter) {
	p.mu.RLock()
	db := p.controlDB
	p.mu.RUnlock()
	if db == nil {
		if act := p.currentActivation(); act != nil {
			db = act.db
		}
	}
	if db == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), commitStatsScrapeTimeout)
	defer cancel()

	if !e.ensureAvailable(ctx, db) {
		return
	}
	rows, err := e.query(ctx, db)
	if err != nil {
		slog.Debug("Failed to poll ducklake_commit_stats(); will retry.", "error", err)
		return
	}
	e.record(rows)
}
