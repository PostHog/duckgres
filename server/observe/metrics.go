package observe

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// connectionsGauge tracks the number of currently open client connections.
// Use IncrementOpenConnections / DecrementOpenConnections to mutate it.
var connectionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_connections_open",
	Help: "Number of currently open client connections",
})

// retainedBindBytesGauge tracks Bind storage currently retained by open portals
// across the process: the wire body plus compact parameter/format metadata. It
// deliberately has no connection, portal, user, or org label so it remains
// safe to scrape at high connection cardinality.
var retainedBindBytesGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_retained_bind_bytes",
	Help: "Aggregate Bind storage bytes retained by open portals.",
})

// openPortalsGauge tracks installed portal shells across the process. It has no
// labels because portal-level labels would have unbounded cardinality.
var openPortalsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_open_portals",
	Help: "Aggregate number of installed portal shells.",
})

// portalPayloadReleasesTotal counts portal payload releases by a fixed
// lifecycle reason. Callers must use only bounded protocol-lifecycle reasons.
var portalPayloadReleasesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_portal_payload_releases_total",
	Help: "Total portal payload releases by lifecycle reason.",
}, []string{"reason"})

// portalBudgetRejectionsTotal counts Bind admissions rejected by one of the
// fixed portal-budget reasons. It intentionally has no tenant/user labels.
var portalBudgetRejectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_portal_budget_rejections_total",
	Help: "Total Bind admissions rejected by portal budget reason.",
}, []string{"reason"})

var portalPayloadReleaseReasons = map[string]struct{}{
	"terminal_success": {},
	"terminal_failure": {},
	"close_portal":     {},
	"close_statement":  {},
	"unnamed_rebind":   {},
	"idle_sync":        {},
	"tx_end":           {},
	"simple_query":     {},
	"connection_close": {},
}

var portalBudgetRejectionReasons = map[string]struct{}{
	"retained_bytes": {},
	"open_portals":   {},
}

// IncrementOpenConnections increments the open connections gauge.
func IncrementOpenConnections() { connectionsGauge.Inc() }

// DecrementOpenConnections decrements the open connections gauge.
func DecrementOpenConnections() { connectionsGauge.Dec() }

// AddRetainedBindBytes applies a per-connection retained-payload delta to the
// process-wide gauge. A delta, rather than a Set operation, keeps concurrent
// client connections from overwriting each other's contribution.
func AddRetainedBindBytes(delta int) { retainedBindBytesGauge.Add(float64(delta)) }

// AddOpenPortals applies a per-connection portal-count delta to the
// process-wide gauge. Callers pair every successful installation with exactly
// one matching negative delta when the shell is removed.
func AddOpenPortals(delta int) { openPortalsGauge.Add(float64(delta)) }

// IncPortalPayloadRelease records one payload release using a fixed lifecycle
// reason (for example terminal_success or close_portal).
func IncPortalPayloadRelease(reason string) {
	if _, ok := portalPayloadReleaseReasons[reason]; !ok {
		return
	}
	portalPayloadReleasesTotal.WithLabelValues(reason).Inc()
}

// IncPortalBudgetRejection records one rejected Bind using a fixed budget
// reason (retained_bytes or open_portals).
func IncPortalBudgetRejection(reason string) {
	if _, ok := portalBudgetRejectionReasons[reason]; !ok {
		return
	}
	portalBudgetRejectionsTotal.WithLabelValues(reason).Inc()
}

// connectionDurationHistogram observes the full lifetime of a client
// connection (accept → disconnect) in seconds, labelled by org. It complements
// the duckgres_connections_open gauge: integrating that gauge over time
// approximates total connection-time, but a coarse scrape interval undercounts
// connections shorter than the scrape window. This histogram records every
// connection's true lifetime, so `_sum` gives exact total connection-seconds
// (per org) with no scrape bias and the buckets give the lifetime distribution.
// Org is empty for single-tenant/standalone connections. Buckets span 1s
// (health probes / short clients) to 24h (long-lived pooled connections).
var connectionDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_connection_duration_seconds",
	Help:    "Client connection lifetime in seconds (accept to disconnect)",
	Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 18000, 36000, 86400},
}, []string{"org"})

// ObserveConnectionDuration records one completed connection's lifetime.
func ObserveConnectionDuration(org string, seconds float64) {
	connectionDurationHistogram.WithLabelValues(org).Observe(seconds)
}

// S3BytesReadTotal counts bytes read from S3 by DuckDB, labeled by org.
// Bumped from EnrichSpanWithProfiling when DuckDB reports total_bytes_read
// in its profiling output.
var S3BytesReadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_s3_bytes_read_total",
	Help: "Total bytes read from S3 by DuckDB",
}, []string{"org"})

// ScanWallSecondsHistogram observes estimated wall-clock scan time per query.
var ScanWallSecondsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_scan_wall_seconds",
	Help:    "Estimated wall-clock scan time per query",
	Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
}, []string{"org"})

// ScanRowsPerSecondHistogram observes scan throughput (estimated wall-clock
// rows scanned per second). High values (>1e10) indicate buffer pool / cache
// hits; the bucket range spans S3 cold reads (1e5-1e8) through in-memory
// cache hits (1e9-1e12).
var ScanRowsPerSecondHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_scan_rows_per_second",
	Help:    "Scan throughput: estimated wall-clock rows scanned per second. High values (>1e10) indicate buffer pool/cache hits.",
	Buckets: []float64{1e5, 5e5, 1e6, 5e6, 1e7, 5e7, 1e8, 5e8, 1e9, 1e10, 1e11, 1e12},
}, []string{"org"})

// PostgresScanSecondsHistogram observes thread-time spent in postgres_scan
// operators per query. This is the DuckDB-side view of time spent in the
// DuckLake metadata DB roundtrips — distinguishable from time spent on S3
// data reads (which dominate ScanWallSecondsHistogram). A regression here
// is the first signal of metadata-DB pressure (slow metadata Postgres, conn-pool
// starvation, pgbouncer queueing).
var PostgresScanSecondsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_postgres_scan_seconds",
	Help:    "Time spent in postgres_scan operators per query (DuckLake metadata DB roundtrips).",
	Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
}, []string{"org"})

// MetadataPoolMaxConnections is the configured ceiling for the
// postgres_scanner connection pool that backs DuckLake metadata access.
// Pulled from DuckDB's pg_pool_max_connections setting periodically; emitted
// per-org so we can correlate pool size with conflict-rate / latency.
var MetadataPoolMaxConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_ducklake_metadata_pool_max_connections",
	Help: "Configured postgres_scanner pool ceiling for DuckLake metadata (pg_pool_max_connections).",
}, []string{"org"})

var queryLogBufferedEntries = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_query_log_buffered_entries",
	Help: "Current number of query-log entries buffered in memory before storage flush.",
})

var queryLogEnqueuedEntriesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_query_log_enqueued_entries_total",
	Help: "Total number of query-log entries accepted into the in-memory buffer.",
})

var queryLogFlushedEntriesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_query_log_flushed_entries_total",
	Help: "Total number of query-log entries flushed successfully to durable storage.",
})

var queryLogDroppedEntriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_query_log_dropped_entries_total",
	Help: "Total number of query-log entries dropped before reaching durable storage.",
}, []string{"reason"})

var queryLogFlushErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_query_log_flush_errors_total",
	Help: "Total number of failed query-log storage flush attempts.",
})

var queryLogFlushDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_query_log_flush_duration_seconds",
	Help:    "Duration of query-log storage flush attempts in seconds.",
	Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
})

func SetQueryLogBufferedEntries(entries int) {
	if entries < 0 {
		entries = 0
	}
	queryLogBufferedEntries.Set(float64(entries))
}

func IncQueryLogEnqueuedEntries() {
	queryLogEnqueuedEntriesTotal.Inc()
}

func AddQueryLogFlushedEntries(entries int) {
	if entries <= 0 {
		return
	}
	queryLogFlushedEntriesTotal.Add(float64(entries))
}

func AddQueryLogDroppedEntries(reason string, entries int) {
	if entries <= 0 {
		return
	}
	queryLogDroppedEntriesTotal.WithLabelValues(reason).Add(float64(entries))
}

func IncQueryLogFlushErrors() {
	queryLogFlushErrorsTotal.Inc()
}

func ObserveQueryLogFlushDuration(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	queryLogFlushDurationHistogram.Observe(duration.Seconds())
}
