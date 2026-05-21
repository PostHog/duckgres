package observe

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// connectionsGauge tracks the number of currently open client connections.
// Use IncrementOpenConnections / DecrementOpenConnections to mutate it.
var connectionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_connections_open",
	Help: "Number of currently open client connections",
})

// IncrementOpenConnections increments the open connections gauge.
func IncrementOpenConnections() { connectionsGauge.Inc() }

// DecrementOpenConnections decrements the open connections gauge.
func DecrementOpenConnections() { connectionsGauge.Dec() }

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
// is the first signal of metadata-DB pressure (slow Aurora, conn-pool
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
