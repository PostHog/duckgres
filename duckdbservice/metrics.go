package duckdbservice

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for DuckLake transaction conflict tracking in the Flight SQL worker.
// These use the "duckgres_worker_" prefix to distinguish from standalone-mode
// metrics defined in server/server.go (which use "duckgres_ducklake_").
var (
	cacheProxyModeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "duckgres_worker_cache_proxy_mode",
		Help: "Current node-local cache-proxy mode (one active mode has value 1)",
	}, []string{"mode"})
	cacheProxyBypassTransitionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_cache_proxy_bypass_transitions_total",
		Help: "Total transitions into cache-proxy bypass mode",
	}, []string{"reason"})
	cacheProxyBypassedOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_cache_proxy_bypassed_operations_total",
		Help: "Total operations routed around the node-local cache proxy",
	}, []string{"reason"})
	cacheProxyReconnectAttemptsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "duckgres_worker_cache_proxy_reconnect_attempts_total",
		Help: "Total cache-proxy health probes while recovering or monitoring",
	})
	cacheProxyRecoveriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "duckgres_worker_cache_proxy_recoveries_total",
		Help: "Total recoveries that re-enabled the node-local cache proxy",
	})

	ducklakeConflictTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_conflict_total",
		Help: "Total number of DuckLake transaction conflicts encountered (worker)",
	})
	ducklakeConflictRetriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_conflict_retries_total",
		Help: "Total number of DuckLake transaction conflict retry attempts (worker)",
	})
	ducklakeConflictRetrySuccessesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_conflict_retry_successes_total",
		Help: "Total number of DuckLake transaction conflict retries that succeeded (worker)",
	})
	ducklakeConflictRetriesExhaustedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_conflict_retries_exhausted_total",
		Help: "Total number of DuckLake transaction conflicts where all retries were exhausted (worker)",
	})
)

// Commit-loop stats re-exported from the ducklake extension's
// ducklake_commit_stats() table function (see commit_stats.go). These see
// INSIDE the extension's internal commit retry loop, unlike the
// duckgres_worker_ducklake_conflict_* counters above, which only see conflicts
// that escape it. Labelled by DuckLake catalog name; conflicts additionally by
// conflict cause.
var (
	ducklakeCommitAttemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_commit_attempts_total",
		Help: "Total DuckLake commit attempts inside the ducklake extension's commit retry loop (worker)",
	}, []string{"catalog"})
	ducklakeCommitSuccessesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_commit_successes_total",
		Help: "Total DuckLake commits that succeeded inside the ducklake extension's commit retry loop (worker)",
	}, []string{"catalog"})
	ducklakeCommitRetriesExhaustedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_commit_retries_exhausted_total",
		Help: "Total DuckLake commits that exhausted the ducklake extension's internal retries (worker)",
	}, []string{"catalog"})
	ducklakeCommitNonretryableErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_commit_nonretryable_errors_total",
		Help: "Total DuckLake commit failures the ducklake extension classified as non-retryable (worker)",
	}, []string{"catalog"})
	ducklakeCommitBackoffMsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_commit_backoff_ms_total",
		Help: "Total milliseconds spent backing off between DuckLake commit retries inside the ducklake extension (worker)",
	}, []string{"catalog"})
	ducklakeCommitDurationMsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_commit_duration_ms_total",
		Help: "Total milliseconds spent in DuckLake commits inside the ducklake extension's commit loop (worker)",
	}, []string{"catalog"})
	ducklakeCommitConflictsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_worker_ducklake_commit_conflicts_total",
		Help: "Total DuckLake commit conflicts observed inside the ducklake extension's commit retry loop, by cause (worker)",
	}, []string{"catalog", "cause"})
)
