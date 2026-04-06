package duckdbservice

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for DuckLake transaction conflict tracking in the Flight SQL worker.
// These use the "duckgres_worker_" prefix to distinguish from standalone-mode
// metrics defined in server/server.go (which use "duckgres_ducklake_").
var (
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
