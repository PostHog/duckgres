//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- Worker-acquire phase latency (remote/k8s backend) ---
//
// These histograms split OrgReservedPool.AcquireWorker latency by phase so a
// stalled bootstrap can be attributed from a dashboard: time blocked in the
// per-org FIFO gate vs claiming a hot-idle worker vs spawning a pod (EC2 node
// boot can take minutes) vs tenant activation.

// acquirePhaseBuckets covers 50ms (hot-idle reuse) through 10min (pod spawn
// waiting on an EC2 node boot).
var acquirePhaseBuckets = []float64{0.05, 0.25, 1, 5, 15, 30, 60, 120, 300, 600}

const (
	acquirePhaseHotIdleClaim = "hot_idle_claim"
	acquirePhaseSpawn        = "spawn"
	acquirePhaseActivate     = "activate"

	acquireOutcomeOK       = "ok"
	acquireOutcomeError    = "error"
	acquireOutcomeCapacity = "capacity"
	acquireOutcomeCanceled = "canceled"

	acquireGateOutcomeAcquired = "acquired"
	acquireGateOutcomeCanceled = "canceled"
)

// errHotIdleImageMismatch marks a hot-idle claim that yielded a worker of a
// stale image (retired instead of reused) so the attempt is observed as an
// error rather than dropped from the phase histogram.
var errHotIdleImageMismatch = errors.New("hot-idle worker image mismatch")

var workerAcquireGateWaitHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_acquire_gate_wait_seconds",
	Help:    "Time a connection spent blocked in the per-org FIFO acquire gate (orgAcquireGate) before owning the slow acquisition path, partitioned by outcome (acquired|canceled).",
	Buckets: acquirePhaseBuckets,
}, []string{"outcome"})

var workerAcquirePhaseHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_acquire_phase_seconds",
	Help:    "Duration of individual worker-acquire phases on the remote/k8s backend, partitioned by phase (hot_idle_claim|spawn|activate) and outcome (ok|error).",
	Buckets: acquirePhaseBuckets,
}, []string{"phase", "outcome"})

var workerAcquireTotalHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_acquire_total_seconds",
	Help:    "End-to-end OrgReservedPool.AcquireWorker duration, partitioned by outcome (ok|capacity|error|canceled).",
	Buckets: acquirePhaseBuckets,
}, []string{"outcome"})

func observeAcquireGateWait(d time.Duration, outcome string) {
	if d < 0 {
		d = 0
	}
	workerAcquireGateWaitHistogram.WithLabelValues(outcome).Observe(d.Seconds())
}

// observeAcquirePhase records one attempt of a single acquire phase. err==nil
// observes outcome=ok, otherwise outcome=error.
func observeAcquirePhase(phase string, d time.Duration, err error) {
	if d < 0 {
		d = 0
	}
	outcome := acquireOutcomeOK
	if err != nil {
		outcome = acquireOutcomeError
	}
	workerAcquirePhaseHistogram.WithLabelValues(phase, outcome).Observe(d.Seconds())
}

func observeAcquireTotal(d time.Duration, outcome string) {
	if d < 0 {
		d = 0
	}
	workerAcquireTotalHistogram.WithLabelValues(outcome).Observe(d.Seconds())
}

// acquireTotalOutcome classifies an AcquireWorker error for the end-to-end
// histogram: ok | capacity (org/global cap backpressure) | canceled (client
// ctx) | error (everything else).
func acquireTotalOutcome(err error) string {
	switch {
	case err == nil:
		return acquireOutcomeOK
	case isCapacityExhaustedError(err):
		return acquireOutcomeCapacity
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return acquireOutcomeCanceled
	default:
		return acquireOutcomeError
	}
}

func isCapacityExhaustedError(err error) bool {
	var capErr *WorkerCapacityExhaustedError
	return errors.As(err, &capErr)
}
