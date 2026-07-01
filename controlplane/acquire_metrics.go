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

	// acquireSource* is how a completed AcquireWorker got its worker — the
	// "phase that was allocated" dimension on the end-to-end total histogram:
	// reusing this org's own idle Hot worker (near-instant), claiming a parked
	// hot-idle worker from the shared pool (fast), or cold-spawning a fresh pod
	// (slow — EC2 node boot). "none" when no worker was allocated (capacity
	// backpressure / ctx cancel).
	acquireSourceIdleReuse    = "idle_reuse"
	acquireSourceHotIdleClaim = "hot_idle_claim"
	acquireSourceSpawn        = "spawn"
	acquireSourceNone         = "none"
)

// errHotIdleImageMismatch marks a hot-idle claim that yielded a worker of a
// stale image (retired instead of reused) so the attempt is observed as an
// error rather than dropped from the phase histogram.
var errHotIdleImageMismatch = errors.New("hot-idle worker image mismatch")

// All three histograms carry an `org` label so per-org acquire latency is
// sliceable from a dashboard (which tenant is eating cold-spawn waits). Orgs are
// bounded managed-warehouse tenants, so the added cardinality is acceptable.

var workerAcquireGateWaitHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_acquire_gate_wait_seconds",
	Help:    "Time a connection spent blocked in the per-org FIFO acquire gate (orgAcquireGate) before owning the slow acquisition path, partitioned by org and outcome (acquired|canceled).",
	Buckets: acquirePhaseBuckets,
}, []string{"org", "outcome"})

var workerAcquirePhaseHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_acquire_phase_seconds",
	Help:    "Duration of individual worker-acquire phases on the remote/k8s backend, partitioned by org, phase (hot_idle_claim|spawn|activate) and outcome (ok|error).",
	Buckets: acquirePhaseBuckets,
}, []string{"org", "phase", "outcome"})

var workerAcquireTotalHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_acquire_total_seconds",
	Help:    "End-to-end OrgReservedPool.AcquireWorker duration (the time a pending session waits for a worker), partitioned by org, the allocation source (idle_reuse|hot_idle_claim|spawn|none) and outcome (ok|capacity|error|canceled).",
	Buckets: acquirePhaseBuckets,
}, []string{"org", "source", "outcome"})

func observeAcquireGateWait(d time.Duration, org, outcome string) {
	if d < 0 {
		d = 0
	}
	workerAcquireGateWaitHistogram.WithLabelValues(org, outcome).Observe(d.Seconds())
}

// observeAcquirePhase records one attempt of a single acquire phase. err==nil
// observes outcome=ok, otherwise outcome=error.
func observeAcquirePhase(phase, org string, d time.Duration, err error) {
	if d < 0 {
		d = 0
	}
	outcome := acquireOutcomeOK
	if err != nil {
		outcome = acquireOutcomeError
	}
	workerAcquirePhaseHistogram.WithLabelValues(org, phase, outcome).Observe(d.Seconds())
}

// observeAcquireTotal records the end-to-end acquire latency for one session,
// tagged by the org, how the worker was ultimately obtained (source), and the
// outcome.
func observeAcquireTotal(d time.Duration, org, source, outcome string) {
	if d < 0 {
		d = 0
	}
	workerAcquireTotalHistogram.WithLabelValues(org, source, outcome).Observe(d.Seconds())
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
