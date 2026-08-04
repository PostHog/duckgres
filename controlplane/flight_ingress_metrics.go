package controlplane

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var flightAuthSessionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_flight_auth_sessions_active",
	Help: "Number of active Flight auth sessions on the control plane",
})

var controlPlaneWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_control_plane_workers_active",
	Help: "Number of active control-plane worker processes",
})

var controlPlaneWorkerAcquireHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_control_plane_worker_acquire_seconds",
	Help:    "Time spent acquiring a worker for a new session.",
	Buckets: prometheus.DefBuckets,
})

var controlPlaneWorkerSpawnHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_control_plane_worker_spawn_seconds",
	Help:    "Time spent spawning and health-checking a new control-plane worker.",
	Buckets: prometheus.DefBuckets,
})

var controlPlaneWorkerQueueDepthGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_control_plane_worker_queue_depth",
	Help: "Approximate number of session requests waiting on worker acquisition.",
})

var controlPlaneWorkerAcquireFailuresCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_control_plane_worker_acquire_failures_total",
	Help: "Total worker acquisition failures by reason.",
}, []string{"reason"})

var controlPlaneWorkerQueueDepth atomic.Int64

var flightSessionsReapedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_flight_sessions_reaped_total",
	Help: "Number of Flight auth sessions reaped",
}, []string{"trigger"})

func observeFlightAuthSessions(count int) {
	if count < 0 {
		count = 0
	}
	flightAuthSessionsGauge.Set(float64(count))
}

func observeControlPlaneWorkers(count int) {
	if count < 0 {
		count = 0
	}
	controlPlaneWorkersGauge.Set(float64(count))
}

func observeControlPlaneWorkerAcquire(d time.Duration) {
	if d < 0 {
		d = 0
	}
	controlPlaneWorkerAcquireHistogram.Observe(d.Seconds())
}

func observeControlPlaneWorkerSpawn(d time.Duration) {
	if d < 0 {
		d = 0
	}
	controlPlaneWorkerSpawnHistogram.Observe(d.Seconds())
}

func observeControlPlaneWorkerQueueDepthDelta(delta int64) {
	newDepth := controlPlaneWorkerQueueDepth.Add(delta)
	if newDepth < 0 {
		controlPlaneWorkerQueueDepth.Store(0)
		newDepth = 0
	}
	controlPlaneWorkerQueueDepthGauge.Set(float64(newDepth))
}

func observeControlPlaneWorkerAcquireFailure(reason string) {
	controlPlaneWorkerAcquireFailuresCounter.WithLabelValues(reason).Inc()
}

// controlPlaneWorkerSessionCapDriftCounter counts times a worker rejected a
// control-plane-scheduled CreateSession because it already held its max session
// — a CP↔worker accounting drift that must never happen under the
// one-session-per-worker contract. Should sit at 0; a sustained nonzero rate
// means scheduling is double-assigning workers (alert on it).
var controlPlaneWorkerSessionCapDriftCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_control_plane_worker_session_cap_drift_total",
	Help: "Times a worker rejected a CP-scheduled CreateSession at its session cap (CP↔worker accounting drift; recovered by recycling the worker and retrying).",
})

// controlPlaneWorkerConnPoolWedgeCounter counts sessions rejected by a worker
// whose single DB connection never returned to its pool (wedged worker). The
// CP recycles the worker and re-acquires; a nonzero rate means worker-side
// session cleanup is hanging — find the leak, don't lean on the recycle.
var controlPlaneWorkerConnPoolWedgeCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_control_plane_worker_conn_pool_wedge_total",
	Help: "Sessions rejected by a wedged worker (DB connection pool timeout); the worker is recycled and the session retried on a fresh one.",
})

func observeWorkerSessionCapDrift() {
	controlPlaneWorkerSessionCapDriftCounter.Inc()
}

func observeWorkerConnPoolWedge() {
	controlPlaneWorkerConnPoolWedgeCounter.Inc()
}

func observeFlightSessionsReaped(trigger string, count int) {
	if count <= 0 {
		return
	}
	flightSessionsReapedCounter.WithLabelValues(trigger).Add(float64(count))
}
