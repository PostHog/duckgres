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

func observeFlightSessionsReaped(trigger string, count int) {
	if count <= 0 {
		return
	}
	flightSessionsReapedCounter.WithLabelValues(trigger).Add(float64(count))
}

// --- Per-team metrics (multi-tenant mode) ---

var teamWorkersActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_team_workers_active",
	Help: "Number of active workers per team",
}, []string{"team"})

var teamWorkersIdleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_team_workers_idle",
	Help: "Number of idle workers per team",
}, []string{"team"})

var teamSessionsActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_team_sessions_active",
	Help: "Number of active sessions per team",
}, []string{"team"})

var teamWorkerSpawnsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_team_worker_spawns_total",
	Help: "Total worker spawns per team",
}, []string{"team"})

var teamWorkerCrashesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_team_worker_crashes_total",
	Help: "Total worker crashes per team",
}, []string{"team"})

func observeTeamWorkersActive(team string, count int) {
	teamWorkersActiveGauge.WithLabelValues(team).Set(float64(count))
}

func observeTeamWorkersIdle(team string, count int) {
	teamWorkersIdleGauge.WithLabelValues(team).Set(float64(count))
}

func observeTeamSessionsActive(team string, count int) {
	teamSessionsActiveGauge.WithLabelValues(team).Set(float64(count))
}

func observeTeamWorkerSpawn(team string) {
	teamWorkerSpawnsCounter.WithLabelValues(team).Inc()
}

func observeTeamWorkerCrash(team string) {
	teamWorkerCrashesCounter.WithLabelValues(team).Inc()
}
