//go:build kubernetes

package controlplane

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- Warm-worker lifecycle gauges ---

var warmWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_warm_workers",
	Help: "Number of idle (unassigned) warm workers in the shared pool",
})

var reservedWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_reserved_workers",
	Help: "Number of workers reserved for an org but not yet activated",
})

var activatingWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_activating_workers",
	Help: "Number of workers currently in the activating state",
})

var hotWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_hot_workers",
	Help: "Number of activated, tenant-bound workers serving sessions",
})

var hotIdleWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_hot_idle_workers",
	Help: "Number of activated workers retaining org assignment between sessions",
})

var drainingWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_draining_workers",
	Help: "Number of workers currently draining sessions before retirement",
})

// --- Activation latency and failure counters ---

var activationDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_activation_duration_seconds",
	Help:    "Time from worker reservation to the worker becoming hot",
	Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
})

var activationFailuresCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_activation_failures_total",
	Help: "Total number of failed worker activations",
})

// --- Retirement metrics ---

var workerRetirementsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_retirements_total",
	Help: "Total number of worker retirements",
}, []string{"reason"})

var hotWorkerSessionsHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_hot_worker_sessions_total",
	Help:    "Number of sessions served per hot worker at retirement time",
	Buckets: []float64{0, 1, 2, 5, 10, 25, 50, 100},
})

// Retirement reason constants.
const (
	RetireReasonNormal            = "normal"
	RetireReasonActivationFailure = "activation_failure"
	RetireReasonOrphaned          = "orphaned"
	RetireReasonCrash             = "crash"
	RetireReasonShutdown          = "shutdown"
	RetireReasonIdleTimeout       = "idle_timeout"
	RetireReasonStuckActivating   = "stuck_activating"
	RetireReasonMismatchedVersion = "mismatched_version"
)

// observeWarmPoolLifecycleGauges recalculates all lifecycle gauges from the
// current worker map. Must be called with p.mu held (at least RLock).
func observeWarmPoolLifecycleGauges(workers map[int]*ManagedWorker) {
	var idle, reserved, activating, hot, hotIdle, draining int
	for _, w := range workers {
		select {
		case <-w.done:
			continue
		default:
		}
		switch w.SharedState().NormalizedLifecycle() {
		case WorkerLifecycleIdle:
			idle++
		case WorkerLifecycleReserved:
			reserved++
		case WorkerLifecycleActivating:
			activating++
		case WorkerLifecycleHot:
			hot++
		case WorkerLifecycleHotIdle:
			hotIdle++
		case WorkerLifecycleDraining:
			draining++
		}
	}
	warmWorkersGauge.Set(float64(idle))
	reservedWorkersGauge.Set(float64(reserved))
	activatingWorkersGauge.Set(float64(activating))
	hotWorkersGauge.Set(float64(hot))
	hotIdleWorkersGauge.Set(float64(hotIdle))
	drainingWorkersGauge.Set(float64(draining))
}

func observeActivationDuration(d time.Duration) {
	activationDurationHistogram.Observe(d.Seconds())
}

func observeActivationFailure() {
	activationFailuresCounter.Inc()
}

func observeWorkerRetirement(reason string) {
	workerRetirementsCounter.WithLabelValues(reason).Inc()
}

func observeHotWorkerSessions(sessionCount int) {
	hotWorkerSessionsHistogram.Observe(float64(sessionCount))
}
