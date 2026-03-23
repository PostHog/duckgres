//go:build kubernetes

package controlplane

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- Per-org metrics (multi-tenant mode) ---

var orgWorkersActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_workers_active",
	Help: "Number of active workers per org",
}, []string{"org"})

var orgWorkersIdleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_workers_idle",
	Help: "Number of idle workers per org",
}, []string{"org"})

var orgSessionsActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_sessions_active",
	Help: "Number of active sessions per org",
}, []string{"org"})

var orgWorkerSpawnsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_worker_spawns_total",
	Help: "Total worker spawns per org",
}, []string{"org"})

var orgWorkerCrashesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_worker_crashes_total",
	Help: "Total worker crashes per org",
}, []string{"org"})

func observeOrgWorkersActive(org string, count int) {
	orgWorkersActiveGauge.WithLabelValues(org).Set(float64(count))
}

func observeOrgWorkersIdle(org string, count int) {
	orgWorkersIdleGauge.WithLabelValues(org).Set(float64(count))
}

func observeOrgSessionsActive(org string, count int) {
	orgSessionsActiveGauge.WithLabelValues(org).Set(float64(count))
}

func observeOrgWorkerSpawn(org string) {
	orgWorkerSpawnsCounter.WithLabelValues(org).Inc()
}

func observeOrgWorkerCrash(org string) {
	orgWorkerCrashesCounter.WithLabelValues(org).Inc()
}
