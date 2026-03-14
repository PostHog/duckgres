//go:build kubernetes

package controlplane

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
