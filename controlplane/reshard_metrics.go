//go:build kubernetes

package controlplane

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	reshardActiveOperationsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_reshard_active_operations",
		Help: "Number of pending or running reshard operations.",
	})
	reshardStaleOperationsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_reshard_stale_operations",
		Help: "Number of reshard operations currently requiring runner reconciliation.",
	})
	reshardRecoveryRequiredGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_reshard_recovery_required_operations",
		Help: "Number of active reshards whose automatic runner respawns are paused.",
	})
	reshardRespawnCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_reshard_runner_respawns_total",
		Help: "Reshard runner respawn interventions by outcome.",
	}, []string{"outcome"})
)
