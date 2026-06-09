//go:build kubernetes

package controlplane

import (
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var workerLifecycleCountGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_worker_lifecycle_count",
	Help: "Cluster-wide active worker count by image, lifecycle state, and tenant binding.",
}, []string{"image", "state", "binding"})

// observeWorkerLifecycleStats refreshes the per-image worker lifecycle gauge from
// the latest cluster snapshot. previous (the stats observed on the prior tick)
// lets it delete series for images that have gone away so they don't linger in
// Prometheus.
func observeWorkerLifecycleStats(stats []configstore.WorkerLifecycleStats, previous ...[]configstore.WorkerLifecycleStats) {
	currentImages := workerLifecycleImages(stats)
	previousImages := map[string]struct{}{}
	for _, prev := range previous {
		for _, stat := range prev {
			if image := strings.TrimSpace(stat.Image); image != "" {
				previousImages[image] = struct{}{}
			}
		}
	}
	for image := range previousImages {
		if _, ok := currentImages[image]; !ok {
			for _, state := range observedWorkerLifecycleStates {
				for _, binding := range observedWorkerLifecycleBindings {
					workerLifecycleCountGauge.DeleteLabelValues(image, string(state), binding)
				}
			}
		}
	}
	for image := range currentImages {
		for _, state := range observedWorkerLifecycleStates {
			for _, binding := range observedWorkerLifecycleBindings {
				workerLifecycleCountGauge.WithLabelValues(image, string(state), binding).Set(0)
			}
		}
	}
	for _, stat := range stats {
		image := strings.TrimSpace(stat.Image)
		state := strings.TrimSpace(string(stat.State))
		binding := strings.TrimSpace(stat.Binding)
		if image == "" || state == "" || binding == "" {
			continue
		}
		workerLifecycleCountGauge.WithLabelValues(image, state, binding).Set(float64(nonNegativeInt64(stat.Count)))
	}
}

var observedWorkerLifecycleStates = []configstore.WorkerState{
	configstore.WorkerStateSpawning,
	configstore.WorkerStateIdle,
	configstore.WorkerStateReserved,
	configstore.WorkerStateActivating,
	configstore.WorkerStateHot,
	configstore.WorkerStateHotIdle,
	configstore.WorkerStateDraining,
}

var observedWorkerLifecycleBindings = []string{"neutral", "org_bound"}

func workerLifecycleImages(stats []configstore.WorkerLifecycleStats) map[string]struct{} {
	images := make(map[string]struct{})
	for _, stat := range stats {
		image := strings.TrimSpace(stat.Image)
		if image != "" {
			images[image] = struct{}{}
		}
	}
	return images
}

// resetLeaderOwnedClusterMetrics clears the cluster-wide gauges this CP owns only
// while it holds the janitor leader lease, so stale per-image counts don't linger
// in Prometheus after leadership hands off during a rollout.
func resetLeaderOwnedClusterMetrics() {
	workerLifecycleCountGauge.Reset()
}

func nonNegativeInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
