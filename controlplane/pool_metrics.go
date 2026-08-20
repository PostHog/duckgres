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
	Help: "Cluster-wide active worker count by image, lifecycle state, tenant binding, and org.",
}, []string{"image", "state", "binding", "org"})

// observeWorkerLifecycleStats refreshes the per-image/org worker lifecycle gauge
// from the latest cluster snapshot. previous (the stats observed on the prior
// tick) lets it delete series for image/org pairs that have gone away so they
// don't linger in Prometheus.
func observeWorkerLifecycleStats(stats []configstore.WorkerLifecycleStats, previous ...[]configstore.WorkerLifecycleStats) {
	currentScopes := workerLifecycleMetricScopes(stats)
	previousScopes := map[workerLifecycleMetricScope]struct{}{}
	for _, prev := range previous {
		for scope := range workerLifecycleMetricScopes(prev) {
			previousScopes[scope] = struct{}{}
		}
	}
	for scope := range previousScopes {
		if _, ok := currentScopes[scope]; !ok {
			for _, state := range observedWorkerLifecycleStates {
				for _, binding := range observedWorkerLifecycleBindings {
					workerLifecycleCountGauge.DeleteLabelValues(scope.image, string(state), binding, scope.org)
				}
			}
		}
	}
	for scope := range currentScopes {
		for _, state := range observedWorkerLifecycleStates {
			for _, binding := range observedWorkerLifecycleBindings {
				workerLifecycleCountGauge.WithLabelValues(scope.image, string(state), binding, scope.org).Set(0)
			}
		}
	}
	for _, stat := range stats {
		image := strings.TrimSpace(stat.Image)
		state := strings.TrimSpace(string(stat.State))
		binding := strings.TrimSpace(stat.Binding)
		org := strings.TrimSpace(stat.Org)
		if image == "" || state == "" || binding == "" {
			continue
		}
		workerLifecycleCountGauge.WithLabelValues(image, state, binding, org).Set(float64(nonNegativeInt64(stat.Count)))
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

// "neutral" is legacy: remote workers are always org-bound from spawn now (no
// warm pool), so binding="neutral" reports 0 in production. Kept so any
// pre-existing/legacy rows still surface and old dashboards don't break.
var observedWorkerLifecycleBindings = []string{"neutral", "org_bound"}

type workerLifecycleMetricScope struct {
	image string
	org   string
}

func workerLifecycleMetricScopes(stats []configstore.WorkerLifecycleStats) map[workerLifecycleMetricScope]struct{} {
	scopes := make(map[workerLifecycleMetricScope]struct{})
	for _, stat := range stats {
		image := strings.TrimSpace(stat.Image)
		if image != "" {
			scopes[workerLifecycleMetricScope{image: image, org: strings.TrimSpace(stat.Org)}] = struct{}{}
		}
	}
	return scopes
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
