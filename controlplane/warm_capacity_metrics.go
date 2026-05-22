//go:build kubernetes

package controlplane

import (
	"sort"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var warmCapacityMissesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_warm_capacity_misses_total",
	Help: "Total foreground warm-capacity misses, partitioned by image scope and reason.",
}, []string{"scope", "reason"})

var warmCapacityRecentMissesGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_recent_misses",
	Help: "Recent warm-capacity misses read by dynamic target reconciliation, partitioned by scope and reason.",
}, []string{"scope", "reason"})

var warmCapacityBaseTargetGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_base_target",
	Help: "Configured/base warm-capacity target by scope.",
}, []string{"scope"})

var warmCapacityDemandTargetGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_demand_target",
	Help: "Dynamic warm-capacity target added above the base target by scope.",
}, []string{"scope"})

var warmCapacityEffectiveTargetGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_effective_target",
	Help: "Effective warm-capacity target after applying dynamic demand and caps by scope.",
}, []string{"scope"})

var warmCapacityHeadroomGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_headroom",
	Help: "Remaining global warm-capacity target headroom by scope; -1 means unbounded.",
}, []string{"scope"})

var warmCapacityReconcileSpawnsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_warm_capacity_reconcile_spawns_total",
	Help: "Warm-capacity worker spawn slots accepted by reconciliation, partitioned by scope and result.",
}, []string{"scope", "result"})

var workerLifecycleCountGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_worker_lifecycle_count",
	Help: "Cluster-wide active worker count by image, lifecycle state, and tenant binding.",
}, []string{"image", "state", "binding"})

func observeWarmCapacityMiss(scope string, reason configstore.WorkerClaimMissReason) {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return
	}
	policy := warmCapacityMissPolicyForReason(reason)
	warmCapacityMissesCounter.WithLabelValues(scope, string(policy.reason)).Inc()
}

func observeWarmCapacityRecentMisses(aggregates []configstore.WarmCapacityMissAggregate, previous ...[]configstore.WarmCapacityMissAggregate) {
	for _, prev := range previous {
		for _, aggregate := range prev {
			scope := strings.TrimSpace(aggregate.Scope)
			if scope == "" {
				continue
			}
			policy := warmCapacityMissPolicyForReason(aggregate.Reason)
			warmCapacityRecentMissesGauge.WithLabelValues(scope, string(policy.reason)).Set(0)
		}
	}
	for _, aggregate := range aggregates {
		scope := strings.TrimSpace(aggregate.Scope)
		if scope == "" || aggregate.Count < 0 {
			continue
		}
		policy := warmCapacityMissPolicyForReason(aggregate.Reason)
		warmCapacityRecentMissesGauge.WithLabelValues(scope, string(policy.reason)).Set(float64(aggregate.Count))
	}
}

func observeWarmCapacityTargets(baseTargets, effectiveTargets map[string]int, maxWorkers int, previousTargets ...map[string]int) {
	scopeMaps := []map[string]int{baseTargets, effectiveTargets}
	scopeMaps = append(scopeMaps, previousTargets...)
	scopes := warmCapacityTargetScopes(scopeMaps...)
	for _, image := range scopes {
		scope := warmCapacityImageScope(image)
		base := positiveMapValue(baseTargets, image)
		effective := positiveMapValue(effectiveTargets, image)
		demand := effective - base
		if demand < 0 {
			demand = 0
		}
		warmCapacityBaseTargetGauge.WithLabelValues(scope).Set(float64(base))
		warmCapacityDemandTargetGauge.WithLabelValues(scope).Set(float64(demand))
		warmCapacityEffectiveTargetGauge.WithLabelValues(scope).Set(float64(effective))
	}

	headroom := -1.0
	if maxWorkers > 0 {
		headroom = float64(maxWorkers - sumIntMap(effectiveTargets))
		if headroom < 0 {
			headroom = 0
		}
	}
	warmCapacityHeadroomGauge.WithLabelValues("global").Set(headroom)
}

func observeWorkerLifecycleStats(stats []configstore.WorkerLifecycleStats, previous ...[]configstore.WorkerLifecycleStats) {
	for _, prev := range previous {
		for _, stat := range prev {
			image := strings.TrimSpace(stat.Image)
			state := strings.TrimSpace(string(stat.State))
			binding := strings.TrimSpace(stat.Binding)
			if image == "" || state == "" || binding == "" {
				continue
			}
			workerLifecycleCountGauge.WithLabelValues(image, state, binding).Set(0)
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

func resetLeaderOwnedClusterMetrics() {
	warmCapacityRecentMissesGauge.Reset()
	warmCapacityBaseTargetGauge.Reset()
	warmCapacityDemandTargetGauge.Reset()
	warmCapacityEffectiveTargetGauge.Reset()
	warmCapacityHeadroomGauge.Reset()
	workerLifecycleCountGauge.Reset()
}

func observeWarmCapacityReconcileSpawns(scope, result string, count int) {
	scope = strings.TrimSpace(scope)
	result = strings.TrimSpace(result)
	if scope == "" || result == "" || count <= 0 {
		return
	}
	warmCapacityReconcileSpawnsCounter.WithLabelValues(scope, result).Add(float64(count))
}

func warmCapacityImageScope(image string) string {
	image = strings.TrimSpace(image)
	if image == "" {
		return ""
	}
	return "image:" + image
}

func warmCapacityTargetScopes(maps ...map[string]int) []string {
	seen := make(map[string]struct{})
	for _, values := range maps {
		for image, value := range values {
			image = strings.TrimSpace(image)
			if image == "" || value <= 0 {
				continue
			}
			seen[image] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for image := range seen {
		out = append(out, image)
	}
	sort.Strings(out)
	return out
}

func positiveMapValue(values map[string]int, key string) int {
	value := values[key]
	if value < 0 {
		return 0
	}
	return value
}

func nonNegativeInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
