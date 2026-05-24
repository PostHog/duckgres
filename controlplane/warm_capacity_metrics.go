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
	Help: "Total foreground warm-capacity misses, partitioned by image and reason.",
}, []string{"image", "reason"})

var warmCapacityRecentMissesGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_recent_misses",
	Help: "Recent warm-capacity misses read by dynamic target reconciliation, partitioned by image and reason.",
}, []string{"image", "reason"})

var warmCapacityBaseTargetGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_base_target",
	Help: "Configured/base warm-capacity target by image.",
}, []string{"image"})

var warmCapacityDemandTargetGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_demand_target",
	Help: "Dynamic warm-capacity target added above the base target by image.",
}, []string{"image"})

var warmCapacityEffectiveTargetGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_effective_target",
	Help: "Effective warm-capacity target after applying dynamic demand and caps by image.",
}, []string{"image"})

var warmCapacityHeadroomGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_headroom",
	Help: "Remaining global warm-capacity target headroom; -1 means unbounded.",
})

var warmCapacityReconcileSpawnsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_warm_capacity_reconcile_spawns_total",
	Help: "Warm-capacity worker spawn slots accepted by reconciliation, partitioned by image and result.",
}, []string{"image", "result"})

var workerLifecycleCountGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_worker_lifecycle_count",
	Help: "Cluster-wide active worker count by image, lifecycle state, and tenant binding.",
}, []string{"image", "state", "binding"})

func observeWarmCapacityMiss(image string, reason configstore.WorkerClaimMissReason) {
	image = strings.TrimSpace(image)
	if image == "" {
		return
	}
	policy := warmCapacityMissPolicyForReason(reason)
	warmCapacityMissesCounter.WithLabelValues(image, string(policy.reason)).Inc()
}

func observeWarmCapacityRecentMisses(aggregates []configstore.WarmCapacityMissAggregate, previous ...[]configstore.WarmCapacityMissAggregate) {
	for _, prev := range previous {
		for _, aggregate := range prev {
			image := warmCapacityImageFromScope(aggregate.Scope)
			if image == "" {
				continue
			}
			policy := warmCapacityMissPolicyForReason(aggregate.Reason)
			warmCapacityRecentMissesGauge.WithLabelValues(image, string(policy.reason)).Set(0)
		}
	}
	for _, aggregate := range aggregates {
		image := warmCapacityImageFromScope(aggregate.Scope)
		if image == "" || aggregate.Count < 0 {
			continue
		}
		policy := warmCapacityMissPolicyForReason(aggregate.Reason)
		warmCapacityRecentMissesGauge.WithLabelValues(image, string(policy.reason)).Set(float64(aggregate.Count))
	}
}

func observeWarmCapacityTargets(baseTargets, effectiveTargets map[string]int, maxWorkers int, previousTargets ...map[string]int) {
	targetMaps := []map[string]int{baseTargets, effectiveTargets}
	targetMaps = append(targetMaps, previousTargets...)
	images := warmCapacityTargetImages(targetMaps...)
	for _, image := range images {
		base := positiveMapValue(baseTargets, image)
		effective := positiveMapValue(effectiveTargets, image)
		demand := effective - base
		if demand < 0 {
			demand = 0
		}
		warmCapacityBaseTargetGauge.WithLabelValues(image).Set(float64(base))
		warmCapacityDemandTargetGauge.WithLabelValues(image).Set(float64(demand))
		warmCapacityEffectiveTargetGauge.WithLabelValues(image).Set(float64(effective))
	}

	headroom := -1.0
	if maxWorkers > 0 {
		headroom = float64(maxWorkers - sumIntMap(effectiveTargets))
		if headroom < 0 {
			headroom = 0
		}
	}
	warmCapacityHeadroomGauge.Set(headroom)
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
	warmCapacityHeadroomGauge.Set(0)
	workerLifecycleCountGauge.Reset()
	// Both gauges below are leader-only: the recovery sweep + the
	// inventory divergence comparison run from the janitor lambdas
	// which only fire when this CP holds the leader lease. Resetting
	// on lease loss prevents a stale value lingering after a peer takes
	// over (the new leader's first sweep will repopulate them).
	janitorRecoveryLastSuccessGauge.Set(0)
	workerInventoryDivergenceGauge.Reset()
}

func observeWarmCapacityReconcileSpawns(image, result string, count int) {
	image = strings.TrimSpace(image)
	result = strings.TrimSpace(result)
	if image == "" || result == "" || count <= 0 {
		return
	}
	warmCapacityReconcileSpawnsCounter.WithLabelValues(image, result).Add(float64(count))
}

func warmCapacityScopeForImage(image string) string {
	image = strings.TrimSpace(image)
	if image == "" {
		return ""
	}
	return "image:" + image
}

func warmCapacityImageFromScope(scope string) string {
	image := strings.TrimSpace(scope)
	if strings.HasPrefix(image, "org:") {
		return ""
	}
	image = strings.TrimPrefix(image, "image:")
	return strings.TrimSpace(image)
}

func warmCapacityTargetImages(maps ...map[string]int) []string {
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
