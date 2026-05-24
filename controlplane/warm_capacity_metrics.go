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

var warmCapacityEffectiveTargetGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_effective_target",
	Help: "Effective warm-capacity target after applying dynamic demand and caps, by image.",
}, []string{"image"})

var warmCapacityHeadroomGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_warm_capacity_headroom",
	Help: "Remaining global warm-capacity target headroom; -1 means unbounded.",
})

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

// observeWarmCapacityTargets refreshes the effective-target gauge per
// image plus the global headroom gauge. baseTargets is still accepted
// so the signature stays stable for callers (and so future deltas
// against base can be reintroduced cheaply); it is otherwise unused.
func observeWarmCapacityTargets(baseTargets, effectiveTargets map[string]int, maxWorkers int, previousTargets ...map[string]int) {
	_ = baseTargets
	targetMaps := []map[string]int{effectiveTargets}
	targetMaps = append(targetMaps, previousTargets...)
	images := warmCapacityTargetImages(targetMaps...)
	for _, image := range images {
		effective := positiveMapValue(effectiveTargets, image)
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
	warmCapacityEffectiveTargetGauge.Reset()
	// Reset headroom to the "unbounded / unknown" sentinel rather than
	// 0 — 0 is the alertable capacity-exhausted state and would page
	// spuriously every time leadership hands off during a rollout.
	warmCapacityHeadroomGauge.Set(-1)
	workerLifecycleCountGauge.Reset()
}

func warmCapacityScopeForImage(image string) string {
	image = strings.TrimSpace(image)
	if image == "" {
		return ""
	}
	return "image:" + image
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
