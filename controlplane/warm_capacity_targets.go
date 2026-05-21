package controlplane

import (
	"sort"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type dynamicWarmCapacityConfig struct {
	Enabled             bool
	MissesPerWorker     int
	DynamicImageCeiling int
	DynamicTotalCeiling int
	MaxWorkers          int
}

func dynamicWarmCapacityConfigFromK8s(cfg K8sConfig) dynamicWarmCapacityConfig {
	return dynamicWarmCapacityConfig{
		Enabled:             cfg.DynamicWarmCapacityEnabled,
		MissesPerWorker:     cfg.WarmCapacityMissesPerWorker,
		DynamicImageCeiling: cfg.WarmCapacityDynamicImageCeiling,
		DynamicTotalCeiling: cfg.WarmCapacityDynamicTotalCeiling,
		MaxWorkers:          cfg.MaxWorkers,
	}
}

func computeBaseWarmCapacityTargets(defaultImage string, sharedWarmTarget int, perImageFloors map[string]int) map[string]int {
	out := sanitizeWarmCapacityTargets(perImageFloors)
	defaultImage = strings.TrimSpace(defaultImage)
	if defaultImage == "" {
		return out
	}
	if sharedWarmTarget < 0 {
		sharedWarmTarget = 0
	}
	if sharedWarmTarget > out[defaultImage] {
		out[defaultImage] = sharedWarmTarget
	}
	return out
}

func computeDynamicWarmCapacityTargets(baseTargets map[string]int, aggregates []configstore.WarmCapacityMissAggregate, cfg dynamicWarmCapacityConfig) map[string]int {
	targets := sanitizeWarmCapacityTargets(baseTargets)
	if !cfg.Enabled {
		return targets
	}
	cfg = normalizeDynamicWarmCapacityConfig(cfg)

	missCounts := imageWarmCapacityMissCounts(aggregates)
	requestedExtra := make(map[string]int, len(missCounts))
	for image, count := range missCounts {
		extra := ceilDivInt64(count, cfg.MissesPerWorker)
		if cfg.DynamicImageCeiling > 0 && extra > cfg.DynamicImageCeiling {
			extra = cfg.DynamicImageCeiling
		}
		if extra > 0 {
			requestedExtra[image] = extra
		}
	}
	if len(requestedExtra) == 0 {
		return targets
	}

	totalCap := sumIntMap(requestedExtra)
	if cfg.DynamicTotalCeiling > 0 && totalCap > cfg.DynamicTotalCeiling {
		totalCap = cfg.DynamicTotalCeiling
	}
	if cfg.MaxWorkers > 0 {
		headroom := cfg.MaxWorkers - sumIntMap(targets)
		if headroom < 0 {
			headroom = 0
		}
		if totalCap > headroom {
			totalCap = headroom
		}
	}
	if totalCap <= 0 {
		return targets
	}

	allocatedExtra := capDynamicWarmCapacityExtras(requestedExtra, missCounts, totalCap)
	for image, extra := range allocatedExtra {
		if extra <= 0 {
			continue
		}
		targets[image] += extra
	}
	return targets
}

func normalizeDynamicWarmCapacityConfig(cfg dynamicWarmCapacityConfig) dynamicWarmCapacityConfig {
	if cfg.MissesPerWorker <= 0 {
		cfg.MissesPerWorker = DefaultWarmCapacityMissesPerWorker
	}
	if cfg.DynamicImageCeiling < 0 {
		cfg.DynamicImageCeiling = 0
	}
	if cfg.DynamicTotalCeiling < 0 {
		cfg.DynamicTotalCeiling = 0
	}
	if cfg.MaxWorkers < 0 {
		cfg.MaxWorkers = 0
	}
	return cfg
}

func imageWarmCapacityMissCounts(aggregates []configstore.WarmCapacityMissAggregate) map[string]int64 {
	counts := make(map[string]int64)
	for _, aggregate := range aggregates {
		if aggregate.Count <= 0 {
			continue
		}
		policy := warmCapacityMissPolicyForReason(aggregate.Reason)
		if !policy.recordDynamicDemand {
			continue
		}
		image, ok := strings.CutPrefix(strings.TrimSpace(aggregate.Scope), "image:")
		if !ok {
			continue
		}
		image = strings.TrimSpace(image)
		if image == "" {
			continue
		}
		counts[image] += aggregate.Count
	}
	return counts
}

func capDynamicWarmCapacityExtras(requested map[string]int, missCounts map[string]int64, cap int) map[string]int {
	out := make(map[string]int, len(requested))
	if cap <= 0 {
		return out
	}

	images := make([]string, 0, len(requested))
	total := 0
	for image, extra := range requested {
		if image == "" || extra <= 0 {
			continue
		}
		images = append(images, image)
		total += extra
	}
	if total <= cap {
		for _, image := range images {
			out[image] = requested[image]
		}
		return out
	}

	sort.Slice(images, func(i, j int) bool {
		left := images[i]
		right := images[j]
		if missCounts[left] != missCounts[right] {
			return missCounts[left] > missCounts[right]
		}
		if requested[left] != requested[right] {
			return requested[left] > requested[right]
		}
		return left < right
	})

	remaining := cap
	for _, image := range images {
		if remaining <= 0 {
			break
		}
		extra := requested[image]
		if extra > remaining {
			extra = remaining
		}
		if extra > 0 {
			out[image] = extra
			remaining -= extra
		}
	}
	return out
}

func sanitizeWarmCapacityTargets(targets map[string]int) map[string]int {
	out := make(map[string]int, len(targets))
	for image, target := range targets {
		image = strings.TrimSpace(image)
		if image == "" || target <= 0 {
			continue
		}
		out[image] = target
	}
	return out
}

func ceilDivInt64(n int64, d int) int {
	if n <= 0 {
		return 0
	}
	if d <= 0 {
		d = DefaultWarmCapacityMissesPerWorker
	}
	divisor := int64(d)
	return int((n + divisor - 1) / divisor)
}

func sumIntMap(values map[string]int) int {
	total := 0
	for _, value := range values {
		if value > 0 {
			total += value
		}
	}
	return total
}
