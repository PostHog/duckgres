//go:build kubernetes

package controlplane

import "strings"

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
