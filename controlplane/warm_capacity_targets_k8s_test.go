//go:build kubernetes

package controlplane

import (
	"reflect"
	"testing"
)

func TestDynamicWarmCapacityConfigFromK8s(t *testing.T) {
	cfg := dynamicWarmCapacityConfigFromK8s(K8sConfig{
		DynamicWarmCapacityEnabled:      true,
		WarmCapacityMissesPerWorker:     7,
		WarmCapacityDynamicImageCeiling: 3,
		WarmCapacityDynamicTotalCeiling: 5,
		MaxWorkers:                      12,
	})
	want := dynamicWarmCapacityConfig{
		Enabled:             true,
		MissesPerWorker:     7,
		DynamicImageCeiling: 3,
		DynamicTotalCeiling: 5,
		MaxWorkers:          12,
	}
	if cfg != want {
		t.Fatalf("expected dynamic config %v, got %v", want, cfg)
	}
}

func TestComputeBaseWarmCapacityTargetsMergesSharedAndPerImageFloors(t *testing.T) {
	got := computeBaseWarmCapacityTargets("posthog/duckgres:default", 4, map[string]int{
		"posthog/duckgres:pinned":  1,
		"posthog/duckgres:ignored": 0,
		"":                         3,
	})
	want := map[string]int{
		"posthog/duckgres:default": 4,
		"posthog/duckgres:pinned":  1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected targets %v, got %v", want, got)
	}
}

func TestComputeBaseWarmCapacityTargetsPreservesHigherPerImageDefaultFloor(t *testing.T) {
	got := computeBaseWarmCapacityTargets("posthog/duckgres:default", 2, map[string]int{
		"posthog/duckgres:default": 5,
	})
	want := map[string]int{"posthog/duckgres:default": 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected targets %v, got %v", want, got)
	}
}
