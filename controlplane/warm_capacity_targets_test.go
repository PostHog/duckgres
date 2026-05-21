package controlplane

import (
	"reflect"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
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

func TestComputeDynamicWarmCapacityTargetsDisabledReturnsBaseOnly(t *testing.T) {
	got := computeDynamicWarmCapacityTargets(
		map[string]int{
			"posthog/duckgres:default": 4,
			"ignored":                  0,
		},
		[]configstore.WarmCapacityMissAggregate{
			warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonNoIdle, 80),
			warmCapacityMissAggregate("image:posthog/duckgres:pinned", configstore.WorkerClaimMissReasonNoIdle, 80),
		},
		dynamicWarmCapacityConfig{Enabled: false, MissesPerWorker: 8},
	)
	want := map[string]int{"posthog/duckgres:default": 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected targets %v, got %v", want, got)
	}
}

func TestComputeDynamicWarmCapacityTargetsScalesByRecentMisses(t *testing.T) {
	tests := []struct {
		name  string
		base  map[string]int
		count int64
		want  map[string]int
	}{
		{
			name:  "one miss raises zero base",
			base:  map[string]int{},
			count: 1,
			want:  map[string]int{"posthog/duckgres:default": 1},
		},
		{
			name:  "eight misses adds one extra worker",
			base:  map[string]int{"posthog/duckgres:default": 4},
			count: 8,
			want:  map[string]int{"posthog/duckgres:default": 5},
		},
		{
			name:  "nine misses adds two extra workers",
			base:  map[string]int{"posthog/duckgres:default": 4},
			count: 9,
			want:  map[string]int{"posthog/duckgres:default": 6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeDynamicWarmCapacityTargets(
				tt.base,
				[]configstore.WarmCapacityMissAggregate{
					warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonNoIdle, tt.count),
				},
				dynamicWarmCapacityConfig{Enabled: true, MissesPerWorker: 8},
			)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("expected targets %v, got %v", tt.want, got)
			}
		})
	}
}

func TestComputeDynamicWarmCapacityTargetsKeepsPinnedImageDemandSeparate(t *testing.T) {
	got := computeDynamicWarmCapacityTargets(
		map[string]int{
			"posthog/duckgres:default": 4,
			"posthog/duckgres:v1.5.1":  1,
		},
		[]configstore.WarmCapacityMissAggregate{
			warmCapacityMissAggregate("image:posthog/duckgres:v1.5.1", configstore.WorkerClaimMissReasonNoIdle, 9),
		},
		dynamicWarmCapacityConfig{Enabled: true, MissesPerWorker: 8},
	)
	want := map[string]int{
		"posthog/duckgres:default": 4,
		"posthog/duckgres:v1.5.1":  3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected targets %v, got %v", want, got)
	}
}

func TestComputeDynamicWarmCapacityTargetsIgnoresNonDemandMisses(t *testing.T) {
	got := computeDynamicWarmCapacityTargets(
		map[string]int{"posthog/duckgres:default": 2},
		[]configstore.WarmCapacityMissAggregate{
			warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonOrgCap, 80),
			warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonGlobalCap, 80),
			warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonShuttingDown, 80),
			warmCapacityMissAggregate("org:analytics", configstore.WorkerClaimMissReasonNoIdle, 80),
			warmCapacityMissAggregate("image:", configstore.WorkerClaimMissReasonNoIdle, 80),
		},
		dynamicWarmCapacityConfig{Enabled: true, MissesPerWorker: 8},
	)
	want := map[string]int{"posthog/duckgres:default": 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected targets %v, got %v", want, got)
	}
}

func TestComputeDynamicWarmCapacityTargetsAppliesDynamicCeilings(t *testing.T) {
	got := computeDynamicWarmCapacityTargets(
		map[string]int{
			"posthog/duckgres:default": 1,
			"posthog/duckgres:pinned":  1,
		},
		[]configstore.WarmCapacityMissAggregate{
			warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonNoIdle, 16),
			warmCapacityMissAggregate("image:posthog/duckgres:pinned", configstore.WorkerClaimMissReasonNoIdle, 16),
		},
		dynamicWarmCapacityConfig{
			Enabled:             true,
			MissesPerWorker:     8,
			DynamicImageCeiling: 2,
			DynamicTotalCeiling: 3,
		},
	)
	want := map[string]int{
		"posthog/duckgres:default": 3,
		"posthog/duckgres:pinned":  2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected targets %v, got %v", want, got)
	}
}

func TestComputeDynamicWarmCapacityTargetsAppliesPositiveMaxWorkersHeadroom(t *testing.T) {
	base := map[string]int{
		"posthog/duckgres:default": 2,
		"posthog/duckgres:pinned":  1,
	}
	aggregates := []configstore.WarmCapacityMissAggregate{
		warmCapacityMissAggregate("image:posthog/duckgres:default", configstore.WorkerClaimMissReasonNoIdle, 16),
		warmCapacityMissAggregate("image:posthog/duckgres:pinned", configstore.WorkerClaimMissReasonNoIdle, 16),
	}

	got := computeDynamicWarmCapacityTargets(base, aggregates, dynamicWarmCapacityConfig{
		Enabled:         true,
		MissesPerWorker: 8,
		MaxWorkers:      4,
	})
	want := map[string]int{
		"posthog/duckgres:default": 3,
		"posthog/duckgres:pinned":  1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected capped targets %v, got %v", want, got)
	}

	got = computeDynamicWarmCapacityTargets(base, aggregates, dynamicWarmCapacityConfig{
		Enabled:         true,
		MissesPerWorker: 8,
		MaxWorkers:      0,
	})
	want = map[string]int{
		"posthog/duckgres:default": 4,
		"posthog/duckgres:pinned":  3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected unbounded targets %v, got %v", want, got)
	}
}

func warmCapacityMissAggregate(scope string, reason configstore.WorkerClaimMissReason, count int64) configstore.WarmCapacityMissAggregate {
	return configstore.WarmCapacityMissAggregate{
		Scope:  scope,
		Reason: reason,
		Count:  count,
	}
}
