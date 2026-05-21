package configresolve

import (
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/configloader"
	"github.com/posthog/duckgres/controlplane"
)

func TestResolveEffectiveDefaultsK8sWorkerServiceAccountToNeutralWorker(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if resolved.K8sWorkerServiceAccount != "duckgres-worker" {
		t.Fatalf("expected default K8s worker service account duckgres-worker, got %q", resolved.K8sWorkerServiceAccount)
	}
}

func TestResolveEffectiveExposesDuckLakeDefaultSpecVersionForControlPlane(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_DUCKLAKE_DEFAULT_SPEC_VERSION" {
			return "1.1"
		}
		return ""
	}, nil)

	if resolved.DuckLakeDefaultSpecVersion != "1.1" {
		t.Fatalf("expected DuckLake default spec version 1.1, got %q", resolved.DuckLakeDefaultSpecVersion)
	}
}

func TestResolveEffectiveDefaultsK8sDynamicWarmCapacityConfig(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if !resolved.K8sDynamicWarmCapacityEnabled {
		t.Fatal("expected dynamic warm capacity to default enabled")
	}
	if resolved.K8sWarmCapacityMissWindow != controlplane.DefaultWarmCapacityMissWindow {
		t.Fatalf("expected miss window %s, got %s", controlplane.DefaultWarmCapacityMissWindow, resolved.K8sWarmCapacityMissWindow)
	}
	if resolved.K8sWarmCapacityMissesPerWorker != controlplane.DefaultWarmCapacityMissesPerWorker {
		t.Fatalf("expected misses per worker %d, got %d", controlplane.DefaultWarmCapacityMissesPerWorker, resolved.K8sWarmCapacityMissesPerWorker)
	}
	if resolved.K8sWarmCapacityDemandTTL != controlplane.DefaultWarmCapacityDemandTTL {
		t.Fatalf("expected demand TTL %s, got %s", controlplane.DefaultWarmCapacityDemandTTL, resolved.K8sWarmCapacityDemandTTL)
	}
	if resolved.K8sWarmCapacityDynamicImageCeiling != 0 {
		t.Fatalf("expected image ceiling 0, got %d", resolved.K8sWarmCapacityDynamicImageCeiling)
	}
	if resolved.K8sWarmCapacityDynamicTotalCeiling != 0 {
		t.Fatalf("expected total ceiling 0, got %d", resolved.K8sWarmCapacityDynamicTotalCeiling)
	}
}

func TestResolveEffectiveK8sDynamicWarmCapacityPrecedence(t *testing.T) {
	fileEnabled := true
	fileCfg := &configloader.FileConfig{
		K8s: configloader.K8sFileConfig{
			DynamicWarmCapacityEnabled:      &fileEnabled,
			WarmCapacityMissWindow:          "3m",
			WarmCapacityMissesPerWorker:     5,
			WarmCapacityDemandTTL:           "12m",
			WarmCapacityDynamicImageCeiling: 2,
			WarmCapacityDynamicTotalCeiling: 4,
		},
	}
	env := map[string]string{
		"DUCKGRES_K8S_DYNAMIC_WARM_CAPACITY_ENABLED":       "true",
		"DUCKGRES_K8S_WARM_CAPACITY_MISS_WINDOW":           "4m",
		"DUCKGRES_K8S_WARM_CAPACITY_MISSES_PER_WORKER":     "6",
		"DUCKGRES_K8S_WARM_CAPACITY_DEMAND_TTL":            "13m",
		"DUCKGRES_K8S_WARM_CAPACITY_DYNAMIC_IMAGE_CEILING": "3",
		"DUCKGRES_K8S_WARM_CAPACITY_DYNAMIC_TOTAL_CEILING": "5",
	}
	cli := CLIInputs{
		Set: map[string]bool{
			"k8s-dynamic-warm-capacity-enabled":       true,
			"k8s-warm-capacity-miss-window":           true,
			"k8s-warm-capacity-misses-per-worker":     true,
			"k8s-warm-capacity-demand-ttl":            true,
			"k8s-warm-capacity-dynamic-image-ceiling": true,
			"k8s-warm-capacity-dynamic-total-ceiling": true,
		},
		K8sDynamicWarmCapacityEnabled:      false,
		K8sWarmCapacityMissWindow:          "5m",
		K8sWarmCapacityMissesPerWorker:     7,
		K8sWarmCapacityDemandTTL:           "14m",
		K8sWarmCapacityDynamicImageCeiling: 8,
		K8sWarmCapacityDynamicTotalCeiling: 9,
	}

	resolved := ResolveEffective(fileCfg, cli, func(key string) string {
		return env[key]
	}, nil)

	if resolved.K8sDynamicWarmCapacityEnabled {
		t.Fatal("expected explicit CLI false to override file/env true")
	}
	if resolved.K8sWarmCapacityMissWindow != 5*time.Minute {
		t.Fatalf("expected miss window 5m, got %s", resolved.K8sWarmCapacityMissWindow)
	}
	if resolved.K8sWarmCapacityMissesPerWorker != 7 {
		t.Fatalf("expected misses per worker 7, got %d", resolved.K8sWarmCapacityMissesPerWorker)
	}
	if resolved.K8sWarmCapacityDemandTTL != 14*time.Minute {
		t.Fatalf("expected demand TTL 14m, got %s", resolved.K8sWarmCapacityDemandTTL)
	}
	if resolved.K8sWarmCapacityDynamicImageCeiling != 8 {
		t.Fatalf("expected image ceiling 8, got %d", resolved.K8sWarmCapacityDynamicImageCeiling)
	}
	if resolved.K8sWarmCapacityDynamicTotalCeiling != 9 {
		t.Fatalf("expected total ceiling 9, got %d", resolved.K8sWarmCapacityDynamicTotalCeiling)
	}
}

func TestResolveEffectiveK8sDynamicWarmCapacityRejectsInvalidValues(t *testing.T) {
	fileEnabled := true
	fileCfg := &configloader.FileConfig{
		K8s: configloader.K8sFileConfig{
			DynamicWarmCapacityEnabled:      &fileEnabled,
			WarmCapacityMissWindow:          "not-a-duration",
			WarmCapacityMissesPerWorker:     -1,
			WarmCapacityDemandTTL:           "also-not-a-duration",
			WarmCapacityDynamicImageCeiling: -2,
			WarmCapacityDynamicTotalCeiling: -3,
		},
	}
	var warnings []string

	resolved := ResolveEffective(fileCfg, CLIInputs{}, nil, func(msg string) {
		warnings = append(warnings, msg)
	})

	if !resolved.K8sDynamicWarmCapacityEnabled {
		t.Fatal("expected valid enabled flag to be preserved")
	}
	if resolved.K8sWarmCapacityMissWindow != controlplane.DefaultWarmCapacityMissWindow {
		t.Fatalf("expected default miss window, got %s", resolved.K8sWarmCapacityMissWindow)
	}
	if resolved.K8sWarmCapacityMissesPerWorker != controlplane.DefaultWarmCapacityMissesPerWorker {
		t.Fatalf("expected default misses per worker, got %d", resolved.K8sWarmCapacityMissesPerWorker)
	}
	if resolved.K8sWarmCapacityDemandTTL != controlplane.DefaultWarmCapacityDemandTTL {
		t.Fatalf("expected default demand TTL, got %s", resolved.K8sWarmCapacityDemandTTL)
	}
	if resolved.K8sWarmCapacityDynamicImageCeiling != 0 {
		t.Fatalf("expected negative image ceiling to reset to 0, got %d", resolved.K8sWarmCapacityDynamicImageCeiling)
	}
	if resolved.K8sWarmCapacityDynamicTotalCeiling != 0 {
		t.Fatalf("expected negative total ceiling to reset to 0, got %d", resolved.K8sWarmCapacityDynamicTotalCeiling)
	}
	wantSubstrings := []string{
		"Invalid k8s.warm_capacity_miss_window duration",
		"Invalid k8s.warm_capacity_demand_ttl duration",
		"k8s warm_capacity_misses_per_worker must be > 0",
		"k8s warm_capacity_dynamic_image_ceiling must be >= 0",
		"k8s warm_capacity_dynamic_total_ceiling must be >= 0",
	}
	for _, want := range wantSubstrings {
		if !containsWarning(warnings, want) {
			t.Fatalf("expected warning containing %q in %v", want, warnings)
		}
	}
}

func TestResolveEffectiveK8sDynamicWarmCapacityClampsDemandTTLToMissWindow(t *testing.T) {
	fileCfg := &configloader.FileConfig{
		K8s: configloader.K8sFileConfig{
			WarmCapacityMissWindow: "2m",
			WarmCapacityDemandTTL:  "30s",
		},
	}
	var warnings []string

	resolved := ResolveEffective(fileCfg, CLIInputs{}, nil, func(msg string) {
		warnings = append(warnings, msg)
	})

	if resolved.K8sWarmCapacityMissWindow != 2*time.Minute {
		t.Fatalf("expected miss window 2m, got %s", resolved.K8sWarmCapacityMissWindow)
	}
	if resolved.K8sWarmCapacityDemandTTL != 2*time.Minute {
		t.Fatalf("expected demand TTL to clamp to 2m, got %s", resolved.K8sWarmCapacityDemandTTL)
	}
	if !containsWarning(warnings, "k8s warm_capacity_demand_ttl must be >= warm_capacity_miss_window") {
		t.Fatalf("expected clamp warning in %v", warnings)
	}
}

func containsWarning(warnings []string, substr string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, substr) {
			return true
		}
	}
	return false
}
