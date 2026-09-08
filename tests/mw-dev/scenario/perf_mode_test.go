package scenario

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

func TestLoadScenarioForRunTrinoOnly(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_PERF_MODE", "trino-only")
	path := filepath.Join("scenarios", "posthog_frozen_perf.yaml")
	original, err := core.LoadScenario(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range original.RequiredEnv {
		value := "test-value"
		if strings.HasPrefix(key, "DUCKGRES_SCENARIO_ATHENA_") {
			value = ""
		}
		t.Setenv(key, value)
	}
	got, absPath, err := loadScenarioForRun(path)
	if err != nil {
		t.Fatal(err)
	}
	if missing := missingRequiredEnv(got); len(missing) != 0 {
		t.Fatalf("Trino-only requires unrelated environment: %v", missing)
	}
	if _, err := resolveRunTemplates(got, "test-run"); err != nil {
		t.Fatalf("Trino-only must resolve without Athena configuration: %v", err)
	}
	want := resolveScenarioFilePaths(original, filepath.Dir(absPath))
	if len(got.Steps) != len(want.Steps) {
		t.Fatal("Trino-only must retain provisioning, table setup, validation and cleanup")
	}
	for i, step := range got.Steps {
		if step.Type != "perf_queries" {
			if !reflect.DeepEqual(step, want.Steps[i]) {
				t.Fatalf("non-perf step %s changed", step.ID)
			}
			continue
		}
		if !reflect.DeepEqual(step.With["targets"], []any{"trino"}) {
			t.Fatalf("targets=%v, want only trino", step.With["targets"])
		}
		for key, value := range want.Steps[i].With {
			if strings.HasPrefix(key, "athena_") {
				if _, exists := step.With[key]; exists {
					t.Errorf("unused Athena field %s retained", key)
				}
			} else if key != "targets" && !reflect.DeepEqual(step.With[key], value) {
				t.Errorf("shared perf field %s changed", key)
			}
		}
	}
	for _, key := range got.RequiredEnv {
		if strings.HasPrefix(key, "DUCKGRES_SCENARIO_ATHENA_") {
			t.Errorf("unused required environment retained: %s", key)
		}
	}
}

func TestLoadScenarioForRunFullModeUnchanged(t *testing.T) {
	for _, file := range []string{"posthog_frozen_perf.yaml", "full-suite.yaml"} {
		path := filepath.Join("scenarios", file)
		t.Setenv("DUCKGRES_SCENARIO_PERF_MODE", "")
		want, _, err := loadScenarioForRun(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("DUCKGRES_SCENARIO_PERF_MODE", "full")
		got, _, err := loadScenarioForRun(path)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("full mode changed %s: %v", file, err)
		}
	}
}

func TestLoadScenarioForRunRejectsInvalidPerfMode(t *testing.T) {
	for _, tc := range []struct{ file, mode string }{
		{"posthog_frozen_perf.yaml", "typo"},
		{"full-suite.yaml", "trino-only"},
	} {
		t.Setenv("DUCKGRES_SCENARIO_PERF_MODE", tc.mode)
		if _, _, err := loadScenarioForRun(filepath.Join("scenarios", tc.file)); err == nil {
			t.Fatalf("accepted mode %q for %s", tc.mode, tc.file)
		}
	}
}
