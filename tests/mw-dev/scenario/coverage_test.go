package scenario

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

func TestCoverageScenariosPreserveBaselineAndFocusedIsolation(t *testing.T) {
	for _, focused := range []bool{false, true} {
		name := "posthog_frozen_perf"
		if focused {
			name += "_coverage_uncached"
		}
		t.Run(name, func(t *testing.T) {
			scenario, err := core.LoadScenario(filepath.Join("scenarios", name+".yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var coverage *core.Step
			var validation *core.Step
			for i := range scenario.Steps {
				step := &scenario.Steps[i]
				if step.ID == "validate_coverage" {
					validation = step
				}
				if step.ID == "coverage_queries" {
					coverage = step
				}
				if focused && (strings.Contains(step.Type, "trino") || step.Type == "setup_hoglake" || step.Type == "properties_comparison") {
					t.Fatalf("focused run includes %s", step.Type)
				}
			}
			if validation == nil || validation.With["file"] != "../sql/validate_posthog_coverage.sql" {
				t.Fatal("missing coverage fixture validation")
			}
			if coverage == nil {
				t.Fatal("missing coverage phase")
			}
			if !reflect.DeepEqual(coverage.DependsOn, []string{"validate_coverage"}) {
				t.Fatal("coverage must depend on fixture validation")
			}
			if coverage.With["catalog_file"] != "../../../perf/queries/ducklake_posthog_coverage.yaml" {
				t.Fatal("wrong coverage catalog")
			}
			if coverage.With["output_subdir"] != "perf-coverage" || coverage.With["run_id"] != "${run_id}-coverage" || coverage.With["nightly_run_id"] != "${run_id}" {
				t.Fatal("coverage must have separate artifacts and shared nightly identity")
			}
			if coverage.With["suite"] != "coverage" {
				t.Fatal("coverage must have its own comparison suite")
			}
			if coverage.With["fail_on_query_errors"] != true {
				t.Fatal("coverage errors must fail")
			}
			if focused {
				if !reflect.DeepEqual(coverage.With["targets"], []any{"pgwire_uncached"}) {
					t.Fatalf("focused targets = %v", coverage.With["targets"])
				}
				for _, env := range scenario.RequiredEnv {
					if strings.Contains(env, "TRINO") || strings.Contains(env, "ATHENA") || strings.Contains(env, "HOGLAKE") {
						t.Fatalf("unnecessary focused requirement %s", env)
					}
				}
				request := scenario.Steps[0].With["request"].(map[string]any)
				if _, ok := request["trino"]; ok {
					t.Fatal("focused run provisions Trino")
				}
			} else {
				assertPerfTargetsPGWireTrinoAndAthena(t, *coverage)
			}
		})
	}
}
