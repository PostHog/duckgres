package scenario

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestScenarioRunScriptValidatesRequiredEnvVars(t *testing.T) {
	script := filepath.Join("..", "..", "scripts", "scenario_run.sh")
	cmd := exec.Command("bash", script, "--check-env")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected script to fail without required env vars")
	}
	text := string(out)
	for _, name := range []string{
		"DUCKGRES_SCENARIO_API_BASE",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET",
		"DUCKGRES_SCENARIO_PG_HOST",
		"DUCKGRES_SCENARIO_SNI_SUFFIX",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("script output %q missing %s", text, name)
		}
	}
}

func TestScenarioRunScriptCheckEnvIncludesScenarioRequiredEnv(t *testing.T) {
	script := filepath.Join("..", "..", "scripts", "scenario_run.sh")
	cmd := exec.Command("bash", script, "--check-env", "tests/scenario/scenarios/posthog_frozen_perf.yaml")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DUCKGRES_SCENARIO_API_BASE=http://127.0.0.1",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET=test-secret",
		"DUCKGRES_SCENARIO_PG_HOST=127.0.0.1",
		"DUCKGRES_SCENARIO_SNI_SUFFIX=.dev.example",
	}

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected script to fail without frozen perf required env vars")
	}
	text := string(out)
	for _, name := range []string{
		"DUCKGRES_SCENARIO_FROZEN_S3_URI",
		"DUCKGRES_SCENARIO_FLIGHT_ADDR",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("script output %q missing %s", text, name)
		}
	}
}

func TestDevScenarioWorkflowDefaultsToIsolatedDeploymentWithSharedDevOverride(t *testing.T) {
	workflowPath := filepath.Join("..", "..", ".github", "workflows", "scenario-dev.yml")
	raw, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("read dev scenario workflow: %v", err)
	}
	workflow := string(raw)

	for _, required := range []string{
		"name: scenario-dev",
		"workflow_dispatch:",
		"skip_slow:",
		"use_shared_dev:",
		"default: false",
		"schedule:",
		"id-token: write",
		"uses: ./.github/workflows/_image-build.yml",
		"image-name: duckgres",
		"tag: scenario-runner-${{ github.run_id }}-${{ github.run_attempt }}-arm64",
		"tag: scenario-duckgres-${{ github.run_id }}-${{ github.run_attempt }}-arm64",
		"tests/scenario-dev/run.sh deploy",
		"tests/scenario-dev/run.sh test",
		"tests/scenario-dev/run.sh diagnostics",
		"tests/scenario-dev/run.sh teardown",
		"tests/scenario/scenarios/provision_smoke.yaml",
		"tests/scenario/scenarios/provision_rejection.yaml",
		"tests/scenario/scenarios/posthog_frozen_metadata.yaml",
		"tests/scenario/scenarios/posthog_frozen_perf.yaml",
		"tests/scenario/scenarios/posthog_frozen_dbt.yaml",
		"if: ${{ github.event_name != 'workflow_dispatch' || !inputs.skip_slow || !matrix.slow }}",
		"if: ${{ github.event_name != 'workflow_dispatch' || !inputs.use_shared_dev }}",
		"if: ${{ github.event_name == 'workflow_dispatch' && inputs.use_shared_dev }}",
		"slow: true",
		"go test -count=1 ./tests/scenario ./tests/scenario-dev ./tests/e2e-mw-dev",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("workflow missing %q", required)
		}
	}

	for _, forbidden := range []string{
		"DUCKGRES_SCENARIO_API_BASE: ${{ secrets.",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET: ${{ secrets.",
		"DUCKGRES_SCENARIO_PG_HOST: ${{ secrets.",
		"DUCKGRES_SCENARIO_SNI_SUFFIX: ${{ secrets.",
		"DUCKGRES_SCENARIO_FROZEN_S3_URI: ${{ secrets.",
		"DUCKGRES_SCENARIO_FLIGHT_ADDR: ${{ secrets.",
		"posthog-mw-dev",
		"373313242555",
		"645773004826",
		"posthog-duckgres-scenario-frozen-data",
		"posthog-duckling-perfprodus",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("workflow contains internal detail %q", forbidden)
		}
	}
}
