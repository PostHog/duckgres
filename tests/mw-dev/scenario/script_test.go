package scenario

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestScenarioRunScriptValidatesRequiredEnvVars(t *testing.T) {
	script := filepath.Join("..", "..", "..", "scripts", "scenario_run.sh")
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
	script := filepath.Join("..", "..", "..", "scripts", "scenario_run.sh")
	cmd := exec.Command("bash", script, "--check-env", "tests/mw-dev/scenario/scenarios/posthog_frozen_perf.yaml")
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

func TestDevScenarioWorkflowUsesUnifiedMwDevHarness(t *testing.T) {
	workflowPath := filepath.Join("..", "..", "..", ".github", "workflows", "scenario-dev.yml")
	raw, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("read dev scenario workflow: %v", err)
	}
	workflow := string(raw)

	for _, required := range []string{
		"name: scenario-dev",
		"workflow_dispatch:",
		"scenario:",
		"default: full-suite",
		"- name: Scenario dev target selected\n        env:\n          SCENARIO_NAME: ${{ inputs.scenario || 'full-suite' }}",
		"- name: Run selected scenario\n        env:\n          SCENARIO_NAME: ${{ inputs.scenario || 'full-suite' }}",
		"schedule:",
		"id-token: write",
		"uses: ./.github/workflows/_image-build.yml",
		"image-name: duckgres",
		"tag: scenario-runner-${{ github.run_id }}-${{ github.run_attempt }}-arm64",
		"tag: scenario-duckgres-${{ github.run_id }}-${{ github.run_attempt }}-arm64",
		"inputs.duckgres_image != ''",
		"KUBE_CONTEXT: posthog-mw-dev",
		"CLUSTER_NAME: posthog-mw-dev",
		"EKS_CLUSTER_NAME: posthog-mw-dev",
		"CP_POD_IDENTITY_ROLE: arn:aws:iam::${{ secrets.MW_DEV_ACCOUNT_ID }}:role/duckgres-control-plane-dev",
		"tests/mw-dev/run.sh deploy",
		"tests/mw-dev/run.sh test-scenario",
		"tests/mw-dev/run.sh diagnostics",
		"tests/mw-dev/run.sh teardown",
		"go test -count=1 ./tests/mw-dev/scenario ./tests/mw-dev",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("workflow missing %q", required)
		}
	}

	for _, forbidden := range []string{
		"skip_slow:",
		"inputs.skip_slow",
		"scenario-skipped:",
		"test-scenario-full",
		"use_shared_dev:",
		"USE_SHARED_DEV",
		"SCENARIO_SHARED_",
		"DUCKGRES_SCENARIO_KUBE_CONTEXT",
		"DUCKGRES_SCENARIO_EKS_CLUSTER_NAME",
		"DUCKGRES_SCENARIO_CP_POD_IDENTITY_ROLE",
		"DUCKGRES_SCENARIO_CONFIG_SECRET",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET_NAME",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET_KEY",
		"matrix:",
		"DUCKGRES_SCENARIO_API_BASE: ${{ secrets.",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET: ${{ secrets.",
		"DUCKGRES_SCENARIO_PG_HOST: ${{ secrets.",
		"DUCKGRES_SCENARIO_SNI_SUFFIX: ${{ secrets.",
		"DUCKGRES_SCENARIO_FROZEN_S3_URI: ${{ secrets.",
		"DUCKGRES_SCENARIO_FLIGHT_ADDR: ${{ secrets.",
		"373313242555",
		"645773004826",
		"posthog-duckling-perfprodus",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("workflow contains internal detail %q", forbidden)
		}
	}
}

func TestScenarioRunnerImageCachesGoDependencies(t *testing.T) {
	dockerfilePath := filepath.Join("Dockerfile")
	raw, err := os.ReadFile(dockerfilePath)
	if err != nil {
		t.Fatalf("read scenario runner Dockerfile: %v", err)
	}
	dockerfile := string(raw)

	for _, required := range []string{
		"go mod download",
		"go test -run '^$' ./tests/mw-dev/scenario",
	} {
		if !strings.Contains(dockerfile, required) {
			t.Fatalf("scenario runner Dockerfile missing %q", required)
		}
	}
}
