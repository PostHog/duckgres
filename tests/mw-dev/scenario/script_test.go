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

func TestScenarioWorkflowsUseNode24Actions(t *testing.T) {
	scenarioWorkflow, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", "scenario-dev.yml"))
	if err != nil {
		t.Fatalf("read scenario workflow: %v", err)
	}
	imageWorkflow, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", "_image-build.yml"))
	if err != nil {
		t.Fatalf("read image workflow: %v", err)
	}

	for _, required := range []string{
		"actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0",
		"actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16 # v6.5.0",
		"actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a # v7.0.1",
		"aws-actions/configure-aws-credentials@517a711dbcd0e402f90c77e7e2f81e849156e31d # v6.2.2",
		"azure/setup-kubectl@829323503d1be3d00ca8346e5391ca0b07a9ab0d # v5.1.0",
	} {
		if !strings.Contains(string(scenarioWorkflow), required) {
			t.Fatalf("scenario workflow missing Node 24 action pin %q", required)
		}
	}
	for _, required := range []string{
		"actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0",
		"docker/setup-buildx-action@bb05f3f5519dd87d3ba754cc423b652a5edd6d2c # v4.2.0",
		"aws-actions/configure-aws-credentials@517a711dbcd0e402f90c77e7e2f81e849156e31d # v6.2.2",
		"aws-actions/amazon-ecr-login@d539f0932e70871a027e9d5a9d8fc38589180a64 # v2.1.6",
		"docker/build-push-action@53b7df96c91f9c12dcc8a07bcb9ccacbed38856a # v7.3.0",
	} {
		if !strings.Contains(string(imageWorkflow), required) {
			t.Fatalf("image workflow missing Node 24 action pin %q", required)
		}
	}
}
