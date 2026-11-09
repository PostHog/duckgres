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
		"DUCKGRES_SCENARIO_ORG_ID",
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
	cmd := exec.Command("bash", script, "--check-env", "tests/mw-dev/scenario/scenarios/full-suite.yaml")
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"DUCKGRES_SCENARIO_API_BASE=http://127.0.0.1",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET=test-secret",
		"DUCKGRES_SCENARIO_PG_HOST=127.0.0.1",
		"DUCKGRES_SCENARIO_SNI_SUFFIX=.dev.example",
	}

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected script to fail without full-suite required env vars")
	}
	text := string(out)
	for _, name := range []string{
		"DUCKGRES_SCENARIO_ORG_ID",
		"DUCKGRES_SCENARIO_FROZEN_S3_URI",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("script output %q missing %s", text, name)
		}
	}
	if strings.Contains(text, "DUCKGRES_SCENARIO_FLIGHT") {
		t.Fatalf("full-suite should not require the deprecated Flight endpoint; output: %q", text)
	}
}

func TestDevScenarioWorkflowPreservesIsolationCleanupAndPublishing(t *testing.T) {
	workflowPath := filepath.Join("..", "..", "..", ".github", "workflows", "scenario-dev.yml")
	raw, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("read dev scenario workflow: %v", err)
	}
	workflow := string(raw)

	for _, required := range []string{
		"PR_NUMBER: ${{ github.run_id }}\n",
		"NAMESPACE: duckgres-ci-pr-${{ github.run_id }}\n",
		"- name: Teardown\n        if: always()\n        run: tests/mw-dev/run.sh teardown",
		"github.ref == 'refs/heads/main'",
		"--connection-secret-stdin",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("workflow missing %q", required)
		}
	}

	for _, forbidden := range []string{
		"use_shared_dev:",
		"USE_SHARED_DEV",
		"SCENARIO_SHARED_",
		"posthog_frozen_perf",
		"coverage_target:",
		"properties_s3_uri:",
		"DUCKGRES_SCENARIO_API_BASE: ${{ secrets.",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET: ${{ secrets.",
		"DUCKGRES_SCENARIO_PG_HOST: ${{ secrets.",
		"postgres://",
		"postgresql://",
		"--dsn",
		"--password",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("workflow contains internal detail %q", forbidden)
		}
	}

	teardownIndex := strings.Index(workflow, "- name: Teardown")
	publishPerfIndex := strings.Index(workflow, "- name: Publish scenario perf results")
	uploadIndex := strings.Index(workflow, "- name: Upload scenario artifacts")
	if teardownIndex < 0 || uploadIndex < teardownIndex || publishPerfIndex < uploadIndex {
		t.Fatalf("artifact upload must run after teardown and before perf publishing")
	}
	for _, required := range []string{
		"timeout-minutes: 10",
		"mapfile -d '' perf_summaries",
		"for perf_summary in \"${perf_summaries[@]}\"",
		"--publish-timeout 2m",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("perf publishing must support bounded publication of every result: missing %q", required)
		}
	}
	if strings.Contains(workflow, "-path '*/perf/summary.json' -print -quit") {
		t.Fatal("perf publishing must not silently select only the first result")
	}
}
