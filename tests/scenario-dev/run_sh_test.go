package scenariodev_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestDeploySharedDevSkipsIsolatedStack(t *testing.T) {
	cmd := exec.Command("bash", "run.sh", "deploy")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"KUBE_CONTEXT=test-context",
		"USE_SHARED_DEV=true",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("shared deploy skip failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "Skipping isolated Duckgres deploy") {
		t.Fatalf("shared deploy did not explain skip:\n%s", out)
	}
}

func TestSharedDevTestRequiresSharedConnectionConfig(t *testing.T) {
	cmd := exec.Command("bash", "run.sh", "test")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"KUBE_CONTEXT=test-context",
		"USE_SHARED_DEV=true",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=provision-smoke",
		"SCENARIO_FILE=tests/scenario/scenarios/provision_smoke.yaml",
		"DUCKGRES_SCENARIO_RUN_ID=scenario-dev-test",
		"DUCKGRES_SCENARIO_MAX_RUNTIME=30m",
		"DUCKGRES_SCENARIO_GO_TEST_TIMEOUT=45m",
	)

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("shared test succeeded without shared connection config:\n%s", out)
	}
	if !strings.Contains(string(out), "SCENARIO_SHARED_API_BASE is required") {
		t.Fatalf("shared test did not report missing API base:\n%s", out)
	}
}
