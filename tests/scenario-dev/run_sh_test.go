package scenariodev_test

import (
	"os"
	"os/exec"
	"path/filepath"
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

func TestProvisionScenarioDoesNotRequireFrozenConfigSecret(t *testing.T) {
	binDir, callsPath := newFakeBin(t)
	cmd := exec.Command("bash", "run.sh", "test")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"SCENARIO_DEV_TEST_CALLS="+callsPath,
		"KUBE_CONTEXT=test-context",
		"SCENARIO_NAMESPACE=duckgres-ci-pr-1231",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=provision-smoke",
		"SCENARIO_FILE=tests/scenario/scenarios/provision_smoke.yaml",
		"DUCKGRES_SCENARIO_RUN_ID=scenario-dev-test",
		"DUCKGRES_SCENARIO_MAX_RUNTIME=30m",
		"DUCKGRES_SCENARIO_GO_TEST_TIMEOUT=45m",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("provision scenario should not require frozen config secret: %v\n%s", err, out)
	}
	callsRaw, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatalf("read fake kubectl calls: %v", err)
	}
	calls := string(callsRaw)
	if strings.Contains(calls, "get secret duckgres-scenario-config") {
		t.Fatalf("provision scenario unexpectedly read frozen config secret; calls:\n%s", calls)
	}
	if !strings.Contains(calls, "apply -f -") {
		t.Fatalf("provision scenario did not create the scenario Job; calls:\n%s", calls)
	}
}

func TestScenarioJobPinsArm64RunnerImage(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	script := string(raw)
	for _, required := range []string{
		"nodeSelector:",
		"kubernetes.io/arch: arm64",
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("scenario job manifest missing %q", required)
		}
	}
}

func TestScenarioDevScriptUsesRunSpecificJobNameAndClusterIPHostaddr(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	script := string(raw)
	for _, required := range []string{
		"DUCKGRES_SCENARIO_RUN_ID",
		"cksum",
		"jsonpath='{.spec.clusterIP}'",
		"SCENARIO_ISOLATED_SNI_SUFFIX",
		"SCENARIO_SHARED_SNI_SUFFIX",
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("scenario-dev script missing %q", required)
		}
	}
}

func newFakeBin(t *testing.T) (string, string) {
	t.Helper()

	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatalf("mkdir fake bin: %v", err)
	}
	callsPath := filepath.Join(dir, "calls.log")

	writeFake(t, binDir, "kubectl", `#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >> "$SCENARIO_DEV_TEST_CALLS"
if [[ "$*" == *" get secret duckgres-scenario-config "* ]]; then
  printf 'frozen config secret should not be required for this scenario\n' >&2
  exit 1
fi
if [[ "$*" == *" get svc duckgres-control-plane "* ]]; then
  printf '10.96.0.20'
  exit 0
fi
if [[ "$*" == *" apply -f -"* ]]; then
  cat >/dev/null
  exit 0
fi
if [[ "$*" == *" get job duckgres-scenario-provision-smoke-"* ]]; then
  printf 'True'
  exit 0
fi
if [[ "$*" == *" get pod -l job-name=duckgres-scenario-provision-smoke-"* ]]; then
  printf 'duckgres-scenario-provision-smoke-pod'
  exit 0
fi
exit 0
`)
	writeFake(t, binDir, "date", `#!/usr/bin/env bash
printf 'date %s\n' "$*" >> "$SCENARIO_DEV_TEST_CALLS"
if [[ "$*" == "+%s" ]]; then
  printf '2000000000'
  exit 0
fi
exec /bin/date "$@"
`)
	writeFake(t, binDir, "sleep", `#!/usr/bin/env bash
printf 'sleep %s\n' "$*" >> "$SCENARIO_DEV_TEST_CALLS"
`)

	return binDir, callsPath
}

func writeFake(t *testing.T, binDir, name, body string) {
	t.Helper()

	path := filepath.Join(binDir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write fake %s: %v", name, err)
	}
}
