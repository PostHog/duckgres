package e2emwdev_test

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestDeployFailsWhenSamePRDucklingsDoNotDelete(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "deploy")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("deploy succeeded despite stuck same-PR Ducklings; output:\n%s", out)
	}

	calls := fakes.calls(t)
	if strings.Contains(calls, "kubectl --context test-context apply -f -") {
		t.Fatalf("deploy applied manifests after Duckling delete wait failed; calls:\n%s", calls)
	}
}

func TestDeployFailureDoesNotDumpDucklingYAML(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "deploy")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("deploy succeeded despite stuck same-PR Ducklings; output:\n%s", out)
	}

	output := string(out)
	for _, secret := range []string{
		"internal-ci-bucket-name",
		"arn:aws:iam::123456789012:role/internal-ci-role",
		"internal-provider-config",
	} {
		if strings.Contains(output, secret) {
			t.Fatalf("deploy output leaked raw Duckling YAML value %q:\n%s", secret, output)
		}
	}
	if !strings.Contains(output, "finalizers=") {
		t.Fatalf("deploy output did not include a narrow stuck-Duckling summary:\n%s", output)
	}
}

func TestScheduledCleanupKeepsGoingWhenDucklingsDoNotDelete(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "e2e-cleanup")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("e2e-cleanup should be best-effort when stale Ducklings do not delete: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	if !strings.Contains(calls, "kubectl --context test-context delete namespace duckgres-ci-pr-123 --ignore-not-found --wait=false") {
		t.Fatalf("e2e-cleanup did not continue to namespace cleanup; calls:\n%s", calls)
	}
}

func TestScheduledCleanupDropsCnpgIdentifiers(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "e2e-cleanup")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("e2e-cleanup failed: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	for _, want := range []string{
		"DROP DATABASE IF EXISTS mdstore_ci_pr_123_cnpg WITH (FORCE);",
		"DROP ROLE IF EXISTS mdstore_ci_pr_123_cnpg;",
	} {
		if !strings.Contains(calls, want) {
			t.Fatalf("cleanup did not drop expected cnpg identifier %q; calls:\n%s", want, calls)
		}
	}
}

func TestTeardownContinuesCleanupWhenDucklingsDoNotDelete(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "teardown")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("teardown should report stuck same-PR Ducklings; output:\n%s", out)
	}

	calls := fakes.calls(t)
	for _, want := range []string{
		"aws eks list-pod-identity-associations",
		"kubectl --context test-context delete clusterrolebinding -l duckgres.posthog.com/ci-pr=123 --ignore-not-found",
		"kubectl --context test-context delete namespace duckgres-ci-pr-123 --ignore-not-found --wait=false",
	} {
		if !strings.Contains(calls, want) {
			t.Fatalf("teardown did not continue cleanup after Duckling delete wait failed; missing %q in calls:\n%s", want, calls)
		}
	}
}

func TestDeployRejectsMissingPRNumberBeforeCleanup(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "deploy", "PR_NUMBER=", "NAMESPACE=duckgres-ci-pr-")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("deploy succeeded without a PR number; output:\n%s", out)
	}

	calls := fakes.calls(t)
	if strings.Contains(calls, "duckling/ci-pr--") {
		t.Fatalf("deploy touched empty-PR Duckling names before validating PR identity; calls:\n%s", calls)
	}
}

func TestScenarioRunsSelectedScenarioAgainstIsolatedStack(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("selected scenario failed: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	for _, want := range []string{
		"kubectl --context test-context -n duckgres-ci-pr-123 get svc duckgres-control-plane -o jsonpath={.spec.clusterIP}",
		"kubectl --context test-context -n duckgres-ci-pr-123 apply -f -",
		"kubectl --context test-context -n duckgres-ci-pr-123 logs -f job/duckgres-scenario-fast-suite-",
	} {
		if !strings.Contains(calls, want) {
			t.Fatalf("selected scenario missing expected call %q; calls:\n%s", want, calls)
		}
	}
	for _, unwanted := range []string{"scenario-provision-rejection", "scenario-provision-smoke", "scenario-full-suite"} {
		if strings.Contains(calls, unwanted) {
			t.Fatalf("selected scenario unexpectedly ran %q; calls:\n%s", unwanted, calls)
		}
	}
	if !strings.Contains(calls, "s3://posthog-duckgres-scenario-frozen-data-mw-dev/frozen_v1/") {
		t.Fatalf("selected scenario did not pass the frozen dataset URI in the Job manifest; calls:\n%s", calls)
	}
	if strings.Contains(calls, "get secret duckgres-scenario-config") {
		t.Fatalf("selected scenario still reads a scenario config secret; calls:\n%s", calls)
	}
}

func TestScenarioDefaultsToFullSuite(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("default scenario failed: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	if !strings.Contains(calls, "kubectl --context test-context -n duckgres-ci-pr-123 logs -f job/duckgres-scenario-full-suite-") {
		t.Fatalf("default did not run full-suite; calls:\n%s", calls)
	}
	for _, unwanted := range []string{"scenario-provision-rejection", "scenario-provision-smoke"} {
		if strings.Contains(calls, unwanted) {
			t.Fatalf("default unexpectedly ran %q; calls:\n%s", unwanted, calls)
		}
	}
}

func TestRunScriptUsesMwDevPayloadLayout(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	script := string(raw)
	for _, want := range []string{
		"$HERE/e2e/harness.sh",
		"test-scenario",
		"SCENARIO_NAME",
		".ci.duckgres.local",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("run.sh missing %q", want)
		}
	}
	for _, forbidden := range []string{
		"test-scenario-full",
		"SCENARIO_FULL_FILES",
		"USE_SHARED_DEV",
		"SCENARIO_SHARED_",
		"SCENARIO_CONFIG_SECRET",
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("run.sh still contains shared-dev/config-secret path %q", forbidden)
		}
	}
}

func TestControlPlaneServiceExposesFlight(t *testing.T) {
	raw, err := os.ReadFile("manifests.tmpl.yaml")
	if err != nil {
		t.Fatalf("read manifests template: %v", err)
	}

	rendered := strings.NewReplacer(
		"${NAMESPACE}", "test-namespace",
		"${PR_NUMBER}", "123",
		"${CONTROLPLANE_IMAGE}", "example.invalid/duckgres:test",
		"${WORKER_IMAGE}", "example.invalid/duckgres:test",
		"${INTERNAL_SECRET}", "test-secret",
		"${INTERNAL_SECRET_FALLBACK}", "test-secret-fallback",
		"${USER_SECRET_KEY}", "test-user-secret-key",
	).Replace(string(raw))
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
	for {
		var manifest map[string]any
		err := decoder.Decode(&manifest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode manifests template: %v", err)
		}
		if manifest["kind"] != "Service" || manifestName(manifest) != "duckgres-control-plane" {
			continue
		}

		for _, port := range manifestPorts(manifest) {
			if port["name"] == "flight" && port["port"] == float64(8815) && port["targetPort"] == "flight" {
				return
			}
		}
		t.Fatalf("duckgres-control-plane Service does not expose flight port 8815 to targetPort flight")
	}

	t.Fatal("duckgres-control-plane Service missing from manifests template")
}

func TestE2EHarnessPodIsProtectedFromKarpenterDisruption(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}

	const jobMarker = "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: duckgres-harness"
	jobStart := strings.Index(string(raw), jobMarker)
	if jobStart < 0 {
		t.Fatal("duckgres-harness Job manifest missing from run.sh")
	}
	jobYAML := string(raw)[jobStart:]
	if jobEnd := strings.Index(jobYAML, "\nYAML"); jobEnd >= 0 {
		jobYAML = jobYAML[:jobEnd]
	}

	var manifest map[string]any
	if err := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(jobYAML), 4096).Decode(&manifest); err != nil {
		t.Fatalf("decode duckgres-harness Job manifest: %v", err)
	}
	spec, _ := manifest["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	metadata, _ := template["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	if got := annotations["karpenter.sh/do-not-disrupt"]; got != "true" {
		t.Fatalf("duckgres-harness Pod karpenter.sh/do-not-disrupt = %v, want true", got)
	}
}

func manifestName(manifest map[string]any) string {
	metadata, _ := manifest["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	return name
}

func manifestPorts(manifest map[string]any) []map[string]any {
	spec, _ := manifest["spec"].(map[string]any)
	rawPorts, _ := spec["ports"].([]any)
	ports := make([]map[string]any, 0, len(rawPorts))
	for _, rawPort := range rawPorts {
		if port, ok := rawPort.(map[string]any); ok {
			ports = append(ports, port)
		}
	}
	return ports
}

type runSHFakes struct {
	binDir  string
	logPath string
}

func newRunSHFakes(t *testing.T) runSHFakes {
	t.Helper()

	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatalf("mkdir fake bin: %v", err)
	}
	logPath := filepath.Join(dir, "calls.log")
	const internalSecretPath = "/tmp/duckgres-ci-internal-secret"
	if err := os.WriteFile(internalSecretPath, []byte("test-secret\n"), 0o600); err != nil {
		t.Fatalf("write fake internal secret: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(internalSecretPath) })

	writeFake(t, binDir, "kubectl", `#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >> "$RUN_SH_TEST_CALLS"

if [[ "$*" == *" apply -f -"* ]]; then
  tee -a "$RUN_SH_TEST_CALLS" >/dev/null
  exit 0
fi
if [[ "$*" == *" get svc duckgres-control-plane "* ]]; then
  printf '10.96.0.20'
  exit 0
fi
if [[ "$*" == *" get job duckgres-scenario-"* ]]; then
  if [[ -n "${SCENARIO_DEV_FAIL_JOB:-}" && "$*" == *"duckgres-scenario-${SCENARIO_DEV_FAIL_JOB}-"* ]]; then
    if [[ "$*" == *'@.type=="Failed"'* ]]; then
      printf 'True'
    fi
    exit 0
  fi
  if [[ "$*" == *'@.type=="Failed"'* ]]; then
    exit 0
  fi
  printf 'True'
  exit 0
fi
if [[ "$*" == *" get pod -l job-name=duckgres-scenario-"* ]]; then
  printf 'duckgres-scenario-pod'
  exit 0
fi
if [[ "$*" == *" wait --for=delete duckling/ci-pr-123-"* ]]; then
  exit 1
fi
if [[ "$*" == *" get duckling/ci-pr-123-"* && "$*" == *"-o jsonpath="* ]]; then
  printf '2026-06-30T00:00:00Z|[finalizer.crossplane.io]|Ready=False:Deleting;'
  exit 0
fi
if [[ "$*" == *" get duckling/ci-pr-123-"* ]]; then
  cat <<'YAML'
apiVersion: ducklings.posthog.com/v1
kind: Duckling
metadata:
  name: ci-pr-123-cnpg
  deletionTimestamp: "2026-06-30T00:00:00Z"
  finalizers:
    - finalizer.crossplane.io
spec:
  providerConfigRef:
    name: internal-provider-config
status:
  dataStore:
    bucketName: internal-ci-bucket-name
  roleArn: arn:aws:iam::123456789012:role/internal-ci-role
  conditions:
    - type: Ready
      status: "False"
      reason: Deleting
YAML
  exit 0
fi
if [[ "$*" == *" get ns -l app.kubernetes.io/managed-by=e2e-mw-dev "* ]]; then
  printf 'duckgres-ci-pr-123 2026-01-01T00:00:00Z\n'
  exit 0
fi
if [[ "$*" == *" get pod -l app=duckgres-control-plane "* ]]; then
  printf 'duckgres-control-plane-test'
  exit 0
fi
if [[ "$*" == *" exec duckgres-control-plane-test -- sh -c "* ]]; then
  printf 'http://pod-identity-credentials'
  exit 0
fi

exit 0
`)

	writeFake(t, binDir, "aws", `#!/usr/bin/env bash
printf 'aws %s\n' "$*" >> "$RUN_SH_TEST_CALLS"
if [[ "$*" == *" list-pod-identity-associations "* ]]; then
  printf 'None\n'
fi
exit 0
`)

	writeFake(t, binDir, "openssl", `#!/usr/bin/env bash
printf 'openssl %s\n' "$*" >> "$RUN_SH_TEST_CALLS"
printf 'test-secret\n'
`)

	writeFake(t, binDir, "envsubst", `#!/usr/bin/env bash
printf 'envsubst %s\n' "$*" >> "$RUN_SH_TEST_CALLS"
cat
`)

	writeFake(t, binDir, "curl", `#!/usr/bin/env bash
printf 'curl %s\n' "$*" >> "$RUN_SH_TEST_CALLS"
if [[ "$*" == *"/warehouse/status"* ]]; then
  printf '{"state":"deleted"}'
fi
exit 0
`)

	writeFake(t, binDir, "sleep", `#!/usr/bin/env bash
printf 'sleep %s\n' "$*" >> "$RUN_SH_TEST_CALLS"
`)

	writeFake(t, binDir, "date", `#!/usr/bin/env bash
printf 'date %s\n' "$*" >> "$RUN_SH_TEST_CALLS"
if [[ "$*" == "+%s" ]]; then
  printf '2000000000\n'
  exit 0
fi
if [[ "$*" == *"-d 2026-01-01T00:00:00Z +%s"* ]]; then
  printf '1767225600\n'
  exit 0
fi
exec /bin/date "$@"
`)

	return runSHFakes{binDir: binDir, logPath: logPath}
}

func writeFake(t *testing.T, binDir, name, body string) {
	t.Helper()
	path := filepath.Join(binDir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write fake %s: %v", name, err)
	}
}

func runSHCommand(t *testing.T, binDir, subcommand string, extraEnv ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.Command("bash", "run.sh", subcommand)
	cmd.Dir = "."
	env := append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"RUN_SH_TEST_CALLS="+filepath.Join(filepath.Dir(binDir), "calls.log"),
		"KUBE_CONTEXT=test-context",
		"NAMESPACE=duckgres-ci-pr-123",
		"PR_NUMBER=123",
		"WORKER_IMAGE=example.invalid/duckgres:test",
		"CONTROLPLANE_IMAGE=example.invalid/duckgres:test",
		"CP_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/duckgres-control-plane-dev",
		"EKS_CLUSTER_NAME=test-cluster",
		"AWS_REGION=us-east-1",
	)
	env = append(env, extraEnv...)
	cmd.Env = env
	return cmd
}

func (f runSHFakes) calls(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(f.logPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read fake calls: %v", err)
	}
	return string(b)
}
