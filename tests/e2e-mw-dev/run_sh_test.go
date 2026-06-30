package e2emwdev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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

func TestScheduledCleanupDropsCurrentAndLegacyCnpgIdentifiers(t *testing.T) {
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
		"DROP DATABASE IF EXISTS lakekeeper_ci_pr_123_cnpg WITH (FORCE);",
		"DROP ROLE IF EXISTS lakekeeper_ci_pr_123_cnpg;",
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

	writeFake(t, binDir, "kubectl", `#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >> "$RUN_SH_TEST_CALLS"

if [[ "$*" == *" apply -f -"* ]]; then
  cat >/dev/null
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
