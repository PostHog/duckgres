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
		"name: artifact-keeper",
		"value: \"isolated-test-secret\"",
		"name: DUCKGRES_SCENARIO_ORG_ID, value: \"ci-pr-123-cnpg\"",
		"kubectl --context test-context -n duckgres-ci-pr-123 logs -f pod/duckgres-scenario-pod",
		"-c scenario",
		"kubectl --context test-context -n duckgres-ci-pr-123 wait --for=jsonpath={.status.containerStatuses[?(@.name==\"scenario\")].state.terminated.reason} pod/duckgres-scenario-pod",
		"kubectl --context test-context -n duckgres-ci-pr-123 cp -c artifact-keeper duckgres-scenario-pod:/artifacts/scenario-dev/scenario-dev-fast-suite-123/.",
		"kubectl --context test-context -n duckgres-ci-pr-123 exec -c artifact-keeper duckgres-scenario-pod -- touch /artifacts/artifacts-collected",
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
	if strings.Contains(calls, "wait --for=condition=ready pod") {
		t.Fatalf("selected scenario waited for whole-Pod readiness instead of following the selected container; calls:\n%s", calls)
	}
	copyAt := strings.Index(calls, " cp -c artifact-keeper ")
	terminatedAt := strings.Index(calls, " wait --for=jsonpath={.status.containerStatuses")
	waitAt := strings.Index(calls, " get job duckgres-scenario-fast-suite-")
	if terminatedAt < 0 || copyAt < 0 || waitAt < 0 || terminatedAt > copyAt || copyAt > waitAt {
		t.Fatalf("scenario must terminate, copy from the live keeper, then wait for Job completion; calls:\n%s", calls)
	}
}

func TestScenarioStopsJobWithoutCollectingArtifactsWhenContainerTerminationCannotBeConfirmed(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
		"SCENARIO_DEV_FAIL_CONTAINER_WAIT=1",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("scenario succeeded without confirmed container termination; output:\n%s", out)
	}

	calls := fakes.calls(t)
	waitAt := strings.Index(calls, " wait --for=jsonpath={.status.containerStatuses")
	deleteAt := strings.LastIndex(calls, " delete job duckgres-scenario-fast-suite-")
	if waitAt < 0 || deleteAt < 0 || waitAt > deleteAt {
		t.Fatalf("scenario did not stop the Job after termination wait failed; calls:\n%s", calls)
	}
	if !strings.Contains(calls[deleteAt:], "--cascade=foreground --wait=true --timeout=180s") {
		t.Fatalf("scenario did not wait for foreground Job cleanup; calls:\n%s", calls)
	}
	for _, unwanted := range []string{" cp -c artifact-keeper ", " exec -c artifact-keeper "} {
		if strings.Contains(calls, unwanted) {
			t.Fatalf("scenario collected or released artifacts after unconfirmed termination (%q); calls:\n%s", unwanted, calls)
		}
	}
}

func TestScenarioCleansUpWhenPodCannotBeDiscovered(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
		"SCENARIO_DEV_NO_POD=1",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("scenario succeeded without a discoverable pod; output:\n%s", out)
	}

	calls := fakes.calls(t)
	if !strings.Contains(calls, "delete job duckgres-scenario-fast-suite-") || !strings.Contains(calls, "--cascade=foreground --wait=true --timeout=180s") {
		t.Fatalf("scenario did not clean up a Job whose pod could not be discovered; calls:\n%s", calls)
	}
}

func TestScenarioDoesNotAcceptStaleArtifactsAfterEmptyCopy(t *testing.T) {
	fakes := newRunSHFakes(t)
	staleDir := filepath.Join(filepath.Dir(fakes.binDir), "scenario-artifacts", "fast-suite", "old-run")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatalf("create stale artifact dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleDir, "scenario_summary.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write stale artifact: %v", err)
	}

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
		"SCENARIO_DEV_EMPTY_COPY=1",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("scenario accepted stale artifacts after an empty copy; output:\n%s", out)
	}
	if !strings.Contains(string(out), "missing required scenario artifact") {
		t.Fatalf("scenario did not report missing required artifacts; output:\n%s", out)
	}
	assertVisiblePartialArtifact(t, filepath.Join(filepath.Dir(fakes.binDir), "scenario-artifacts"), "fast-suite")
}

func TestScenarioPreservesContainerExitWhenKeeperReleaseFails(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
		"SCENARIO_DEV_FAIL_RELEASE=1",
		"SCENARIO_DEV_EXIT_CODE=7",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("scenario succeeded despite failed scenario and keeper release; output:\n%s", out)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 7 {
		t.Fatalf("scenario exit = %v, want preserved container exit 7; output:\n%s", err, out)
	}

	calls := fakes.calls(t)
	exitAt := strings.Index(calls, "state.terminated.exitCode")
	deleteAt := strings.LastIndex(calls, "delete job duckgres-scenario-fast-suite-")
	if exitAt < 0 || deleteAt < 0 || exitAt > deleteAt {
		t.Fatalf("scenario did not capture container exit before deleting stuck Job; calls:\n%s", calls)
	}
}

func TestScenarioFailsWhenArtifactCopyFails(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
		"SCENARIO_DEV_FAIL_COPY=1",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("scenario succeeded despite artifact copy failure; output:\n%s", out)
	}
	if !strings.Contains(string(out), "Failed to copy scenario artifacts") {
		t.Fatalf("scenario did not report artifact copy failure; output:\n%s", out)
	}

	calls := fakes.calls(t)
	if !strings.Contains(calls, "exec -c artifact-keeper duckgres-scenario-pod -- touch /artifacts/artifacts-collected") {
		t.Fatalf("scenario did not release artifact keeper after copy failure; calls:\n%s", calls)
	}
	assertVisiblePartialArtifact(t, filepath.Join(filepath.Dir(fakes.binDir), "scenario-artifacts"), "fast-suite")
}

func TestScenarioSuccessfulRerunsPreserveEachArtifactSet(t *testing.T) {
	fakes := newRunSHFakes(t)
	artifactRoot := filepath.Join(filepath.Dir(fakes.binDir), "scenario-artifacts")

	for run := 1; run <= 2; run++ {
		cmd := runSHCommand(t, fakes.binDir, "test-scenario",
			"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
			"SCENARIO_NAME=fast-suite",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("scenario rerun %d failed: %v\n%s", run, err, out)
		}
	}

	entries, err := os.ReadDir(artifactRoot)
	if err != nil {
		t.Fatalf("read artifact root: %v", err)
	}
	var successful []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			t.Fatalf("artifact staging directory remained hidden after success: %s", entry.Name())
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "fast-suite-") && !strings.HasSuffix(entry.Name(), ".partial") {
			successful = append(successful, filepath.Join(artifactRoot, entry.Name()))
		}
	}
	if len(successful) != 2 {
		t.Fatalf("successful artifact directories = %v, want two run-specific directories", successful)
	}
	for _, dir := range successful {
		for _, artifact := range []string{"scenario_summary.json", "scenario_summary.md", "step_results.csv", "events.jsonl"} {
			if _, err := os.Stat(filepath.Join(dir, artifact)); err != nil {
				t.Fatalf("successful artifact directory %s missing %s: %v", dir, artifact, err)
			}
		}
	}
}

func TestScenarioFailsWhenMarkdownSummaryIsMissing(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
		"SCENARIO_DEV_MISSING_MARKDOWN_SUMMARY=1",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("scenario succeeded without scenario_summary.md; output:\n%s", out)
	}
	if !strings.Contains(string(out), "scenario_summary.md") {
		t.Fatalf("scenario did not identify the missing Markdown summary; output:\n%s", out)
	}
}

func TestScenarioArtifactTokenCollisionCreatesANewResultDirectory(t *testing.T) {
	fakes := newRunSHFakes(t)
	artifactRoot := filepath.Join(filepath.Dir(fakes.binDir), "scenario-artifacts")
	priorDir := filepath.Join(artifactRoot, "fast-suite-COLLIDE")
	if err := os.MkdirAll(priorDir, 0o755); err != nil {
		t.Fatalf("create prior artifact directory: %v", err)
	}
	priorMarker := filepath.Join(priorDir, "prior-run.txt")
	if err := os.WriteFile(priorMarker, []byte("prior\n"), 0o600); err != nil {
		t.Fatalf("write prior artifact marker: %v", err)
	}

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
		"SCENARIO_NAME=fast-suite",
		"SCENARIO_DEV_MKTEMP_COLLISION_ONCE=1",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scenario failed after artifact token collision: %v\n%s", err, out)
	}

	if contents, err := os.ReadFile(priorMarker); err != nil || string(contents) != "prior\n" {
		t.Fatalf("prior artifact directory was modified: contents=%q err=%v", contents, err)
	}
	if _, err := os.Stat(filepath.Join(priorDir, ".fast-suite.COLLIDE")); !os.IsNotExist(err) {
		t.Fatalf("new artifact staging directory was nested into the prior result: %v", err)
	}
	entries, err := os.ReadDir(artifactRoot)
	if err != nil {
		t.Fatalf("read artifact root: %v", err)
	}
	var successful int
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "fast-suite-") && !strings.HasSuffix(entry.Name(), ".partial") {
			successful++
		}
	}
	if successful != 2 {
		t.Fatalf("successful artifact directory count = %d, want prior and new results; entries=%v", successful, entries)
	}
}

func TestScenarioDefaultsToFullSuite(t *testing.T) {
	t.Setenv("SCENARIO_NAME", "")
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "test-scenario",
		"SCENARIO_RUNNER_IMAGE=example.invalid/duckgres:scenario",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("default scenario failed: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	if !strings.Contains(calls, "kubectl --context test-context -n duckgres-ci-pr-123 logs -f pod/duckgres-scenario-pod -c scenario") {
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
		"internal_secret_file=\"/tmp/duckgres-ci-internal-secret\"",
		"internal_secret_fallback_file=\"/tmp/duckgres-ci-internal-secret-fallback\"",
		"user_secret_key_file=\"/tmp/duckgres-ci-user-secret-key\"",
		"USE_SHARED_DEV",
		"SCENARIO_SHARED_",
		"SCENARIO_CONFIG_SECRET",
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("run.sh still contains shared-dev/config-secret path %q", forbidden)
		}
	}
}

func TestE2EHarnessUsesOnlyCnpgMetadataStores(t *testing.T) {
	harnessRaw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}
	runRaw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}

	harness := string(harnessRaw)
	for _, legacy := range []string{
		`EXT="ci-pr-${PR_NUMBER}-ext"`,
		"EXT_BODY=",
		"EXT_RDS_",
		"ext_rds_",
		"mdstore_e2e_",
		"lane_ext",
		"reshard_ext_to_cnpg",
		`"metadata_store":{"type":"external"`,
	} {
		if strings.Contains(harness, legacy) {
			t.Errorf("e2e harness still contains external-metadata lane or RDS lifecycle marker %q", legacy)
		}
	}
	if strings.Contains(string(runRaw), "ci-pr-${pr}-ext") {
		t.Error("run.sh still includes the external-metadata org in e2e lifecycle cleanup")
	}
}

func TestDeployCreatesConfiguredSecretDirectoryPrivately(t *testing.T) {
	fakes := newRunSHFakes(t)
	secretDir := filepath.Join(filepath.Dir(fakes.binDir), "generated", "secrets")

	cmd := runSHCommand(t, fakes.binDir, "deploy",
		"SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1",
		"DUCKGRES_CI_SECRET_DIR="+secretDir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("deploy did not create configured secret directory: %v\n%s", err, out)
	}
	for _, name := range []string{
		"duckgres-ci-internal-secret",
		"duckgres-ci-internal-secret-fallback",
		"duckgres-ci-user-secret-key",
	} {
		info, statErr := os.Stat(filepath.Join(secretDir, name))
		if statErr != nil {
			t.Fatalf("generated secret %s missing: %v", name, statErr)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("generated secret %s mode = %o, want 600", name, got)
		}
	}
	info, statErr := os.Stat(secretDir)
	if statErr != nil {
		t.Fatalf("configured secret directory missing: %v", statErr)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("configured secret directory mode = %o, want 700", got)
	}
}

func TestScenarioPodIsProtectedFromKarpenterDisruption(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}

	const jobMarker = "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: $job"
	jobStart := strings.Index(string(raw), jobMarker)
	if jobStart < 0 {
		t.Fatal("scenario Job manifest missing from run.sh")
	}
	jobYAML := string(raw)[jobStart:]
	if jobEnd := strings.Index(jobYAML, "\nYAML"); jobEnd >= 0 {
		jobYAML = jobYAML[:jobEnd]
	}

	var manifest map[string]any
	if err := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(jobYAML), 4096).Decode(&manifest); err != nil {
		t.Fatalf("decode scenario Job manifest: %v", err)
	}
	spec, _ := manifest["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	metadata, _ := template["metadata"].(map[string]any)
	annotations, _ := metadata["annotations"].(map[string]any)
	if got := annotations["karpenter.sh/do-not-disrupt"]; got != "true" {
		t.Fatalf("scenario Pod karpenter.sh/do-not-disrupt = %v, want true", got)
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

func TestControlPlaneWorkerDefaultsAreConfigurable(t *testing.T) {
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
		"${DUCKGRES_K8S_WORKER_CPU_REQUEST}", "2",
		"${DUCKGRES_K8S_WORKER_MEMORY_REQUEST}", "4Gi",
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
		if manifest["kind"] != "Deployment" || manifestName(manifest) != "duckgres-control-plane" {
			continue
		}
		env := deploymentContainerEnv(manifest, "controlplane")
		if got := env["DUCKGRES_K8S_WORKER_CPU_REQUEST"]; got != "2" {
			t.Fatalf("worker CPU request = %q, want configurable 2", got)
		}
		if got := env["DUCKGRES_K8S_WORKER_MEMORY_REQUEST"]; got != "4Gi" {
			t.Fatalf("worker memory request = %q, want configurable 4Gi", got)
		}
		return
	}
	t.Fatal("duckgres-control-plane Deployment missing from manifests template")
}

func TestRenderDocumentsSafeWorkerResourceDefaults(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	script := string(raw)
	for _, want := range []string{
		`DUCKGRES_K8S_WORKER_CPU_REQUEST="${DUCKGRES_K8S_WORKER_CPU_REQUEST:-750m}"`,
		`DUCKGRES_K8S_WORKER_MEMORY_REQUEST="${DUCKGRES_K8S_WORKER_MEMORY_REQUEST:-1536Mi}"`,
		`$DUCKGRES_K8S_WORKER_CPU_REQUEST $DUCKGRES_K8S_WORKER_MEMORY_REQUEST`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("run.sh missing worker render contract %q", want)
		}
	}
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

func deploymentContainerEnv(manifest map[string]any, containerName string) map[string]string {
	spec, _ := manifest["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	containers, _ := podSpec["containers"].([]any)
	for _, rawContainer := range containers {
		container, _ := rawContainer.(map[string]any)
		if container["name"] != containerName {
			continue
		}
		values := make(map[string]string)
		env, _ := container["env"].([]any)
		for _, rawVar := range env {
			variable, _ := rawVar.(map[string]any)
			name, _ := variable["name"].(string)
			value, _ := variable["value"].(string)
			values[name] = value
		}
		return values
	}
	return nil
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
	secretDir := filepath.Join(dir, "secrets")
	if err := os.Mkdir(secretDir, 0o700); err != nil {
		t.Fatalf("mkdir fake secret dir: %v", err)
	}
	internalSecretPath := filepath.Join(secretDir, "duckgres-ci-internal-secret")
	if err := os.WriteFile(internalSecretPath, []byte("isolated-test-secret\n"), 0o600); err != nil {
		t.Fatalf("write fake internal secret: %v", err)
	}

	writeFake(t, binDir, "kubectl", `#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >> "$RUN_SH_TEST_CALLS"

if [[ "$*" == *" apply -f -"* ]]; then
  tee -a "$RUN_SH_TEST_CALLS" >/dev/null
  exit 0
fi
if [[ -n "${SCENARIO_DEV_FAIL_COPY:-}" && "$*" == *" cp "* ]]; then
  exit 1
fi
if [[ -n "${SCENARIO_DEV_EMPTY_COPY:-}" && "$*" == *" cp "* ]]; then
  exit 0
fi
if [[ "$*" == *" cp "* ]]; then
  dest="${@: -1}"
  mkdir -p "$dest"
  printf '{}\n' > "$dest/scenario_summary.json"
  if [[ -z "${SCENARIO_DEV_MISSING_MARKDOWN_SUMMARY:-}" ]]; then
    printf '# Scenario result\n' > "$dest/scenario_summary.md"
  fi
  printf 'header\n' > "$dest/step_results.csv"
  printf '{}\n' > "$dest/events.jsonl"
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
if [[ -n "${SCENARIO_DEV_NO_POD:-}" && "$*" == *" get pod -l job-name=duckgres-scenario-"* ]]; then
  exit 0
fi
if [[ -n "${SCENARIO_DEV_FAIL_CONTAINER_WAIT:-}" && "$*" == *" wait --for=jsonpath="* && "$*" == *'@.name=="scenario"'* ]]; then
  exit 1
fi
if [[ "$*" == *"state.terminated.exitCode"* ]]; then
  printf '%s' "${SCENARIO_DEV_EXIT_CODE:-0}"
  exit 0
fi
if [[ "$*" == *" get pod -l job-name=duckgres-scenario-"* ]]; then
  printf 'duckgres-scenario-pod'
  exit 0
fi
if [[ -n "${SCENARIO_DEV_FAIL_RELEASE:-}" && "$*" == *" exec -c artifact-keeper "* ]]; then
  exit 1
fi
if [[ -n "${SCENARIO_DEV_ALLOW_DUCKLING_DELETE:-}" && "$*" == *" wait --for=delete duckling/ci-pr-123-"* ]]; then
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

	writeFake(t, binDir, "mktemp", `#!/usr/bin/env bash
if [[ -n "${SCENARIO_DEV_MKTEMP_COLLISION_ONCE:-}" && ! -e "$RUN_SH_TEST_MKTEMP_STATE" ]]; then
  touch "$RUN_SH_TEST_MKTEMP_STATE"
  template="${@: -1}"
  path="${template%XXXXXX}COLLIDE"
  mkdir -p "$path"
  printf '%s\n' "$path"
  exit 0
fi
exec /usr/bin/mktemp "$@"
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
		"RUN_SH_TEST_MKTEMP_STATE="+filepath.Join(filepath.Dir(binDir), "mktemp-state"),
		"KUBE_CONTEXT=test-context",
		"NAMESPACE=duckgres-ci-pr-123",
		"PR_NUMBER=123",
		"WORKER_IMAGE=example.invalid/duckgres:test",
		"CONTROLPLANE_IMAGE=example.invalid/duckgres:test",
		"CP_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/duckgres-control-plane-dev",
		"EKS_CLUSTER_NAME=test-cluster",
		"AWS_REGION=us-east-1",
		"SCENARIO_ARTIFACTS_DIR="+filepath.Join(filepath.Dir(binDir), "scenario-artifacts"),
		"DUCKGRES_CI_SECRET_DIR="+filepath.Join(filepath.Dir(binDir), "secrets"),
	)
	env = append(env, extraEnv...)
	cmd.Env = env
	return cmd
}

func assertVisiblePartialArtifact(t *testing.T, artifactRoot, scenarioName string) {
	t.Helper()
	entries, err := os.ReadDir(artifactRoot)
	if err != nil {
		t.Fatalf("read artifact root: %v", err)
	}
	var partials []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			t.Fatalf("artifact staging directory remained hidden after failure: %s", entry.Name())
		}
		if entry.IsDir() && strings.HasPrefix(entry.Name(), scenarioName+"-") && strings.HasSuffix(entry.Name(), ".partial") {
			partials = append(partials, filepath.Join(artifactRoot, entry.Name()))
		}
	}
	if len(partials) != 1 {
		t.Fatalf("partial artifact directories = %v, want exactly one", partials)
	}
	marker := filepath.Join(partials[0], "artifact_collection_error.txt")
	contents, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read partial artifact marker: %v", err)
	}
	if len(strings.TrimSpace(string(contents))) == 0 {
		t.Fatalf("partial artifact marker %s is empty", marker)
	}
}

func (f runSHFakes) calls(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(f.logPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read fake calls: %v", err)
	}
	return string(b)
}
