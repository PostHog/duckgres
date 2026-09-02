package e2emwdev_test

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	kjsonpath "k8s.io/client-go/util/jsonpath"
)

func stringSlice(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

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

func TestDeployWaitsForReshardRollbackBeforeReusingNamespace(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	script := string(raw)
	start := strings.Index(script, "reset_pr_stack() {")
	end := strings.Index(script, "\ncmd_deploy() {")
	if start < 0 || end <= start {
		t.Fatal("could not isolate reset_pr_stack")
	}
	reset := script[start:end]
	if !strings.Contains(reset, `delete namespace "$NS" --ignore-not-found --wait=true --timeout=720s`) {
		t.Fatal("namespace reset must outlive the reshard runner's 600s rollback grace period")
	}
	for _, unsafe := range []string{"--force", "--grace-period=0"} {
		if strings.Contains(reset, unsafe) {
			t.Fatalf("namespace reset bypasses rollback-safe termination with %q", unsafe)
		}
	}
}

func TestDiagnosticsHarnessPodJSONPathParses(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	match := regexp.MustCompile(`(?s)get pods -l job-name=duckgres-harness\s+-o jsonpath='([^']+)'`).FindSubmatch(raw)
	if len(match) != 2 {
		t.Fatal("diagnostics harness-pod JSONPath is missing")
	}
	if err := kjsonpath.New("diagnostics-harness-pod").Parse(string(match[1])); err != nil {
		t.Fatalf("diagnostics harness-pod JSONPath does not parse: %v", err)
	}
}

func TestTrinoDeployStartsWorkloadsWithoutScaleSubresource(t *testing.T) {
	fakes := newRunSHFakes(t)
	secretDir := filepath.Join(filepath.Dir(fakes.binDir), "secrets")
	for _, name := range []string{"duckgres-ci-trino-ca.crt", "duckgres-ci-trino-server.p12"} {
		if err := os.WriteFile(filepath.Join(secretDir, name), []byte("test-tls-material\n"), 0o600); err != nil {
			t.Fatalf("write fake Trino TLS material: %v", err)
		}
	}

	cmd := runSHCommand(t, fakes.binDir, "deploy",
		"SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1",
		"E2E_SUITE=trino",
		"TRINO_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/trino-dev",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Trino deploy failed: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	if strings.Contains(calls, " scale ") {
		t.Fatalf("Trino deploy requires deployments/scale RBAC; calls:\n%s", calls)
	}
	for deployment, replicas := range map[string]int{
		"duckgres-trino-coordinator": 1,
		"duckgres-trino-worker":      3,
	} {
		want := "patch deployment " + deployment + " --type=merge -p {\"spec\":{\"replicas\":" + strconv.Itoa(replicas) + "}}"
		if !strings.Contains(calls, want) {
			t.Errorf("Trino deploy did not start %s with %d replicas through the deployment resource; calls:\n%s", deployment, replicas, calls)
		}
	}
}

func TestTrinoWorkersMatchDuckgresAggregateCompute(t *testing.T) {
	raw, err := os.ReadFile("manifests.trino.tmpl.yaml")
	if err != nil {
		t.Fatalf("read Trino manifests template: %v", err)
	}
	rendered := strings.NewReplacer(
		"${NAMESPACE}", "test-namespace",
		"${PR_NUMBER}", "123",
		"${TRINO_IMAGE}", "example.invalid/trino:test",
		"${TRINO_TLS_PASSWORD}", "test-password",
		"${TRINO_CA_CERT_B64}", "dGVzdA==",
		"${TRINO_SERVER_P12_B64}", "dGVzdA==",
	).Replace(string(raw))

	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
	var workerDeployment, coordinatorConfig, workerConfig map[string]any
	for {
		var manifest map[string]any
		err := decoder.Decode(&manifest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode Trino manifests template: %v", err)
		}
		switch {
		case manifest["kind"] == "Deployment" && manifestName(manifest) == "duckgres-trino-worker":
			workerDeployment = manifest
		case manifest["kind"] == "ConfigMap" && manifestName(manifest) == "duckgres-trino-coordinator":
			coordinatorConfig = manifest
		case manifest["kind"] == "ConfigMap" && manifestName(manifest) == "duckgres-trino-worker":
			workerConfig = manifest
		}
	}
	if workerDeployment == nil || coordinatorConfig == nil || workerConfig == nil {
		t.Fatalf("missing Trino manifests: worker deployment=%t coordinator config=%t worker config=%t", workerDeployment != nil, coordinatorConfig != nil, workerConfig != nil)
	}

	spec := workerDeployment["spec"].(map[string]any)
	template := spec["template"].(map[string]any)
	podSpec := template["spec"].(map[string]any)
	containers := podSpec["containers"].([]any)
	worker := containers[0].(map[string]any)
	resources := worker["resources"].(map[string]any)
	for _, field := range []string{"requests", "limits"} {
		values := resources[field].(map[string]any)
		if values["cpu"] != "1" || values["memory"] != "4Gi" {
			t.Errorf("Trino worker %s = %v, want 1 CPU and 4Gi", field, values)
		}
	}
	workerData := workerConfig["data"].(map[string]any)
	if jvmConfig := workerData["jvm.config"].(string); !strings.Contains(jvmConfig, "-Xmx3G") {
		t.Errorf("Trino worker JVM config does not fit its 4Gi pod:\n%s", jvmConfig)
	}

	for name, expectedPerNode := range map[string]string{
		"coordinator": "1GB",
		"worker":      "2GB",
	} {
		manifest := coordinatorConfig
		if name == "worker" {
			manifest = workerConfig
		}
		data := manifest["data"].(map[string]any)
		config := data["config.properties"].(string)
		for _, want := range []string{"query.max-memory=6GB", "query.max-memory-per-node=" + expectedPerNode} {
			if !strings.Contains(config, want) {
				t.Errorf("Trino %s config missing %q:\n%s", name, want, config)
			}
		}
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

func TestTeardownDropsCNPGIdentifiersOnDiscoveredPrimary(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "teardown",
		"SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1",
		"CNPG_DEV_PRIMARY=shard-001-2",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("teardown failed: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	if !strings.Contains(calls, "get pod -l cnpg.io/cluster=shard-001,cnpg.io/instanceRole=primary") {
		t.Fatalf("teardown did not discover the shard-001 primary by CNPG labels; calls:\n%s", calls)
	}
	if !strings.Contains(calls, "exec shard-001-2 -c postgres -- psql -U postgres -c DROP DATABASE IF EXISTS mdstore_ci_pr_123_cnpg WITH (FORCE);") {
		t.Fatalf("teardown did not drop the database on the discovered primary; calls:\n%s", calls)
	}
	if !strings.Contains(calls, "exec shard-001-2 -c postgres -- psql -U postgres -c DROP ROLE IF EXISTS mdstore_ci_pr_123_cnpg;") {
		t.Fatalf("teardown did not drop the role on the discovered primary; calls:\n%s", calls)
	}
	if strings.Contains(calls, "exec shard-001-1 -c postgres -- psql") {
		t.Fatalf("teardown still targets the fixed shard-001-1 pod; calls:\n%s", calls)
	}
}

func TestTeardownRetriesCNPGCleanupAfterPrimaryFailover(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "teardown",
		"SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1",
		"CNPG_DEV_PRIMARY_SEQUENCE=shard-001-1,shard-001-2",
		"CNPG_DEV_FAILOVER_ONCE=1",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("teardown failed after retrying a primary transition: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	firstDiscovery := strings.Index(calls, "get pod -l cnpg.io/cluster=shard-001,cnpg.io/instanceRole=primary")
	staleExec := strings.Index(calls, "exec shard-001-1 -c postgres -- psql")
	secondDiscovery := -1
	if firstDiscovery >= 0 {
		if next := strings.Index(calls[firstDiscovery+1:], "get pod -l cnpg.io/cluster=shard-001,cnpg.io/instanceRole=primary"); next >= 0 {
			secondDiscovery = firstDiscovery + 1 + next
		}
	}
	newPrimaryDBExec := strings.Index(calls, "exec shard-001-2 -c postgres -- psql -U postgres -c DROP DATABASE IF EXISTS mdstore_ci_pr_123_cnpg WITH (FORCE);")
	newPrimaryRoleExec := strings.Index(calls, "exec shard-001-2 -c postgres -- psql -U postgres -c DROP ROLE IF EXISTS mdstore_ci_pr_123_cnpg;")
	if firstDiscovery < 0 || staleExec < firstDiscovery || secondDiscovery < staleExec || newPrimaryDBExec < secondDiscovery || newPrimaryRoleExec < newPrimaryDBExec {
		t.Fatalf("teardown did not rediscover and retry on the replacement primary; calls:\n%s", calls)
	}
}

func TestTeardownFailsWhenCNPGCleanupCannotReachAPrimary(t *testing.T) {
	tests := []struct {
		name string
		env  string
	}{
		{name: "discovery fails", env: "CNPG_DEV_FAIL_DISCOVERY=1"},
		{name: "all psql executions fail", env: "CNPG_DEV_FAIL_EXEC=1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakes := newRunSHFakes(t)

			cmd := runSHCommand(t, fakes.binDir, "teardown",
				"SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1",
				"CNPG_DEV_PRIMARY=shard-001-2",
				tt.env,
			)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("teardown succeeded despite %s; output:\n%s", tt.name, out)
			}
			if !strings.Contains(string(out), "after 3 attempts") {
				t.Fatalf("teardown did not report exhausted CNPG cleanup retries; output:\n%s", out)
			}

			calls := fakes.calls(t)
			if got := strings.Count(calls, "get pod -l cnpg.io/cluster=shard-001,cnpg.io/instanceRole=primary"); got != 15 {
				t.Fatalf("primary discovery calls = %d, want 15 (three retries for each CI org); calls:\n%s", got, calls)
			}
			if tt.name == "all psql executions fail" && strings.Count(calls, "exec shard-001-2 -c postgres -- psql") != 15 {
				t.Fatalf("psql attempts were not bounded to three per CI org; calls:\n%s", calls)
			}
			if !strings.Contains(calls, "delete namespace duckgres-ci-pr-123 --ignore-not-found --wait=false") {
				t.Fatalf("teardown did not continue namespace cleanup after CNPG cleanup failed; calls:\n%s", calls)
			}
		})
	}
}

func TestTeardownSucceedsWhenCNPGIdentifiersAreAlreadyAbsent(t *testing.T) {
	fakes := newRunSHFakes(t)

	cmd := runSHCommand(t, fakes.binDir, "teardown",
		"SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1",
		"CNPG_DEV_PRIMARY=shard-001-2",
		"CNPG_DEV_MISSING_IDENTIFIERS=1",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("teardown failed when CNPG identifiers were already absent: %v\n%s", err, out)
	}

	calls := fakes.calls(t)
	for _, want := range []string{
		"DROP DATABASE IF EXISTS mdstore_ci_pr_123_cnpg WITH (FORCE);",
		"DROP ROLE IF EXISTS mdstore_ci_pr_123_cnpg;",
	} {
		if !strings.Contains(calls, want) {
			t.Fatalf("teardown did not preserve idempotent SQL %q; calls:\n%s", want, calls)
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
		"name: DUCKGRES_SCENARIO_TRINO_CA_CERT, value: \"/trino-ca/ca.crt\"",
		"name: trino-ca, mountPath: /trino-ca, readOnly: true",
		"name: trino-ca, secret: { secretName: duckgres-trino-tls, optional: true",
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

func TestE2EHarnessCoversNativeMetadataProxy(t *testing.T) {
	manifestRaw, err := os.ReadFile("manifests.tmpl.yaml")
	if err != nil {
		t.Fatalf("read manifests template: %v", err)
	}
	rendered := strings.NewReplacer(
		"${NAMESPACE}", "duckgres-ci-pr-123",
		"${PR_NUMBER}", "123",
		"${CONTROLPLANE_IMAGE}", "example.invalid/duckgres:test",
		"${WORKER_IMAGE}", "example.invalid/duckgres:test",
		"${INTERNAL_SECRET}", "test-internal-secret",
		"${INTERNAL_SECRET_FALLBACK}", "test-internal-secret-fallback",
		"${USER_SECRET_KEY}", "test-user-secret-key",
		"${DUCKGRES_K8S_WORKER_CPU_REQUEST}", "750m",
		"${DUCKGRES_K8S_WORKER_MEMORY_REQUEST}", "1536Mi",
	).Replace(string(manifestRaw))
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
	foundDeployment := false
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
		foundDeployment = true
		env := deploymentContainerEnv(manifest, "controlplane")
		if got := env["DUCKGRES_METADATA_HOSTNAME_SUFFIXES"]; got != ".md.ci.duckgres.local" {
			t.Fatalf("DUCKGRES_METADATA_HOSTNAME_SUFFIXES = %q, want .md.ci.duckgres.local", got)
		}
	}
	if !foundDeployment {
		t.Fatal("duckgres-control-plane Deployment missing from manifests template")
	}

	harnessRaw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}
	harness := string(harnessRaw)
	for _, want := range []string{
		`METADATA_SNI_SUFFIX=".md.ci.duckgres.local"`,
		"metadata_proxy_e2e()",
		`{"metadata_proxy_enabled":true}`,
		`database = ""`,
		`public.ducklake_metadata`,
		`metadata endpoint is unavailable`,
	} {
		if !strings.Contains(harness, want) {
			t.Errorf("e2e harness is missing native metadata proxy coverage marker %q", want)
		}
	}
}

func TestE2EHarnessWorkerInspectionJSONPathParsesSnapshot(t *testing.T) {
	raw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}
	const prefix = "WORKER_INSPECTION_JSONPATH='"
	_, expression, found := strings.Cut(string(raw), prefix)
	if !found {
		t.Fatal("worker inspection JSONPath is missing")
	}
	expression, _, found = strings.Cut(expression, "'\n")
	if !found {
		t.Fatal("worker inspection JSONPath is not a single quoted expression")
	}

	pod := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{
				"app":                    "duckgres-worker",
				"duckgres/control-plane": "control-plane",
				"duckgres/worker-id":     "worker-123",
			},
		},
		"status": map[string]any{"phase": "Running"},
		"spec": map[string]any{
			"securityContext": map[string]any{
				"runAsNonRoot": true,
				"runAsUser":    1000,
			},
			"volumes": []any{map[string]any{"name": "data"}},
			"containers": []any{map[string]any{
				"name": "duckdb-worker",
				"securityContext": map[string]any{
					"allowPrivilegeEscalation": false,
				},
				"env": []any{
					map[string]any{"name": "POD_NAME", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.name"}}},
					map[string]any{"name": "NODE_NAME", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "spec.nodeName"}}},
					map[string]any{"name": "DUCKGRES_MEMORY_LIMIT", "value": "921MB"},
					map[string]any{"name": "GOMEMLIMIT", "value": "192MiB"},
					map[string]any{"name": "DUCKGRES_THREADS", "value": "2"},
				},
				"volumeMounts": []any{map[string]any{"mountPath": "/data"}},
				"resources": map[string]any{
					"requests": map[string]any{"cpu": "750m", "memory": "1536Mi"},
				},
			}},
		},
	}

	template := kjsonpath.New("worker-inspection").AllowMissingKeys(true)
	if err := template.Parse(expression); err != nil {
		t.Fatalf("parse worker inspection JSONPath: %v", err)
	}
	var got bytes.Buffer
	if err := template.Execute(&got, pod); err != nil {
		t.Fatalf("execute worker inspection JSONPath: %v", err)
	}
	const want = "|Running|duckgres-worker|control-plane|worker-123|true|1000|false|metadata.name|spec.nodeName|data,|/data,|750m|1536Mi|921MB|192MiB|2"
	if got.String() != want {
		t.Fatalf("worker inspection snapshot = %q, want %q", got.String(), want)
	}
}

func TestE2EHarnessWorkerPodInspectionReacquiresReplacement(t *testing.T) {
	raw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}

	// Run only the assertion against a fake in-cluster kubectl. The first list
	// returns a worker that disappears before its GET; a replacement is then
	// immediately available. This is the normal worker-retirement race the e2e
	// harness must tolerate without turning the NotFound into empty assertions.
	harness := strings.Replace(string(raw), "\nstart_kubectl_download\nk() {", "\nKUBECTL=\"${TEST_KUBECTL:?}\"\nk() {", 1)
	harness = strings.Replace(harness, "\nmain \"$@\"\n", "\nassert_worker_pod test-org test-password\n", 1)
	if harness == string(raw) {
		t.Fatal("could not prepare e2e harness assertion fixture")
	}

	dir := t.TempDir()
	harnessPath := filepath.Join(dir, "harness.sh")
	if err := os.WriteFile(harnessPath, []byte(harness), 0o755); err != nil {
		t.Fatalf("write harness fixture: %v", err)
	}

	callsPath := filepath.Join(dir, "kubectl-calls.log")
	selectionPath := filepath.Join(dir, "worker-selection")
	kubectlPath := filepath.Join(dir, "kubectl")
	writeFake(t, dir, "kubectl", `#!/usr/bin/env bash
printf '%s\n' "$*" >> "$HARNESS_TEST_CALLS"
if [[ "$*" == *"get pods -l app=duckgres-worker,duckgres/active-org=test-org"* ]]; then
  selection="$(cat "$HARNESS_TEST_SELECTION" 2>/dev/null || true)"
  case "$selection" in
  "")
    printf 'stale' > "$HARNESS_TEST_SELECTION"
    printf 'stale-worker'
    ;;
  stale)
    printf 'transitioned' > "$HARNESS_TEST_SELECTION"
    if [[ "$*" == *"--field-selector=status.phase=Running"* ]]; then
      printf 'transitioned-worker'
    else
      printf 'pending-worker'
    fi
    ;;
  *)
    if [[ "$*" == *"--field-selector=status.phase=Running"* ]]; then
      printf 'replacement-worker'
    else
      printf 'pending-worker'
    fi
    ;;
  esac
  exit 0
fi
if [[ "$*" == *"POSTHOG_API_KEY"* ]]; then
  # Fixture workers have no PostHog env. Empty output is the success case
  # for the plaintext-forbidden assert (and skips the secretKeyRef copy).
  exit 0
fi
if [[ "$*" == *"get pod stale-worker"* ]]; then
  echo 'Error from server (NotFound): pods "stale-worker" not found' >&2
  exit 1
fi
if [[ "$*" == *"get pod pending-worker"* && "$*" == *"-o jsonpath="* ]] || \
   [[ "$*" == *"get pod transitioned-worker"* && "$*" == *"-o jsonpath="* ]] || \
   [[ "$*" == *"get pod replacement-worker"* && "$*" == *"-o jsonpath="* ]]; then
  phase=""
  case "$*" in
    *"get pod pending-worker"*) phase=Pending ;;
    *"get pod transitioned-worker"*) phase=Succeeded ;;
    *"get pod replacement-worker"*) phase=Running ;;
  esac
  if [[ "$*" == *".status.phase"* ]]; then
    printf '|%s' "$phase"
  fi
  printf '%s' '|duckgres-worker|control-plane|worker-123|true|1000|false|metadata.name|spec.nodeName|data,|/data,|750m|1536Mi|921MB|192MiB|2'
  exit 0
fi
echo "unexpected kubectl invocation: $*" >&2
exit 1
`)
	writeFake(t, dir, "sleep", "#!/usr/bin/env bash\nexit 0\n")

	cmd := exec.Command("sh", harnessPath)
	cmd.Env = append(os.Environ(),
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_KUBECTL="+kubectlPath,
		"HARNESS_TEST_CALLS="+callsPath,
		"HARNESS_TEST_SELECTION="+selectionPath,
		"CP_API=http://test.invalid",
		"CP_PG_HOST=control-plane.test",
		"INTERNAL_SECRET=test-secret",
		"NAMESPACE=test-namespace",
		"PR_NUMBER=123",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("worker pod inspection did not reacquire a replacement: %v\n%s", err, out)
	}

	calls, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatalf("read kubectl calls: %v", err)
	}
	callLog := string(calls)
	if got := strings.Count(callLog, "get pods -l app=duckgres-worker,duckgres/active-org=test-org"); got != 3 {
		t.Fatalf("worker selections = %d, want 3 (stale, terminal, then replacement); calls:\n%s", got, callLog)
	}
	if got := strings.Count(callLog, "app=duckgres-worker,duckgres/active-org=test-org --field-selector=status.phase=Running"); got != 3 {
		t.Fatalf("running-worker selections = %d, want 3; calls:\n%s", got, callLog)
	}
	if !strings.Contains(callLog, "get pod stale-worker") {
		t.Fatalf("fixture did not inspect the stale worker first; calls:\n%s", callLog)
	}
	if strings.Contains(callLog, "get pod pending-worker") {
		t.Fatalf("inspection accepted a non-Running pod; calls:\n%s", callLog)
	}
	if got := strings.Count(callLog, "get pod transitioned-worker"); got != 1 {
		t.Fatalf("terminal transition inspections = %d, want 1; calls:\n%s", got, callLog)
	}
	if got := strings.Count(callLog, "get pod replacement-worker -o jsonpath={.metadata.deletionTimestamp"); got != 1 {
		t.Fatalf("replacement inspection snapshots = %d, want 1; calls:\n%s", got, callLog)
	}
	if !strings.Contains(callLog, "get pod replacement-worker -o jsonpath={range .spec.containers") {
		t.Fatalf("did not run plaintext POSTHOG_API_KEY check on replacement; calls:\n%s", callLog)
	}
}

func TestE2EHarnessColdBurstAcceptsStaggeredWorkerReadiness(t *testing.T) {
	raw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}

	harness := strings.Replace(string(raw), "\nstart_kubectl_download\nk() {", "\nKUBECTL=\"${TEST_KUBECTL:?}\"\nk() {", 1)
	harness = strings.Replace(harness, "\nmain \"$@\"\n", "\nkill() { return 1; }\ncold_burst_parallel_spawns test-org test-password\n", 1)
	if harness == string(raw) {
		t.Fatal("could not prepare cold-burst harness fixture")
	}

	dir := t.TempDir()
	harnessPath := filepath.Join(dir, "harness.sh")
	if err := os.WriteFile(harnessPath, []byte(harness), 0o755); err != nil {
		t.Fatalf("write harness fixture: %v", err)
	}

	kubectlPath := filepath.Join(dir, "kubectl")
	writeFake(t, dir, "kubectl", `#!/usr/bin/env bash
case "$*" in
  *"delete pods -l app=duckgres-worker,duckgres/active-org=test-org"*|*"wait --for=delete pods -l app=duckgres-worker,duckgres/active-org=test-org"*)
    exit 0
    ;;
  *"get pods -l app=duckgres-worker,duckgres/active-org=test-org"*"-o jsonpath="*)
    printf '%s' '2024-01-01T00:00:00Z 2024-01-01T00:00:01Z 2024-01-01T00:00:02Z'
    exit 0
    ;;
esac
echo "unexpected kubectl invocation: $*" >&2
exit 1
`)
	writeFake(t, dir, "psql", "#!/usr/bin/env sh\nprintf 1500000000\n")
	writeFake(t, dir, "date", `#!/usr/bin/env sh
case "$*" in
  *"2024-01-01T00:00:00Z"*) printf 100 ;;
  *"2024-01-01T00:00:02Z"*) printf 102 ;;
  *) exit 1 ;;
esac
`)

	cmd := exec.Command("sh", harnessPath)
	cmd.Env = append(os.Environ(),
		"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TEST_KUBECTL="+kubectlPath,
		"CP_API=http://test.invalid",
		"CP_PG_HOST=control-plane.test",
		"INTERNAL_SECRET=test-secret",
		"NAMESPACE=test-namespace",
		"PR_NUMBER=123",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cold burst rejected three distinct parallel-created workers after their clients exited: %v\n%s", err, out)
	}
}

func TestE2EHarnessCoversRemoteBinaryCopy(t *testing.T) {
	harnessRaw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}
	harness := string(harnessRaw)
	for _, want := range []string{
		"binary_copy_round_trip()",
		`PGPASSWORD="$2" timeout 120 psql "$conn"`,
		"binary COPY: backpressure never cleared",
		`CREATE TABLE $route_guard_src AS SELECT 7::INTEGER AS id;`,
		`CREATE TABLE $route_guard_dst (id BIGINT);`,
		`\copy $native_src TO '$native_file' WITH (FORMAT binary)`,
		`\copy $native_dst (label, id, payload, enabled, ratio, event_date, event_time, received_at, amount) FROM '$native_file' WITH (FORMAT binary)`,
		`\copy $route_guard_src TO '$route_guard_file' WITH (FORMAT binary)`,
		`\copy $route_guard_dst (id) FROM '$route_guard_file' WITH (FORMAT binary)`,
		`\echo route_guard_sqlstate=:SQLSTATE`,
		`route_guard_sqlstate=22P02`,
		"BIGINT field length 4, expected 8",
		`want_copies="$(printf 'COPY 3\nCOPY 3\nCOPY 1\nCOPY 1\nCOPY 1\nCOPY 1')"`,
		`(SELECT count(*) FROM $route_guard_dst)`,
		`binary_copy_round_trip "$CNPG" "$cnpg_pw"`,
	} {
		if !strings.Contains(harness, want) {
			t.Errorf("e2e harness is missing remote binary COPY coverage marker %q", want)
		}
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
	env := deploymentContainerEnv(manifest, "scenario")
	if got := env["DUCKGRES_K8S_WORKER_CPU_REQUEST"]; got != "$DUCKGRES_K8S_WORKER_CPU_REQUEST" {
		t.Fatalf("scenario worker CPU env = %q, want deployed standard worker CPU", got)
	}
	if got := env["DUCKGRES_K8S_WORKER_MEMORY_REQUEST"]; got != "$DUCKGRES_K8S_WORKER_MEMORY_REQUEST" {
		t.Fatalf("scenario worker memory env = %q, want deployed standard worker memory", got)
	}
}

func TestControlPlaneServiceDoesNotExposeFlight(t *testing.T) {
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
			if port["name"] == "flight" || port["port"] == float64(8815) || port["targetPort"] == "flight" {
				t.Fatalf("duckgres-control-plane Service exposes obsolete Flight port: %#v", port)
			}
		}
		return
	}

	t.Fatal("duckgres-control-plane Service missing from manifests template")
}

func TestE2ELanesCanReadOnlyRequiredDucklingsSecrets(t *testing.T) {
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
	var role, binding map[string]any
	for {
		var manifest map[string]any
		err := decoder.Decode(&manifest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode manifests template: %v", err)
		}
		switch {
		case manifest["kind"] == "Role" && manifestName(manifest) == "duckgres-ci-pr-123-cnpg-provisioner-secret-reader":
			role = manifest
		case manifest["kind"] == "RoleBinding" && manifestName(manifest) == "duckgres-ci-pr-123-cnpg-provisioner-secret-reader":
			binding = manifest
		}
	}
	if role == nil || binding == nil {
		t.Fatalf("scoped provisioner Role/RoleBinding missing: role=%t binding=%t", role != nil, binding != nil)
	}
	metadata := role["metadata"].(map[string]any)
	if metadata["namespace"] != "ducklings" {
		t.Fatalf("Role namespace = %v, want ducklings", metadata["namespace"])
	}
	labels := metadata["labels"].(map[string]any)
	if labels["duckgres.posthog.com/ci-pr"] != "123" {
		t.Fatalf("Role cleanup label = %v, want PR number", labels)
	}
	rules := role["rules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("Role rules = %v, want exactly one", rules)
	}
	rule := rules[0].(map[string]any)
	if got := strings.Join(stringSlice(rule["apiGroups"]), ","); got != "" {
		t.Fatalf("apiGroups = %q, want core API group", got)
	}
	if got := strings.Join(stringSlice(rule["resources"]), ","); got != "secrets" {
		t.Fatalf("resources = %q, want secrets", got)
	}
	if got := strings.Join(stringSlice(rule["verbs"]), ","); got != "get" {
		t.Fatalf("verbs = %q, want get", got)
	}
	wantResourceNames := "cnpg-shard-001-provisioner,cnpg-shard-002-provisioner," +
		"cnpg-tenant-ci-pr-123-cnpg-password," +
		"cnpg-tenant-ci-pr-123-trinoa-password,cnpg-tenant-ci-pr-123-trinob-password"
	if got := strings.Join(stringSlice(rule["resourceNames"]), ","); got != wantResourceNames {
		t.Fatalf("resourceNames = %q", got)
	}
	bindingMetadata := binding["metadata"].(map[string]any)
	if bindingMetadata["namespace"] != "ducklings" {
		t.Fatalf("RoleBinding namespace = %v, want ducklings", bindingMetadata["namespace"])
	}
	subjects := binding["subjects"].([]any)
	if len(subjects) != 1 {
		t.Fatalf("RoleBinding subjects = %v, want exactly one", subjects)
	}
	subject := subjects[0].(map[string]any)
	if subject["kind"] != "ServiceAccount" || subject["name"] != "duckgres" || subject["namespace"] != "test-namespace" {
		t.Fatalf("RoleBinding subject = %v, want disposable lane ServiceAccount", subject)
	}
	roleRef := binding["roleRef"].(map[string]any)
	if roleRef["kind"] != "Role" || roleRef["name"] != "duckgres-ci-pr-123-cnpg-provisioner-secret-reader" {
		t.Fatalf("RoleBinding roleRef = %v, want scoped Role", roleRef)
	}
}

func TestReshardE2EUsesReachableDestinationAndForcesRollbackAfterPreflight(t *testing.T) {
	raw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read harness: %v", err)
	}
	script := string(raw)
	if strings.Contains(script, `"cnpg_shard":"shard-009"`) || strings.Contains(script, `"cnpg_shard":"shard-099"`) {
		t.Fatal("reshard e2e still relies on an unreachable destination rejected by preflight")
	}
	if !strings.Contains(script, `"cnpg_shard":"shard-002"`) ||
		!strings.Contains(script, `"cutover_timeout_seconds":1`) {
		t.Fatal("reshard rollback e2e must use reachable shard-002 with a forced one-second cutover timeout")
	}
}

func TestReshardTerminalPollBoundsAndRetriesTransientAPIFailures(t *testing.T) {
	raw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}
	script := string(raw)
	start := strings.Index(script, "reshard_wait_terminal() {")
	if start < 0 {
		t.Fatal("could not find reshard_wait_terminal")
	}
	end := strings.Index(script[start:], "\n}\n")
	if end < 0 {
		t.Fatal("could not isolate reshard_wait_terminal")
	}
	body := script[start : start+end]
	for _, want := range []string{"--connect-timeout", "--max-time", "|| true"} {
		if !strings.Contains(body, want) {
			t.Errorf("reshard terminal poll must bound and retry transient API failures; missing %q", want)
		}
	}
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

func TestE2EHarnessJobReceivesSuiteSelection(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	script := string(raw)
	for _, want := range []string{
		`E2E_SUITE="${E2E_SUITE:-neutral}"`,
		`{ name: E2E_SUITE, value: "$E2E_SUITE" }`,
		`case "$E2E_SUITE" in`,
		`neutral|duckdb|trino|reshard)`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("run.sh missing e2e suite contract %q", want)
		}
	}
}

func TestE2EWorkflowRunsNeutralDuckDBAndTrinoSuitesInParallelNamespaces(t *testing.T) {
	raw, err := os.ReadFile("../../.github/workflows/e2e-mw-dev.yml")
	if err != nil {
		t.Fatalf("read e2e workflow: %v", err)
	}
	workflow := string(raw)
	for _, want := range []string{
		"matrix:",
		"suite: neutral",
		"suite: duckdb",
		"suite: trino",
		"suite: reshard",
		`lane_prefix: "1"`,
		`lane_prefix: "2"`,
		`lane_prefix: "3"`,
		`lane_prefix: "4"`,
		"github.event.pull_request.number || github.run_id",
		"github.event_name == 'workflow_dispatch' && github.run_id",
		"NAMESPACE: duckgres-ci-pr-${{ format('{0}{1}', matrix.lane_prefix",
		"E2E_SUITE: ${{ matrix.suite }}",
		"- name: Test e2e harness scripts\n        env:\n          E2E_SUITE: neutral\n        run: go test -count=1 ./tests/mw-dev",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("e2e workflow missing parallel suite contract %q", want)
		}
	}
	for _, duplicated := range []string{
		"suite: neutral",
		"suite: duckdb",
		"suite: trino",
		"suite: reshard",
		`lane_prefix: "1"`,
		`lane_prefix: "2"`,
		`lane_prefix: "3"`,
		`lane_prefix: "4"`,
	} {
		if got := strings.Count(workflow, duplicated); got != 2 {
			t.Fatalf("e2e and teardown matrices must share %q exactly twice; got %d", duplicated, got)
		}
	}
	for _, want := range []string{
		"e2e:\n    needs: [e2e_lanes]",
		"LANES_RESULT: ${{ needs.e2e_lanes.result }}",
		`run: test "$LANES_RESULT" = success`,
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("e2e workflow missing stable aggregate gate %q", want)
		}
	}
}

func TestE2ESuitesKeepNeutralAndEngineSpecificCoverageDisjoint(t *testing.T) {
	raw, err := os.ReadFile("e2e/harness.sh")
	if err != nil {
		t.Fatalf("read e2e harness: %v", err)
	}
	script := string(raw)
	for _, want := range []string{
		`lane_neutral()`,
		`lane_duckdb()`,
		`case "${E2E_SUITE:-neutral}" in`,
		`neutral) lane_neutral ;;`,
		`duckdb) lane_duckdb ;;`,
		`reshard) lane_reshard ;;`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("e2e harness missing focused-suite contract %q", want)
		}
	}
	neutralStart := strings.Index(script, "lane_neutral() {")
	duckdbStart := strings.Index(script, "lane_duckdb() {")
	engineStart := strings.Index(script, "engine_main() {")
	mainStart := strings.Index(script, "\nmain() {")
	if mainStart >= 0 {
		mainStart++
	}
	if neutralStart < 0 || duckdbStart <= neutralStart || engineStart <= duckdbStart || mainStart <= engineStart {
		t.Fatal("could not isolate neutral and engine lane bodies")
	}
	neutralBody := script[neutralStart:duckdbStart]
	engineBody := script[engineStart:mainStart]
	for _, call := range []string{
		"admin_dashboard_no_query_token",
		"internal_secret_fallback_auth",
		"models_explorer_api",
		"provision_team_contract",
		"org_teams_crud",
		"discovery_endpoints",
		"admin_ducklings_metadata",
		"duckling_shard_backfill",
		"lifecycle_teardown_cnpg",
	} {
		if !strings.Contains(neutralBody, call) {
			t.Errorf("neutral lane does not own %s", call)
		}
		if strings.Contains(engineBody, call) {
			t.Errorf("engine lane duplicates neutral assertion %s", call)
		}
	}
	if !strings.Contains(engineBody, "metadata_proxy_e2e") || strings.Contains(neutralBody, "metadata_proxy_e2e") {
		t.Error("native metadata proxy coverage must belong only to the DuckDB/PGWire engine lane")
	}
}

func TestTrinoSuiteUsesOnlyItsOwnCoordinatorAndProjectionNamespace(t *testing.T) {
	manifestRaw, err := os.ReadFile("manifests.trino.tmpl.yaml")
	if err != nil {
		t.Fatalf("read manifests template: %v", err)
	}
	patchRaw, err := os.ReadFile("trino-controlplane-patch.tmpl.json")
	if err != nil {
		t.Fatalf("read Trino control-plane patch: %v", err)
	}
	manifest := string(manifestRaw) + string(patchRaw)
	for _, want := range []string{
		`name: duckgres-trino`,
		`name: duckgres-trino-opa`,
		`"value": "https://duckgres-trino.${NAMESPACE}.svc:8443"`,
		`"name": "DUCKGRES_TRINO_NAMESPACE"`,
		`"value": "${NAMESPACE}"`,
		`"name": "DUCKGRES_TRINO_CELL_ID"`,
		`"value": "ci-pr-${PR_NUMBER}"`,
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("isolated Trino manifest missing %q", want)
		}
	}
	for _, unsafe := range []string{
		`trino.trino.svc`,
		`value: "trino" # shared`,
	} {
		if strings.Contains(manifest, unsafe) {
			t.Fatalf("per-PR control plane must not reference shared Trino state %q", unsafe)
		}
	}

	runRaw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatalf("read run.sh: %v", err)
	}
	for _, want := range []string{
		`TRINO_IMAGE="${TRINO_IMAGE:-`,
		`envsubst '$NAMESPACE $PR_NUMBER $TRINO_IMAGE`,
		`if [ "$E2E_SUITE" = "trino" ]; then`,
		`e2e/trino.sh`,
	} {
		if !strings.Contains(string(runRaw), want) {
			t.Fatalf("run.sh missing isolated Trino contract %q", want)
		}
	}

	harnessRaw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatalf("read Trino harness: %v", err)
	}
	harness := string(harnessRaw)
	for _, want := range []string{
		`"$API/api/v1/trino/queries/$query_id"`,
		`'.query_id == $q and .org == $org'`,
		`catalog_absent=false`,
	} {
		if !strings.Contains(harness, want) {
			t.Errorf("Trino harness missing live admin/detail cleanup contract %q", want)
		}
	}
}

func TestTrinoHarnessCanObserveDeploymentsInItsIsolatedNamespace(t *testing.T) {
	raw, err := os.ReadFile("manifests.tmpl.yaml")
	if err != nil {
		t.Fatalf("read manifests template: %v", err)
	}
	manifest := string(raw)
	for _, want := range []string{
		`resources: ["deployments"]`,
		`verbs: ["get", "list", "watch"]`,
	} {
		if !strings.Contains(manifest, want) {
			t.Errorf("Trino restart rollout RBAC missing %q", want)
		}
	}
}

func TestTrinoHarnessBootstrapsDuckLakeBeforeCatalogQueries(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatalf("read Trino harness: %v", err)
	}
	script := string(raw)

	bootstrap := strings.Index(script, `bootstrap_ducklake "$ORG_A" "$pw_a"`)
	firstCatalogQuery := strings.Index(script, `log "TLS/password auth, discovery, and DDL/DML"`)
	if bootstrap < 0 {
		t.Fatal("Trino harness does not initialize the fresh DuckLake metadata store through Duckgres")
	}
	if firstCatalogQuery < 0 || bootstrap >= firstCatalogQuery {
		t.Fatal("fresh DuckLake metadata must be initialized before the first Trino catalog query")
	}
	warehouseB := strings.Index(script, `wait_warehouse "$ORG_B"`)
	bootstrapB := strings.Index(script, `bootstrap_ducklake "$ORG_B" "$pw_b"`)
	trinoB := strings.Index(script, `wait_trino "$ORG_B" "$DB_B" "$CAT_B"`)
	if warehouseB < 0 || bootstrapB <= warehouseB || trinoB <= bootstrapB {
		t.Fatal("hot-added tenant DuckLake metadata must be initialized after warehouse readiness and before Trino readiness")
	}
	for _, want := range []string{
		`bootstrap_ducklake()`,
		`dbname=ducklake`,
		`host=$1$SNI_SUFFIX`,
		`hostaddr=$CP_IP`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("Trino DuckLake bootstrap is missing %q", want)
		}
	}
}

func TestTrinoKillQueryWorkloadHonorsSequenceLimit(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatalf("read Trino harness: %v", err)
	}
	matches := regexp.MustCompile(`sequence\(1,\s*([0-9]+)\)`).FindAllSubmatch(raw, -1)
	if len(matches) < 3 {
		t.Fatalf("long-query workload uses %d sequences, want at least three bounded factors", len(matches))
	}
	for _, match := range matches {
		limit, err := strconv.Atoi(string(match[1]))
		if err != nil {
			t.Fatalf("parse sequence limit %q: %v", match[1], err)
		}
		if limit > 10_000 {
			t.Errorf("Trino sequence limit %d exceeds the engine maximum of 10000", limit)
		}
	}
}

func TestTrinoPasswordRotationAllowsSecretProjectionWindow(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatalf("read Trino harness: %v", err)
	}
	script := string(raw)

	const minimumProjectionWindowSeconds = 150
	values := make(map[string]int)
	for _, match := range regexp.MustCompile(`(?m)^(TRINO_AUTH_ROTATION_ATTEMPTS|TRINO_AUTH_ROTATION_RETRY_SECONDS)=([0-9]+)$`).FindAllStringSubmatch(script, -1) {
		value, err := strconv.Atoi(match[2])
		if err != nil {
			t.Fatalf("parse %s=%q: %v", match[1], match[2], err)
		}
		values[match[1]] = value
	}
	attempts := values["TRINO_AUTH_ROTATION_ATTEMPTS"]
	retrySeconds := values["TRINO_AUTH_ROTATION_RETRY_SECONDS"]
	if attempts*retrySeconds < minimumProjectionWindowSeconds {
		t.Fatalf("Trino password rotation wait = %ds, want at least %ds for provisioner, kubelet Secret projection, and Trino reload", attempts*retrySeconds, minimumProjectionWindowSeconds)
	}
	for _, want := range []string{
		`while [ "$i" -lt "$TRINO_AUTH_ROTATION_ATTEMPTS" ]; do`,
		`sleep "$TRINO_AUTH_ROTATION_RETRY_SECONDS"`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("Trino password rotation does not use bounded wait setting %q", want)
		}
	}
}

func TestTrinoNodeEnvironmentUsesValidIdentifier(t *testing.T) {
	raw, err := os.ReadFile("manifests.trino.tmpl.yaml")
	if err != nil {
		t.Fatalf("read Trino manifests template: %v", err)
	}

	environments := regexp.MustCompile(`(?m)^\s*node\.environment=(\S+)$`).FindAllStringSubmatch(string(raw), -1)
	if len(environments) != 2 {
		t.Fatalf("Trino node.environment values = %d, want coordinator and worker", len(environments))
	}
	valid := regexp.MustCompile(`^[a-z0-9][_a-z0-9]*$`)
	for _, match := range environments {
		rendered := strings.ReplaceAll(match[1], "${PR_NUMBER}", "123")
		if !valid.MatchString(rendered) {
			t.Errorf("Trino node.environment %q violates the Trino identifier grammar", rendered)
		}
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

if [[ "$*" == *" -n cnpg-shards get pod -l cnpg.io/cluster=shard-001,cnpg.io/instanceRole=primary "* ]]; then
  if [[ -n "${CNPG_DEV_FAIL_DISCOVERY:-}" ]]; then
    exit 1
  fi
  primary="${CNPG_DEV_PRIMARY:-shard-001-1}"
  if [[ -n "${CNPG_DEV_PRIMARY_SEQUENCE:-}" ]]; then
    IFS=',' read -r -a primaries <<< "$CNPG_DEV_PRIMARY_SEQUENCE"
    discovery=0
    if [[ -e "$RUN_SH_TEST_CNPG_DISCOVERY_STATE" ]]; then
      discovery="$(<"$RUN_SH_TEST_CNPG_DISCOVERY_STATE")"
    fi
    if (( discovery < ${#primaries[@]} )); then
      primary="${primaries[$discovery]}"
    else
      primary="${primaries[${#primaries[@]}-1]}"
    fi
    printf '%s' $((discovery + 1)) > "$RUN_SH_TEST_CNPG_DISCOVERY_STATE"
  fi
  printf '%s' "$primary"
  exit 0
fi
if [[ "$*" == *" -n cnpg-shards exec "* && "$*" == *" psql -U postgres -c "* ]]; then
  if [[ -n "${CNPG_DEV_FAILOVER_ONCE:-}" && ! -e "$RUN_SH_TEST_CNPG_FAILOVER_STATE" ]]; then
    touch "$RUN_SH_TEST_CNPG_FAILOVER_STATE"
    exit 1
  fi
  if [[ -n "${CNPG_DEV_FAIL_EXEC:-}" ]]; then
    exit 1
  fi
  if [[ -n "${CNPG_DEV_PRIMARY:-}" && "$*" != *" exec ${CNPG_DEV_PRIMARY} -c postgres "* ]]; then
    exit 1
  fi
  if [[ -n "${CNPG_DEV_MISSING_IDENTIFIERS:-}" && "$*" != *"DROP DATABASE IF EXISTS"* && "$*" != *"DROP ROLE IF EXISTS"* ]]; then
    exit 1
  fi
  exit 0
fi
if [[ "$*" == *" apply -f -"* ]]; then
  tee -a "$RUN_SH_TEST_CALLS" >/dev/null
  exit 0
fi
if [[ "$*" == *" --patch-file=/dev/stdin"* ]]; then
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
		"RUN_SH_TEST_CNPG_DISCOVERY_STATE="+filepath.Join(filepath.Dir(binDir), "cnpg-discovery-state"),
		"RUN_SH_TEST_CNPG_FAILOVER_STATE="+filepath.Join(filepath.Dir(binDir), "cnpg-failover-state"),
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
