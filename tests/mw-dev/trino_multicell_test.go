package e2emwdev_test

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestTrinoMulticellFixtureKeepsIndependentBackendState(t *testing.T) {
	raw, err := os.ReadFile("trino-multicell.tmpl.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`name: ${TRINO_CELL_NAMESPACE}`, `namespace: ${NAMESPACE}`,
		`"id":"cell-test"`, `"running":false`, `"routing_active":false`,
		`trino-blue-internal`, `trino-green-internal`,
		`name: trino-cell-projection`, `resourceNames: ["trino-auth", "trino-tenant-secrets", "trino-opa-bundle-token"]`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("missing isolated fleet fixture contract %q", want)
		}
	}
	raw, err = os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`TRINO_CELL_NS="duckgres-ci-pr-0${PR_NUMBER}"`, `render_trino_backend`, `delete_trino_cell_stack`, `DUCKGRES_TRINO_CELLS_FILE`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("missing fleet lifecycle contract %q", want)
		}
	}
}

func TestTrinoRegistryOnlyHarnessFollowsLegacyCompatibility(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-multicell.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	phase := strings.Index(text, `log "registry-only startup without legacy"`)
	compat := strings.Index(text, `legacy failed during green hydration`)
	if phase < compat || compat < 0 {
		t.Fatal("registry-only phase must follow legacy and green validation")
	}
	for _, want := range []string{`DUCKGRES_TRINO_REGISTRY_ONLY`, `DUCKGRES_TRINO_COORDINATOR_URL`, `"$code" = 409`, `"$code" = 404`, `registry-only registered query failed`, `restore legacy fixture configuration`, `registry-only initial selection required`} {
		if !strings.Contains(text, want) {
			t.Errorf("missing registry-only E2E contract %q", want)
		}
	}
}

func TestTrinoCellRegistryWaitsForServiceConvergence(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-multicell.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start := strings.Index(text, "wait_cell_registry() {")
	if start < 0 {
		t.Fatal("missing bounded registry convergence helper")
	}
	end := strings.Index(text[start:], "\n}\n")
	if end < 0 {
		t.Fatal("missing registry helper end")
	}
	helper := text[start : start+end+3]
	for _, tc := range []struct {
		name, responses string
		wantSuccess     bool
	}{
		{"connection refused then ready", "refused\ncell-test\n", true},
		{"old endpoint then ready", "cell-test,legacy\ncell-test\n", true},
		{"wrong registry never accepted", "cell-test,legacy\n", false},
		{"unavailable API times out", "refused\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			responses := filepath.Join(t.TempDir(), "responses")
			if err := os.WriteFile(responses, []byte(tc.responses), 0o600); err != nil {
				t.Fatal(err)
			}
			script := `set -eu
api() {
  [ "$1" = --max-time ] && [ "$2" = 5 ] || exit 90
  response=$(head -n 1 "$RESPONSES")
  if [ "$(wc -l < "$RESPONSES")" -gt 1 ]; then
    tail -n +2 "$RESPONSES" > "$RESPONSES.next"
    mv "$RESPONSES.next" "$RESPONSES"
  fi
  [ "$response" != refused ] || return 7
  printf %s "$response" | jq -Rc '{cells:(split(",") | map({id:.}))}'
}
sleep() { :; }
fail() { echo "$*" >&2; exit 1; }
API=http://fixture.invalid
` + helper + `
wait_cell_registry '["cell-test"]'
`
			cmd := exec.Command("sh", "-c", script)
			cmd.Env = append(os.Environ(), "RESPONSES="+responses)
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.wantSuccess {
				t.Fatalf("success=%v, want=%v: %s", err == nil, tc.wantSuccess, output)
			}
			if !tc.wantSuccess && !strings.Contains(string(output), "cell registry did not converge") {
				t.Fatalf("missing actionable timeout: %s", output)
			}
		})
	}
}

func TestTrinoMulticellRenderedBackendsAreIsolated(t *testing.T) {
	envsubst, err := exec.LookPath("envsubst")
	if err != nil {
		t.Fatal("envsubst is required to verify the real renderer")
	}
	fakes := newRunSHFakes(t)
	writeFake(t, fakes.binDir, "envsubst", "#!/usr/bin/env bash\nexec "+envsubst+" \"$@\"\n")
	openssl, err := exec.LookPath("openssl")
	if err != nil {
		t.Fatal(err)
	}
	writeFake(t, fakes.binDir, "openssl", "#!/usr/bin/env bash\nexec "+openssl+" \"$@\"\n")
	secretDir := filepath.Join(filepath.Dir(fakes.binDir), "secrets")
	for _, name := range []string{"duckgres-ci-trino-ca.crt", "duckgres-ci-trino-server.p12"} {
		if err := os.WriteFile(filepath.Join(secretDir, name), []byte("test-tls-material"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	renderedFile := filepath.Join(t.TempDir(), "rendered.yaml")
	cmd := runSHCommand(t, fakes.binDir, "deploy", "SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1", "SCENARIO_NAME=full-suite", "E2E_SUITE=trino", "TRINO_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/test-trino", "RUN_SH_TEST_RENDERED="+renderedFile, "GITHUB_ACTIONS=true")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("render/deploy: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(renderedFile)
	if err != nil {
		t.Fatal(err)
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(string(raw)), 4096)
	configs := map[string]map[string]any{}
	deployments := map[string]map[string]any{}
	secrets := []map[string]any{}
	publicManifests := []map[string]any{}
	workerPermissions := map[string]bool{}
	for {
		var manifest map[string]any
		if err := decoder.Decode(&manifest); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode real manifests: %v", err)
		}
		switch manifest["kind"] {
		case "ClusterRoleBinding":
			if manifestName(manifest) != "duckgres-ci-pr-123-duckling-reader" {
				t.Errorf("multicell renderer added cluster privileges: %s", manifestName(manifest))
			}
		case "NetworkPolicy", "CiliumNetworkPolicy", "CiliumClusterwideNetworkPolicy", "ClusterRole":
			t.Errorf("multicell renderer must preserve baseline network policy and cluster privileges, got %s %s", manifest["kind"], manifestName(manifest))
		}
		if manifest["kind"] == "ConfigMap" {
			configs[manifestName(manifest)] = manifest["data"].(map[string]any)
		}
		if manifest["kind"] == "Deployment" {
			deployments[manifestName(manifest)] = manifest
		}
		if manifest["kind"] == "Role" && manifestName(manifest) == "trino-cell-projection" {
			if manifest["metadata"].(map[string]any)["namespace"] != "duckgres-ci-pr-0123" {
				t.Fatal("worker inspection permissions must remain in the isolated secondary namespace")
			}
			for _, rawRule := range manifest["rules"].([]any) {
				rule := rawRule.(map[string]any)
				for _, resource := range rule["resources"].([]any) {
					if resource != "pods" && resource != "pods/exec" {
						continue
					}
					verbs, err := json.Marshal(rule["verbs"])
					if err != nil {
						t.Fatal(err)
					}
					want := `["get","list"]`
					if resource == "pods/exec" {
						want = `["create","get"]`
					}
					if string(verbs) != want {
						t.Fatalf("unexpected worker inspection verbs: %s", verbs)
					}
					workerPermissions[resource.(string)] = true
				}
			}
		}
		if manifest["kind"] == "Secret" {
			secrets = append(secrets, manifest)
		} else {
			publicManifests = append(publicManifests, manifest)
		}
	}
	if !workerPermissions["pods"] || !workerPermissions["pods/exec"] {
		t.Fatal("missing isolated worker mount inspection permissions")
	}
	passwordBytes, err := os.ReadFile(filepath.Join(secretDir, "duckgres-ci-config-store-password"))
	if err != nil {
		t.Fatal("renderer must generate a per-run config-store credential:", err)
	}
	password := strings.TrimSpace(string(passwordBytes))
	if len(password) != 64 || strings.Trim(password, "0123456789abcdef") != "" {
		t.Fatal("config store must use a random, URL-safe 32-byte password")
	}
	mask := "::add-mask::" + password + "\n"
	if !strings.Contains(string(output), mask) || strings.Contains(strings.ReplaceAll(string(output), mask, ""), password) {
		t.Fatal("password must only appear in the GitHub masking directive")
	}
	info, err := os.Stat(filepath.Join(secretDir, "duckgres-ci-config-store-password"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("local credential must be owner-readable only")
	}
	credentialCount := 0
	for _, secret := range secrets {
		switch manifestName(secret) {
		case "duckgres-config-store-credentials", "duckgres-trino-catalog-store":
			credentialCount++
			data := secret["stringData"].(map[string]any)
			if data["password"] != password {
				t.Fatal("config-store credentials differ between consumers")
			}
			if manifestName(secret) == "duckgres-config-store-credentials" && data["dsn"] != "postgres://duckgres:"+password+"@duckgres-config-store.duckgres-ci-pr-123.svc:5432/duckgres?sslmode=disable" {
				t.Fatal("control-plane DSN does not match config-store password")
			}
		}
	}
	if credentialCount != 4 {
		t.Fatalf("expected primary credential and three catalog-store Secrets, got %d", credentialCount)
	}
	for _, manifest := range publicManifests {
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), password) {
			t.Fatalf("credential leaked into non-Secret %s %s", manifest["kind"], manifestName(manifest))
		}
	}
	for deploymentName, envName := range map[string]string{
		"duckgres-config-store": "POSTGRES_PASSWORD", "duckgres-control-plane": "DUCKGRES_CONFIG_STORE",
	} {
		deployment := deployments[deploymentName]
		containers := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
		found := false
		for _, container := range containers {
			for _, value := range container.(map[string]any)["env"].([]any) {
				env := value.(map[string]any)
				if env["name"] != envName {
					continue
				}
				found = true
				ref := env["valueFrom"].(map[string]any)["secretKeyRef"].(map[string]any)
				key := "dsn"
				if envName == "POSTGRES_PASSWORD" {
					key = "password"
				}
				if ref["name"] != "duckgres-config-store-credentials" || ref["key"] != key {
					t.Fatalf("incorrect credential reference for %s", envName)
				}
			}
		}
		if !found {
			t.Fatalf("missing credential reference for %s", envName)
		}
	}
	for _, sameRun := range []bool{true, false} {
		repeatFakes := fakes
		if !sameRun {
			repeatFakes = newRunSHFakes(t)
			writeFake(t, repeatFakes.binDir, "openssl", "#!/usr/bin/env bash\nexec "+openssl+" \"$@\"\n")
		}
		cmd := runSHCommand(t, repeatFakes.binDir, "deploy", "SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1", "GITHUB_ACTIONS=false")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("repeat render: %v %s", err, out)
		}
		repeated, err := os.ReadFile(filepath.Join(filepath.Dir(repeatFakes.binDir), "secrets", "duckgres-ci-config-store-password"))
		if err != nil {
			t.Fatal(err)
		}
		if (strings.TrimSpace(string(repeated)) == password) != sameRun {
			t.Fatal("credential must be stable within a run and different across fresh runs")
		}
	}
	for _, color := range []string{"blue", "green"} {
		name := "duckgres-trino-" + color
		config := configs[name+"-coordinator"]
		if config == nil {
			t.Fatalf("missing %s config", color)
		}
		for key, want := range map[string]string{
			"config.properties":        "discovery.uri=http://" + name + ".duckgres-ci-pr-0123.svc:8080",
			"node.properties":          "node.environment=ci_pr_123_" + color,
			"catalog-store.properties": "catalog-store.cell-id=ci-pr-123-" + color,
		} {
			if !strings.Contains(config[key].(string), want) {
				t.Fatalf("%s %s missing %q", color, key, want)
			}
		}
		if !strings.Contains(config["catalog-store.properties"].(string), "duckgres-config-store.duckgres-ci-pr-123.svc") {
			t.Fatal("catalog store must remain in primary namespace")
		}
		for _, role := range []string{"coordinator", "worker"} {
			deployment := deployments[name+"-"+role]
			if deployment == nil {
				t.Fatalf("missing %s %s", color, role)
			}
			spec := deployment["spec"].(map[string]any)
			if spec["replicas"] != float64(0) {
				t.Fatal("all Trino pods must await projection bootstrap")
			}
			labels := spec["selector"].(map[string]any)["matchLabels"].(map[string]any)
			if labels["app"] != name {
				t.Fatalf("backend selector overlaps: %+v", labels)
			}
		}
	}
	if !strings.Contains(configs["duckgres-trino-opa"]["config.yaml"].(string), "/bundles/trino/cell-test") {
		t.Fatal("new cell must use its scoped OPA bundle")
	}
	calls := fakes.calls(t)
	if strings.Contains(calls, "patch deployment duckgres-trino-green-") {
		t.Fatal("deploy must not start stopped green")
	}
	if !strings.Contains(calls, "--namespace duckgres-ci-pr-0123 --service-account trino") {
		t.Fatal("new cell must receive its own Pod Identity association")
	}
}

func TestTrinoSecondaryNamespaceCleanupFailsClosed(t *testing.T) {
	for _, inventory := range []string{
		"duckgres-ci-pr-0123 2026-01-01T00:00:00Z 999 trino-cell",
		"duckgres-ci-pr-123 2026-01-01T00:00:00Z",
		"unrelated 2026-01-01T00:00:00Z 123",
	} {
		fakes := newRunSHFakes(t)
		cmd := runSHCommand(t, fakes.binDir, "e2e-cleanup", "RUN_SH_TEST_NAMESPACE_INVENTORY="+inventory)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("cleanup: %v %s", err, out)
		}
		calls := fakes.calls(t)
		if strings.Contains(calls, " delete ") || strings.Contains(calls, "aws eks") {
			t.Fatalf("unowned namespace triggered cleanup: %s", calls)
		}
	}
}

func TestTrinoSecondaryNamespaceCleanupRequiresOwnershipAndUID(t *testing.T) {
	for _, tc := range []struct {
		name, object string
		allowed      bool
	}{
		{"owned", `{"metadata":{"name":"duckgres-ci-pr-0123","uid":"test-uid","labels":{"app.kubernetes.io/managed-by":"e2e-mw-dev","duckgres.posthog.com/ci-pr":"123","duckgres.posthog.com/ci-component":"trino-cell"}}}`, true},
		{"unowned", `{"metadata":{"name":"duckgres-ci-pr-0123","uid":"test-uid","labels":{"app.kubernetes.io/managed-by":"e2e-mw-dev","duckgres.posthog.com/ci-pr":"999","duckgres.posthog.com/ci-component":"trino-cell"}}}`, false},
		{"missing-uid", `{"metadata":{"name":"duckgres-ci-pr-0123","labels":{"app.kubernetes.io/managed-by":"e2e-mw-dev","duckgres.posthog.com/ci-pr":"123","duckgres.posthog.com/ci-component":"trino-cell"}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakes := newRunSHFakes(t)
			cmd := runSHCommand(t, fakes.binDir, "e2e-cleanup", "RUN_SH_TEST_NAMESPACE_INVENTORY=duckgres-ci-pr-0123 2026-01-01T00:00:00Z 123 trino-cell", "RUN_SH_TEST_SECONDARY_NAMESPACE="+tc.object)
			out, err := cmd.CombinedOutput()
			if tc.allowed && err != nil {
				t.Fatalf("owned cleanup failed: %v %s", err, out)
			}
			if !tc.allowed && err == nil {
				t.Fatal("unowned cleanup must fail")
			}
			calls := fakes.calls(t)
			if strings.Contains(calls, "delete --raw /api/v1/namespaces/duckgres-ci-pr-0123") != tc.allowed {
				t.Fatalf("wrong deletion decision: %s", calls)
			}
			if tc.allowed && !strings.Contains(calls, `"uid": "test-uid"`) {
				t.Fatal("namespace delete lost UID precondition")
			}
			if strings.Contains(calls, "ducklings") || strings.Contains(calls, "cnpg-shards") {
				t.Fatal("secondary cleanup touched primary warehouse resources")
			}
		})
	}
}

func TestTrinoNamespaceRejectsNoncanonicalPRNumbers(t *testing.T) {
	for _, pr := range []string{"0", "0123", "-1", "123x"} {
		fakes := newRunSHFakes(t)
		cmd := runSHCommand(t, fakes.binDir, "deploy", "PR_NUMBER="+pr, "NAMESPACE=duckgres-ci-pr-"+pr)
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("noncanonical PR accepted: %s %s", pr, out)
		}
	}
}

func TestTrinoMulticellHarnessExercisesRealPlacementAndHydration(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-multicell.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`/trino/cell`, `registered:cell-test`, `green catalog hydration`,
		`/bundles/trino/cell-test`, `trino-blue-internal`, `trino-green-internal`,
		`legacy remains queryable`, `SELECT COUNT(*)`,
		`'{"enabled":true,"tier":"free"}'`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("missing real fleet assertion %q", want)
		}
	}
}
