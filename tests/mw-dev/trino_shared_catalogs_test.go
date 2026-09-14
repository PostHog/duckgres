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

func TestSharedCatalogLaneRequiresRealPinnedGateway(t *testing.T) {
	for _, tc := range []struct{ name, image, suite, want string }{
		{"missing image", "", "trino", "digest-pinned candidate Gateway image"},
		{"mutable image", "ghcr.io/example/gateway:latest", "trino", "digest-pinned candidate Gateway image"},
		{"wrong lane", "ghcr.io/example/gateway@sha256:" + strings.Repeat("a", 64), "duckdb", "require the full-suite Trino lane"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakes := newRunSHFakes(t)
			cmd := runSHCommand(t, fakes.binDir, "deploy", "TRINO_SHARED_CATALOGS_ENABLED=true", "TRINO_GATEWAY_IMAGE="+tc.image, "E2E_SUITE="+tc.suite, "SCENARIO_NAME=full-suite")
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), tc.want) {
				t.Fatalf("expected preflight failure %q, got %v: %s", tc.want, err, out)
			}
			if strings.Contains(fakes.calls(t), "kubectl") {
				t.Fatal("invalid optional lane reached cluster operations")
			}
		})
	}
}

func TestSharedCatalogMissingImageDoesNotBlockCleanup(t *testing.T) {
	fakes := newRunSHFakes(t)
	cmd := runSHCommand(t, fakes.binDir, "teardown", "SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1", "TRINO_SHARED_CATALOGS_ENABLED=true", "TRINO_GATEWAY_IMAGE=", "E2E_SUITE=trino", "SCENARIO_NAME=full-suite")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("missing image prevented cleanup: %v: %s", err, out)
	}
	if !strings.Contains(fakes.calls(t), "duckling/ci-pr-123-trinod") {
		t.Fatal("cleanup did not include the new isolated warehouse")
	}
}

func TestSharedCatalogGatewayFixtureUsesIsolatedSecretsAndPinnedImage(t *testing.T) {
	fakes := newRunSHFakes(t)
	for _, tool := range []string{"envsubst", "openssl"} {
		binary, err := exec.LookPath(tool)
		if err != nil {
			t.Fatal(err)
		}
		writeFake(t, fakes.binDir, tool, "#!/usr/bin/env bash\nexec "+binary+" \"$@\"\n")
	}
	image := "ghcr.io/example/gateway@sha256:" + strings.Repeat("a", 64)
	renderedFile := filepath.Join(t.TempDir(), "rendered.yaml")
	cmd := runSHCommand(t, fakes.binDir, "deploy", "SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1", "SCENARIO_NAME=full-suite", "E2E_SUITE=trino", "TRINO_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/test-trino", "RUN_SH_TEST_RENDERED="+renderedFile, "TRINO_SHARED_CATALOGS_ENABLED=true", "TRINO_GATEWAY_IMAGE="+image)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("optional fixture render failed: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(renderedFile)
	if err != nil {
		t.Fatal(err)
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(string(raw)), 4096)
	foundConfig, foundDeployment := false, false
	foundPendingCredential := false
	forwarded := 0
	for {
		var object map[string]any
		if err := decoder.Decode(&object); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		metadata, _ := object["metadata"].(map[string]any)
		if object["kind"] == "Role" && metadata["name"] == "duckgres-ci-pr-123-shared-catalog-secret-reader" {
			if metadata["namespace"] != "ducklings" {
				t.Fatal("pending tenant credential grant uses the wrong namespace")
			}
			rules := object["rules"].([]any)
			if len(rules) != 1 {
				t.Fatal("pending tenant credential grant must have one rule")
			}
			rule := rules[0].(map[string]any)
			if strings.Join(stringSlice(rule["resourceNames"]), ",") != "cnpg-tenant-ci-pr-123-trinod-password" || strings.Join(stringSlice(rule["verbs"]), ",") != "get" {
				t.Fatal("pending tenant credential grant must be exact-name read-only")
			}
			foundPendingCredential = true
		}
		if object["kind"] == "ConfigMap" {
			data, _ := object["data"].(map[string]any)
			properties, _ := data["config.properties"].(string)
			if strings.Contains(properties, "http-server.process-forwarded=true") {
				if metadata["name"] != "duckgres-trino-blue-coordinator" && metadata["name"] != "duckgres-trino-green-coordinator" {
					t.Fatal("optional Gateway changed forwarded headers on an unrelated backend")
				}
				forwarded++
			}
		}
		if metadata["name"] == "trino-shared-gateway" && object["kind"] == "Secret" {
			data := object["stringData"].(map[string]any)
			var config map[string]any
			configJSON, err := utilyaml.ToJSON([]byte(data["config.yaml"].(string)))
			if err != nil || json.Unmarshal(configJSON, &config) != nil {
				t.Fatal("Gateway private configuration is invalid YAML")
			}
			transaction := config["transactionAwareness"].(map[string]any)
			if transaction["enabled"] != true || transaction["adminToken"] != data["admin-token"] || transaction["adminToken"] == transaction["identityKey"] {
				t.Fatal("Gateway transaction credentials are not independently generated and consistently wired")
			}
			store := config["dataStore"].(map[string]any)
			if !strings.HasSuffix(store["jdbcUrl"].(string), "/duckgres?currentSchema=gateway_rollout_test") {
				t.Fatal("Gateway migration schema must not overlap control-plane migrations")
			}
			for _, key := range []string{"admin-token", "private.pem"} {
				if strings.Contains(string(output), data[key].(string)) {
					t.Fatal("renderer printed private Gateway material")
				}
			}
			foundConfig = true
		}
		if metadata["name"] == "duckgres-trino-gateway" && object["kind"] == "Deployment" {
			spec := object["spec"].(map[string]any)
			if spec["replicas"] != float64(0) {
				t.Fatal("Gateway must remain stopped before isolated database schema setup")
			}
			pod := spec["template"].(map[string]any)["spec"].(map[string]any)
			for _, list := range []string{"containers", "initContainers"} {
				for _, entry := range pod[list].([]any) {
					if entry.(map[string]any)["image"] != image {
						t.Fatal("Gateway fixture substituted another image")
					}
				}
			}
			foundDeployment = true
		}
	}
	if !foundConfig || !foundDeployment || !foundPendingCredential || forwarded != 2 {
		t.Fatal("optional fixture lacks the real Gateway resources")
	}
}

func TestSharedCatalogCheckpointPreservesJSONEvidence(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-shared-catalogs.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start, end := strings.Index(text, "shared_checkpoint() {"), strings.Index(text, "\nshared_mode() {")
	if start < 0 || end <= start {
		t.Fatal("checkpoint helper missing")
	}
	for _, args := range []string{"WARMED", `CLAIMED '{"warmPublication":{"pullRequest":1}}'`} {
		cmd := exec.Command("sh", "-ec", `
shared_operation='{"operationId":"fixture-operation","version":2}'
GATEWAY=https://gateway.invalid
fail() { exit 9; }
shared_gateway() { printf '%s' "$4"; }
`+text[start:end]+"\nshared_checkpoint "+args+"\nprintf '%s' \"$shared_operation\"")
		out, err := cmd.CombinedOutput()
		var body map[string]any
		if err != nil || json.Unmarshal(out, &body) != nil || body["expectedVersion"] != float64(2) {
			t.Fatalf("checkpoint body failed real shell/JSON execution: %v: %s", err, out)
		}
	}
}

func TestSharedCatalogGatewayQueryRejectsForeignContinuations(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-shared-catalogs.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start, end := strings.Index(text, "shared_gateway_query() {"), strings.Index(text, `log "shared-store transition`)
	if start < 0 || end <= start {
		t.Fatal("real Gateway query helper missing")
	}
	for _, tc := range []struct {
		name, next string
		pass       bool
	}{
		{"Gateway continuation", "https://gateway.invalid/backend-token/v1/statement/next", true},
		{"backend bypass", "https://backend.invalid/v1/statement/next", false},
		{"userinfo confusion", "https://gateway.invalid@foreign.invalid/v1/statement/next", false},
		{"query error", "error", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			count := filepath.Join(dir, "count")
			writeFake(t, dir, "curl", `#!/bin/sh
if [ ! -f "$SHARED_TEST_COUNT" ]; then
  printf 1 > "$SHARED_TEST_COUNT"
  if [ "$SHARED_TEST_NEXT" = error ]; then printf '{"error":{"message":"fixture failure"}}'; else
    jq -cn --arg next "$SHARED_TEST_NEXT" '{id:"test",nextUri:$next}'
  fi
else
  printf 2 > "$SHARED_TEST_COUNT"
  printf '{"id":"test","data":[[2,18]]}'
fi
`)
			cmd := exec.Command("sh", "-ec", "GATEWAY=https://gateway.invalid\nCA=fixture-ca\nfail() { echo \"$*\" >&2; exit 9; }\n"+text[start:end]+"\nshared_gateway_query fixture fixture-password 'SELECT 1'")
			cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "SHARED_TEST_COUNT="+count, "SHARED_TEST_NEXT="+tc.next)
			out, runErr := cmd.CombinedOutput()
			if tc.pass {
				if runErr != nil || string(out) != "[[2,18]]" {
					t.Fatalf("Gateway continuation failed: %v: %s", runErr, out)
				}
			} else {
				calls, err := os.ReadFile(count)
				if runErr == nil || err != nil || string(calls) != "1" {
					t.Fatalf("invalid response sent another credentialed request: %v: %s", runErr, out)
				}
			}
		})
	}
}

func TestSharedCatalogLaneKeepsSafetyPhaseOrder(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-shared-catalogs.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	previous := -1
	for _, step := range []string{
		"shared_mode paused", "shared_scale green 0", "shared_mode gateway-shared", "admission freeze never stabilized",
		"frozen cell created a new catalog", "shared_scale green 1", "shared_checkpoint WARMED",
		"target certificate did not bind", "target startup replayed", "shared_checkpoint VERIFIED", "shared_checkpoint CUTOVER",
		"release did not return", "draining blue received", "shared_checkpoint DRAINING", "shared_checkpoint SEALED", "shared_scale blue 0", "shared_checkpoint COMPLETE",
	} {
		index := strings.Index(text, step)
		if index <= previous {
			t.Fatalf("safety phase missing or out of order: %s", step)
		}
		previous = index
	}
}
