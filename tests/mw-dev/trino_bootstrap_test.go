package e2emwdev_test

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func bootstrapHelpers(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(raw), "# BEGIN concurrent bootstrap helpers")
	end := strings.Index(string(raw), "# END concurrent bootstrap helpers")
	if start < 0 || end <= start {
		t.Fatal("missing retained concurrent bootstrap regression")
	}
	return string(raw)[start:end]
}

func TestTrinoBootstrapPatchOnlyRemovesCredentialPairs(t *testing.T) {
	helpers := bootstrapHelpers(t)
	cmd := exec.Command("sh", "-c", "set -eu\n"+helpers+"\nprintf %s \"$SECRET\" | bootstrap_remove_pairs_patch\n")
	cmd.Env = append(os.Environ(), `SECRET={"metadata":{"resourceVersion":"42"},"data":{"admin-password":"YQ==","admin-password-hash":"Yg==","observer-password":"Yw==","observer-password-hash":"ZA==","password.db":"preserved"}}`)
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var patch []map[string]string
	if err := json.Unmarshal(output, &patch); err != nil {
		t.Fatal(err)
	}
	want := []map[string]string{
		{"op": "test", "path": "/metadata/resourceVersion", "value": "42"},
		{"op": "remove", "path": "/data/admin-password"},
		{"op": "remove", "path": "/data/admin-password-hash"},
		{"op": "remove", "path": "/data/observer-password"},
		{"op": "remove", "path": "/data/observer-password-hash"},
	}
	if !reflect.DeepEqual(patch, want) {
		t.Fatal("patch must test resourceVersion and remove only the four credential keys")
	}
	for _, value := range []string{`{}`, `{"metadata":{"resourceVersion":"42"},"data":{}}`, `not-json`} {
		cmd := exec.Command("sh", "-c", "set -eu\n"+helpers+"\nprintf %s \"$SECRET\" | bootstrap_remove_pairs_patch\n")
		cmd.Env = append(os.Environ(), "SECRET="+value)
		if err := cmd.Run(); err == nil {
			t.Fatal("invalid Secret accepted")
		}
	}
}

func TestTrinoBootstrapFingerprintsFailClosed(t *testing.T) {
	helpers := bootstrapHelpers(t)
	for _, function := range []string{"bootstrap_pair_fingerprint", "bootstrap_fixed_secrets_fingerprint"} {
		for _, fake := range []string{"return 1", "printf %s not-json", "printf %s '{}'"} {
			cmd := exec.Command("sh", "-c", "set -eu\nKUBECTL=fake\nNS=duckgres-ci-pr-123\nfake() { "+fake+"; }\n"+helpers+"\n"+function+"\n")
			output, err := cmd.Output()
			if err == nil || len(output) != 0 {
				// A failed read must not produce the hash of an empty payload.
				t.Fatalf("%s accepted a failed or malformed Secret read", function)
			}
		}
	}
}

func TestTrinoBootstrapRBACIsExactAndTrinoOnly(t *testing.T) {
	raw, err := os.ReadFile("manifests.trino.tmpl.yaml")
	if err != nil {
		t.Fatal(err)
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(string(raw)), 4096)
	found := 0
	for {
		var manifest map[string]any
		if err := decoder.Decode(&manifest); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if manifest["kind"] != "Role" || manifestName(manifest) != "trino-bootstrap-harness" {
			continue
		}
		found++
		want := []any{
			map[string]any{"apiGroups": []any{"apps"}, "resources": []any{"deployments"}, "resourceNames": []any{"duckgres-control-plane"}, "verbs": []any{"patch"}},
			map[string]any{"apiGroups": []any{""}, "resources": []any{"secrets"}, "resourceNames": []any{"trino-auth"}, "verbs": []any{"patch"}},
		}
		if !reflect.DeepEqual(manifest["rules"], want) || manifest["metadata"].(map[string]any)["namespace"] != "${NAMESPACE}" {
			t.Fatal("bootstrap permission must be namespace-scoped and limited to two exact patch targets")
		}
	}
	if found != 1 {
		t.Fatal("missing unique bootstrap Role")
	}
	base, err := os.ReadFile("manifests.tmpl.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(base), "trino-bootstrap-harness") {
		t.Fatal("bootstrap Role must not be installed outside the Trino lane")
	}
}

func TestTrinoBootstrapFixtureScope(t *testing.T) {
	helpers := bootstrapHelpers(t)
	for _, tc := range []struct {
		name, pr, namespace string
		ok                  bool
	}{
		{"isolated", "123", "duckgres-ci-pr-123", true},
		{"shared", "123", "trino", false},
		{"other_pr", "123", "duckgres-ci-pr-124", false},
		{"empty_pr", "", "duckgres-ci-pr-", false},
		{"invalid_pr", "x", "duckgres-ci-pr-x", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("sh", "-c", "set -eu\nfail() { exit 1; }\n"+helpers+"\nbootstrap_scope\n")
			cmd.Env = append(os.Environ(), "PR="+tc.pr, "NS="+tc.namespace)
			if err := cmd.Run(); (err == nil) != tc.ok {
				t.Fatalf("scope acceptance = %v, want %v", err == nil, tc.ok)
			}
		})
	}
}

func TestTrinoBootstrapPodHealth(t *testing.T) {
	helpers := bootstrapHelpers(t)
	for _, tc := range []struct {
		name, pods string
		ok         bool
	}{
		{"ready", `{"items":[{"metadata":{"uid":"new"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"controlplane","ready":true,"restartCount":0}]}}]}`, true},
		{"restarted", `{"items":[{"metadata":{"uid":"new"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"controlplane","ready":true,"restartCount":1}]}}]}`, false},
		{"terminating", `{"items":[{"metadata":{"uid":"new","deletionTimestamp":"now"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"controlplane","ready":true,"restartCount":0}]}}]}`, false},
		{"old_uid", `{"items":[{"metadata":{"uid":"old"},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"controlplane","ready":true,"restartCount":0}]}}]}`, false},
		{"missing_status", `{"items":[{"metadata":{"uid":"new"}}]}`, false},
		{"no_pods", `{"items":[]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("sh", "-c", "set -eu\n"+helpers+"\nprintf %s \"$PODS\" | bootstrap_pods_healthy 1 '[\"old\"]'\n")
			cmd.Env = append(os.Environ(), "PODS="+tc.pods)
			if err := cmd.Run(); (err == nil) != tc.ok {
				t.Fatalf("pod acceptance = %v, want %v", err == nil, tc.ok)
			}
		})
	}
}
