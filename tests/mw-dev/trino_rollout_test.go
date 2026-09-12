package e2emwdev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Run the real registry-only phase: rollout completion leaves the old revision
// serving during preStop, so configuration assertions must await its deletion.
func TestTrinoRegistryOnlyRolloutHandoff(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-multicell.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	helpersEnd := strings.Index(script, "CELL_NS=")
	phaseStart := strings.Index(script, `log "registry-only startup without legacy"`)
	if helpersEnd < 0 || phaseStart < 0 {
		t.Fatal("missing real registry-only phase")
	}
	phaseEnd := strings.Index(script[phaseStart:], "\nwait_cell_ready") + phaseStart
	if phaseEnd < phaseStart {
		t.Fatal("missing real registry-only phase")
	}
	for _, tc := range []struct{ name, failure, cells, want string }{
		{"old revision finishes before assertion", "", `{"cells":[{"id":"cell-test"}]}`, ""},
		{"snapshot fails", "snapshot", "", "snapshot control-plane pods"},
		{"empty snapshot fails", "empty", "", "no control-plane pods"},
		{"rollout fails", "rollout", "", "control-plane rollout failed"},
		{"old pod deletion times out", "delete", "", "old control-plane pods did not terminate"},
		{"transport fails", "transport", "", "registry-only cells API request failed"},
		{"legacy cell remains", "", `{"cells":[{"id":"cell-test"},{"id":"legacy"}]}`, "registry-only startup invented a legacy cell"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			calls := filepath.Join(dir, "calls")
			writeFake(t, dir, "kubectl", `#!/bin/sh
printf 'kubectl %s\n' "$*" >> "$ROLLOUT_TEST_CALLS"
case "$*" in
 *"get deployment duckgres-control-plane -o json") printf '%s\n' '{"spec":{"template":{"spec":{"containers":[{"name":"controlplane","env":[{"name":"DUCKGRES_TRINO_COORDINATOR_URL","value":"http://legacy.invalid"}]}]}}}}' ;;
 *"get pods -l app=duckgres-control-plane -o name")
  [ "$ROLLOUT_TEST_FAILURE" != snapshot ] || exit 1
  [ "$ROLLOUT_TEST_FAILURE" != empty ] || exit 0
  printf 'pod/control-plane-old-a\npod/control-plane-old-b\n' ;;
 *"patch deployment duckgres-control-plane"*) ;;
 *"rollout status deployment/duckgres-control-plane --timeout=180s")
  [ "$ROLLOUT_TEST_FAILURE" != rollout ] || exit 1 ;;
 *"wait --for=delete pod/control-plane-old-a pod/control-plane-old-b --timeout=180s")
  [ "$ROLLOUT_TEST_FAILURE" != delete ] || exit 1
  touch "$ROLLOUT_TEST_DELETED" ;;
 *) echo "unexpected kubectl: $*" >&2; exit 1 ;;
esac
`)
			writeFake(t, dir, "curl", `#!/bin/sh
printf 'curl %s\n' "$*" >> "$ROLLOUT_TEST_CALLS"
case "$*" in
 */api/v1/orgs/test-org) printf '%s\n' '{"trino":{"trino_cell_id":"legacy"}}' ;;
 */api/v1/trino/cells)
  [ -f "$ROLLOUT_TEST_DELETED" ] || { echo 'assertion reached retired revision' >&2; exit 99; }
  [ "$ROLLOUT_TEST_FAILURE" != transport ] || { echo 'curl: (7) connection refused' >&2; exit 7; }
  printf '%s\n' "$ROLLOUT_TEST_CELLS" ;;
 *) exit 1 ;;
esac
`)
			harness := `set -eu
log() { :; }
fail() { echo "FAIL: $*" >&2; exit 1; }
api() { curl -fsS "$@"; }
NS=test-namespace
ORG_A=test-org
API=http://control-plane.invalid
` + script[:helpersEnd] + script[phaseStart:phaseEnd]
			cmd := exec.Command("sh", "-c", harness)
			cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "KUBECTL="+filepath.Join(dir, "kubectl"), "ROLLOUT_TEST_CALLS="+calls, "ROLLOUT_TEST_DELETED="+filepath.Join(dir, "deleted"), "ROLLOUT_TEST_FAILURE="+tc.failure, "ROLLOUT_TEST_CELLS="+tc.cells)
			out, runErr := cmd.CombinedOutput()
			if tc.want == "" && runErr != nil {
				t.Fatalf("handoff: %v\n%s", runErr, out)
			}
			if tc.want != "" && (runErr == nil || !strings.Contains(string(out), tc.want)) {
				t.Fatalf("want failure %q; got %v\n%s", tc.want, runErr, out)
			}
			if tc.failure == "transport" && strings.Contains(string(out), "invented a legacy cell") {
				t.Fatal("transport failure misreported as configuration failure")
			}
			callsRaw, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			trace := string(callsRaw)
			if tc.failure == "" || tc.failure == "transport" {
				previous := -1
				for _, step := range []string{"get pods", "patch deployment", "rollout status", "wait --for=delete", "/api/v1/trino/cells"} {
					index := strings.Index(trace, step)
					if index <= previous {
						t.Fatalf("step %q missing or out of order:\n%s", step, trace)
					}
					previous = index
				}
				if strings.Count(trace, "/api/v1/trino/cells") != 1 {
					t.Fatal("configuration assertion must use one request without retries")
				}
			} else if strings.Contains(trace, "/api/v1/trino/cells") {
				t.Fatalf("failed rollout reached behavioral assertion:\n%s", trace)
			}
		})
	}
}
