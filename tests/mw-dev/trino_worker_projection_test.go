package e2emwdev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrinoWorkerProjectionGate(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino-multicell.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	start := strings.Index(text, "wait_worker_tenant_file() {")
	if start < 0 {
		t.Fatal("missing worker file convergence gate")
	}
	end := strings.Index(text[start:], "\n}\n")
	if end < 0 {
		t.Fatal("unterminated worker helper")
	}
	helper := text[start : start+end+3]
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"ready", true}, {"one-worker-lags", true}, {"missing-file", false},
		{"no-pods", false}, {"unready", false}, {"terminating", false},
		{"wrong-label", false}, {"wrong-name", false}, {"denied", false},
		{"stale-generation", false}, {"ready-count", false}, {"updated-count", false},
		{"wrong-identity", false}, {"missing-worker", false},
		{"exec-timeout", false}, {"exec-denied", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := filepath.Join(t.TempDir(), "checks")
			script := `set -eu
PR=123
ORG_C=ci-pr-123-trinoc
CELL_NS=duckgres-ci-pr-0123
[ "$MODE" != wrong-identity ] || CELL_NS=another-namespace
KUBECTL=kubectl
sleep() { :; }
log() { echo "$*"; }
fail() { echo "$*" >&2; exit 1; }
kubectl() {
  [ "$1" = --request-timeout=5s ] && [ "$2" = -n ] && [ "$3" = "$CELL_NS" ] || exit 90
  shift 3
  if [ "$MODE" = denied ]; then return 1; fi
  case "$1 $2" in
    'get deployment')
      [ "$3" = duckgres-trino-blue-worker ] || exit 90
      jq -nc --arg mode "$MODE" '{metadata:{generation:1},spec:{replicas:2},status:{observedGeneration:1,readyReplicas:2,updatedReplicas:2}} |
        if $mode == "stale-generation" then .status.observedGeneration=0
        elif $mode == "ready-count" then .status.readyReplicas=1
        elif $mode == "updated-count" then .status.updatedReplicas=1
        else . end'
      ;;
    'get pods')
      [ "$3" = -l ] && [ "$4" = app=duckgres-trino-blue,component=worker ] || exit 90
      jq -nc --arg mode "$MODE" '{items:[range(1;3) | {metadata:{name:("duckgres-trino-blue-worker-"+tostring),labels:{app:"duckgres-trino-blue",component:"worker"}},status:{phase:"Running",conditions:[{type:"Ready",status:"True"}]}}]} |
        if $mode == "no-pods" then .items=[]
        elif $mode == "missing-worker" then .items=.items[:1]
        elif $mode == "unready" then .items[1].status.conditions[0].status="False"
        elif $mode == "terminating" then .items[1].metadata.deletionTimestamp="now"
        elif $mode == "wrong-label" then .items[1].metadata.labels.app="another-cell"
        elif $mode == "wrong-name" then .items[1].metadata.name="another-worker"
        else . end'
      ;;
    exec*)
      [ "$3" = -c ] && [ "$4" = trino-worker ] && [ "$5" = -- ] && [ "$6" = test ] && [ "$7" = -r ] && [ "$8" = /etc/trino/tenant-secrets/ci-pr-123-trinoc ] || exit 90
      printf '%s\n' "$2" >> "$COUNTER"
      if [ "$MODE" = missing-file ]; then echo 'command terminated with exit code 1' >&2; return 1; fi
      if [ "$MODE" = exec-timeout ]; then echo 'context deadline exceeded private-payload' >&2; return 1; fi
      if [ "$MODE" = exec-denied ]; then echo 'Forbidden private-payload' >&2; return 1; fi
      if [ "$MODE" = one-worker-lags ] && [ "$2" = duckgres-trino-blue-worker-2 ] && [ "$(wc -l < "$COUNTER")" -lt 3 ]; then return 1; fi
      ;;
    *) exit 90 ;;
  esac
}
` + helper + "\nwait_worker_tenant_file blue\n"
			cmd := exec.Command("sh", "-c", script)
			cmd.Env = append(os.Environ(), "MODE="+tc.name, "COUNTER="+counter)
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.ok {
				t.Fatalf("success=%v, want=%v: %s", err == nil, tc.ok, out)
			}
			if tc.name == "missing-file" && !strings.Contains(string(out), "worker-file (exit 1)") {
				t.Fatal("missing safe file-check failure diagnostics")
			}
			if tc.name == "denied" && !strings.Contains(string(out), "deployment-read (exit 1)") {
				t.Fatal("missing safe API failure diagnostics")
			}
			if tc.name == "exec-timeout" && !strings.Contains(string(out), "worker-exec-timeout (exit 1)") {
				t.Fatal("missing safe timeout diagnostics")
			}
			if tc.name == "exec-denied" && !strings.Contains(string(out), "worker-exec-permission (exit 1)") {
				t.Fatal("missing safe permission diagnostics")
			}
			if strings.Contains(string(out), "private-payload") {
				t.Fatal("raw exec stderr must never enter diagnostics")
			}
			if tc.ok {
				checks, err := os.ReadFile(counter)
				if err != nil || !strings.Contains(string(checks), "worker-1") || !strings.Contains(string(checks), "worker-2") {
					t.Fatal("every worker must pass the file check")
				}
			}
		})
	}
	blueGate := strings.Index(text, "wait_worker_tenant_file blue")
	firstWrite := strings.Index(text, `"CREATE SCHEMA $CAT_C.cell_test"`)
	if blueGate < 0 || firstWrite < 0 || blueGate > firstWrite {
		t.Fatal("worker gate must precede all initial writes")
	}
	greenPhase := strings.Index(text, `TRINO="$GREEN_TRINO"`)
	if greenPhase < 0 {
		t.Fatal("missing green phase")
	}
	greenGate := strings.Index(text[greenPhase:], "wait_worker_tenant_file green")
	greenQuery := strings.Index(text[greenPhase:], `result="$(trino_query`)
	if greenGate < 0 || greenQuery < 0 || greenGate > greenQuery {
		t.Fatal("green must check worker projection before its first query")
	}
}
