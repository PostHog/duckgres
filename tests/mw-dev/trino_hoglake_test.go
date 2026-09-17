package e2emwdev_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestHoglakeCleanupRejectsForeignNamespaceAndBroadPrefix(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	identityStart := strings.Index(script, "require_pr_identity() {")
	identityEnd := strings.Index(script[identityStart:], "\nfrozen_perf_scenario()") + identityStart
	cleanupStart := strings.Index(script, "# BEGIN isolated Hoglake cleanup")
	cleanupEnd := strings.Index(script, "# END isolated Hoglake cleanup")
	for _, tc := range []struct {
		name, ns, path string
		ok             bool
	}{
		{"own", "duckgres-ci-pr-123", "s3://example-hoglake/trino/", true},
		{"other-namespace", "duckgres-ci-pr-1234", "s3://example-hoglake/trino/", false},
		{"root", "duckgres-ci-pr-123", "s3://example-hoglake/", false},
		{"other-prefix", "duckgres-ci-pr-123", "s3://example-hoglake/customer/", false},
		{"wildcard", "duckgres-ci-pr-123", "s3://example-hoglake/trino/*/", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code := "set -eu\n" + script[identityStart:identityEnd] + "\n" + script[cleanupStart:cleanupEnd] + "\nKUBECTL=(kubectl)\nkubectl() { return 0; }\naws() { printf '%s\\n' \"$*\"; }\ncleanup_hoglake_storage || exit 1\n"
			cmd := exec.Command("bash", "-c", code)
			cmd.Env = append(os.Environ(), "NS="+tc.ns, "PR_NUMBER=123", "HOGLAKE_DATA_PATH="+tc.path, "AWS_REGION=us-east-1")
			out, err := cmd.CombinedOutput()
			if tc.ok {
				if err != nil || !strings.Contains(string(out), "s3://example-hoglake/trino/ci-pr-123- --recursive") {
					t.Fatalf("scoped cleanup failed: %v %s", err, out)
				}
			} else if err == nil || strings.Contains(string(out), "s3 rm") {
				t.Fatalf("unsafe cleanup admitted: %v %s", err, out)
			}
		})
	}
}

func TestTrinoDeployRequiresHoglakeIdentityBeforeMutatingFixture(t *testing.T) {
	f := newRunSHFakes(t)
	cmd := runSHCommand(t, f.binDir, "deploy", "E2E_SUITE=trino", "HOGLAKE_CI_POD_IDENTITY_ROLE=")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("missing identity accepted: %s", out)
	}
	if calls := f.calls(t); strings.Contains(calls, "kubectl") || strings.Contains(calls, "aws") {
		t.Fatal("missing prerequisite mutated existing fixture")
	}
}

func TestScheduledCleanupRemovesHoglakePrefixAndPropagatesFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			f := newRunSHFakes(t)
			env := []string{"HOGLAKE_DATA_PATH=s3://example-hoglake/trino/"}
			if fail {
				env = append(env, "HOGLAKE_TEST_FAIL_CLEANUP=1")
			}
			out, err := runSHCommand(t, f.binDir, "e2e-cleanup", env...).CombinedOutput()
			if (err != nil) != fail {
				t.Fatalf("cleanup failure propagation: %v %s", err, out)
			}
			calls := f.calls(t)
			deletion := strings.Index(calls, "delete namespace duckgres-ci-pr-123 --ignore-not-found")
			cleanup := strings.Index(calls, "s3 rm s3://example-hoglake/trino/ci-pr-123- --recursive")
			if deletion < 0 || cleanup <= deletion {
				t.Fatalf("cleanup must follow writer deletion: %s", calls)
			}
		})
	}
}
