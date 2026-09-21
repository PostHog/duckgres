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

func TestHoglakeRestartAssertionsMatchInsertedFixture(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	start := strings.Index(script, "\"$KUBECTL\" -n \"$NS\" delete pod -l 'app=duckgres-trino,component=worker'")
	end := strings.Index(script, "\ndeprovision_must_conflict()")
	if start < 0 || end <= start {
		t.Fatal("restart assertions missing")
	}
	// Model the successful database read after both worker and coordinator restarts.
	code := "set -eu\nKUBECTL=true\nNS=fixture\nDB_A=fixture\npw_a=fixture\nCAT_A=fixture\nschema=main\ntable=fixture\nscalar() { echo two; }\nlog() { :; }\nsleep() { :; }\nfail() { echo \"$*\"; exit 1; }\n" + script[start:end]
	if out, err := exec.Command("sh", "-c", code).CombinedOutput(); err != nil {
		t.Fatalf("persisted fixture rejected after restart: %v %s", err, out)
	}
}

func TestDiscoverHoglakeConfiguration(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "ambiguous", "denied"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			fake := `#!/bin/bash
if [[ "$*" == *get-role-policy* ]]; then
 case "$TEST_MODE" in
 denied) exit 1;;
 missing) echo '{"PolicyDocument":{"Statement":[]}}';;
 ambiguous) echo '{"PolicyDocument":{"Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::example-one/trino/ci-pr-*","arn:aws:s3:::example-two/trino/ci-pr-*"]}]}}';;
 *) echo '{"PolicyDocument":{"Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":"arn:aws:s3:::example-hoglake/trino/ci-pr-*"}]}}';;
 esac
else
 echo '{"Role":{"Arn":"arn:aws:iam::123456789012:role/hoglake-ci-dev"}}'
fi
`
			if err := os.WriteFile(dir+"/aws", []byte(fake), 0755); err != nil {
				t.Fatal(err)
			}
			envFile := dir + "/env"
			cmd := exec.Command("bash", "discover-hoglake.sh")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "GITHUB_ENV="+envFile, "TEST_MODE="+mode)
			out, err := cmd.CombinedOutput()
			values, _ := os.ReadFile(envFile)
			if mode == "valid" {
				if err != nil || !strings.Contains(string(values), "HOGLAKE_DATA_PATH=s3://example-hoglake/trino/\n") || !strings.Contains(string(values), "HOGLAKE_CI_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/hoglake-ci-dev\n") {
					t.Fatalf("discovery failed: %v %s %s", err, out, values)
				}
				for _, line := range strings.Split(string(out), "\n") {
					if strings.Contains(line, "123456789012") && !strings.HasPrefix(line, "::add-mask::") {
						t.Fatal("unmasked identifier")
					}
				}
			} else if err == nil || len(values) != 0 {
				t.Fatalf("invalid discovery published configuration: %v %s", err, values)
			}
		})
	}
}
