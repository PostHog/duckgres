package e2emwdev_test

import (
	"encoding/json"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
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

// The per-PR lane enables the cache for managed Hoglake catalogs so trino.sh
// can assert enabled mode. Frozen perf leaves it unset because its runner
// requires an uncached managed tenant catalog. Neither lane sets the DuckLake
// setting.
func TestTrinoDeployEnablesHoglakeFilesystemCacheOutsideFrozenPerf(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		want map[string]string
	}{
		{"per-PR lane", nil, map[string]string{"DUCKGRES_TRINO_HOGLAKE_FILESYSTEM_CACHE_ENABLED": "true"}},
		{"frozen perf", []string{"SCENARIO_NAME=posthog_frozen_perf", "SCENARIO_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/scenario-dev"}, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRunSHFakes(t)
			secretDir := filepath.Join(filepath.Dir(f.binDir), "secrets")
			for _, name := range []string{"duckgres-ci-trino-ca.crt", "duckgres-ci-trino-server.p12"} {
				if err := os.WriteFile(filepath.Join(secretDir, name), []byte("test-tls-material\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			env := append([]string{"SCENARIO_DEV_ALLOW_DUCKLING_DELETE=1", "E2E_SUITE=trino", "TRINO_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/trino-dev"}, tc.env...)
			if out, err := runSHCommand(t, f.binDir, "deploy", env...).CombinedOutput(); err != nil {
				t.Fatalf("Trino deploy failed: %v\n%s", err, out)
			}
			const prefix = "kubectl --context test-context -n duckgres-ci-pr-123 patch deployment duckgres-control-plane --type=strategic -p "
			var patches []string
			for _, line := range strings.Split(f.calls(t), "\n") {
				if strings.HasPrefix(line, prefix) {
					patches = append(patches, strings.TrimPrefix(line, prefix))
				}
			}
			if len(patches) != 1 {
				t.Fatalf("found %d managed Hoglake control-plane patches, want 1", len(patches))
			}
			var patch struct {
				Spec struct {
					Template struct {
						Spec struct {
							Containers []struct {
								Name string
								Env  []struct{ Name, Value string }
							}
						}
					}
				}
			}
			if err := json.Unmarshal([]byte(patches[0]), &patch); err != nil {
				t.Fatalf("parse control-plane patch %s: %v", patches[0], err)
			}
			got := map[string]string{}
			for _, container := range patch.Spec.Template.Spec.Containers {
				for _, env := range container.Env {
					if strings.Contains(env.Name, "FILESYSTEM_CACHE") {
						got[env.Name] = env.Value
					}
				}
			}
			if !maps.Equal(got, tc.want) {
				t.Fatalf("control-plane cache settings = %v, want %v", got, tc.want)
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

// trino.sh reads tenant A's published catalog row and requires exactly one row
// with fs.cache.enabled=true before its first query against that catalog.
func TestTrinoHarnessAssertsCachedManagedHoglakeCatalog(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	start := strings.Index(script, "# BEGIN managed catalog cache assertion")
	end := strings.Index(script, "# END managed catalog cache assertion")
	if start < 0 || end <= start {
		t.Fatal("managed catalog cache assertion missing")
	}
	ready := strings.Index(script, `wait_trino "$ORG_A" "$DB_A" "$CAT_A"`)
	call := strings.Index(script, `assert_catalog_cache_enabled "$CAT_A"`)
	firstQuery := strings.Index(script, "CREATE TABLE $CAT_A.")
	if ready < 0 || call <= ready || firstQuery <= call {
		t.Fatal("tenant A's cache mode must be asserted after readiness and before its first catalog query")
	}

	for _, tc := range []struct {
		name, password, rows string
		psqlFails, ok        bool
	}{
		{name: "one cached row", password: "store-password", rows: "1|true", ok: true},
		{name: "uncached", password: "store-password", rows: "1|false"},
		{name: "property absent", password: "store-password", rows: "1|"},
		{name: "no row", password: "store-password", rows: "0|"},
		{name: "duplicate rows", password: "store-password", rows: "2|true"},
		{name: "store unreachable", password: "store-password", psqlFails: true},
		{name: "no credentials", rows: "1|true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			code := "set -eu\nNS=duckgres-ci-pr-123\nPR=123\nKUBECTL=fake_kubectl\n" +
				"fail() { echo \"FAIL: $*\"; exit 1; }\n" +
				"fake_kubectl() { printf '%s\\n' \"$*\" > \"$CALLS/kubectl\"; printf %s \"$TEST_PASSWORD\" | base64; }\n" +
				"psql() { printf '%s\\n' \"$PGPASSWORD\" \"$@\" > \"$CALLS/psql-args\"; cat > \"$CALLS/psql-sql\"; [ -z \"$TEST_PSQL_FAILS\" ] || return 1; printf '%s\\n' \"$TEST_ROWS\"; }\n" +
				script[start:end] + "\nassert_catalog_cache_enabled org_trino_a_123\necho PASS\n"
			cmd := exec.Command("sh", "-c", code)
			psqlFails := ""
			if tc.psqlFails {
				psqlFails = "1"
			}
			cmd.Env = append(os.Environ(), "CALLS="+dir, "TEST_PASSWORD="+tc.password, "TEST_ROWS="+tc.rows, "TEST_PSQL_FAILS="+psqlFails)
			out, err := cmd.CombinedOutput()
			if tc.ok != (err == nil && strings.Contains(string(out), "PASS")) {
				t.Fatalf("assertion outcome mismatch (want pass %v): %v %s", tc.ok, err, out)
			}
			if !tc.ok && !strings.Contains(string(out), "FAIL: ") {
				t.Fatalf("rejection did not use fail: %s", out)
			}
			kubectl, _ := os.ReadFile(filepath.Join(dir, "kubectl"))
			if !strings.Contains(string(kubectl), "-n duckgres-ci-pr-123 get secret duckgres-config-store-credentials") {
				t.Fatalf("credentials must come from the namespace's own config store: %s", kubectl)
			}
			if tc.password == "" {
				return
			}
			args, _ := os.ReadFile(filepath.Join(dir, "psql-args"))
			for _, want := range []string{"store-password\n", "duckgres-config-store.duckgres-ci-pr-123.svc", "cell=ci-pr-123", "catalog=org_trino_a_123", "ON_ERROR_STOP=1"} {
				if !strings.Contains(string(args), want) {
					t.Errorf("psql invocation missing %q: %s", want, args)
				}
			}
			sql, _ := os.ReadFile(filepath.Join(dir, "psql-sql"))
			for _, want := range []string{"count(*)", "properties::jsonb ->> 'fs.cache.enabled'", "FROM trino_catalogs", "cell_id = :'cell'", "catalog_name = :'catalog'"} {
				if !strings.Contains(string(sql), want) {
					t.Errorf("catalog query missing %q: %s", want, sql)
				}
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
	start := strings.Index(script, "\"$KUBECTL\" -n \"$NS\" delete pod -l 'posthog.com/trino-pool=pool-test,app.kubernetes.io/component=worker'")
	end := strings.Index(script, "\nlog \"immutable pool replacement")
	if start < 0 || end <= start {
		t.Fatal("restart assertions missing")
	}
	// Model a successful read after worker replacement.
	code := "set -eu\nKUBECTL=true\nold_instance=fixture\nNS=fixture\nDB_A=fixture\npw_a=fixture\nCAT_A=fixture\nschema=main\ntable=fixture\nscalar() { echo two; }\nlog() { :; }\nsleep() { :; }\nfail() { echo \"$*\"; exit 1; }\n" + script[start:end]
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
