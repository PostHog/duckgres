package e2emwdev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func managementReadOnlySection(t *testing.T) (script, section string) {
	t.Helper()
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatal(err)
	}
	script = string(raw)
	start := strings.Index(script, "# BEGIN management read-only hand-over")
	end := strings.Index(script, "# END management read-only hand-over")
	if start < 0 || end <= start {
		t.Fatal("management read-only hand-over helpers missing")
	}
	return script, script[start:end]
}

// The Trino lane records hogtower's hand-over marker, expects management
// writes to flip to 409 while reads and Trino keep working, then clears the
// marker and expects writes back BEFORE the teardown deprovisions through
// the API.
func TestTrinoHarnessAssertsReadOnlyManagementAfterHandover(t *testing.T) {
	script, _ := managementReadOnlySection(t)
	order := []string{
		"wait_management_write_status 400",
		"record_handover || fail",
		"wait_management_write_status 409",
		`.managed_by == "hogtower"`,
		`"$API/api/v1/orgs/$ORG_B/teams" | jq -e`,
		`"$API/api/v1/orgs/$ORG_A/warehouse/status"`,
		`scalar "$DB_A" "$pw_a"`,
		"clear_handover || fail",
		"wait_management_write_status 400",
		`log "PASS:`,
	}
	flowStart := strings.Index(script, `log "hand-over marker makes the management API read-only`)
	if flowStart < 0 {
		t.Fatal("read-only hand-over flow missing")
	}
	at := flowStart
	for _, want := range order {
		i := strings.Index(script[at:], want)
		if i < 0 {
			t.Fatalf("read-only hand-over flow missing %q after offset %d", want, at)
		}
		at += i + len(want)
	}
	trapAt := strings.Index(script[flowStart:], "clear_handover >/dev/null 2>&1 || true")
	recordAt := strings.Index(script[flowStart:], "record_handover || fail")
	if trapAt < 0 || trapAt > recordAt {
		t.Fatal("the EXIT trap must clear the marker before it is recorded, so a failure cannot strand teardown behind a 409")
	}
}

func TestTrinoHarnessHandoverHelpers(t *testing.T) {
	_, section := managementReadOnlySection(t)
	dir := t.TempDir()
	code := "set -eu\nNS=duckgres-ci-pr-123\nAPI=http://cp\nH='X-Duckgres-Internal-Secret: s'\nORG_B=ci-pr-123-trinob\nKUBECTL=fake_kubectl\n" +
		"fail() { echo \"FAIL: $*\"; exit 1; }\n" +
		"sleep() { :; }\n" +
		"fake_kubectl() { printf '%s\\n' \"$*\" >> \"$CALLS/kubectl\"; printf %s store-password | base64; }\n" +
		"psql() { printf '%s\\n' \"$PGPASSWORD\" \"$@\" >> \"$CALLS/psql-args\"; cat >> \"$CALLS/psql-sql\"; }\n" +
		"curl() { printf '%s\\n' \"$*\" >> \"$CALLS/curl\"; printf '{}' > /tmp/management_write_body; printf %s \"$TEST_STATUS\"; }\n" +
		section +
		"\nrecord_handover\nclear_handover\nwait_management_write_status \"$WANT\"\necho PASS\n"

	run := func(status, want string) (string, error) {
		cmd := exec.Command("sh", "-c", code)
		cmd.Env = append(os.Environ(), "CALLS="+dir, "TEST_STATUS="+status, "WANT="+want)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if out, err := run("409", "409"); err != nil || !strings.Contains(out, "PASS") {
		t.Fatalf("helpers must pass when the status matches: %v %s", err, out)
	}
	if out, err := run("400", "409"); err == nil || !strings.Contains(out, "FAIL: management write returned 400, want 409") {
		t.Fatalf("a status that never flips must fail the lane: %v %s", err, out)
	}

	kubectl, _ := os.ReadFile(filepath.Join(dir, "kubectl"))
	if !strings.Contains(string(kubectl), "-n duckgres-ci-pr-123 get secret duckgres-config-store-credentials") {
		t.Fatalf("credentials must come from the namespace's own config store: %s", kubectl)
	}
	args, _ := os.ReadFile(filepath.Join(dir, "psql-args"))
	for _, want := range []string{"store-password\n", "duckgres-config-store.duckgres-ci-pr-123.svc", "ON_ERROR_STOP=1"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("psql invocation missing %q: %s", want, args)
		}
	}
	sql, _ := os.ReadFile(filepath.Join(dir, "psql-sql"))
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS duckgres_control_handover (component text PRIMARY KEY, owner text NOT NULL",
		"VALUES ('provisioning', 'hogtower') ON CONFLICT (component) DO UPDATE",
		"DELETE FROM duckgres_control_handover WHERE component = 'provisioning'",
	} {
		if !strings.Contains(string(sql), want) {
			t.Errorf("hand-over SQL missing %q: %s", want, sql)
		}
	}
	calls, _ := os.ReadFile(filepath.Join(dir, "curl"))
	if !strings.Contains(string(calls), "-X POST") || !strings.Contains(string(calls), "-d {}") || !strings.Contains(string(calls), "http://cp/api/v1/orgs/ci-pr-123-trinob/teams") {
		t.Fatalf("the write probe must be the non-mutating invalid team upsert: %s", calls)
	}
}
