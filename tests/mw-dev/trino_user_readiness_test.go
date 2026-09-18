package e2emwdev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrinoUserWaitsForCatalogAuthorization(t *testing.T) {
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	section := strings.Index(script, "log \"per-user duckgres logins")
	if section < 0 {
		t.Fatal("per-user section missing")
	}
	start := strings.Index(script[section:], "\ni=0\n") + section
	end := strings.Index(script[start:], "\ntrino_query \"$analyst_principal\" \"$pw_a\"") + start
	if start < section || end <= start {
		t.Fatal("per-user readiness assertions missing")
	}
	for _, mode := range []string{"delayed", "denied", "wrong-count"} {
		t.Run(mode, func(t *testing.T) {
			calls := filepath.Join(t.TempDir(), "calls")
			code := `set -eu
analyst_principal=fixture.analyst
analyst_pw=fixture
CAT_A=org_fixture
schema=main
table=fixture
TRINO_AUTH_ROTATION_ATTEMPTS=4
TRINO_AUTH_ROTATION_RETRY_SECONDS=0
sleep() { :; }
fail() { echo "$*"; exit 1; }
trino_query() { echo 1; }
scalar() {
 [ "$1" = "$analyst_principal" ] && [ "$2" = "$analyst_pw" ] && [ "$3" = "SELECT count(*) FROM $CAT_A.$schema.$table" ] || exit 2
 echo attempt >> "$TEST_CALLS"
 count=$(wc -l < "$TEST_CALLS")
 if [ "$TEST_MODE" = denied ] || [ "$count" -lt 3 ]; then
  echo 'Access Denied: Cannot access catalog' >&2
  return 1
 fi
 if [ "$TEST_MODE" = wrong-count ]; then echo 2; else echo 1; fi
}
` + script[start:end]
			cmd := exec.Command("sh", "-c", code)
			cmd.Env = append(os.Environ(), "TEST_CALLS="+calls, "TEST_MODE="+mode)
			out, err := cmd.CombinedOutput()
			if mode == "delayed" {
				if err != nil {
					t.Fatalf("independently delayed authorization failed: %v %s", err, out)
				}
			} else if err == nil {
				t.Fatalf("%s incorrectly passed: %s", mode, out)
			}
			observed, _ := os.ReadFile(calls)
			if count := strings.Count(string(observed), "attempt\n"); count < 3 || count > 5 {
				t.Fatalf("expected bounded catalog authorization retries, got %d: %s", count, out)
			}
		})
	}
}
