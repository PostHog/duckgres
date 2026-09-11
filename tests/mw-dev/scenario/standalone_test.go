package scenario

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

const standaloneFixture = `name: standalone-test
standalone: true
steps:
 - id: perf
   type: perf_queries
   with:
    targets: [trino_hoglake]
    username: perf
    password: fixture-password
    trino_server_url: https://localhost:8443
    trino_catalog: hoglake
    trino_reference_catalog: ducklake
`

func TestStandalonePerfNeedsNoControlPlaneOrPGEnvironment(t *testing.T) {
	for _, name := range []string{"DUCKGRES_SCENARIO_API_BASE", "DUCKGRES_SCENARIO_INTERNAL_SECRET", "DUCKGRES_SCENARIO_PG_HOST", "DUCKGRES_SCENARIO_SNI_SUFFIX"} {
		t.Setenv(name, "")
	}
	scenario, err := core.ParseScenario([]byte(standaloneFixture))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := standaloneExecutor(scenario, t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestStandalonePerfRejectsUnsupportedStepCombinations(t *testing.T) {
	for _, invalid := range []string{
		strings.Replace(standaloneFixture, "type: perf_queries", "type: sql", 1),
		strings.Replace(standaloneFixture, "[trino_hoglake]", "[trino_hoglake, pgwire]", 1),
		strings.Replace(standaloneFixture, "    username: perf\n", "", 1),
		strings.Replace(standaloneFixture, "    trino_server_url: https://localhost:8443\n", "", 1),
		strings.Replace(standaloneFixture, "    trino_reference_catalog: ducklake\n", "", 1),
	} {
		scenario, err := core.ParseScenario([]byte(invalid))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := standaloneExecutor(scenario, t.TempDir()); err == nil {
			t.Fatalf("unsupported standalone accepted: %s", invalid)
		}
	}
}

func TestScenarioRunScriptStandaloneChecksOnlyDeclaredEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "standalone.yaml")
	raw := "required_env:\n - STANDALONE_REQUIRED\n" + standaloneFixture
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join("..", "..", "..", "scripts", "scenario_run.sh")
	for _, configured := range []bool{false, true} {
		cmd := exec.Command("bash", script, "--check-env", path)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
		if configured {
			cmd.Env = append(cmd.Env, "STANDALONE_REQUIRED=set")
		}
		out, err := cmd.CombinedOutput()
		if configured && err != nil {
			t.Fatalf("standalone still needs unrelated vars: %s", out)
		}
		if !configured && (err == nil || !strings.Contains(string(out), "STANDALONE_REQUIRED")) {
			t.Fatalf("declared required env ignored: %s", out)
		}
		if strings.Contains(string(out), "DUCKGRES_SCENARIO_PG_HOST") {
			t.Fatalf("standalone requires PG: %s", out)
		}
	}
}
