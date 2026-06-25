package scenario

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestScenarioRunScriptValidatesRequiredEnvVars(t *testing.T) {
	script := filepath.Join("..", "..", "scripts", "scenario_run.sh")
	cmd := exec.Command("bash", script, "--check-env")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected script to fail without required env vars")
	}
	text := string(out)
	for _, name := range []string{
		"DUCKGRES_SCENARIO_API_BASE",
		"DUCKGRES_SCENARIO_INTERNAL_SECRET",
		"DUCKGRES_SCENARIO_PG_HOST",
		"DUCKGRES_SCENARIO_SNI_SUFFIX",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("script output %q missing %s", text, name)
		}
	}
}
