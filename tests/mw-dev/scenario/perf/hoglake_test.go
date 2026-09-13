package perf

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

func TestSetupHoglakeSelectsExactlyOneInput(t *testing.T) {
	script := filepath.Join(t.TempDir(), "check.py")
	if err := os.WriteFile(script, []byte("import sys\nassert '--properties-plan' in sys.argv and '--source' not in sys.argv\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		with      map[string]any
		wantError bool
	}{
		{"plan", map[string]any{"properties_plan": "/tmp/example-plan.json"}, false},
		{"neither", map[string]any{}, true},
		{"both", map[string]any{"source": "s3://example/", "properties_plan": "/tmp/example-plan.json"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			with := map[string]any{"org_id": "example-org", "uri": "http://example.invalid", "file": script}
			for key, value := range tc.with {
				with[key] = value
			}
			err := (&Executor{}).setupHoglake(context.Background(), core.Step{ID: "setup", With: with})
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestPropertiesHoglakeFooterValidation(t *testing.T) {
	if out, err := exec.Command("python3", "-c", "import pyarrow").CombinedOutput(); err != nil {
		t.Skipf("requires scenario Python dependency pyarrow: %s", strings.TrimSpace(string(out)))
	}
	cmd := exec.Command("python3", "-B", "-m", "unittest", "test_setup_hoglake.py")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Python footer validation: %v\n%s", err, out)
	}
}
