package perf

import (
	"os/exec"
	"strings"
	"testing"
)

// wait_for_stats must fail, never return, when a fixture file's stats
// hydration failed or never finishes: a benchmark over stat-less files
// measures no pruning. pyarrow (the importer's only non-stdlib import) is
// stubbed so this needs only python3.
const waitForStatsHarness = `
import importlib.util, sys, types
sys.dont_write_bytecode = True
sys.modules["pyarrow"] = types.ModuleType("pyarrow")
sys.modules["pyarrow.parquet"] = types.ModuleType("pyarrow.parquet")
spec = importlib.util.spec_from_file_location("setup_hoglake", "setup_hoglake.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

states = {"failed": ["provided", "failed"], "timeout": ["provided", "pending"]}[sys.argv[1]]
class API:
    def get(self, path, timeout):
        return [{"path": f"s3://b/{i}.parquet", "stats_state": s} for i, s in enumerate(states)]
now = [0.0]
try:
    m.wait_for_stats(API(), "c", [("posthog", "events")], 30, 5, lambda: now[0], lambda d: now.__setitem__(0, now[0] + d))
except Exception as e:
    print(type(e).__name__, e)
`

func TestSetupHoglakeWaitFailsWithoutHydratedStats(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required to test setup_hoglake.py")
	}
	for tc, want := range map[string]string{
		"failed":  "ValueError Hoglake stats hydration failed for 1 fixture file(s) (e.g. posthog.events:s3://b/1.parquet)",
		"timeout": "TimeoutError Hoglake stats hydration incomplete after 30s: 1 of 2 fixture files still pending",
	} {
		out, err := exec.Command(python, "-c", waitForStatsHarness, tc).CombinedOutput()
		if err != nil || !strings.Contains(string(out), want) {
			t.Errorf("%s: want %q, got err=%v\n%s", tc, want, err, out)
		}
	}
}
