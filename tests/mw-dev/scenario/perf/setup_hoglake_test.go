package perf

import (
	"os/exec"
	"strings"
	"testing"
)

// The frozen importer registers footers only, so its files start with
// stats_state=pending. wait_for_stats must not return until every file is
// provided, and must fail — never return — on a failed file or a timeout,
// because a benchmark over stat-less files measures no pruning at all.
//
// The harness stubs pyarrow (the importer's only non-stdlib import) so the
// test runs wherever python3 does, and drives a fake API and clock.
const waitForStatsHarness = `
import importlib.util, sys, types
sys.dont_write_bytecode = True  # leave no __pycache__ in the source tree
pa = types.ModuleType("pyarrow"); pq = types.ModuleType("pyarrow.parquet")
pa.parquet = pq
sys.modules["pyarrow"] = pa; sys.modules["pyarrow.parquet"] = pq
spec = importlib.util.spec_from_file_location("setup_hoglake", "setup_hoglake.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

class API:
    """One poll round reads every table once; round N serves rounds[N], the last repeats."""
    def __init__(self, rounds):
        self.rounds, self.calls = rounds, []
    def get(self, path):
        self.calls.append(path)
        round_ = self.rounds[min((len(self.calls) - 1) // len(tables), len(self.rounds) - 1)]
        table = path.split("/tables/")[1].split("/")[0]
        return [{"path": f"s3://b/{table}/{i}.parquet", "stats_state": s} for i, s in enumerate(round_[table])]

class Clock:
    def __init__(self): self.t = 0.0
    def now(self): return self.t
    def sleep(self, d): self.t += d

tables = [("posthog", "events"), ("posthog", "persons")]

def run(rounds, timeout=60):
    clock = Clock(); api = API(rounds)
    try:
        return "ok", m.wait_for_stats(api, "org-frozen", tables, timeout, 5, clock.now, clock.sleep), api
    except Exception as e:
        return type(e).__name__ + ": " + str(e), None, api

case = sys.argv[1]
if case == "hydrates":
    out, n, api = run([
        {"events": ["pending", "provided"], "persons": ["pending"]},
        {"events": ["provided", "provided"], "persons": ["pending"]},
        {"events": ["provided", "provided"], "persons": ["provided"]},
    ])
    assert out == "ok" and n == 3, out
    assert api.calls[0] == "/v1/catalogs/org-frozen/namespaces/posthog/tables/events/files", api.calls[0]
elif case == "failed":
    out, _, _ = run([{"events": ["provided", "failed"], "persons": ["provided"]}])
    assert out.startswith("ValueError") and "posthog.events:s3://b/events/1.parquet" in out, out
elif case == "timeout":
    out, _, _ = run([{"events": ["pending"], "persons": ["provided"]}], timeout=30)
    assert out.startswith("TimeoutError") and "1 of 2 fixture files still pending" in out, out
elif case == "empty":
    out, _, _ = run([{"events": [], "persons": ["provided"]}])
    assert out.startswith("ValueError") and "no registered files" in out, out
elif case == "unknown":
    out, _, _ = run([{"events": ["hydrating"], "persons": ["provided"]}])
    assert out.startswith("ValueError") and "unexpected stats_state" in out, out
elif case == "registered":
    # main() waits on exactly the tables the commit registered files into.
    got = m.registered_tables({"snapshot_id": 7}, [
        {"namespace": "posthog", "table": "events", "files": []},
        {"namespace": "posthog", "table": "persons", "files": []},
    ])
    assert got == {"snapshot_id": 7, "tables": [
        {"namespace": "posthog", "table": "events"}, {"namespace": "posthog", "table": "persons"}]}, got
else:
    raise SystemExit("unknown case " + case)
print("PASS")
`

func TestSetupHoglakeWaitsForStatsHydration(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	for _, tc := range []string{"hydrates", "failed", "timeout", "empty", "unknown", "registered"} {
		t.Run(tc, func(t *testing.T) {
			cmd := exec.Command(python, "-c", waitForStatsHarness, tc)
			out, err := cmd.CombinedOutput()
			if err != nil || !strings.Contains(string(out), "PASS") {
				t.Fatalf("wait_for_stats %s: %v\n%s", tc, err, out)
			}
		})
	}
}
