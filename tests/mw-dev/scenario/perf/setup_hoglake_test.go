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

// run_properties registers events_supported from the supported copy and
// events_variant from the canonical copy, typing the column "variant" only
// when the footer carries the native VARIANT annotation (pyarrow reports a
// plain struct). pyarrow is stubbed; footers are fakes.
const runPropertiesHarness = `
import importlib.util, json, sys, types
sys.dont_write_bytecode = True
sys.modules["pyarrow"] = types.ModuleType("pyarrow")
sys.modules["pyarrow.parquet"] = types.ModuleType("pyarrow.parquet")
spec = importlib.util.spec_from_file_location("setup_hoglake", "setup_hoglake.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
m.column_type = lambda f: {"type": f.type}
m.pa.types = types.SimpleNamespace(is_uint64=lambda t: False)

class Field:
    def __init__(self, name, t): self.name, self.type, self.metadata = name, t, None
class Schema(list):
    names = property(lambda self: [f.name for f in self])
class ParquetSchema:
    def __init__(self, fields, text): self.fields, self.text = fields, text
    def to_arrow_schema(self): return Schema(self.fields)
    def __str__(self): return self.text
class Footer:
    num_rows, serialized_size = 10, 100
    def __init__(self, fields, text): self.schema = ParquetSchema(fields, text)

base = [Field("event", "string"), Field("timestamp", "timestamptz"), Field("properties", "string")]
annotated = "  optional group field_id=-1 properties_variant (Variant(1)) {\n    required binary field_id=-1 metadata;\n  }"
nested = "  optional group field_id=-1 outer {\n    optional group field_id=-1 properties_variant (Variant(1)) {\n    }\n  }"
variant_text = {"annotated": annotated, "plain_struct": "  optional group field_id=-1 properties_variant {\n  }", "nested_only": nested}[sys.argv[1]]
variant_bucket = sys.argv[2] if len(sys.argv) > 2 else "b"
class Store:
    def objects(self, uri):
        if "/supported/" in uri: return [{"key": "fx/supported/d1/a.parquet", "size": 1, "etag": "e"}]
        return [{"key": "fx/canonical/d1/data/a.parquet", "size": 1, "etag": "e"}, {"key": "fx/canonical/d1/complete.json", "size": 1, "etag": "e"}]
    def footer(self, bucket, obj):
        if "/supported/" in obj["key"]: return Footer(base + [Field("properties_typed", "struct")], "")
        return Footer(base + [Field("properties_variant", "struct")], variant_text)
class API:
    posts = []
    def get(self, path): return {"data_path": "s3://b/"}
    def post(self, path, body):
        self.posts.append((path, body))
        if path.endswith("/tables"): return {"table_uuid": body["name"], "columns": [{"name": c["name"], "field_id": i + 1} for i, c in enumerate(body["columns"])]}
        return {"snapshot_id": 1}
try:
    api = API()
    m.run_properties(Store(), api, "s3://b/fx/supported/", "c", "variant", "s3://" + variant_bucket + "/fx/canonical/")
    commit = [b for p, b in api.posts if p.endswith("/commit")][0]
    print(json.dumps({a["table"]: [f["path"] for f in a["files"]] for a in commit["appends"]}, sort_keys=True))
    print(json.dumps([c for p, b in api.posts if p.endswith("/tables") and b["name"] == "events_variant" for c in b["columns"] if c["name"] == "properties_variant"]))
except Exception as e:
    print(type(e).__name__, e)
`

func TestSetupHoglakeRegistersShreddedVariantFromCanonicalCopy(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 is required to test setup_hoglake.py")
	}
	for name, tc := range map[string]struct {
		args []string
		want []string
	}{
		"annotated": {[]string{"annotated"}, []string{
			`{"events_supported": ["s3://b/fx/supported/d1/a.parquet"], "events_variant": ["s3://b/fx/canonical/d1/data/a.parquet"]}`,
			`[{"name": "properties_variant", "nullable": true, "type": "variant"}]`,
		}},
		// pyarrow alone reports a struct: without the footer annotation the
		// column is not VARIANT and registration fails before any write.
		"plain_struct": {[]string{"plain_struct"}, []string{"ValueError properties fixture logical column type mismatch"}},
		"nested_only":  {[]string{"nested_only"}, []string{"ValueError properties fixture logical column type mismatch"}},
		"cross_bucket": {[]string{"annotated", "other"}, []string{"ValueError properties variant source must share the fixture bucket"}},
	} {
		out, err := exec.Command(python, append([]string{"-c", runPropertiesHarness}, tc.args...)...).CombinedOutput()
		for _, want := range tc.want {
			if err != nil || !strings.Contains(string(out), want) {
				t.Errorf("%s: want %q, got err=%v\n%s", name, want, err, out)
			}
		}
	}
}
