package properties

import (
	"encoding/json"
	"strings"
	"testing"
)

func validManifest() map[string]any {
	return map[string]any{
		"format_version": 3, "status": "complete", "completed_at": "2026-04-02T00:00:00Z",
		"config":     map[string]any{"destination_prefix": "s3://example-fixture/derived/day/", "start": "2026-04-01T00:00:00Z", "end": "2026-04-02T00:00:00Z"},
		"coverage":   map[string]any{"rows": 3, "browser_types": map[string]int64{"VARCHAR": 2, "MISSING": 1}, "browser_null_rows": 1, "chrome_rows": 1},
		"schema":     [][]string{{"event", "VARCHAR"}, {"timestamp", "TIMESTAMP WITH TIME ZONE"}, {"properties", "VARCHAR"}},
		"checks":     requiredChecks,
		"validation": map[string]any{"mode": "sampled_values", "sampled_files": 1, "sampled_rows": 3},
		"writer":     map[string]any{"shredding": "STRUCT(\"$browser\" VARCHAR)", "duckdb": "1.5.5", "core_commit": strings.Repeat("a", 40), "core_patch_sha256": strings.Repeat("b", 64)},
		"outputs":    []File{{Key: "derived/day/data/part-000000.parquet", Size: 123, ETag: `"0123456789abcdef0123456789abcdef"`}},
	}
}

func TestManifestContract(t *testing.T) {
	cases := map[string]func(map[string]any){
		"incomplete": func(m map[string]any) { m["status"] = "running" },
		"checks":     func(m map[string]any) { m["checks"] = []string{"whole_day_row_count"} },
		"types": func(m map[string]any) {
			m["coverage"].(map[string]any)["browser_types"] = map[string]int64{"BIGINT": 3}
		},
		"count":     func(m map[string]any) { m["coverage"].(map[string]any)["rows"] = 4 },
		"date":      func(m map[string]any) { m["config"].(map[string]any)["end"] = "2026-04-03T00:00:00Z" },
		"schema":    func(m map[string]any) { m["schema"] = [][]string{{"properties", "JSON"}} },
		"shredding": func(m map[string]any) { m["writer"].(map[string]any)["shredding"] = "" },
		"outside": func(m map[string]any) {
			m["outputs"] = []File{{Key: "elsewhere/file.parquet", Size: 12, ETag: `"0123456789abcdef0123456789abcdef"`}}
		},
		"etag":       func(m map[string]any) { m["outputs"].([]File)[0].ETag = "" },
		"wildcard":   func(m map[string]any) { m["outputs"].([]File)[0].Key = "derived/day/data/part-*.parquet" },
		"provenance": func(m map[string]any) { m["writer"].(map[string]any)["core_patch_sha256"] = strings.Repeat("z", 64) },
		"duplicate":  func(m map[string]any) { f := m["outputs"].([]File)[0]; m["outputs"] = []File{f, f} },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			m := validManifest()
			change(m)
			data, _ := json.Marshal(m)
			if _, err := Parse(data); err == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
	data, _ := json.Marshal(validManifest())
	m, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	sql := m.SetupSQL()
	for _, want := range []string{"properties_perf.events_supported", "properties_perf.events_variant", "ignore_extra_columns => true", "part-000000.parquet", "error(", "Chrome"} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(sql, "*.parquet") {
		t.Fatal("registration must use exact inventory")
	}
}
