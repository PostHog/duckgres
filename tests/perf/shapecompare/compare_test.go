package shapecompare

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestGenerateComparison(t *testing.T) {
	dir := completeFixture(t)
	got, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Comparison complete", "baseline | 3 × 1 CPU / 4 GiB | complete", "large-scaleout | 2 × 3 CPU / 12 GiB | complete", "| q_events_total_balanced_v4__ducklake_table | 10.000 | 5.000 | 5.000 | 2.500 |", "| large | 2.00× | 200.0% |", "| scaleout | 2.00× | 100.0% |", "| large-scaleout | 4.00× | 200.0% |"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestGenerateUsesMedianAndIgnoresOtherProtocolLatency(t *testing.T) {
	dir := completeFixture(t)
	mutateCSV(t, dir, func(rows [][]string) [][]string {
		for i, value := range []string{"1000", "3000", "7000", "99000"} {
			rows[i+1][8] = value
		}
		for i := 1; i <= 4; i++ {
			row := slices.Clone(rows[i])
			row[3] = "pgwire_cached"
			row[8] = "1"
			rows = append(rows, row)
		}
		return rows
	})
	writeJSON(t, filepath.Join(dir, "artifact-large", "scenario", "perf", "summary.json"), map[string]any{"total_queries": 8, "total_errors": 0, "dataset_version": "fixture"})
	// The nested provenance copy belongs to this artifact, not another run.
	writeJSON(t, filepath.Join(dir, "artifact-large", "scenario", "trino-perf-shape.json"), map[string]string{"shape": "large"})
	got, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "| q_events_total_balanced_v4__ducklake_table | 10.000 | 5.000 | 5.000 | 2.500 |") {
		t.Fatal(got)
	}
}

func TestGenerateFullBaselineAndTrinoOnlyExperiments(t *testing.T) {
	dir := mixedProtocolFixture(t)
	got, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Comparison complete",
		"Only Trino measurements contribute to latency and speedup comparisons; baseline artifacts may also include other engines.",
		"| large | 2.00× | 200.0% |",
		"| large-scaleout | 4.00× | 200.0% |",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Count(got, "__ducklake_table |") != 7 || strings.Contains(got, "__raw_view") {
		t.Fatalf("comparison must contain exactly the seven Trino table queries:\n%s", got)
	}
}

func TestGenerateMixedProtocolsStillRejectsIncompleteMeasurements(t *testing.T) {
	dir := mixedProtocolFixture(t)
	mutateCSV(t, dir, func(rows [][]string) [][]string { return rows[:len(rows)-1] })
	writeJSON(t, filepath.Join(dir, "artifact-large", "scenario", "perf", "summary.json"), map[string]any{
		"total_queries": 27, "warmup_queries": 7, "total_errors": 0, "dataset_version": "fixture",
	})
	got, err := Generate(dir)
	if err == nil || !strings.Contains(got, "incomplete measured iterations") {
		t.Fatalf("missing fourth Trino iteration must fail comparison: err=%v\n%s", err, got)
	}
}

func TestGenerateValidatesDeclaredPerfMode(t *testing.T) {
	for _, tc := range []struct {
		name, mode, want string
	}{
		{"unknown mode", "unknown", "missing or inconsistent resource provenance"},
		{"Trino-only with other protocols", "trino-only", "query protocol does not match declared perf mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := mixedProtocolFixture(t)
			setPerfMode(t, dir, "baseline", tc.mode)
			got, err := Generate(dir)
			if err == nil || !strings.Contains(got, tc.want) {
				t.Fatalf("invalid declared mode must fail: err=%v\n%s", err, got)
			}
		})
	}
}

func setPerfMode(t *testing.T, dir, shape, mode string) {
	t.Helper()
	path := filepath.Join(dir, "artifact-"+shape, "trino-perf-shape.json")
	var metadata map[string]any
	if err := readJSON(path, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata["perf_mode"] = mode
	writeJSON(t, path, metadata)
}

func mixedProtocolFixture(t *testing.T) string {
	t.Helper()
	dir := completeFixture(t)
	queries := []string{
		"events_total", "events_count_one_day", "events_by_name_march_2026",
		"events_distinct_persons", "persons_total", "persons_daily_april_2026", "events_daily_march_2026",
	}
	type protocolVariant struct{ protocol, representation string }
	for _, s := range shapes {
		mode := "trino-only"
		variants := []protocolVariant{{"trino", "ducklake_table"}}
		if s.name == "baseline" {
			mode = "full"
			variants = append(variants,
				protocolVariant{"pgwire_uncached", "raw_view"},
				protocolVariant{"pgwire_uncached", "ducklake_table"},
				protocolVariant{"pgwire_cached", "raw_view"},
				protocolVariant{"pgwire_cached", "ducklake_table"},
				protocolVariant{"athena", "athena_external"},
			)
		}
		setPerfMode(t, dir, s.name, mode)
		rows := [][]string{{"query_id", "intent_id", "measure_iteration", "protocol", "status", "error", "error_class", "rows", "duration_ms", "started_at"}}
		for _, variant := range variants {
			ms := "1" // Non-Trino timings must not affect shape speedups.
			if variant.protocol == "trino" {
				ms = map[string]string{"baseline": "10000", "large": "5000", "scaleout": "5000", "large-scaleout": "2500"}[s.name]
			}
			for _, query := range queries {
				for iteration := 1; iteration <= 4; iteration++ {
					rows = append(rows, []string{"q_" + query + "_balanced_v4__" + variant.representation, "intent_" + query, strconv.Itoa(iteration), variant.protocol, "ok", "", "", "1", ms, "2026-01-01T00:00:00Z"})
				}
			}
		}
		perf := filepath.Join(dir, "artifact-"+s.name, "scenario", "perf")
		// The runner counts warmups in summary.json but emits only measured
		// executions to query_results.csv: baseline 42/168; experiments 7/28.
		writeJSON(t, filepath.Join(perf, "summary.json"), map[string]any{
			"total_queries": len(rows) - 1, "warmup_queries": len(variants) * len(queries), "total_errors": 0, "dataset_version": "fixture",
		})
		writeCSV(t, filepath.Join(perf, "query_results.csv"), rows)
	}
	return dir
}

func TestGenerateIncompleteAndUnsafeArtifacts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, string)
		want   string
	}{
		{"missing directory", func(t *testing.T, d string) {
			if err := os.RemoveAll(d); err != nil {
				t.Fatal(err)
			}
		}, "missing artifact"},
		{"missing result", func(t *testing.T, d string) { remove(t, filepath.Join(d, "artifact-large", "shape-result.json")) }, "missing or malformed job result"},
		{"missing provenance", func(t *testing.T, d string) { remove(t, filepath.Join(d, "artifact-large", "trino-perf-shape.json")) }, "missing or inconsistent resource provenance"},
		{"different image", func(t *testing.T, d string) {
			p := filepath.Join(d, "artifact-large", "trino-perf-shape.json")
			var metadata map[string]any
			if err := readJSON(p, &metadata); err != nil {
				t.Fatal(err)
			}
			metadata["trino_image"] = "other-image"
			writeJSON(t, p, metadata)
		}, "image differs from baseline"},
		{"different dataset", func(t *testing.T, d string) {
			writeJSON(t, filepath.Join(d, "artifact-large", "scenario", "perf", "summary.json"), map[string]any{"total_queries": 4, "total_errors": 0, "dataset_version": "different"})
		}, "dataset or query set differs from baseline"},
		{"different queries", func(t *testing.T, d string) {
			mutateCSV(t, d, func(rows [][]string) [][]string {
				for _, row := range rows[1:] {
					row[0] = "q_persons_total_balanced_v4__ducklake_table"
				}
				return rows
			})
		}, "dataset or query set differs from baseline"},
		{"teardown failed", func(t *testing.T, d string) {
			writeJSON(t, filepath.Join(d, "artifact-large", "shape-result.json"), map[string]string{"shape": "large", "deploy": "success", "scenario": "success", "teardown": "failure"})
		}, "job did not complete successfully"},
		{"scenario failed", func(t *testing.T, d string) {
			writeJSON(t, filepath.Join(d, "artifact-large", "scenario", "scenario_summary.json"), map[string]any{"status": "failed", "error": "SECRET"})
		}, "scenario failed or incomplete"},
		{"missing csv", func(t *testing.T, d string) {
			remove(t, filepath.Join(d, "artifact-large", "scenario", "perf", "query_results.csv"))
		}, "missing or malformed query results"},
		{"failed row", func(t *testing.T, d string) {
			mutateCSV(t, d, func(rows [][]string) [][]string {
				rows[1][4] = "error"
				rows[1][5] = "SECRET https://private.example"
				rows[1][8] = "0"
				return rows
			})
		}, "failed or invalid query result"},
		{"NaN duration", func(t *testing.T, d string) {
			mutateCSV(t, d, func(rows [][]string) [][]string { rows[1][8] = "NaN"; return rows })
		}, "failed or invalid query result"},
		{"partial rows", func(t *testing.T, d string) {
			mutateCSV(t, d, func(rows [][]string) [][]string { return rows[:len(rows)-1] })
		}, "query count does not match summary"},
		{"duplicate iteration", func(t *testing.T, d string) {
			mutateCSV(t, d, func(rows [][]string) [][]string { rows[2] = rows[1]; return rows })
		}, "duplicate query iteration"},
		{"unsafe query ID", func(t *testing.T, d string) {
			mutateCSV(t, d, func(rows [][]string) [][]string { rows[1][0] = "SECRET | <script>"; return rows })
		}, "failed or invalid query result"},
		{"duplicate shape", func(t *testing.T, d string) { fixture(t, d, "large", "other-large", 5000) }, "duplicate artifacts"},
		{"missing measurements", func(t *testing.T, d string) {
			mutateCSV(t, d, func(rows [][]string) [][]string { return rows[:4] })
			writeJSON(t, filepath.Join(d, "artifact-large", "scenario", "perf", "summary.json"), map[string]any{"total_queries": 3, "total_errors": 0, "dataset_version": "fixture"})
		}, "incomplete measured iterations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := completeFixture(t)
			tc.mutate(t, d)
			got, err := Generate(d)
			if err == nil {
				t.Fatal("expected incomplete error")
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("missing %q in %s", tc.want, got)
			}
			if strings.Contains(got, "SECRET") {
				t.Fatal("untrusted data leaked")
			}
			if !strings.Contains(got, "Comparison incomplete") {
				t.Fatal("missing incomplete notice")
			}
		})
	}
}

func completeFixture(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	for _, s := range []struct {
		name string
		ms   float64
	}{{"baseline", 10000}, {"large", 5000}, {"scaleout", 5000}, {"large-scaleout", 2500}} {
		fixture(t, d, s.name, "artifact-"+s.name, s.ms)
	}
	return d
}

func fixture(t *testing.T, dir, shape, artifact string, ms float64) {
	t.Helper()
	root := filepath.Join(dir, artifact)
	perf := filepath.Join(root, "scenario", "perf")
	if err := os.MkdirAll(perf, 0o755); err != nil {
		t.Fatal(err)
	}
	replicas, cpu := 3, "1"
	if shape == "large" {
		replicas, cpu = 1, "3"
	}
	if shape == "scaleout" {
		replicas = 6
	}
	if shape == "large-scaleout" {
		replicas, cpu = 2, "3"
	}
	total := 3
	if strings.Contains(shape, "scaleout") {
		total = 6
	}
	writeJSON(t, filepath.Join(root, "trino-perf-shape.json"), map[string]any{"shape": shape, "worker_replicas": replicas, "worker_cpu": cpu, "total_worker_cpu": total, "total_worker_memory_gib": total * 4, "trino_image": "trino-fixture"})
	writeJSON(t, filepath.Join(root, "shape-result.json"), map[string]string{"shape": shape, "deploy": "success", "scenario": "success", "teardown": "success"})
	writeJSON(t, filepath.Join(root, "scenario", "scenario_summary.json"), map[string]any{"status": "success", "failed_steps": 0})
	writeJSON(t, filepath.Join(perf, "summary.json"), map[string]any{"total_queries": 4, "total_errors": 0, "dataset_version": "fixture"})
	rows := [][]string{{"query_id", "intent_id", "measure_iteration", "protocol", "status", "error", "error_class", "rows", "duration_ms", "started_at"}}
	for i := 1; i <= 4; i++ {
		rows = append(rows, []string{"q_events_total_balanced_v4__ducklake_table", "intent", string(rune('0' + i)), "trino", "ok", "", "", "1", formatMS(ms), "2026-01-01T00:00:00Z"})
	}
	writeCSV(t, filepath.Join(perf, "query_results.csv"), rows)
}

func formatMS(ms float64) string { return strconv.FormatFloat(ms, 'f', 3, 64) }
func writeJSON(t *testing.T, p string, v any) {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(p, b, 0o600); e != nil {
		t.Fatal(e)
	}
}
func remove(t *testing.T, p string) {
	t.Helper()
	if e := os.Remove(p); e != nil {
		t.Fatal(e)
	}
}
func writeCSV(t *testing.T, p string, rows [][]string) {
	t.Helper()
	f, e := os.Create(p)
	if e != nil {
		t.Fatal(e)
	}
	w := csv.NewWriter(f)
	e = w.WriteAll(rows)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
}
func mutateCSV(t *testing.T, d string, fn func([][]string) [][]string) {
	t.Helper()
	p := filepath.Join(d, "artifact-large", "scenario", "perf", "query_results.csv")
	f, e := os.Open(p)
	if e != nil {
		t.Fatal(e)
	}
	r, e := csv.NewReader(f).ReadAll()
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	writeCSV(t, p, fn(r))
}
