// Package shapecompare summarizes the four-shape Trino experiment without
// publishing SQL, connection details, or raw artifact error messages.
package shapecompare

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type shape struct {
	name          string
	replicas, cpu int
}

var shapes = []shape{{"baseline", 3, 1}, {"large", 1, 3}, {"scaleout", 6, 1}, {"large-scaleout", 2, 3}}
var safeQueryID = regexp.MustCompile(`^q_[a-z0-9_]{1,160}__ducklake_table$`)

type result struct {
	status  string
	medians map[string]float64
	dataset string
	image   string
}

// Generate always returns a Markdown report, including when artifacts are
// missing or malformed. A non-nil error means the comparison is incomplete.
func Generate(dir string) (string, error) {
	results := make(map[string]result)
	candidates := make(map[string][]string)
	entries, readErr := os.ReadDir(dir)
	unknown := false
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		root := filepath.Join(dir, entry.Name())
		var metadata struct {
			Shape string `json:"shape"`
		}
		_ = readJSON(filepath.Join(root, "trino-perf-shape.json"), &metadata)
		name := metadata.Shape
		if !knownShape(name) {
			// Artifact names still identify a shape if deployment failed before
			// provenance was written. Check the longest suffix first.
			for _, suffix := range []string{"large-scaleout", "baseline", "scaleout", "large"} {
				if strings.HasSuffix(entry.Name(), "-"+suffix) {
					name = suffix
					break
				}
			}
		}
		if !knownShape(name) {
			unknown = true
			continue
		}
		candidates[name] = append(candidates[name], root)
	}
	complete := readErr == nil && !unknown
	for _, s := range shapes {
		paths := candidates[s.name]
		var r result
		switch len(paths) {
		case 0:
			r.status = "missing artifact"
		case 1:
			r = load(paths[0], s)
		default:
			r.status = "duplicate artifacts"
		}
		results[s.name] = r
	}
	base := results["baseline"]
	if base.status == "complete" {
		for _, s := range shapes[1:] {
			r := results[s.name]
			if r.status == "complete" && (r.dataset != base.dataset || !sameQueries(r.medians, base.medians)) {
				r.status = "dataset or query set differs from baseline"
				results[s.name] = r
			}
			if r.status == "complete" && r.image != base.image {
				r.status = "image differs from baseline"
				results[s.name] = r
			}
		}
	}
	var out strings.Builder
	out.WriteString("## Trino worker-shape comparison\n\n")
	out.WriteString("Only Trino measurements contribute to latency and speedup comparisons; baseline artifacts may also include other engines.\n\n")
	out.WriteString("| Shape | Execution workers | Status |\n|---|---|---|\n")
	for _, s := range shapes {
		r := results[s.name]
		fmt.Fprintf(&out, "| %s | %d × %d CPU / %d GiB | %s |\n", s.name, s.replicas, s.cpu, s.cpu*4, r.status)
		if r.status != "complete" {
			complete = false
		}
	}
	if unknown {
		out.WriteString("\nUnrecognized artifact directories were found.\n")
	}
	if complete {
		out.WriteString("\nComparison complete.\n")
	} else {
		out.WriteString("\nComparison incomplete. Failed or partial shapes are excluded from latency comparisons.\n")
	}
	querySet := make(map[string]bool)
	for _, r := range results {
		if r.status == "complete" {
			for q := range r.medians {
				querySet[q] = true
			}
		}
	}
	queries := make([]string, 0, len(querySet))
	for q := range querySet {
		queries = append(queries, q)
	}
	slices.Sort(queries)
	if len(queries) > 0 {
		out.WriteString("\nTrino measured-query median latency in seconds (four measured iterations per query):\n\n| Query | baseline | large | scaleout | large-scaleout |\n|---|---:|---:|---:|---:|\n")
		for _, q := range queries {
			fmt.Fprintf(&out, "| %s |", q)
			for _, s := range shapes {
				r := results[s.name]
				value, ok := r.medians[q]
				if r.status == "complete" && ok {
					fmt.Fprintf(&out, " %.3f |", value)
				} else {
					out.WriteString(" — |")
				}
			}
			out.WriteByte('\n')
		}
	}
	if base.status == "complete" {
		out.WriteString("\nWorkload speedup uses the sum of query medians, relative to baseline. Allocated CPU-budget efficiency is speedup divided by the execution CPU increase (3 CPU baseline; 6 CPU for scaleout shapes). This does not measure CPU utilization. Coordinator resources are excluded.\n\n| Shape | Workload speedup | Allocated CPU-budget efficiency |\n|---|---:|---:|\n")
		for _, s := range shapes {
			r := results[s.name]
			if r.status == "complete" {
				speedup := sum(base.medians) / sum(r.medians)
				fmt.Fprintf(&out, "| %s | %.2f× | %.1f%% |\n", s.name, speedup, speedup*3/float64(s.replicas*s.cpu)*100)
			} else {
				fmt.Fprintf(&out, "| %s | — | — |\n", s.name)
			}
		}
	}
	if !complete {
		return out.String(), errors.New("trino shape comparison incomplete; see the Markdown status table")
	}
	return out.String(), nil
}

func knownShape(name string) bool {
	for _, s := range shapes {
		if s.name == name {
			return true
		}
	}
	return false
}

func load(root string, s shape) result {
	fail := func(status string) result { return result{status: status} }
	var job struct {
		Shape    string `json:"shape"`
		Deploy   string `json:"deploy"`
		Scenario string `json:"scenario"`
		Teardown string `json:"teardown"`
	}
	if readJSON(filepath.Join(root, "shape-result.json"), &job) != nil || job.Shape != s.name {
		return fail("missing or malformed job result")
	}
	if job.Deploy != "success" || job.Scenario != "success" || job.Teardown != "success" {
		return fail("job did not complete successfully")
	}
	var provenance struct {
		Shape       string `json:"shape"`
		Replicas    int    `json:"worker_replicas"`
		CPU         string `json:"worker_cpu"`
		TotalCPU    int    `json:"total_worker_cpu"`
		TotalMemory int    `json:"total_worker_memory_gib"`
		Image       string `json:"trino_image"`
		PerfMode    string `json:"perf_mode"`
	}
	if readJSON(filepath.Join(root, "trino-perf-shape.json"), &provenance) != nil || provenance.Shape != s.name || provenance.Replicas != s.replicas || provenance.CPU != strconv.Itoa(s.cpu) || provenance.TotalCPU != s.replicas*s.cpu || provenance.TotalMemory != s.replicas*s.cpu*4 || provenance.Image == "" {
		return fail("missing or inconsistent resource provenance")
	}
	// Older experiment artifacts predate perf_mode and remain comparable.
	if provenance.PerfMode != "" && provenance.PerfMode != "full" && provenance.PerfMode != "trino-only" {
		return fail("missing or inconsistent resource provenance")
	}
	var scenarioFiles []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() && d.Name() == "scenario_summary.json" {
			scenarioFiles = append(scenarioFiles, path)
		}
		return nil
	})
	if err != nil || len(scenarioFiles) != 1 {
		return fail("missing or duplicate scenario summary")
	}
	var scenario struct {
		Status      string `json:"status"`
		FailedSteps int    `json:"failed_steps"`
	}
	if readJSON(scenarioFiles[0], &scenario) != nil || (scenario.Status != "success" && scenario.Status != "success_with_retries") || scenario.FailedSteps != 0 {
		return fail("scenario failed or incomplete")
	}
	perfDir := filepath.Join(filepath.Dir(scenarioFiles[0]), "perf")
	var summary struct {
		TotalQueries int    `json:"total_queries"`
		TotalErrors  int    `json:"total_errors"`
		Dataset      string `json:"dataset_version"`
	}
	if readJSON(filepath.Join(perfDir, "summary.json"), &summary) != nil || summary.TotalQueries <= 0 || summary.TotalErrors != 0 || summary.Dataset == "" {
		return fail("perf summary failed or incomplete")
	}
	rows, e := readCSV(filepath.Join(perfDir, "query_results.csv"))
	if e != nil || len(rows) < 2 {
		return fail("missing or malformed query results")
	}
	header := []string{"query_id", "intent_id", "measure_iteration", "protocol", "status", "error", "error_class", "rows", "duration_ms", "started_at"}
	if !slices.Equal(rows[0], header) {
		return fail("missing or malformed query results")
	}
	if len(rows)-1 != summary.TotalQueries {
		return fail("query count does not match summary")
	}
	seen := make(map[string]bool)
	samples := make(map[string][]float64)
	for _, row := range rows[1:] {
		if len(row) != len(header) {
			return fail("failed or invalid query result")
		}
		if provenance.PerfMode == "trino-only" && row[3] != "trino" {
			return fail("query protocol does not match declared perf mode")
		}
		ms, msErr := strconv.ParseFloat(row[8], 64)
		iteration, iterationErr := strconv.Atoi(row[2])
		count, countErr := strconv.ParseInt(row[7], 10, 64)
		if row[4] != "ok" || row[5] != "" || row[6] != "" || msErr != nil || math.IsNaN(ms) || math.IsInf(ms, 0) || ms <= 0 || iterationErr != nil || iteration < 1 || iteration > 4 || countErr != nil || count < 0 {
			return fail("failed or invalid query result")
		}
		key := row[0] + "\x00" + row[3] + "\x00" + row[2]
		if seen[key] {
			return fail("duplicate query iteration")
		}
		seen[key] = true
		if row[3] == "trino" {
			if !safeQueryID.MatchString(row[0]) {
				return fail("failed or invalid query result")
			}
			samples[row[0]] = append(samples[row[0]], ms/1000)
		}
	}
	if len(samples) == 0 {
		return fail("no Trino measurements")
	}
	medians := make(map[string]float64)
	for q, values := range samples {
		if len(values) != 4 {
			return fail("incomplete measured iterations")
		}
		slices.Sort(values)
		medians[q] = (values[1] + values[2]) / 2
	}
	return result{status: "complete", medians: medians, dataset: summary.Dataset, image: provenance.Image}
}

func readJSON(path string, dst any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, dst)
}
func readCSV(path string) ([][]string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	return csv.NewReader(strings.NewReader(string(b))).ReadAll()
}
func sameQueries(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for q := range a {
		if _, ok := b[q]; !ok {
			return false
		}
	}
	return true
}
func sum(values map[string]float64) float64 {
	var total float64
	for _, v := range values {
		total += v
	}
	return total
}
