package scenario

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestTrinoShapesScriptPlansIsolatedRuns(t *testing.T) {
	type entry struct {
		Shape  string `json:"shape"`
		Suffix string `json:"suffix"`
	}
	for _, tc := range []struct {
		name     string
		scenario string
		shape    string
		want     []entry
	}{
		{name: "default", scenario: "full-suite", want: []entry{{Shape: "baseline"}}},
		{name: "baseline", scenario: "posthog_frozen_perf", shape: "baseline", want: []entry{{Shape: "baseline"}}},
		{name: "large", scenario: "posthog_frozen_perf", shape: "large", want: []entry{{Shape: "large"}}},
		{name: "scaleout", scenario: "posthog_frozen_perf", shape: "scaleout", want: []entry{{Shape: "scaleout"}}},
		{name: "large-scaleout", scenario: "posthog_frozen_perf", shape: "large-scaleout", want: []entry{{Shape: "large-scaleout"}}},
		{name: "all", scenario: "posthog_frozen_perf", shape: "all", want: []entry{
			{Shape: "baseline", Suffix: "1"},
			{Shape: "large", Suffix: "2"},
			{Shape: "scaleout", Suffix: "3"},
			{Shape: "large-scaleout", Suffix: "4"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runTrinoShapesScript(tc.scenario, tc.shape)
			if err != nil {
				t.Fatalf("script failed: %v: %s", err, stderr)
			}
			if stderr != "" {
				t.Fatalf("unexpected stderr: %q", stderr)
			}
			var matrix struct {
				Include []entry `json:"include"`
			}
			if err := json.Unmarshal([]byte(stdout), &matrix); err != nil {
				t.Fatalf("expected JSON-only output, got %q: %v", stdout, err)
			}
			if !reflect.DeepEqual(matrix.Include, tc.want) {
				t.Fatalf("matrix=%+v, want %+v", matrix.Include, tc.want)
			}
			// The harness derives namespaces and identifiers from PR_NUMBER, which
			// the workflow constructs by appending each suffix to the run ID.
			seen := make(map[string]bool)
			for _, item := range matrix.Include {
				id := "12345678901" + item.Suffix
				if _, err := strconv.ParseUint(id, 10, 64); err != nil {
					t.Fatalf("run identifier is not numeric: %q", id)
				}
				if seen[id] {
					t.Fatalf("duplicate run identifier: %q", id)
				}
				seen[id] = true
			}
		})
	}
}

func TestTrinoShapesScriptRejectsInvalidInputBeforeOutput(t *testing.T) {
	for _, tc := range []struct {
		name     string
		scenario string
		shape    string
		want     string
	}{
		{name: "unknown shape", scenario: "posthog_frozen_perf", shape: "huge", want: "Invalid TRINO_PERF_SHAPE"},
		{name: "JSON injection", scenario: "posthog_frozen_perf", shape: "large\"}\n", want: "Invalid TRINO_PERF_SHAPE"},
		{name: "large other scenario", scenario: "full-suite", shape: "large", want: "require posthog_frozen_perf"},
		{name: "scaleout other scenario", scenario: "posthog_frozen_metadata", shape: "scaleout", want: "require posthog_frozen_perf"},
		{name: "large-scaleout other scenario", scenario: "full-suite", shape: "large-scaleout", want: "require posthog_frozen_perf"},
		{name: "all other scenario", scenario: "full-suite", shape: "all", want: "require posthog_frozen_perf"},
		{name: "all missing scenario", shape: "all", want: "require posthog_frozen_perf"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runTrinoShapesScript(tc.scenario, tc.shape)
			if err == nil || !strings.Contains(stderr, tc.want) {
				t.Fatalf("err=%v stderr=%q, want %q", err, stderr, tc.want)
			}
			if stdout != "" {
				t.Fatalf("failed validation emitted matrix output: %q", stdout)
			}
		})
	}
}

func runTrinoShapesScript(scenario, shape string) (string, string, error) {
	cmd := exec.Command("bash", filepath.Join("..", "..", "..", "scripts", "scenario_trino_shapes.sh"))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "SCENARIO_NAME=" + scenario}
	if shape != "" {
		cmd.Env = append(cmd.Env, "TRINO_PERF_SHAPE="+shape)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}
