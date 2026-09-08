package scenario

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestScenarioWorkflowTrinoShapeExperiments(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", ".github", "workflows", "scenario-dev.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		On struct {
			Dispatch struct {
				Inputs map[string]struct {
					Type    string   `yaml:"type"`
					Default string   `yaml:"default"`
					Options []string `yaml:"options"`
				} `yaml:"inputs"`
			} `yaml:"workflow_dispatch"`
		} `yaml:"on"`
		Jobs map[string]struct {
			Needs    []string `yaml:"needs"`
			If       string   `yaml:"if"`
			Strategy struct {
				MaxParallel int    `yaml:"max-parallel"`
				FailFast    *bool  `yaml:"fail-fast"`
				Matrix      string `yaml:"matrix"`
			} `yaml:"strategy"`
			Env   map[string]string `yaml:"env"`
			Steps []struct {
				Name string         `yaml:"name"`
				If   string         `yaml:"if"`
				Run  string         `yaml:"run"`
				With map[string]any `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	input, ok := workflow.On.Dispatch.Inputs["trino_perf_shape"]
	if !ok || input.Type != "choice" || input.Default != "baseline" || !reflect.DeepEqual(input.Options, []string{"baseline", "large", "scaleout", "large-scaleout", "all"}) {
		t.Fatalf("manual dispatch must offer baseline, three experiments, and all; got %+v", input)
	}
	job := workflow.Jobs["scenario"]
	if got := job.Env["TRINO_PERF_SHAPE"]; got != "${{ matrix.shape }}" {
		t.Fatalf("scenario must receive each concrete shape, never all: %q", got)
	}
	if job.Strategy.MaxParallel != 1 || job.Strategy.FailFast == nil || *job.Strategy.FailFast || job.Strategy.Matrix != "${{ fromJSON(needs.plan.outputs.matrix) }}" {
		t.Fatalf("all shapes must run sequentially and retain remaining results on failure: %+v", job.Strategy)
	}
	if job.Env["PR_NUMBER"] != "${{ github.run_id }}${{ matrix.suffix }}" || job.Env["NAMESPACE"] != "duckgres-ci-pr-${{ github.run_id }}${{ matrix.suffix }}" {
		t.Fatal("each shape must have a distinct cleanup and warehouse identity")
	}
	if !reflect.DeepEqual(job.Needs, []string{"plan", "scenario-runner-image", "duckgres-image"}) {
		t.Fatal("shapes must reuse the same pair of image builds")
	}
	plan := workflow.Jobs["plan"]
	if plan.Env["TRINO_PERF_SHAPE"] != "${{ inputs.trino_perf_shape || 'baseline' }}" {
		t.Fatal("scheduled/default runs must plan a single baseline shape")
	}
	var foundPublish, foundSummary bool
	var teardownIndex, outcomeIndex, uploadIndex = -1, -1, -1
	for i, step := range job.Steps {
		if step.Name == "Publish scenario perf results" {
			foundPublish = true
			if step.If != "${{ always() && github.ref == 'refs/heads/main' && env.TRINO_PERF_SHAPE == 'baseline' && inputs.trino_perf_shape != 'all' }}" {
				t.Fatalf("experiment measurements must not enter baseline historical results: %s", step.If)
			}
		}
		if step.Name == "Publish scenario summary" {
			foundSummary = true
			if !strings.Contains(step.Run, "trino-perf-shape.json") {
				t.Fatal("workflow summary must include the shape artifact so results are attributable")
			}
		}
		switch step.Name {
		case "Teardown":
			teardownIndex = i
			if step.If != "always()" {
				t.Fatal("each matrix member must teardown on failure")
			}
		case "Record shape outcome":
			outcomeIndex = i
			if step.If != "always()" || !strings.Contains(step.Run, "shape-result.json") {
				t.Fatal("each shape must record failure/cleanup outcomes")
			}
		case "Upload scenario artifacts":
			uploadIndex = i
			if step.If != "always()" || step.With["name"] != "scenario-dev-${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.shape }}" {
				t.Fatal("each shape must upload uniquely named artifacts even on failure")
			}
		}
	}
	if !foundPublish || !foundSummary {
		t.Fatal("missing summary or performance publication step")
	}
	if teardownIndex < 0 || outcomeIndex <= teardownIndex || uploadIndex <= outcomeIndex {
		t.Fatal("outcome and artifact publication must follow teardown")
	}
	comparison, ok := workflow.Jobs["compare-shapes"]
	if !ok || !reflect.DeepEqual(comparison.Needs, []string{"plan", "scenario"}) || !strings.Contains(comparison.If, "always()") || !strings.Contains(comparison.If, "inputs.trino_perf_shape == 'all'") {
		t.Fatal("all mode must summarize all matrix outcomes, including failed ones")
	}
	var foundCompare, foundComparisonPublish bool
	for _, step := range comparison.Steps {
		if strings.Contains(step.Run, "go run ./cmd/duckgres-perf-shape-summary") {
			foundCompare = true
		}
		if strings.Contains(step.Run, "$GITHUB_STEP_SUMMARY") && step.If == "always()" {
			foundComparisonPublish = true
		}
	}
	if !foundCompare || !foundComparisonPublish {
		t.Fatal("comparison must render results and publish incomplete summaries on failure")
	}
}
