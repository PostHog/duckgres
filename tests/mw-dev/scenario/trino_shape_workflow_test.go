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
			Env   map[string]string `yaml:"env"`
			Steps []struct {
				Name string `yaml:"name"`
				If   string `yaml:"if"`
				Run  string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	input, ok := workflow.On.Dispatch.Inputs["trino_perf_shape"]
	if !ok || input.Type != "choice" || input.Default != "baseline" || !reflect.DeepEqual(input.Options, []string{"baseline", "large", "scaleout", "large-scaleout"}) {
		t.Fatalf("manual dispatch must offer exactly the baseline and three experiments; got %+v", input)
	}
	job := workflow.Jobs["scenario"]
	if got := job.Env["TRINO_PERF_SHAPE"]; got != "${{ inputs.trino_perf_shape || 'baseline' }}" {
		t.Fatalf("scheduled runs must default to baseline and manual runs must forward the selected shape: %q", got)
	}
	var foundPublish, foundSummary bool
	for _, step := range job.Steps {
		if step.Name == "Publish scenario perf results" {
			foundPublish = true
			if step.If != "${{ always() && github.ref == 'refs/heads/main' && env.TRINO_PERF_SHAPE == 'baseline' }}" {
				t.Fatalf("experiment measurements must not enter baseline historical results: %s", step.If)
			}
		}
		if step.Name == "Publish scenario summary" {
			foundSummary = true
			if !strings.Contains(step.Run, "trino-perf-shape.json") {
				t.Fatal("workflow summary must include the shape artifact so results are attributable")
			}
		}
	}
	if !foundPublish || !foundSummary {
		t.Fatal("missing summary or performance publication step")
	}
}
