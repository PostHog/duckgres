package core

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Scenario struct {
	Name        string `yaml:"name" json:"name"`
	RunIDPrefix string `yaml:"run_id_prefix" json:"run_id_prefix,omitempty"`
	Steps       []Step `yaml:"steps" json:"steps"`
}

type Step struct {
	ID        string         `yaml:"id" json:"id"`
	Type      string         `yaml:"type" json:"type"`
	DependsOn []string       `yaml:"depends_on" json:"depends_on,omitempty"`
	AlwaysRun bool           `yaml:"always_run" json:"always_run,omitempty"`
	With      map[string]any `yaml:"with" json:"with,omitempty"`
}

type rawScenario struct {
	Name        string    `yaml:"name"`
	RunIDPrefix string    `yaml:"run_id_prefix"`
	Steps       []rawStep `yaml:"steps"`
}

type rawStep struct {
	ID        string         `yaml:"id"`
	Type      string         `yaml:"type"`
	DependsOn *[]string      `yaml:"depends_on"`
	AlwaysRun bool           `yaml:"always_run"`
	With      map[string]any `yaml:"with"`
}

func LoadScenario(path string) (Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf("read scenario %s: %w", path, err)
	}
	return ParseScenario(b)
}

func ParseScenario(raw []byte) (Scenario, error) {
	var parsed rawScenario
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return Scenario{}, fmt.Errorf("parse scenario: %w", err)
	}
	return normalizeScenario(parsed)
}

func normalizeScenario(raw rawScenario) (Scenario, error) {
	scenario := Scenario{
		Name:        strings.TrimSpace(raw.Name),
		RunIDPrefix: strings.TrimSpace(raw.RunIDPrefix),
		Steps:       make([]Step, 0, len(raw.Steps)),
	}
	if scenario.Name == "" {
		return Scenario{}, fmt.Errorf("scenario name is required")
	}
	if len(raw.Steps) == 0 {
		return Scenario{}, fmt.Errorf("scenario steps must include at least one step")
	}

	seen := make(map[string]struct{}, len(raw.Steps))
	var previousID string
	for i, rawStep := range raw.Steps {
		step := Step{
			ID:        strings.TrimSpace(rawStep.ID),
			Type:      strings.TrimSpace(rawStep.Type),
			AlwaysRun: rawStep.AlwaysRun,
			With:      rawStep.With,
		}
		if step.ID == "" {
			return Scenario{}, fmt.Errorf("step %d id is required", i)
		}
		if step.Type == "" {
			return Scenario{}, fmt.Errorf("step %s type is required", step.ID)
		}
		if _, ok := seen[step.ID]; ok {
			return Scenario{}, fmt.Errorf("duplicate step id %q", step.ID)
		}

		switch {
		case rawStep.DependsOn != nil:
			step.DependsOn = append([]string(nil), (*rawStep.DependsOn)...)
		case previousID != "":
			step.DependsOn = []string{previousID}
		}
		for depIdx, dep := range step.DependsOn {
			dep = strings.TrimSpace(dep)
			if dep == "" {
				return Scenario{}, fmt.Errorf("step %s dependency %d is empty", step.ID, depIdx)
			}
			if _, ok := seen[dep]; !ok {
				return Scenario{}, fmt.Errorf("step %s has unknown dependency %q", step.ID, dep)
			}
			step.DependsOn[depIdx] = dep
		}

		seen[step.ID] = struct{}{}
		previousID = step.ID
		scenario.Steps = append(scenario.Steps, step)
	}
	return scenario, nil
}
