package core

import (
	"strings"
	"testing"
)

func TestParseScenarioDefaultsLinearDependencies(t *testing.T) {
	raw := []byte(`
name: provision-smoke
run_id_prefix: scenario-smoke
steps:
  - id: provision
    type: fake
  - id: query
    type: fake
  - id: deprovision
    type: fake
    always_run: true
`)

	scenario, err := ParseScenario(raw)
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}
	if scenario.Name != "provision-smoke" {
		t.Fatalf("scenario name = %q, want provision-smoke", scenario.Name)
	}
	if len(scenario.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(scenario.Steps))
	}
	if len(scenario.Steps[0].DependsOn) != 0 {
		t.Fatalf("first step should have no default dependency, got %#v", scenario.Steps[0].DependsOn)
	}
	if got := scenario.Steps[1].DependsOn; len(got) != 1 || got[0] != "provision" {
		t.Fatalf("second step dependencies = %#v, want [provision]", got)
	}
	if got := scenario.Steps[2].DependsOn; len(got) != 1 || got[0] != "query" {
		t.Fatalf("third step dependencies = %#v, want [query]", got)
	}
	if !scenario.Steps[2].AlwaysRun {
		t.Fatal("expected deprovision to be marked always_run")
	}
}

func TestParseScenarioHonorsExplicitDependencies(t *testing.T) {
	raw := []byte(`
name: fanout
steps:
  - id: provision
    type: fake
  - id: setup
    type: fake
    depends_on: [provision]
  - id: metadata
    type: fake
    depends_on: [setup]
  - id: perf
    type: fake
    depends_on: [setup]
`)

	scenario, err := ParseScenario(raw)
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}
	if got := scenario.Steps[3].DependsOn; len(got) != 1 || got[0] != "setup" {
		t.Fatalf("perf dependencies = %#v, want [setup]", got)
	}
}

func TestParseScenarioRejectsDuplicateStepIDs(t *testing.T) {
	raw := []byte(`
name: bad
steps:
  - id: provision
    type: fake
  - id: provision
    type: fake
`)

	_, err := ParseScenario(raw)
	if err == nil {
		t.Fatal("expected duplicate step id to fail")
	}
	if !strings.Contains(err.Error(), "duplicate step id") {
		t.Fatalf("expected duplicate step id error, got %v", err)
	}
}

func TestParseScenarioRejectsMissingDependency(t *testing.T) {
	raw := []byte(`
name: bad
steps:
  - id: query
    type: fake
    depends_on: [provision]
`)

	_, err := ParseScenario(raw)
	if err == nil {
		t.Fatal("expected missing dependency to fail")
	}
	if !strings.Contains(err.Error(), "unknown dependency") {
		t.Fatalf("expected unknown dependency error, got %v", err)
	}
}
