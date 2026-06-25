package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunnerWritesArtifacts(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: artifact-smoke
steps:
  - id: provision
    type: fake
  - id: query
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	outputDir := t.TempDir()
	runner := NewRunner(RunnerConfig{
		RunID:      "artifact-run",
		Scenario:   scenario,
		OutputDir:  outputDir,
		Executor:   StepExecutorFunc(func(context.Context, Step) error { return nil }),
		Now:        fixedClock(time.Unix(1700000000, 0).UTC()),
		WriteFiles: true,
	})
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	for _, name := range []string{"scenario_summary.json", "step_results.csv", "events.jsonl"} {
		if _, err := os.Stat(filepath.Join(outputDir, name)); err != nil {
			t.Fatalf("expected artifact %s: %v", name, err)
		}
	}

	csvBody, err := os.ReadFile(filepath.Join(outputDir, "step_results.csv"))
	if err != nil {
		t.Fatalf("read step_results.csv: %v", err)
	}
	if !strings.Contains(string(csvBody), "run_id,scenario_name,step_id,step_type,status,error_class,error,started_at,finished_at,duration_ms,attempts") {
		t.Fatalf("step_results.csv missing expected header:\n%s", string(csvBody))
	}
	if !strings.Contains(string(csvBody), "artifact-run,artifact-smoke,query,fake,ok") {
		t.Fatalf("step_results.csv missing query result:\n%s", string(csvBody))
	}

	summaryBody, err := os.ReadFile(filepath.Join(outputDir, "scenario_summary.json"))
	if err != nil {
		t.Fatalf("read scenario_summary.json: %v", err)
	}
	if !strings.Contains(string(summaryBody), `"run_id": "artifact-run"`) {
		t.Fatalf("summary missing run_id:\n%s", string(summaryBody))
	}

	eventsBody, err := os.ReadFile(filepath.Join(outputDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	if lines := strings.Count(strings.TrimSpace(string(eventsBody)), "\n") + 1; lines != 2 {
		t.Fatalf("expected 2 event lines, got %d:\n%s", lines, string(eventsBody))
	}
}
