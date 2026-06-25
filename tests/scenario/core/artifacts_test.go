package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
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
	records, err := csv.NewReader(bytes.NewReader(csvBody)).ReadAll()
	if err != nil {
		t.Fatalf("parse step_results.csv: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected header plus 2 rows, got %d records: %#v", len(records), records)
	}
	if !equalStrings(records[0], stepResultsHeader) {
		t.Fatalf("step_results.csv header = %#v, want %#v", records[0], stepResultsHeader)
	}
	if got := records[2][:5]; !equalStrings(got, []string{"artifact-run", "artifact-smoke", "query", "fake", "ok"}) {
		t.Fatalf("query result prefix = %#v", got)
	}

	summaryBody, err := os.ReadFile(filepath.Join(outputDir, "scenario_summary.json"))
	if err != nil {
		t.Fatalf("read scenario_summary.json: %v", err)
	}
	var summary RunSummary
	if err := json.Unmarshal(summaryBody, &summary); err != nil {
		t.Fatalf("decode scenario_summary.json: %v", err)
	}
	if summary.RunID != "artifact-run" || summary.TotalSteps != 2 || summary.SucceededSteps != 2 {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	eventsBody, err := os.ReadFile(filepath.Join(outputDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(eventsBody))
	var events []StepResult
	for scanner.Scan() {
		var event StepResult
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("decode event JSON: %v", err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan events.jsonl: %v", err)
	}
	if len(events) != 2 || events[1].StepID != "query" || events[1].Status != StepStatusOK {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestRunnerValidatesArtifactOutputBeforeExecutingSteps(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: artifact-preflight
steps:
  - id: provision
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	executed := false
	runner := NewRunner(RunnerConfig{
		RunID:      "artifact-preflight",
		Scenario:   scenario,
		WriteFiles: true,
		Executor: StepExecutorFunc(func(context.Context, Step) error {
			executed = true
			return nil
		}),
		Now: fixedClock(time.Unix(1700000000, 0).UTC()),
	})

	_, err = runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected missing output dir to fail")
	}
	if executed {
		t.Fatal("executor ran before artifact output validation")
	}
}

func TestRunnerRejectsRegularFileArtifactOutputBeforeExecutingSteps(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: artifact-preflight-file
steps:
  - id: provision
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	outputPath := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(outputPath, []byte("file"), 0o644); err != nil {
		t.Fatalf("write output file: %v", err)
	}

	executed := false
	runner := NewRunner(RunnerConfig{
		RunID:      "artifact-preflight-file",
		Scenario:   scenario,
		OutputDir:  outputPath,
		WriteFiles: true,
		Executor: StepExecutorFunc(func(context.Context, Step) error {
			executed = true
			return nil
		}),
		Now: fixedClock(time.Unix(1700000000, 0).UTC()),
	})

	_, err = runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected regular-file output path to fail")
	}
	if executed {
		t.Fatal("executor ran before artifact output validation")
	}
}
