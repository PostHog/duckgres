package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunnerRunsAlwaysRunStepAfterFailureAndSkipsDependents(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: failure-cleanup
steps:
  - id: provision
    type: fake
  - id: query
    type: fake
  - id: downstream
    type: fake
  - id: deprovision
    type: fake
    depends_on: [query]
    always_run: true
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	var executed []string
	runner := NewRunner(RunnerConfig{
		RunID:    "run-1",
		Scenario: scenario,
		Executor: StepExecutorFunc(func(_ context.Context, step Step) error {
			executed = append(executed, step.ID)
			if step.ID == "query" {
				return errors.New("query failed")
			}
			return nil
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	summary, err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected runner to return failure")
	}
	wantExecuted := []string{"provision", "query", "deprovision"}
	if !equalStrings(executed, wantExecuted) {
		t.Fatalf("executed steps = %#v, want %#v", executed, wantExecuted)
	}
	if summary.TotalSteps != 4 || summary.SucceededSteps != 2 || summary.FailedSteps != 1 || summary.SkippedSteps != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	results := runner.Results()
	if len(results) != 4 {
		t.Fatalf("expected 4 step results, got %d", len(results))
	}
	if results[2].StepID != "downstream" || results[2].Status != StepStatusSkipped {
		t.Fatalf("expected downstream to be skipped, got %+v", results[2])
	}
	if results[3].StepID != "deprovision" || results[3].Status != StepStatusOK {
		t.Fatalf("expected deprovision to run successfully, got %+v", results[3])
	}
}

func TestRunnerReturnsSuccessForAllSuccessfulSteps(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: success
steps:
  - id: provision
    type: fake
  - id: query
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	runner := NewRunner(RunnerConfig{
		RunID:    "run-2",
		Scenario: scenario,
		Executor: StepExecutorFunc(func(context.Context, Step) error {
			return nil
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if summary.RunID != "run-2" {
		t.Fatalf("summary run id = %q, want run-2", summary.RunID)
	}
	if summary.TotalSteps != 2 || summary.SucceededSteps != 2 || summary.FailedSteps != 0 || summary.SkippedSteps != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func fixedClock(ts time.Time) func() time.Time {
	return func() time.Time { return ts }
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
