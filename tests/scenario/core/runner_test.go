package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

type contextKey string

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

func TestRunnerDoesNotLetAlwaysRunCleanupUnblockLaterNormalSteps(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: cleanup-does-not-unblock
steps:
  - id: query
    type: fake
  - id: cleanup
    type: fake
    always_run: true
  - id: after_cleanup
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	var executed []string
	runner := NewRunner(RunnerConfig{
		RunID:    "run-cleanup",
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
	if want := []string{"query", "cleanup"}; !equalStrings(executed, want) {
		t.Fatalf("executed steps = %#v, want %#v", executed, want)
	}
	if summary.FailedSteps != 1 || summary.SkippedSteps != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	results := runner.Results()
	if results[2].StepID != "after_cleanup" || results[2].Status != StepStatusSkipped {
		t.Fatalf("expected after_cleanup to be skipped, got %+v", results[2])
	}
}

func TestRunnerUsesUncancelledContextForAlwaysRunCleanup(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: cancelled-cleanup
steps:
  - id: query
    type: fake
  - id: deprovision
    type: fake
    always_run: true
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	baseCtx := context.WithValue(context.Background(), contextKey("trace"), "scenario-trace")
	ctx, cancel := context.WithCancel(baseCtx)
	var cleanupContextErr error
	var cleanupContextValue any
	runner := NewRunner(RunnerConfig{
		RunID:    "run-cancelled",
		Scenario: scenario,
		Executor: StepExecutorFunc(func(stepCtx context.Context, step Step) error {
			switch step.ID {
			case "query":
				cancel()
				return context.Canceled
			case "deprovision":
				cleanupContextErr = stepCtx.Err()
				cleanupContextValue = stepCtx.Value(contextKey("trace"))
			}
			return nil
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	_, err = runner.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runner error = %v, want context.Canceled", err)
	}
	if cleanupContextErr != nil {
		t.Fatalf("cleanup context err = %v, want nil", cleanupContextErr)
	}
	if cleanupContextValue != "scenario-trace" {
		t.Fatalf("cleanup context value = %v, want scenario-trace", cleanupContextValue)
	}
	results := runner.Results()
	if results[1].StepID != "deprovision" || results[1].Status != StepStatusOK {
		t.Fatalf("expected deprovision to run successfully, got %+v", results[1])
	}
}

func TestRunnerPreservesExecutorError(t *testing.T) {
	sentinel := errors.New("sentinel failure")
	scenario, err := ParseScenario([]byte(`
name: sentinel
steps:
  - id: query
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	runner := NewRunner(RunnerConfig{
		RunID:    "run-sentinel",
		Scenario: scenario,
		Executor: StepExecutorFunc(func(context.Context, Step) error {
			return sentinel
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	_, err = runner.Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("runner error = %v, want sentinel failure", err)
	}
}

func TestRunnerRecordsClassifiedExecutorError(t *testing.T) {
	sentinel := classifiedTestError{class: "cleanup_timeout", message: "cleanup timed out"}
	scenario, err := ParseScenario([]byte(`
name: classified
steps:
  - id: cleanup
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	runner := NewRunner(RunnerConfig{
		RunID:    "run-classified",
		Scenario: scenario,
		Executor: StepExecutorFunc(func(context.Context, Step) error {
			return sentinel
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	_, err = runner.Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("runner error = %v, want classified sentinel", err)
	}
	results := runner.Results()
	if len(results) != 1 {
		t.Fatalf("results = %+v, want one result", results)
	}
	if results[0].ErrorClass != "cleanup_timeout" {
		t.Fatalf("error class = %q, want cleanup_timeout", results[0].ErrorClass)
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

type classifiedTestError struct {
	class   string
	message string
}

func (e classifiedTestError) Error() string {
	return e.message
}

func (e classifiedTestError) ErrorClass() string {
	return e.class
}
