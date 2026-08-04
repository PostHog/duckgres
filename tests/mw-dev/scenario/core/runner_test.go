package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

type contextKey string

func TestRunnerRunsIndependentDAGBranchesAfterSiblingFailure(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: independent-dag-branches
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
  - id: dbt
    type: fake
    depends_on: [setup]
  - id: deprovision
    type: fake
    depends_on: [metadata, perf, dbt]
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
			if step.ID == "metadata" {
				return errors.New("metadata failed")
			}
			return nil
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	summary, err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected runner to return failure")
	}
	wantExecuted := []string{"provision", "setup", "metadata", "perf", "dbt", "deprovision"}
	if !equalStrings(executed, wantExecuted) {
		t.Fatalf("executed steps = %#v, want %#v", executed, wantExecuted)
	}
	if summary.TotalSteps != 6 || summary.SucceededSteps != 5 || summary.FailedSteps != 1 || summary.SkippedSteps != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	results := runner.Results()
	if len(results) != 6 {
		t.Fatalf("expected 6 step results, got %d", len(results))
	}
	for _, result := range results[3:5] {
		if result.Status != StepStatusOK {
			t.Fatalf("independent branch %s should run successfully, got %+v", result.StepID, result)
		}
	}
	if results[5].StepID != "deprovision" || results[5].Status != StepStatusOK {
		t.Fatalf("expected deprovision to run successfully, got %+v", results[5])
	}
}

func TestRunnerSkipsDAGDependentsAfterDependencyFailureAndRunsCleanup(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: dependency-failure-cleanup
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
  - id: dbt
    type: fake
    depends_on: [setup]
  - id: deprovision
    type: fake
    depends_on: [metadata, perf, dbt]
    always_run: true
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
			if step.ID == "setup" {
				return errors.New("setup failed")
			}
			return nil
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	summary, err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected runner to return failure")
	}
	if want := []string{"provision", "setup", "deprovision"}; !equalStrings(executed, want) {
		t.Fatalf("executed steps = %#v, want %#v", executed, want)
	}
	if summary.FailedSteps != 1 || summary.SkippedSteps != 3 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	results := runner.Results()
	for _, result := range results[2:5] {
		if result.Status != StepStatusSkipped || result.ErrorClass != "dependency_failed" {
			t.Fatalf("dependent step %s should be skipped for its failed dependency, got %+v", result.StepID, result)
		}
	}
	if result := results[5]; result.StepID != "deprovision" || result.Status != StepStatusOK {
		t.Fatalf("expected deprovision to run successfully, got %+v", result)
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

func TestRunnerMarksRecoveredAttemptFailuresAsSuccessfulWithRetries(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: recovered
steps:
  - id: dbt_models
    type: dbt_run
  - id: downstream
    type: fake
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	executor := &metadataExecutor{
		results: map[string]StepResultMetadata{
			"dbt_models": {
				Attempts:       2,
				FailedAttempts: 1,
				Recovered:      true,
				AttemptDetails: []StepAttemptResult{{
					Attempt:     1,
					CommandName: "run",
					Status:      "failed",
					ExitCode:    2,
					Error:       "flight EOF",
				}, {
					Attempt:     2,
					CommandName: "retry",
					Status:      "ok",
					RetryOf:     "run",
				}},
			},
		},
	}
	runner := NewRunner(RunnerConfig{
		RunID:    "run-recovered",
		Scenario: scenario,
		Executor: executor,
		Now:      fixedClock(time.Unix(1700000000, 0)),
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error for recovered failure: %v", err)
	}
	if summary.SucceededSteps != 2 || summary.RecoveredSteps != 1 || summary.FailedAttempts != 1 || summary.Status != RunStatusSuccessWithRetries {
		t.Fatalf("summary = %+v", summary)
	}
	results := runner.Results()
	if results[0].Status != StepStatusSuccessAfterRetry || !results[0].Recovered || results[0].FailedAttempts != 1 {
		t.Fatalf("dbt result = %+v", results[0])
	}
	if len(results[0].AttemptDetails) != 2 || results[0].AttemptDetails[0].Status != "failed" {
		t.Fatalf("attempt details = %+v", results[0].AttemptDetails)
	}
	if results[1].StepID != "downstream" || results[1].Status != StepStatusOK {
		t.Fatalf("downstream should run after recovered step, got %+v", results[1])
	}
}

func TestRunnerIncludesAttemptMetadataForExpectedErrors(t *testing.T) {
	scenario, err := ParseScenario([]byte(`
name: expected-error-attempts
steps:
  - id: dbt_models
    type: dbt_run
    expect_error:
      error_class: dbt_execution
      contains: ["exit code 2"]
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	executor := &metadataExecutor{
		errs: map[string]error{
			"dbt_models": classifiedTestError{class: "dbt_execution", message: "dbt command run failed with exit code 2"},
		},
		results: map[string]StepResultMetadata{
			"dbt_models": {
				Attempts:       2,
				FailedAttempts: 2,
				AttemptDetails: []StepAttemptResult{{
					Attempt:     1,
					CommandName: "run",
					Status:      "failed",
					ExitCode:    2,
				}, {
					Attempt:     2,
					CommandName: "retry",
					Status:      "failed",
					ExitCode:    2,
					RetryOf:     "run",
				}},
			},
		},
	}
	runner := NewRunner(RunnerConfig{
		RunID:    "run-expected-error-attempts",
		Scenario: scenario,
		Executor: executor,
		Now:      fixedClock(time.Unix(1700000000, 0)),
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error for expected dbt failure: %v", err)
	}
	if summary.SucceededSteps != 1 || summary.FailedAttempts != 2 || summary.Status != RunStatusSuccessWithRetries {
		t.Fatalf("summary = %+v", summary)
	}
	results := runner.Results()
	if len(results) != 1 {
		t.Fatalf("results = %+v", results)
	}
	if results[0].Status != StepStatusOK || results[0].Attempts != 2 || results[0].FailedAttempts != 2 {
		t.Fatalf("result = %+v", results[0])
	}
	if len(results[0].AttemptDetails) != 2 || results[0].AttemptDetails[1].RetryOf != "run" {
		t.Fatalf("attempt details = %+v", results[0].AttemptDetails)
	}
}

type metadataExecutor struct {
	errs    map[string]error
	results map[string]StepResultMetadata
}

func (e *metadataExecutor) ExecuteStep(_ context.Context, step Step) error {
	return e.errs[step.ID]
}

func (e *metadataExecutor) StepResultMetadata(stepID string) (StepResultMetadata, bool) {
	result, ok := e.results[stepID]
	return result, ok
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

func TestRunnerTreatsExpectedErrorAsSuccessfulAssertion(t *testing.T) {
	sentinel := classifiedTestError{class: "provision_step_error", message: "HTTP 400: org id must be a canonical UUID or a slug of at most 35 characters"}
	scenario, err := ParseScenario([]byte(`
name: expected-provision-rejection
steps:
  - id: provision
    type: provision_warehouse
    expect_error:
      error_class: provision_step_error
      contains:
        - HTTP 400
        - slug of at most 35 characters
`))
	if err != nil {
		t.Fatalf("ParseScenario returned error: %v", err)
	}

	runner := NewRunner(RunnerConfig{
		RunID:    "run-expected-error",
		Scenario: scenario,
		Executor: StepExecutorFunc(func(context.Context, Step) error {
			return sentinel
		}),
		Now: fixedClock(time.Unix(1700000000, 0)),
	})

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error for matching expected error: %v", err)
	}
	if summary.SucceededSteps != 1 || summary.FailedSteps != 0 || summary.SkippedSteps != 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	results := runner.Results()
	if len(results) != 1 {
		t.Fatalf("results = %+v, want one result", results)
	}
	if results[0].Status != StepStatusOK {
		t.Fatalf("expected matching error assertion to be ok, got %+v", results[0])
	}
	if results[0].ErrorClass != "provision_step_error" || results[0].Error == "" {
		t.Fatalf("expected matched error details to be recorded, got %+v", results[0])
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
