package core

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type StepStatus string

const (
	StepStatusOK      StepStatus = "ok"
	StepStatusFailed  StepStatus = "failed"
	StepStatusSkipped StepStatus = "skipped"
)

type StepExecutor interface {
	ExecuteStep(context.Context, Step) error
}

type ClassifiedError interface {
	error
	ErrorClass() string
}

type StepExecutorFunc func(context.Context, Step) error

func (f StepExecutorFunc) ExecuteStep(ctx context.Context, step Step) error {
	return f(ctx, step)
}

type RunnerConfig struct {
	RunID      string
	Scenario   Scenario
	Executor   StepExecutor
	OutputDir  string
	WriteFiles bool
	// CleanupTimeout bounds always_run steps. These steps use a context whose
	// cancellation is detached from the main scenario context so cleanup can run
	// after the scenario times out.
	CleanupTimeout time.Duration
	Now            func() time.Time
}

type Runner struct {
	cfg     RunnerConfig
	results []StepResult
}

func NewRunner(cfg RunnerConfig) *Runner {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Runner{cfg: cfg}
}

func (r *Runner) Run(ctx context.Context) (RunSummary, error) {
	startedAt := r.cfg.Now()
	runID := r.cfg.RunID
	if runID == "" {
		runID = defaultRunID(r.cfg.Scenario, startedAt)
	}
	summary := RunSummary{
		RunID:        runID,
		ScenarioName: r.cfg.Scenario.Name,
		StartedAt:    startedAt,
		FinishedAt:   startedAt,
		TotalSteps:   len(r.cfg.Scenario.Steps),
	}

	if r.cfg.Executor == nil {
		return summary, fmt.Errorf("scenario runner executor is required")
	}
	r.results = r.results[:0]
	if r.cfg.WriteFiles {
		if err := PrepareArtifactDir(r.cfg.OutputDir); err != nil {
			summary.FinishedAt = r.cfg.Now()
			return summary, err
		}
	}

	statusByStep := make(map[string]StepStatus, len(r.cfg.Scenario.Steps))
	normalStepsBlocked := false
	var runErrs []error
	for _, step := range r.cfg.Scenario.Steps {
		result := r.runStep(ctx, runID, step, statusByStep, normalStepsBlocked)
		r.results = append(r.results, result)
		statusByStep[step.ID] = result.Status
		if result.Err != nil {
			runErrs = append(runErrs, result.Err)
		}
		if !step.AlwaysRun && result.Status != StepStatusOK {
			normalStepsBlocked = true
		}
		switch result.Status {
		case StepStatusOK:
			summary.SucceededSteps++
		case StepStatusFailed:
			summary.FailedSteps++
		case StepStatusSkipped:
			summary.SkippedSteps++
		}
	}
	summary.FinishedAt = r.cfg.Now()

	if r.cfg.WriteFiles {
		if err := WriteArtifacts(r.cfg.OutputDir, summary, r.results); err != nil {
			return summary, err
		}
	}
	if summary.FailedSteps > 0 || summary.SkippedSteps > 0 {
		return summary, scenarioRunError(summary, runErrs)
	}
	return summary, nil
}

func (r *Runner) Results() []StepResult {
	out := make([]StepResult, len(r.results))
	copy(out, r.results)
	return out
}

func (r *Runner) runStep(ctx context.Context, runID string, step Step, statusByStep map[string]StepStatus, normalStepsBlocked bool) StepResult {
	if !step.AlwaysRun {
		if normalStepsBlocked {
			return r.skippedResult(runID, step, "prior_step_failed", "a prior non-cleanup step did not complete successfully", nil)
		}
		if err := ctx.Err(); err != nil {
			return r.skippedResult(runID, step, "context_canceled", "scenario context is canceled", err)
		}
		if dep, ok := firstUnsuccessfulDependency(step, statusByStep); ok {
			return r.skippedResult(runID, step, "dependency_failed", fmt.Sprintf("dependency %s did not complete successfully", dep), nil)
		}
	}

	startedAt := r.cfg.Now()
	stepCtx, cancel := r.contextForStep(ctx, step)
	defer cancel()
	err := r.cfg.Executor.ExecuteStep(stepCtx, step)
	finishedAt := r.cfg.Now()
	result := StepResult{
		RunID:        runID,
		ScenarioName: r.cfg.Scenario.Name,
		StepID:       step.ID,
		StepType:     step.Type,
		Status:       StepStatusOK,
		StartedAt:    startedAt,
		FinishedAt:   finishedAt,
		Duration:     finishedAt.Sub(startedAt),
		Attempts:     1,
	}
	if err != nil {
		result.Status = StepStatusFailed
		result.ErrorClass = classifyStepError(err)
		result.Error = err.Error()
		result.Err = err
	}
	return result
}

func (r *Runner) skippedResult(runID string, step Step, errorClass, message string, err error) StepResult {
	now := r.cfg.Now()
	return StepResult{
		RunID:        runID,
		ScenarioName: r.cfg.Scenario.Name,
		StepID:       step.ID,
		StepType:     step.Type,
		Status:       StepStatusSkipped,
		ErrorClass:   errorClass,
		Error:        message,
		StartedAt:    now,
		FinishedAt:   now,
		Err:          err,
	}
}

func (r *Runner) contextForStep(ctx context.Context, step Step) (context.Context, context.CancelFunc) {
	if !step.AlwaysRun {
		return ctx, func() {}
	}
	base := context.WithoutCancel(ctx)
	if r.cfg.CleanupTimeout <= 0 {
		return base, func() {}
	}
	return context.WithTimeout(base, r.cfg.CleanupTimeout)
}

func firstUnsuccessfulDependency(step Step, statusByStep map[string]StepStatus) (string, bool) {
	for _, dep := range step.DependsOn {
		if statusByStep[dep] != StepStatusOK {
			return dep, true
		}
	}
	return "", false
}

func classifyStepError(err error) string {
	var classified ClassifiedError
	if errors.As(err, &classified) && classified.ErrorClass() != "" {
		return classified.ErrorClass()
	}
	return "execution_error"
}

func defaultRunID(s Scenario, startedAt time.Time) string {
	prefix := s.RunIDPrefix
	if prefix == "" {
		prefix = "scenario"
	}
	return fmt.Sprintf("%s-%s", prefix, startedAt.UTC().Format("20060102T150405Z"))
}

func scenarioRunError(summary RunSummary, causes []error) error {
	aggregate := fmt.Errorf("scenario %s failed: %d failed, %d skipped", summary.ScenarioName, summary.FailedSteps, summary.SkippedSteps)
	if len(causes) == 0 {
		return aggregate
	}
	return errors.Join(append([]error{aggregate}, causes...)...)
}
