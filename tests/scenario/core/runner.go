package core

import (
	"context"
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
	Now        func() time.Time
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

	statusByStep := make(map[string]StepStatus, len(r.cfg.Scenario.Steps))
	r.results = r.results[:0]
	for _, step := range r.cfg.Scenario.Steps {
		result := r.runStep(ctx, runID, step, statusByStep)
		r.results = append(r.results, result)
		statusByStep[step.ID] = result.Status
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
		return summary, fmt.Errorf("scenario %s failed: %d failed, %d skipped", summary.ScenarioName, summary.FailedSteps, summary.SkippedSteps)
	}
	return summary, nil
}

func (r *Runner) Results() []StepResult {
	out := make([]StepResult, len(r.results))
	copy(out, r.results)
	return out
}

func (r *Runner) runStep(ctx context.Context, runID string, step Step, statusByStep map[string]StepStatus) StepResult {
	if !step.AlwaysRun {
		if dep, ok := firstUnsuccessfulDependency(step, statusByStep); ok {
			now := r.cfg.Now()
			return StepResult{
				RunID:        runID,
				ScenarioName: r.cfg.Scenario.Name,
				StepID:       step.ID,
				StepType:     step.Type,
				Status:       StepStatusSkipped,
				ErrorClass:   "dependency_failed",
				Error:        fmt.Sprintf("dependency %s did not complete successfully", dep),
				StartedAt:    now,
				FinishedAt:   now,
			}
		}
	}

	startedAt := r.cfg.Now()
	err := r.cfg.Executor.ExecuteStep(ctx, step)
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
		result.ErrorClass = "execution_error"
		result.Error = err.Error()
	}
	return result
}

func firstUnsuccessfulDependency(step Step, statusByStep map[string]StepStatus) (string, bool) {
	for _, dep := range step.DependsOn {
		if statusByStep[dep] != StepStatusOK {
			return dep, true
		}
	}
	return "", false
}

func defaultRunID(s Scenario, startedAt time.Time) string {
	prefix := s.RunIDPrefix
	if prefix == "" {
		prefix = "scenario"
	}
	return fmt.Sprintf("%s-%s", prefix, startedAt.UTC().Format("20060102T150405Z"))
}
