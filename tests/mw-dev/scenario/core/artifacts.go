package core

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type RunSummary struct {
	RunID          string    `json:"run_id"`
	ScenarioName   string    `json:"scenario_name"`
	Status         RunStatus `json:"status"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	TotalSteps     int       `json:"total_steps"`
	SucceededSteps int       `json:"succeeded_steps"`
	FailedSteps    int       `json:"failed_steps"`
	SkippedSteps   int       `json:"skipped_steps"`
	RecoveredSteps int       `json:"recovered_steps"`
	FailedAttempts int       `json:"failed_attempts"`
}

type StepResult struct {
	RunID          string              `json:"run_id"`
	ScenarioName   string              `json:"scenario_name"`
	StepID         string              `json:"step_id"`
	StepType       string              `json:"step_type"`
	Status         StepStatus          `json:"status"`
	ErrorClass     string              `json:"error_class,omitempty"`
	Error          string              `json:"error,omitempty"`
	StartedAt      time.Time           `json:"started_at"`
	FinishedAt     time.Time           `json:"finished_at"`
	Duration       time.Duration       `json:"duration_ns"`
	Attempts       int                 `json:"attempts"`
	FailedAttempts int                 `json:"failed_attempts"`
	Recovered      bool                `json:"recovered"`
	AttemptDetails []StepAttemptResult `json:"attempt_details,omitempty"`
	Err            error               `json:"-"`
}

type StepResultMetadata struct {
	Attempts       int
	FailedAttempts int
	Recovered      bool
	AttemptDetails []StepAttemptResult
}

type StepAttemptResult struct {
	Attempt     int    `json:"attempt"`
	CommandName string `json:"command_name"`
	Status      string `json:"status"`
	ExitCode    int    `json:"exit_code,omitempty"`
	Error       string `json:"error,omitempty"`
	RetryOf     string `json:"retry_of,omitempty"`
	OutputDir   string `json:"output_dir,omitempty"`
}

var stepResultsHeader = []string{
	"run_id",
	"scenario_name",
	"step_id",
	"step_type",
	"status",
	"error_class",
	"error",
	"started_at",
	"finished_at",
	"duration_ms",
	"attempts",
	"failed_attempts",
	"recovered",
}

func WriteArtifacts(dir string, summary RunSummary, results []StepResult) error {
	if err := PrepareArtifactDir(dir); err != nil {
		return err
	}
	if err := writeSummary(filepath.Join(dir, "scenario_summary.json"), summary); err != nil {
		return err
	}
	if err := writeStepResults(filepath.Join(dir, "step_results.csv"), results); err != nil {
		return err
	}
	if err := writeEvents(filepath.Join(dir, "events.jsonl"), results); err != nil {
		return err
	}
	return nil
}

func PrepareArtifactDir(dir string) error {
	if dir == "" {
		return fmt.Errorf("artifact output dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create artifact dir %s: %w", dir, err)
	}
	return nil
}

func writeSummary(path string, summary RunSummary) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create scenario summary: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		_ = f.Close()
		return fmt.Errorf("encode scenario summary: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close scenario summary: %w", err)
	}
	return nil
}

func writeStepResults(path string, results []StepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create step results csv: %w", err)
	}
	w := csv.NewWriter(f)
	if err := w.Write(stepResultsHeader); err != nil {
		_ = f.Close()
		return fmt.Errorf("write step results header: %w", err)
	}
	for _, result := range results {
		if err := w.Write(stepResultRecord(result)); err != nil {
			_ = f.Close()
			return fmt.Errorf("write step result row: %w", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		_ = f.Close()
		return fmt.Errorf("flush step results csv: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close step results csv: %w", err)
	}
	return nil
}

func stepResultRecord(result StepResult) []string {
	return []string{
		result.RunID,
		result.ScenarioName,
		result.StepID,
		result.StepType,
		string(result.Status),
		result.ErrorClass,
		result.Error,
		result.StartedAt.UTC().Format(time.RFC3339Nano),
		result.FinishedAt.UTC().Format(time.RFC3339Nano),
		strconv.FormatFloat(float64(result.Duration)/float64(time.Millisecond), 'f', 6, 64),
		strconv.Itoa(result.Attempts),
		strconv.Itoa(result.FailedAttempts),
		strconv.FormatBool(result.Recovered),
	}
}

func writeEvents(path string, results []StepResult) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create events jsonl: %w", err)
	}
	enc := json.NewEncoder(f)
	for _, result := range results {
		if err := enc.Encode(result); err != nil {
			_ = f.Close()
			return fmt.Errorf("encode event: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close events jsonl: %w", err)
	}
	return nil
}
