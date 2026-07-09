package dbt

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/posthog/duckgres/tests/scenario/core"
	"github.com/posthog/duckgres/tests/scenario/provision"
	scenariosql "github.com/posthog/duckgres/tests/scenario/sql"
)

const StepTypeDBTRun = "dbt_run"

type CommandRunner interface {
	Run(context.Context, CommandRequest) CommandResult
}

type CommandRequest struct {
	Binary      string
	CommandName string
	Args        []string
	Env         []string
	Dir         string
}

type CommandResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Err      error
}

type ExecutorConfig struct {
	ProvisionState *provision.State
	Connection     scenariosql.ConnectionConfig
	OutputDir      string
	DBTBinary      string
	CommandRunner  CommandRunner
	State          *State
}

type Executor struct {
	provisionState *provision.State
	connection     scenariosql.ConnectionConfig
	outputDir      string
	dbtBinary      string
	commandRunner  CommandRunner
	state          *State
}

type State struct {
	mu      sync.Mutex
	results map[string]StepResult
}

type StepResult struct {
	StepID         string
	OutputDir      string
	CommandsRun    int
	Attempts       int
	FailedAttempts int
	Recovered      bool
	AttemptResults []core.StepAttemptResult
}

type stepSpec struct {
	OrgID          string
	Username       string
	Password       string
	ProjectDir     string
	ProfilesDir    string
	OutputSubdir   string
	DBTBinary      string
	Database       string
	Schema         string
	SSLMode        string
	ConnectTimeout int
	Commands       []commandSpec
	Retry          retrySpec
}

type commandSpec struct {
	Name string
	Args []string
}

type retrySpec struct {
	Enabled     bool
	MaxAttempts int
}

type defaultCommandRunner struct{}

func NewExecutor(cfg ExecutorConfig) *Executor {
	runner := cfg.CommandRunner
	if runner == nil {
		runner = defaultCommandRunner{}
	}
	state := cfg.State
	if state == nil {
		state = NewState()
	}
	dbtBinary := cfg.DBTBinary
	if dbtBinary == "" {
		dbtBinary = "dbt"
	}
	return &Executor{
		provisionState: cfg.ProvisionState,
		connection:     cfg.Connection,
		outputDir:      cfg.OutputDir,
		dbtBinary:      dbtBinary,
		commandRunner:  runner,
		state:          state,
	}
}

func NewState() *State {
	return &State{results: make(map[string]StepResult)}
}

func (e *Executor) State() *State {
	return e.state
}

func (e *Executor) OutputDir() string {
	return e.outputDir
}

func (s *State) StoreResult(result StepResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[result.StepID] = result
}

func (s *State) Result(stepID string) (StepResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.results[stepID]
	return result, ok
}

func (e *Executor) StepResultMetadata(stepID string) (core.StepResultMetadata, bool) {
	result, ok := e.state.Result(stepID)
	if !ok {
		return core.StepResultMetadata{}, false
	}
	return core.StepResultMetadata{
		Attempts:       result.Attempts,
		FailedAttempts: result.FailedAttempts,
		Recovered:      result.Recovered,
		AttemptDetails: result.AttemptResults,
	}, true
}

func (e *Executor) ExecuteStep(ctx context.Context, step core.Step) error {
	if step.Type != StepTypeDBTRun {
		return classified(ErrorClassUnsupportedStep, fmt.Errorf("unsupported dbt step type %q", step.Type))
	}
	spec, err := e.parseStep(step)
	if err != nil {
		return err
	}
	dbtDir := filepath.Join(e.outputDir, spec.OutputSubdir)
	if err := os.MkdirAll(dbtDir, 0o755); err != nil {
		return classified(ErrorClassConfig, fmt.Errorf("create dbt artifact dir: %w", err))
	}

	commandsRun := 0
	failedAttempts := 0
	recovered := false
	attemptResults := []core.StepAttemptResult{}
	for _, command := range spec.Commands {
		result, attempt, err := e.runCommandAttempt(ctx, spec, dbtDir, command, "", 1)
		commandsRun++
		attemptResults = append(attemptResults, attempt)
		if err != nil {
			return err
		}
		if result.Err != nil {
			failedAttempts++
			e.state.StoreResult(stepResult(step.ID, dbtDir, commandsRun, failedAttempts, recovered, attemptResults))
			return classified(ErrorClassDBT, fmt.Errorf("dbt command %s failed: %w", command.Name, result.Err))
		}
		if result.ExitCode != 0 {
			failedAttempts++
			if spec.Retry.Enabled && spec.Retry.MaxAttempts > 1 && commandSupportsRetry(command.Name) {
				var retryResult CommandResult
				recoveredThisCommand := false
				for attemptNumber := 2; attemptNumber <= spec.Retry.MaxAttempts; attemptNumber++ {
					var retryAttempt core.StepAttemptResult
					var err error
					retryResult, retryAttempt, err = e.runCommandAttempt(ctx, spec, dbtDir, commandSpec{Name: "retry", Args: []string{"retry"}}, command.Name, attemptNumber)
					commandsRun++
					attemptResults = append(attemptResults, retryAttempt)
					if err != nil {
						return err
					}
					if retryResult.Err == nil && retryResult.ExitCode == 0 {
						recoveredThisCommand = true
						break
					}
					failedAttempts++
				}
				if recoveredThisCommand {
					recovered = true
					continue
				}
				e.state.StoreResult(stepResult(step.ID, dbtDir, commandsRun, failedAttempts, recovered, attemptResults))
				if retryResult.Err != nil {
					return classified(ErrorClassDBT, fmt.Errorf("dbt command %s failed with exit code %d; retry failed: %w", command.Name, result.ExitCode, retryResult.Err))
				}
				return classified(ErrorClassDBT, fmt.Errorf("dbt command %s failed with exit code %d; retry failed with exit code %d", command.Name, result.ExitCode, retryResult.ExitCode))
			}
			e.state.StoreResult(stepResult(step.ID, dbtDir, commandsRun, failedAttempts, recovered, attemptResults))
			return classified(ErrorClassDBT, fmt.Errorf("dbt command %s failed with exit code %d", command.Name, result.ExitCode))
		}
	}
	e.state.StoreResult(stepResult(step.ID, dbtDir, commandsRun, failedAttempts, recovered, attemptResults))
	return nil
}

func stepResult(stepID, dbtDir string, commandsRun, failedAttempts int, recovered bool, attemptResults []core.StepAttemptResult) StepResult {
	return StepResult{
		StepID:         stepID,
		OutputDir:      dbtDir,
		CommandsRun:    commandsRun,
		Attempts:       len(attemptResults),
		FailedAttempts: failedAttempts,
		Recovered:      recovered,
		AttemptResults: append([]core.StepAttemptResult{}, attemptResults...),
	}
}

func (e *Executor) parseStep(step core.Step) (stepSpec, error) {
	orgID, err := requiredString(step, "org_id")
	if err != nil {
		return stepSpec{}, err
	}
	projectDir, err := requiredString(step, "project_dir")
	if err != nil {
		return stepSpec{}, err
	}
	if e.outputDir == "" {
		return stepSpec{}, classified(ErrorClassConfig, fmt.Errorf("dbt output dir is required"))
	}

	username := stringFromWith(step, "username", "root")
	password := stringFromWith(step, "password", "")
	if password == "" {
		if e.provisionState == nil {
			return stepSpec{}, classified(ErrorClassConfig, fmt.Errorf("provision state is required when with.password is omitted"))
		}
		resp, ok := e.provisionState.ProvisionResponse(orgID)
		if !ok {
			return stepSpec{}, classified(ErrorClassConfig, fmt.Errorf("no provision response found for org %q", orgID))
		}
		if resp.Username != "" {
			username = resp.Username
		}
		password = resp.Password
	}

	commands, err := commandsFromStep(step)
	if err != nil {
		return stepSpec{}, err
	}
	retry, err := retryFromStep(step)
	if err != nil {
		return stepSpec{}, err
	}
	return stepSpec{
		OrgID:          orgID,
		Username:       username,
		Password:       password,
		ProjectDir:     projectDir,
		ProfilesDir:    stringFromWith(step, "profiles_dir", projectDir),
		OutputSubdir:   stringFromWith(step, "output_subdir", "dbt"),
		DBTBinary:      stringFromWith(step, "dbt_bin", e.dbtBinary),
		Database:       stringFromWith(step, "catalog", "ducklake"),
		Schema:         stringFromWith(step, "schema", "dbt_models"),
		SSLMode:        stringFromWith(step, "sslmode", "require"),
		ConnectTimeout: intFromWith(step, "connect_timeout", e.connection.ConnectTimeout),
		Commands:       commands,
		Retry:          retry,
	}, nil
}

func (e *Executor) runCommandAttempt(ctx context.Context, spec stepSpec, dbtDir string, command commandSpec, retryOf string, attemptNumber int) (CommandResult, core.StepAttemptResult, error) {
	args := append([]string{}, command.Args...)
	args = append(args,
		"--project-dir", spec.ProjectDir,
		"--profiles-dir", spec.ProfilesDir,
		"--log-path", filepath.Join(dbtDir, "logs"),
	)
	if command.Name != "debug" {
		args = append(args, "--target-path", filepath.Join(dbtDir, "target"))
	}
	req := CommandRequest{
		Binary:      spec.DBTBinary,
		CommandName: command.Name,
		Args:        args,
		Env:         e.commandEnv(spec),
		Dir:         spec.ProjectDir,
	}
	result := e.commandRunner.Run(ctx, req)
	if err := writeCommandLogs(dbtDir, command.Name, result, spec.Password); err != nil {
		return result, core.StepAttemptResult{}, err
	}

	status := "ok"
	message := ""
	if result.Err != nil {
		status = "failed"
		message = result.Err.Error()
	} else if result.ExitCode != 0 {
		status = "failed"
		message = fmt.Sprintf("exit code %d", result.ExitCode)
	}
	attemptDir := filepath.Join(dbtDir, "attempts", attemptRoot(command.Name, retryOf), fmt.Sprintf("attempt_%d", attemptNumber))
	if err := writeCommandLogs(attemptDir, command.Name, result, spec.Password); err != nil {
		return result, core.StepAttemptResult{}, err
	}
	if err := snapshotDBTArtifacts(dbtDir, attemptDir); err != nil {
		return result, core.StepAttemptResult{}, err
	}
	return result, core.StepAttemptResult{
		Attempt:     attemptNumber,
		CommandName: command.Name,
		Status:      status,
		ExitCode:    result.ExitCode,
		Error:       message,
		RetryOf:     retryOf,
		OutputDir:   attemptDir,
	}, nil
}

func (e *Executor) commandEnv(spec stepSpec) []string {
	port := e.connection.Port
	if port == 0 {
		port = 5432
	}
	connectTimeout := spec.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = 10
	}
	env := []string{
		"DUCKGRES_DBT_HOST=" + spec.OrgID + e.connection.SNISuffix,
		"DUCKGRES_DBT_PORT=" + strconv.Itoa(port),
		"DUCKGRES_DBT_USER=" + spec.Username,
		"DBT_ENV_SECRET_DUCKGRES_PASSWORD=" + spec.Password,
		"DUCKGRES_DBT_DBNAME=" + spec.Database,
		"DUCKGRES_DBT_SCHEMA=" + spec.Schema,
		"DUCKGRES_DBT_SSLMODE=" + spec.SSLMode,
		"DUCKGRES_DBT_CONNECT_TIMEOUT=" + strconv.Itoa(connectTimeout),
	}
	if _, err := netip.ParseAddr(e.connection.HostAddr); err == nil {
		env = append(env, "PGHOSTADDR="+e.connection.HostAddr)
	}
	return env
}

func commandsFromStep(step core.Step) ([]commandSpec, error) {
	raw, ok := step.With["commands"]
	if !ok {
		return []commandSpec{
			{Name: "debug", Args: []string{"debug"}},
			{Name: "run", Args: []string{"run"}},
			{Name: "test", Args: []string{"test"}},
			{Name: "docs_generate", Args: []string{"docs", "generate"}},
		}, nil
	}
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		return nil, classified(ErrorClassConfig, fmt.Errorf("step %s with.commands must be a non-empty list", step.ID))
	}
	commands := make([]commandSpec, 0, len(items))
	for i, item := range items {
		name, ok := item.(string)
		if !ok || strings.TrimSpace(name) == "" {
			return nil, classified(ErrorClassConfig, fmt.Errorf("step %s with.commands[%d] must be a non-empty string", step.ID, i))
		}
		command, err := parseCommandName(strings.TrimSpace(name))
		if err != nil {
			return nil, classified(ErrorClassConfig, fmt.Errorf("step %s with.commands[%d]: %w", step.ID, i, err))
		}
		commands = append(commands, command)
	}
	return commands, nil
}

func retryFromStep(step core.Step) (retrySpec, error) {
	raw, ok := step.With["retry"]
	if !ok {
		return retrySpec{MaxAttempts: 1}, nil
	}
	values, ok := raw.(map[string]any)
	if !ok {
		return retrySpec{}, classified(ErrorClassConfig, fmt.Errorf("step %s with.retry must be an object", step.ID))
	}
	enabled := boolFromMap(values, "enabled", false)
	maxAttempts := intFromMap(values, "max_attempts", 2)
	if enabled && maxAttempts < 2 {
		return retrySpec{}, classified(ErrorClassConfig, fmt.Errorf("step %s with.retry.max_attempts must be at least 2 when retry is enabled", step.ID))
	}
	return retrySpec{Enabled: enabled, MaxAttempts: maxAttempts}, nil
}

func commandSupportsRetry(name string) bool {
	switch name {
	case "run", "test", "docs_generate":
		return true
	default:
		return false
	}
}

func parseCommandName(name string) (commandSpec, error) {
	switch name {
	case "debug":
		return commandSpec{Name: name, Args: []string{"debug"}}, nil
	case "run":
		return commandSpec{Name: name, Args: []string{"run"}}, nil
	case "test":
		return commandSpec{Name: name, Args: []string{"test"}}, nil
	case "docs_generate", "docs generate":
		return commandSpec{Name: "docs_generate", Args: []string{"docs", "generate"}}, nil
	default:
		return commandSpec{}, fmt.Errorf("unsupported dbt command %q", name)
	}
}

func writeCommandLogs(dir, commandName string, result CommandResult, secret string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return classified(ErrorClassConfig, fmt.Errorf("create dbt log dir: %w", err))
	}
	for suffix, body := range map[string]string{
		"stdout": result.Stdout,
		"stderr": result.Stderr,
	} {
		path := filepath.Join(dir, commandName+"."+suffix+".log")
		redacted := redactSecret(body, secret)
		if err := os.WriteFile(path, []byte(redacted), 0o644); err != nil {
			return classified(ErrorClassConfig, fmt.Errorf("write dbt %s log: %w", suffix, err))
		}
	}
	return nil
}

func attemptRoot(commandName, retryOf string) string {
	if retryOf != "" {
		return retryOf
	}
	return commandName
}

func snapshotDBTArtifacts(dbtDir, attemptDir string) error {
	for _, name := range []string{"target", "logs"} {
		src := filepath.Join(dbtDir, name)
		if _, err := os.Stat(src); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return classified(ErrorClassConfig, fmt.Errorf("stat dbt artifact %s: %w", name, err))
		}
		if err := copyDir(src, filepath.Join(attemptDir, name)); err != nil {
			return classified(ErrorClassConfig, fmt.Errorf("snapshot dbt artifact %s: %w", name, err))
		}
	}
	return nil
}

func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		body, err := os.ReadFile(srcPath)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dstPath, body, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func redactSecret(text, secret string) string {
	if secret == "" {
		return text
	}
	return strings.ReplaceAll(text, secret, "[REDACTED]")
}

func (defaultCommandRunner) Run(ctx context.Context, req CommandRequest) CommandResult {
	cmd := exec.CommandContext(ctx, req.Binary, req.Args...)
	cmd.Dir = req.Dir
	cmd.Env = append(os.Environ(), req.Env...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := CommandResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}
	if err == nil {
		return result
	}
	result.Err = err
	if exitErr, ok := err.(*exec.ExitError); ok {
		result.ExitCode = exitErr.ExitCode()
		result.Err = nil
	}
	return result
}

func requiredString(step core.Step, key string) (string, error) {
	value, ok := step.With[key].(string)
	if !ok || value == "" {
		return "", classified(ErrorClassConfig, fmt.Errorf("step %s with.%s must be a non-empty string", step.ID, key))
	}
	return value, nil
}

func stringFromWith(step core.Step, key, fallback string) string {
	value, ok := step.With[key].(string)
	if !ok || value == "" {
		return fallback
	}
	return value
}

func intFromWith(step core.Step, key string, fallback int) int {
	raw, ok := step.With[key]
	if !ok {
		return fallback
	}
	switch value := raw.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case string:
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func intFromMap(values map[string]any, key string, fallback int) int {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	switch value := raw.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case string:
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func boolFromMap(values map[string]any, key string, fallback bool) bool {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	switch value := raw.(type) {
	case bool:
		return value
	case string:
		parsed, err := strconv.ParseBool(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}
