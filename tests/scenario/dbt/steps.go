package dbt

import (
	"bytes"
	"context"
	"fmt"
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
	StepID      string
	OutputDir   string
	CommandsRun int
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
}

type commandSpec struct {
	Name string
	Args []string
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
	for _, command := range spec.Commands {
		req := CommandRequest{
			Binary:      spec.DBTBinary,
			CommandName: command.Name,
			Args: append(append([]string{}, command.Args...),
				"--project-dir", spec.ProjectDir,
				"--profiles-dir", spec.ProfilesDir,
				"--target-path", filepath.Join(dbtDir, "target"),
				"--log-path", filepath.Join(dbtDir, "logs"),
			),
			Env: e.commandEnv(spec),
			Dir: spec.ProjectDir,
		}
		result := e.commandRunner.Run(ctx, req)
		commandsRun++
		if err := writeCommandLogs(dbtDir, command.Name, result, spec.Password); err != nil {
			return err
		}
		if result.Err != nil {
			e.state.StoreResult(StepResult{StepID: step.ID, OutputDir: dbtDir, CommandsRun: commandsRun})
			return classified(ErrorClassDBT, fmt.Errorf("dbt command %s failed: %w", command.Name, result.Err))
		}
		if result.ExitCode != 0 {
			e.state.StoreResult(StepResult{StepID: step.ID, OutputDir: dbtDir, CommandsRun: commandsRun})
			return classified(ErrorClassDBT, fmt.Errorf("dbt command %s failed with exit code %d", command.Name, result.ExitCode))
		}
	}
	e.state.StoreResult(StepResult{StepID: step.ID, OutputDir: dbtDir, CommandsRun: commandsRun})
	return nil
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
	return []string{
		"DUCKGRES_DBT_HOST=" + spec.OrgID + e.connection.SNISuffix,
		"DUCKGRES_DBT_HOSTADDR=" + e.connection.HostAddr,
		"DUCKGRES_DBT_PORT=" + strconv.Itoa(port),
		"DUCKGRES_DBT_USER=" + spec.Username,
		"DBT_ENV_SECRET_DUCKGRES_PASSWORD=" + spec.Password,
		"DUCKGRES_DBT_DBNAME=" + spec.Database,
		"DUCKGRES_DBT_SCHEMA=" + spec.Schema,
		"DUCKGRES_DBT_SSLMODE=" + spec.SSLMode,
		"DUCKGRES_DBT_CONNECT_TIMEOUT=" + strconv.Itoa(connectTimeout),
		"PGHOSTADDR=" + e.connection.HostAddr,
	}
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
