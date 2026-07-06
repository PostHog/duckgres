package dbt

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/scenario/core"
	"github.com/posthog/duckgres/tests/scenario/provision"
	scenariosql "github.com/posthog/duckgres/tests/scenario/sql"
)

func TestExecutorRunsCommandsCapturesLogsAndCopiesTargetArtifacts(t *testing.T) {
	projectDir := writeDBTProject(t)
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	runner := &fakeCommandRunner{
		onRun: func(req CommandRequest) CommandResult {
			if req.CommandName == "run" {
				targetDir := argValue(req.Args, "--target-path")
				if targetDir == "" {
					t.Fatal("missing --target-path")
				}
				if err := os.MkdirAll(targetDir, 0o755); err != nil {
					t.Fatalf("create target dir: %v", err)
				}
				if err := os.WriteFile(filepath.Join(targetDir, "run_results.json"), []byte(`{"metadata":{}}`), 0o644); err != nil {
					t.Fatalf("write run_results: %v", err)
				}
			}
			return CommandResult{
				Stdout: "connected with root-password",
				Stderr: "warning root-password",
			}
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			HostAddr:        "10.0.0.10",
			SNISuffix:       ".dev.example",
			Port:            5432,
			SSLMode:         "require",
			ConnectTimeout:  10,
			ApplicationName: "duckgres-scenario-runner",
		},
		OutputDir:     t.TempDir(),
		CommandRunner: runner,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "dbt_models",
		Type: StepTypeDBTRun,
		With: map[string]any{
			"org_id":      "scenario-org",
			"project_dir": projectDir,
			"commands":    []any{"debug", "run", "test", "docs_generate"},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if got := runner.commandNames(); !reflect.DeepEqual(got, []string{"debug", "run", "test", "docs_generate"}) {
		t.Fatalf("commands = %#v, want debug/run/test/docs_generate", got)
	}
	first := runner.requests[0]
	if first.Dir != projectDir {
		t.Fatalf("command dir = %q, want project dir", first.Dir)
	}
	if !containsAll(first.Args, "--project-dir", projectDir, "--profiles-dir", projectDir) {
		t.Fatalf("dbt args = %#v, want project/profiles dirs", first.Args)
	}
	if targetPath := argValue(first.Args, "--target-path"); targetPath != "" {
		t.Fatalf("debug target path = %q, want omitted because dbt debug does not support --target-path", targetPath)
	}
	if logPath := argValue(first.Args, "--log-path"); logPath != filepath.Join(executor.OutputDir(), "dbt", "logs") {
		t.Fatalf("log path = %q", logPath)
	}
	runReq := runner.requests[1]
	if targetPath := argValue(runReq.Args, "--target-path"); targetPath != filepath.Join(executor.OutputDir(), "dbt", "target") {
		t.Fatalf("run target path = %q", targetPath)
	}
	if envValue(first.Env, "DUCKGRES_DBT_HOST") != "scenario-org.dev.example" {
		t.Fatalf("DUCKGRES_DBT_HOST = %q", envValue(first.Env, "DUCKGRES_DBT_HOST"))
	}
	if envValue(first.Env, "DUCKGRES_DBT_HOSTADDR") != "" {
		t.Fatal("expected command environment to avoid DUCKGRES_DBT_HOSTADDR because dbt-postgres expects hostaddr to be numeric")
	}
	if envValue(first.Env, "PGHOSTADDR") != "" {
		t.Fatal("expected command environment to avoid PGHOSTADDR because dbt-postgres expects hostaddr to be numeric")
	}
	if envValue(first.Env, "DBT_ENV_SECRET_DUCKGRES_PASSWORD") != "root-password" {
		t.Fatal("expected command environment to include provision password as a dbt secret")
	}
	if envValue(first.Env, "DUCKGRES_DBT_PASSWORD") != "" {
		t.Fatal("expected command environment to avoid non-secret dbt password variable")
	}

	dbtDir := filepath.Join(executor.OutputDir(), "dbt")
	for _, path := range []string{
		filepath.Join(dbtDir, "debug.stdout.log"),
		filepath.Join(dbtDir, "run.stderr.log"),
		filepath.Join(dbtDir, "target", "run_results.json"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected artifact %s: %v", path, err)
		}
	}
	logBytes, err := os.ReadFile(filepath.Join(dbtDir, "debug.stdout.log"))
	if err != nil {
		t.Fatalf("read dbt stdout log: %v", err)
	}
	if strings.Contains(string(logBytes), "root-password") || !strings.Contains(string(logBytes), "[REDACTED]") {
		t.Fatalf("stdout log was not redacted: %q", string(logBytes))
	}
	result, ok := executor.State().Result("dbt_models")
	if !ok {
		t.Fatal("expected dbt result to be recorded")
	}
	if result.CommandsRun != 4 || result.OutputDir != dbtDir {
		t.Fatalf("result = %+v", result)
	}
}

func TestExecutorStopsAfterFailedDBTCommand(t *testing.T) {
	projectDir := writeDBTProject(t)
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	runner := &fakeCommandRunner{
		onRun: func(req CommandRequest) CommandResult {
			if req.CommandName == "run" {
				return CommandResult{ExitCode: 2, Stderr: "database error"}
			}
			return CommandResult{}
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			SSLMode:   "require",
		},
		OutputDir:     t.TempDir(),
		CommandRunner: runner,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "dbt_models",
		Type: StepTypeDBTRun,
		With: map[string]any{
			"org_id":      "scenario-org",
			"project_dir": projectDir,
			"commands":    []any{"debug", "run", "test"},
		},
	})
	if err == nil {
		t.Fatal("expected dbt run failure")
	}
	if !strings.Contains(err.Error(), "dbt command run failed with exit code 2") {
		t.Fatalf("error = %v, want failed command", err)
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.ErrorClass() != ErrorClassDBT {
		t.Fatalf("error = %T %v, want class %q", err, err, ErrorClassDBT)
	}
	if got := runner.commandNames(); !reflect.DeepEqual(got, []string{"debug", "run"}) {
		t.Fatalf("commands = %#v, want debug/run only", got)
	}
}

func writeDBTProject(t *testing.T) string {
	t.Helper()
	projectDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(projectDir, "models"), 0o755); err != nil {
		t.Fatalf("create models dir: %v", err)
	}
	for path, body := range map[string]string{
		"dbt_project.yml":  "name: scenario_dbt\nversion: '1.0'\nprofile: duckgres_scenario\nmodel-paths: [models]\n",
		"profiles.yml":     "duckgres_scenario:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n      host: \"{{ env_var('DUCKGRES_DBT_HOST') }}\"\n",
		"models/model.sql": "SELECT 1 AS id\n",
	} {
		if err := os.WriteFile(filepath.Join(projectDir, path), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return projectDir
}

type fakeCommandRunner struct {
	requests []CommandRequest
	onRun    func(CommandRequest) CommandResult
}

func (r *fakeCommandRunner) Run(_ context.Context, req CommandRequest) CommandResult {
	r.requests = append(r.requests, req)
	if r.onRun != nil {
		return r.onRun(req)
	}
	return CommandResult{}
}

func (r *fakeCommandRunner) commandNames() []string {
	names := make([]string, 0, len(r.requests))
	for _, req := range r.requests {
		names = append(names, req.CommandName)
	}
	return names
}

func containsAll(values []string, wants ...string) bool {
	for _, want := range wants {
		found := false
		for _, value := range values {
			if value == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimPrefix(item, prefix)
		}
	}
	return ""
}

func argValue(args []string, key string) string {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == key {
			return args[i+1]
		}
	}
	return ""
}
