package publishercli

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/perf/publisher"
)

func TestRunPublishesRunDirFromConnectionSecret(t *testing.T) {
	t.Parallel()

	var gotConfig publisher.Config
	var gotRunDir string
	deps := Dependencies{
		Stdin: strings.NewReader(`{
  "host": "perf.example.test",
  "port": 5432,
  "database": "ducklake",
  "username": "scenario-writer",
  "password": "secret value"
}`),
		Stdout: io.Discard,
		Publish: func(_ context.Context, cfg publisher.Config, runDir string) error {
			gotConfig = cfg
			gotRunDir = runDir
			return nil
		},
	}

	err := Run(context.Background(), []string{
		"--run-dir", "/artifacts/run/perf",
		"--connection-secret-stdin",
		"--schema", "duckgres_scenario_perf",
		"--bootstrap-schema=true",
	}, deps)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if gotRunDir != "/artifacts/run/perf" {
		t.Fatalf("run dir = %q", gotRunDir)
	}
	if gotConfig.DSN != "postgresql://scenario-writer@perf.example.test:5432/ducklake?connect_timeout=15&sslmode=require" {
		t.Fatalf("dsn = %q", gotConfig.DSN)
	}
	if gotConfig.Password != "secret value" {
		t.Fatal("publisher password was not passed separately")
	}
	if gotConfig.Schema != "duckgres_scenario_perf" || !gotConfig.BootstrapSchema {
		t.Fatalf("publisher config = %#v", gotConfig)
	}
}

func TestRunRejectsMissingRunDir(t *testing.T) {
	t.Parallel()

	err := Run(context.Background(), []string{"--connection-secret-stdin"}, Dependencies{
		Stdin:   strings.NewReader(`{}`),
		Stdout:  io.Discard,
		Publish: func(context.Context, publisher.Config, string) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "run-dir is required") {
		t.Fatalf("expected missing run-dir error, got %v", err)
	}
}

func TestRunRejectsIncompleteConnectionSecret(t *testing.T) {
	t.Parallel()

	err := Run(context.Background(), []string{
		"--run-dir", "/artifacts/run/perf",
		"--connection-secret-stdin",
	}, Dependencies{
		Stdin:  strings.NewReader(`{"host":"perf.example.test","port":5432}`),
		Stdout: io.Discard,
		Publish: func(context.Context, publisher.Config, string) error {
			t.Fatal("publisher should not be called")
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "connection secret is missing database") {
		t.Fatalf("expected incomplete secret error, got %v", err)
	}
}

func TestRunRequiresExactlyOneConnectionSource(t *testing.T) {
	t.Parallel()

	err := Run(context.Background(), []string{
		"--run-dir", "/artifacts/run/perf",
		"--dsn", "host=localhost",
		"--connection-secret-stdin",
	}, Dependencies{
		Stdin:   strings.NewReader(`{}`),
		Stdout:  io.Discard,
		Publish: func(context.Context, publisher.Config, string) error { return nil },
	})
	if err == nil || !strings.Contains(err.Error(), "exactly one of --dsn and --connection-secret-stdin") {
		t.Fatalf("expected connection-source error, got %v", err)
	}
}
