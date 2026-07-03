package perf

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	perfcore "github.com/posthog/duckgres/tests/perf/core"
	"github.com/posthog/duckgres/tests/scenario/core"
	"github.com/posthog/duckgres/tests/scenario/provision"
	scenariosql "github.com/posthog/duckgres/tests/scenario/sql"
)

func TestExecutorRunsPerfStepAndWritesArtifacts(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolPGWire, perfcore.ProtocolFlight})
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	factory := &fakeDriverFactory{}
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
		FlightAddr:    "flight.dev.example:443",
		DriverFactory: factory,
		Now: func() time.Time {
			return time.Unix(1700000000, 0)
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "perf_queries",
		Type: StepTypePerfQueries,
		With: map[string]any{
			"org_id":          "scenario-org",
			"catalog_file":    catalogPath,
			"run_id":          "scenario-run-1",
			"dataset_version": "posthog-file-views-v1",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}

	result, ok := executor.State().Result("perf_queries")
	if !ok {
		t.Fatal("expected perf result to be recorded")
	}
	if result.Summary.RunID != "scenario-run-1" || result.Summary.TotalQueries != 2 || result.Summary.TotalErrors != 0 {
		t.Fatalf("summary = %+v", result.Summary)
	}
	if got := factory.pgwireDSN; !strings.Contains(got, "host=scenario-org.dev.example") || !strings.Contains(got, "password=root-password") {
		t.Fatalf("pgwire dsn = %q, want scenario org host and provision password", got)
	}
	if factory.flightAddr != "flight.dev.example:443" || factory.flightUsername != "root" || factory.flightPassword != "root-password" {
		t.Fatalf("flight config = %q/%q/%q", factory.flightAddr, factory.flightUsername, factory.flightPassword)
	}
	if factory.flightServerName != "scenario-org.dev.example" {
		t.Fatalf("flight server name = %q, want managed scenario hostname", factory.flightServerName)
	}
	if factory.flightInsecureSkipVerify {
		t.Fatal("flight insecure skip verify should come from executor config when step does not override it")
	}

	perfDir := filepath.Join(executor.OutputDir(), "perf")
	for _, name := range []string{"summary.json", "query_results.csv", "server_metrics.prom"} {
		if _, err := os.Stat(filepath.Join(perfDir, name)); err != nil {
			t.Fatalf("expected perf artifact %s: %v", name, err)
		}
	}
	csvBytes, err := os.ReadFile(filepath.Join(perfDir, "query_results.csv"))
	if err != nil {
		t.Fatalf("read query_results.csv: %v", err)
	}
	csvText := string(csvBytes)
	if !strings.Contains(csvText, "query_id,intent_id,measure_iteration,protocol,status,error,error_class,rows,duration_ms,started_at") {
		t.Fatalf("query_results.csv header changed: %q", csvText)
	}
	if !strings.Contains(csvText, "\nq1,i1,1,pgwire,ok,") || !strings.Contains(csvText, "\nq1,i1,1,flight,ok,") {
		t.Fatalf("query_results.csv missing measured pgwire/flight rows: %q", csvText)
	}
}

func TestExecutorFailsPerfStepWhenMeasuredQueryErrors(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolPGWire})
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			SSLMode:   "require",
		},
		OutputDir: t.TempDir(),
		DriverFactory: &fakeDriverFactory{
			pgwireErr: errors.New("query failed"),
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "perf_queries",
		Type: StepTypePerfQueries,
		With: map[string]any{
			"org_id":       "scenario-org",
			"catalog_file": catalogPath,
			"run_id":       "scenario-run-1",
		},
	})
	if err == nil {
		t.Fatal("expected perf query error to fail the scenario step")
	}
	if !strings.Contains(err.Error(), "recorded 1 query error") {
		t.Fatalf("error = %v, want query error count", err)
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.ErrorClass() != ErrorClassPerf {
		t.Fatalf("error = %T %v, want class %q", err, err, ErrorClassPerf)
	}
	if _, err := os.Stat(filepath.Join(executor.OutputDir(), "perf", "query_results.csv")); err != nil {
		t.Fatalf("expected perf artifacts to be closed before failure: %v", err)
	}
}

func TestExecutorCanReportPerfQueryErrorsWithoutFailingStep(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolPGWire})
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			SSLMode:   "require",
		},
		OutputDir: t.TempDir(),
		DriverFactory: &fakeDriverFactory{
			pgwireErr: errors.New("query failed"),
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "perf_queries",
		Type: StepTypePerfQueries,
		With: map[string]any{
			"org_id":               "scenario-org",
			"catalog_file":         catalogPath,
			"run_id":               "scenario-run-1",
			"fail_on_query_errors": false,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	result, ok := executor.State().Result("perf_queries")
	if !ok {
		t.Fatal("expected perf result to be recorded")
	}
	if result.Summary.TotalErrors != 1 {
		t.Fatalf("summary errors = %d, want reported query error", result.Summary.TotalErrors)
	}
	csvBytes, err := os.ReadFile(filepath.Join(executor.OutputDir(), "perf", "query_results.csv"))
	if err != nil {
		t.Fatalf("read query_results.csv: %v", err)
	}
	if !strings.Contains(string(csvBytes), ",pgwire,error,query failed,execution_error,") {
		t.Fatalf("query_results.csv should report query failure: %q", string(csvBytes))
	}
}

func TestExecutorRequiresFlightAddrWhenCatalogTargetsFlight(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolFlight})
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			SSLMode:   "require",
		},
		OutputDir:     t.TempDir(),
		DriverFactory: &fakeDriverFactory{},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "perf_queries",
		Type: StepTypePerfQueries,
		With: map[string]any{
			"org_id":       "scenario-org",
			"catalog_file": catalogPath,
			"run_id":       "scenario-run-1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "flight_addr") {
		t.Fatalf("error = %v, want missing flight_addr", err)
	}
}

func TestExecutorClosesCreatedDriversWhenLaterDriverCreationFails(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolPGWire, perfcore.ProtocolFlight})
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	factory := &fakeDriverFactory{flightErr: errors.New("flight unavailable")}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: scenariosql.ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			SSLMode:   "require",
		},
		OutputDir:     t.TempDir(),
		FlightAddr:    "flight.dev.example:443",
		DriverFactory: factory,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "perf_queries",
		Type: StepTypePerfQueries,
		With: map[string]any{
			"org_id":       "scenario-org",
			"catalog_file": catalogPath,
			"run_id":       "scenario-run-1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "create flight perf driver") {
		t.Fatalf("error = %v, want flight driver creation failure", err)
	}
	if factory.pgwireDriver == nil || !factory.pgwireDriver.closed {
		t.Fatalf("expected pgwire driver to be closed after flight creation failure")
	}
}

func writePerfCatalog(t *testing.T, targets []perfcore.Protocol) string {
	t.Helper()
	var targetLines strings.Builder
	for _, target := range targets {
		targetLines.WriteString("  - ")
		targetLines.WriteString(string(target))
		targetLines.WriteByte('\n')
	}
	path := filepath.Join(t.TempDir(), "perf_catalog.yaml")
	body := "name: scenario-perf\n" +
		"description: perf adapter test\n" +
		"seed: 42\n" +
		"dataset_scale: 1\n" +
		"targets:\n" + targetLines.String() +
		"warmup_iterations: 1\n" +
		"measure_iterations: 1\n" +
		"queries:\n" +
		"  - query_id: q1\n" +
		"    intent_id: i1\n" +
		"    tags: [test]\n" +
		"    params: {}\n" +
		"    pgwire_sql: SELECT 1\n" +
		"    duckhog_sql: SELECT 1\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write perf catalog: %v", err)
	}
	return path
}

type fakeDriverFactory struct {
	pgwireDSN                string
	pgwireErr                error
	pgwireDriver             *fakeProtocolDriver
	flightAddr               string
	flightServerName         string
	flightUsername           string
	flightPassword           string
	flightInsecureSkipVerify bool
	flightErr                error
}

func (f *fakeDriverFactory) NewPGWire(dsn string) (perfcore.ProtocolDriver, error) {
	f.pgwireDSN = dsn
	f.pgwireDriver = &fakeProtocolDriver{protocol: perfcore.ProtocolPGWire, err: f.pgwireErr}
	return f.pgwireDriver, nil
}

func (f *fakeDriverFactory) NewFlight(addr, serverName, username, password string, insecureSkipVerify bool) (perfcore.ProtocolDriver, error) {
	f.flightAddr = addr
	f.flightServerName = serverName
	f.flightUsername = username
	f.flightPassword = password
	f.flightInsecureSkipVerify = insecureSkipVerify
	if f.flightErr != nil {
		return nil, f.flightErr
	}
	return &fakeProtocolDriver{protocol: perfcore.ProtocolFlight}, nil
}

type fakeProtocolDriver struct {
	protocol perfcore.Protocol
	err      error
	closed   bool
}

func (d *fakeProtocolDriver) Protocol() perfcore.Protocol { return d.protocol }

func (d *fakeProtocolDriver) Execute(context.Context, perfcore.Query, []any) (perfcore.ExecutionResult, error) {
	return perfcore.ExecutionResult{Rows: 1, Duration: time.Millisecond}, d.err
}

func (d *fakeProtocolDriver) Close() error {
	d.closed = true
	return nil
}
