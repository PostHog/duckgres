package perf

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
	"github.com/posthog/duckgres/tests/mw-dev/scenario/provision"
	scenariosql "github.com/posthog/duckgres/tests/mw-dev/scenario/sql"
	perfcore "github.com/posthog/duckgres/tests/perf/core"
	trinodriver "github.com/posthog/duckgres/tests/perf/drivers/trino"
)

func TestExecutorRunsPerfStepAndWritesArtifacts(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolPGWire})
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
			DialHost:        "10.0.0.10",
			SNISuffix:       ".dev.example",
			Port:            5432,
			SSLMode:         "require",
			ConnectTimeout:  10,
			ApplicationName: "duckgres-scenario-runner",
		},
		OutputDir:     t.TempDir(),
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
	if result.Summary.RunID != "scenario-run-1" || result.Summary.TotalQueries != 1 || result.Summary.TotalErrors != 0 {
		t.Fatalf("summary = %+v", result.Summary)
	}
	pgwireDSN := factory.pgwireConnection.DSN
	if !strings.Contains(pgwireDSN, "host=scenario-org.dev.example") || !strings.Contains(pgwireDSN, "password=root-password") {
		t.Fatalf("pgwire dsn = %q, want scenario org host and provision password", pgwireDSN)
	}
	if strings.Contains(pgwireDSN, "hostaddr=") {
		t.Fatalf("pgwire dsn = %q, should not use unsupported lib/pq hostaddr", pgwireDSN)
	}
	if factory.pgwireConnection.DialAddress != "10.0.0.10:5432" {
		t.Fatalf("pgwire direct address = %q, want 10.0.0.10:5432", factory.pgwireConnection.DialAddress)
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
	if !strings.Contains(csvText, "\nq1,i1,1,pgwire,ok,") {
		t.Fatalf("query_results.csv missing measured pgwire row: %q", csvText)
	}
}

func TestExecutorRestrictsCatalogToStepTargets(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolPGWire})
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
			DialHost:  "10.0.0.10",
			SNISuffix: ".dev.example",
			SSLMode:   "require",
		},
		OutputDir:     t.TempDir(),
		DriverFactory: factory,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "perf_queries",
		Type: StepTypePerfQueries,
		With: map[string]any{
			"org_id":       "scenario-org",
			"catalog_file": catalogPath,
			"run_id":       "scenario-run-1",
			"targets":      []any{"pgwire"},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}

	result, ok := executor.State().Result("perf_queries")
	if !ok {
		t.Fatal("expected perf result to be recorded")
	}
	if result.Summary.TotalQueries != 1 || result.Summary.TotalErrors != 0 {
		t.Fatalf("summary = %+v, want one successful pgwire query", result.Summary)
	}
	csvBytes, err := os.ReadFile(filepath.Join(executor.OutputDir(), "perf", "query_results.csv"))
	if err != nil {
		t.Fatalf("read query_results.csv: %v", err)
	}
	if !strings.Contains(string(csvBytes), ",pgwire,") {
		t.Fatalf("query_results.csv does not contain pgwire result: %q", string(csvBytes))
	}
}

func TestExecutorBuildsTrinoDriverFromReadinessState(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolTrino})
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	provisionState.StoreTrinoStatus("scenario-org", provision.TrinoStatus{
		Cell: provision.TrinoCell{
			ID:             "cell-a",
			CoordinatorURL: "https://trino.example.test:8443",
		},
		Enabled:   true,
		Available: true,
		Status: &provision.TrinoOrgStatus{
			Org:       "scenario-org",
			Principal: "org_database",
			Catalog:   "org_catalog",
			Cell:      "cell-a",
			State:     provision.WarehouseStateReady,
		},
	})
	factory := &fakeDriverFactory{}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		OutputDir:      t.TempDir(),
		DriverFactory:  factory,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "perf_queries",
		Type: StepTypePerfQueries,
		With: map[string]any{
			"org_id":                      "scenario-org",
			"catalog_file":                catalogPath,
			"run_id":                      "scenario-run-1",
			"targets":                     []any{"trino"},
			"trino_ca_cert_file":          "/trino-ca/ca.crt",
			"trino_startup_timeout":       "45s",
			"trino_startup_poll_interval": "3s",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}

	got := factory.trinoConnection
	if got.ServerURL != "https://trino.example.test:8443" || got.Username != "org_database" {
		t.Fatalf("Trino identity = %+v, want coordinator and status principal", got)
	}
	if got.Username == "root" {
		t.Fatal("Trino must use the derived status principal, not root")
	}
	if got.Password != "root-password" || got.Catalog != "org_catalog" || got.Schema != "posthog" {
		t.Fatalf("Trino auth/catalog = %+v, want provision password, status catalog, and posthog schema", got)
	}
	if got.CACertFile != "/trino-ca/ca.crt" || got.Startup.Timeout != 45*time.Second || got.Startup.PollInterval != 3*time.Second {
		t.Fatalf("Trino TLS/startup = %+v, want explicit verified CA and retry bounds", got)
	}
	if factory.trinoContext == nil {
		t.Fatal("Trino factory did not receive scenario context for untimed startup smoke")
	}

	csvBytes, err := os.ReadFile(filepath.Join(executor.OutputDir(), "perf", "query_results.csv"))
	if err != nil {
		t.Fatalf("read query_results.csv: %v", err)
	}
	if !strings.Contains(string(csvBytes), "\nq1,i1,1,trino,ok,") {
		t.Fatalf("query_results.csv missing measured Trino row: %q", string(csvBytes))
	}
}

func TestExecutorRejectsTrinoWithoutReadinessState(t *testing.T) {
	catalogPath := writePerfCatalog(t, []perfcore.Protocol{perfcore.ProtocolTrino})
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{Password: "root-password"})
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		OutputDir:      t.TempDir(),
		DriverFactory:  &fakeDriverFactory{},
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
	if err == nil || !strings.Contains(err.Error(), "wait_trino_ready") {
		t.Fatalf("error = %v, want missing readiness-state guidance", err)
	}
}

func TestExecutorRejectsTargetOverrideOutsideCatalog(t *testing.T) {
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
			DialHost:  "10.0.0.10",
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
			"targets":      []any{"flight"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported perf protocol") {
		t.Fatalf("error = %v, want unsupported protocol error", err)
	}
}

func TestExecutorRejectsInvalidTargetOverride(t *testing.T) {
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
			DialHost:  "10.0.0.10",
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
			"targets":      "pgwire",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "with.targets must be a non-empty list") {
		t.Fatalf("error = %v, want invalid target list error", err)
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
			DialHost:  "10.0.0.10",
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
			DialHost:  "10.0.0.10",
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
		"    pgwire_sql: SELECT 1\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write perf catalog: %v", err)
	}
	return path
}

type fakeDriverFactory struct {
	pgwireConnection scenariosql.PGWireConnection
	pgwireErr        error
	pgwireDriver     *fakeProtocolDriver
	trinoConnection  trinodriver.ConnectionConfig
	trinoContext     context.Context
	trinoDriver      *fakeProtocolDriver
}

func (f *fakeDriverFactory) NewPGWire(connection scenariosql.PGWireConnection) (perfcore.ProtocolDriver, error) {
	f.pgwireConnection = connection
	f.pgwireDriver = &fakeProtocolDriver{protocol: perfcore.ProtocolPGWire, err: f.pgwireErr}
	return f.pgwireDriver, nil
}

func (f *fakeDriverFactory) NewTrino(ctx context.Context, connection trinodriver.ConnectionConfig) (perfcore.ProtocolDriver, error) {
	f.trinoContext = ctx
	f.trinoConnection = connection
	f.trinoDriver = &fakeProtocolDriver{protocol: perfcore.ProtocolTrino}
	return f.trinoDriver, nil
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
