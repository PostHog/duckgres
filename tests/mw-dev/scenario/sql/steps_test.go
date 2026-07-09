package sql

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
)

func TestExecutorRetriesTransientStartupErrors(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	var attempts int
	driver := &fakeDriver{
		executeFunc: func(context.Context, QueryRequest) (QueryResult, error) {
			attempts++
			if attempts < 3 {
				return QueryResult{}, errors.New("no Duckgres worker is currently available; retry in about 45 seconds")
			}
			return QueryResult{Rows: 1}, nil
		},
	}
	var sleeps []time.Duration
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
		Retry: RetryConfig{
			MaxAttempts:  3,
			RetryBackoff: 50 * time.Millisecond,
			Sleep: func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				return nil
			},
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "select_one",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"sql":     "SELECT 1",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(sleeps) != 2 || sleeps[0] != 50*time.Millisecond || sleeps[1] != 50*time.Millisecond {
		t.Fatalf("sleeps = %#v, want two 50ms sleeps", sleeps)
	}
	result, ok := executor.State().Result("select_one")
	if !ok {
		t.Fatal("expected SQL result to be recorded")
	}
	if result.Rows != 1 || result.Attempts != 3 {
		t.Fatalf("result = %+v, want rows=1 attempts=3", result)
	}
}

func TestExecutorRetriesEOFStartupErrors(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	var attempts int
	driver := &fakeDriver{
		executeFunc: func(context.Context, QueryRequest) (QueryResult, error) {
			attempts++
			if attempts == 1 {
				return QueryResult{}, errors.New("EOF")
			}
			return QueryResult{Rows: 1}, nil
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
		Retry: RetryConfig{
			MaxAttempts:  2,
			RetryBackoff: time.Millisecond,
			Sleep: func(context.Context, time.Duration) error {
				return nil
			},
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "setup_frozen_views",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"sql":     "CREATE VIEW frozen_v1.events_file_view AS SELECT 1",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestExecutorRetriesConnectionResetStartupErrors(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	var attempts int
	driver := &fakeDriver{
		executeFunc: func(context.Context, QueryRequest) (QueryResult, error) {
			attempts++
			if attempts == 1 {
				return QueryResult{}, errors.New("read tcp 127.0.0.1:5432: read: connection reset by peer")
			}
			return QueryResult{Rows: 1}, nil
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
		Retry: RetryConfig{
			MaxAttempts:  2,
			RetryBackoff: time.Millisecond,
			Sleep: func(context.Context, time.Duration) error {
				return nil
			},
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "setup_frozen_views",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"sql":     "CREATE VIEW frozen_v1.events_file_view AS SELECT 1",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestExecutorRetriesIOTimeoutStartupErrors(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	var attempts int
	driver := &fakeDriver{
		executeFunc: func(context.Context, QueryRequest) (QueryResult, error) {
			attempts++
			if attempts == 1 {
				return QueryResult{}, errors.New("read tcp 127.0.0.1:5432: i/o timeout")
			}
			return QueryResult{Rows: 1}, nil
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
		Retry: RetryConfig{
			MaxAttempts:  2,
			RetryBackoff: time.Millisecond,
			Sleep: func(context.Context, time.Duration) error {
				return nil
			},
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "setup_frozen_views",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"sql":     "CREATE VIEW frozen_v1.events_file_view AS SELECT 1",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestExecutorDoesNotRetryNonTransientSQLErrors(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	var attempts int
	driver := &fakeDriver{
		executeFunc: func(context.Context, QueryRequest) (QueryResult, error) {
			attempts++
			return QueryResult{}, errors.New("syntax error at or near \"SELEC\"")
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
		Retry:  RetryConfig{MaxAttempts: 5},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "bad_sql",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"sql":     "SELEC 1",
		},
	})
	if err == nil {
		t.Fatal("expected non-transient SQL error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.ErrorClass() != ErrorClassSQL {
		t.Fatalf("error = %T %v, want class %q", err, err, ErrorClassSQL)
	}
}

func TestExecutorFailsWhenSQLReturnsFewerThanMinRows(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: &fakeDriver{
			executeFunc: func(context.Context, QueryRequest) (QueryResult, error) {
				return QueryResult{Rows: 0}, nil
			},
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "validate_manifest",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":   "scenario-org",
			"catalog":  "ducklake",
			"sql":      "SELECT 1 WHERE false",
			"min_rows": 1,
		},
	})
	if err == nil {
		t.Fatal("expected min_rows assertion failure")
	}
	if !strings.Contains(err.Error(), "returned 0 rows, want at least 1") {
		t.Fatalf("error = %v, want min_rows message", err)
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.ErrorClass() != ErrorClassSQL {
		t.Fatalf("error = %T %v, want class %q", err, err, ErrorClassSQL)
	}
}

func TestExecutorRunsInlineSQLCatalog(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	var queries []string
	driver := &fakeDriver{
		executeFunc: func(_ context.Context, req QueryRequest) (QueryResult, error) {
			queries = append(queries, req.SQL)
			return QueryResult{Rows: 1}, nil
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "catalog",
		Type: StepTypeSQLCatalog,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"queries": []any{
				map[string]any{"id": "one", "sql": "SELECT 1"},
				map[string]any{"id": "two", "sql": "SELECT 2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if len(queries) != 2 || queries[0] != "SELECT 1" || queries[1] != "SELECT 2" {
		t.Fatalf("queries = %#v, want SELECT 1 and SELECT 2", queries)
	}
	if _, ok := executor.State().Result("catalog/one"); !ok {
		t.Fatal("expected first catalog query result")
	}
	if _, ok := executor.State().Result("catalog/two"); !ok {
		t.Fatal("expected second catalog query result")
	}
}

func TestExecutorRunsSQLCatalogFile(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	catalogFile := filepath.Join(t.TempDir(), "metadata_catalog.yaml")
	if err := os.WriteFile(catalogFile, []byte(`
name: metadata-smoke
queries:
  - id: schemata
    sql: SELECT schema_name FROM information_schema.schemata
  - id: tables
    catalog: ducklake
    sql: SELECT table_name FROM information_schema.tables
`), 0o644); err != nil {
		t.Fatalf("write catalog file: %v", err)
	}

	var queryIDs []string
	driver := &fakeDriver{
		executeFunc: func(_ context.Context, req QueryRequest) (QueryResult, error) {
			queryIDs = append(queryIDs, req.QueryID)
			return QueryResult{Rows: 1}, nil
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "metadata",
		Type: StepTypeSQLCatalog,
		With: map[string]any{
			"org_id": "scenario-org",
			"file":   catalogFile,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if got := strings.Join(queryIDs, ","); got != "schemata,tables" {
		t.Fatalf("query IDs = %q, want schemata,tables", got)
	}
	if _, ok := executor.State().Result("metadata/schemata"); !ok {
		t.Fatal("expected schemata result")
	}
	if _, ok := executor.State().Result("metadata/tables"); !ok {
		t.Fatal("expected tables result")
	}
}

func TestRepositoryMetadataCatalogFileParses(t *testing.T) {
	step := core.Step{
		ID:   "metadata",
		Type: StepTypeSQLCatalog,
		With: map[string]any{
			"catalog": "ducklake",
			"file":    filepath.Join("metadata_catalog.yaml"),
		},
	}
	specs, err := parseCatalogStep(step)
	if err != nil {
		t.Fatalf("parseCatalogStep returned error: %v", err)
	}
	if len(specs) == 0 {
		t.Fatal("expected repository metadata catalog to contain queries")
	}
}

func TestExecutorTemplatesSQLFileEnvVars(t *testing.T) {
	t.Setenv("DUCKGRES_SCENARIO_FROZEN_S3_URI", "s3://example-frozen/frozen_v1/")
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "root",
		Password: "root-password",
	})
	sqlFile := filepath.Join(t.TempDir(), "setup.sql")
	if err := os.WriteFile(sqlFile, []byte("SELECT '${env:DUCKGRES_SCENARIO_FROZEN_S3_URI}persons/*.parquet'"), 0o644); err != nil {
		t.Fatalf("write SQL file: %v", err)
	}

	var gotSQL string
	driver := &fakeDriver{
		executeFunc: func(_ context.Context, req QueryRequest) (QueryResult, error) {
			gotSQL = req.SQL
			return QueryResult{Rows: 1}, nil
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "setup_frozen",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id": "scenario-org",
			"file":   sqlFile,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	if !strings.Contains(gotSQL, "s3://example-frozen/frozen_v1/persons/*.parquet") {
		t.Fatalf("SQL = %q, want templated frozen S3 URI", gotSQL)
	}
	if strings.Contains(gotSQL, "${env:") {
		t.Fatalf("SQL still contains env template: %q", gotSQL)
	}
}

func TestExecutorUsesProvisionCredentialsInDSN(t *testing.T) {
	provisionState := provision.NewState()
	provisionState.StoreProvisionResponse("scenario-org", provision.ProvisionResponse{
		Org:      "scenario-org",
		Username: "custom-root",
		Password: "root password",
	})
	var gotDSN string
	driver := &fakeDriver{
		executeFunc: func(_ context.Context, req QueryRequest) (QueryResult, error) {
			gotDSN = req.DSN
			return QueryResult{Rows: 1}, nil
		},
	}
	executor := NewExecutor(ExecutorConfig{
		ProvisionState: provisionState,
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: driver,
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "select_one",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"sql":     "SELECT 1",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	for _, want := range []string{"user=custom-root", "password='root password'", "dbname=ducklake"} {
		if !strings.Contains(gotDSN, want) {
			t.Fatalf("DSN %q missing %q", gotDSN, want)
		}
	}
}

func TestExecutorFailsWithoutProvisionState(t *testing.T) {
	executor := NewExecutor(ExecutorConfig{
		Connection: ConnectionConfig{
			HostAddr:  "10.0.0.10",
			SNISuffix: ".dev.example",
			Port:      5432,
			SSLMode:   "require",
		},
		Driver: &fakeDriver{
			executeFunc: func(context.Context, QueryRequest) (QueryResult, error) {
				t.Fatal("driver should not run without provision state")
				return QueryResult{}, nil
			},
		},
	})

	err := executor.ExecuteStep(context.Background(), core.Step{
		ID:   "select_one",
		Type: StepTypeSQL,
		With: map[string]any{
			"org_id":  "scenario-org",
			"catalog": "ducklake",
			"sql":     "SELECT 1",
		},
	})
	if err == nil {
		t.Fatal("expected missing provision state to fail")
	}
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.ErrorClass() != ErrorClassInvalidStepConfig {
		t.Fatalf("error = %T %v, want class %q", err, err, ErrorClassInvalidStepConfig)
	}
}

type fakeDriver struct {
	executeFunc func(context.Context, QueryRequest) (QueryResult, error)
}

func (d *fakeDriver) Execute(ctx context.Context, req QueryRequest) (QueryResult, error) {
	return d.executeFunc(ctx, req)
}
