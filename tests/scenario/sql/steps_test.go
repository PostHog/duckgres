package sql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/scenario/core"
	"github.com/posthog/duckgres/tests/scenario/provision"
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
			"catalog": "iceberg",
			"sql":     "SELECT 1",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStep returned error: %v", err)
	}
	for _, want := range []string{"user=custom-root", "password='root password'", "dbname=iceberg"} {
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
