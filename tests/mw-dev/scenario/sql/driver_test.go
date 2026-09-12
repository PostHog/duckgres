package sql

import (
	"context"
	stdsql "database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/tests/mw-dev/scenario/core"
)

func TestExecOnlyPropagatesLateErrorWithoutReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setup.duckdb")
	db, err := stdsql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE attempts (n INTEGER)"); err != nil {
		t.Fatal(err)
	}
	d := &DatabaseDriver{openDB: func(PGWireConnection) (*stdsql.DB, error) { return db, nil }}
	_, err = d.Execute(context.Background(), QueryRequest{ExecOnly: true, SQL: "INSERT INTO attempts VALUES (1); SELECT 1; SELECT error('late validation failure');"})
	if err == nil || !strings.Contains(err.Error(), "late validation failure") {
		t.Fatalf("late error swallowed: %v", err)
	}
	verify, err := stdsql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = verify.Close() })
	var attempts int
	if err := verify.QueryRow("SELECT count(*) FROM attempts").Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("script replayed: attempts=%d", attempts)
	}
}

func TestSQLStepPassesExecOnlyExplicitly(t *testing.T) {
	for _, execOnly := range []bool{false, true} {
		var captured QueryRequest
		e := NewExecutor(ExecutorConfig{Connection: ConnectionConfig{DialHost: "127.0.0.1", SNISuffix: ".example", SSLMode: "disable"}, Driver: &fakeDriver{executeFunc: func(_ context.Context, q QueryRequest) (QueryResult, error) { captured = q; return QueryResult{}, nil }}})
		step := core.Step{ID: "setup", Type: StepTypeSQL, With: map[string]any{"org_id": "example-org", "password": "example", "sql": "SELECT 1", "exec_only": execOnly}}
		if err := e.ExecuteStep(context.Background(), step); err != nil {
			t.Fatal(err)
		}
		if captured.ExecOnly != execOnly {
			t.Fatalf("ExecOnly=%v want %v", captured.ExecOnly, execOnly)
		}
	}
}
