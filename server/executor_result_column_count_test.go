package server

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server/sqlcore"
)

func TestExecutorResultColumnCountPreparesWithoutExecuting(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		new  func(t *testing.T, db *sql.DB) (sqlcore.ResultColumnCounter, func())
	}{
		{
			name: "local",
			new: func(t *testing.T, db *sql.DB) (sqlcore.ResultColumnCounter, func()) {
				return NewLocalExecutor(db), func() {}
			},
		},
		{
			name: "pinned",
			new: func(t *testing.T, db *sql.DB) (sqlcore.ResultColumnCounter, func()) {
				t.Helper()
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatalf("open pinned connection: %v", err)
				}
				return NewPinnedExecutor(conn, db), func() { _ = conn.Close() }
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", ":memory:")
			if err != nil {
				t.Fatalf("open DuckDB: %v", err)
			}
			defer func() { _ = db.Close() }()
			// An in-memory DuckDB database is connection-local. Keeping one
			// connection also verifies LocalExecutor pins the helper's three SQL
			// commands to a common session.
			db.SetMaxOpenConns(1)
			if _, err := db.ExecContext(ctx, "CREATE TABLE result_count_target (id INTEGER, name VARCHAR)"); err != nil {
				t.Fatalf("create target: %v", err)
			}

			executor, closeExecutor := tt.new(t, db)
			count, err := executor.ResultColumnCount(ctx, " INSERT INTO result_count_target VALUES (?, ?) RETURNING *; ")
			if err != nil {
				t.Fatalf("ResultColumnCount: %v", err)
			}
			if count != 2 {
				t.Fatalf("result column count = %d, want 2", count)
			}
			closeExecutor()

			var rows int
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM result_count_target").Scan(&rows); err != nil {
				t.Fatalf("count target rows: %v", err)
			}
			if rows != 0 {
				t.Fatalf("metadata prepare executed INSERT: rows = %d, want 0", rows)
			}
			var prepared int
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_prepared_statements() WHERE name LIKE '__duckgres_result_columns_%'").Scan(&prepared); err != nil {
				t.Fatalf("count helper prepared statements: %v", err)
			}
			if prepared != 0 {
				t.Fatalf("helper leaked %d prepared statements", prepared)
			}
		})
	}
}
