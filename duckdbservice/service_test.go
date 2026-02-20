package duckdbservice

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestInitSearchPath(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("fallback when user schema does not exist", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// "nonexistent_user" is not a schema — should fall back to 'main' without error
		initSearchPath(conn, "nonexistent_user")

		var searchPath string
		if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('search_path')").Scan(&searchPath); err != nil {
			t.Fatalf("failed to query search_path: %v", err)
		}
		if searchPath != "main" {
			t.Errorf("expected search_path 'main', got %q", searchPath)
		}
	})

	t.Run("includes user schema when it exists", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// Create a schema matching the username
		if _, err := conn.ExecContext(context.Background(), "CREATE SCHEMA myuser"); err != nil {
			t.Fatalf("failed to create schema: %v", err)
		}

		initSearchPath(conn, "myuser")

		var searchPath string
		if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('search_path')").Scan(&searchPath); err != nil {
			t.Fatalf("failed to query search_path: %v", err)
		}
		if searchPath != "myuser,main" {
			t.Errorf("expected search_path 'myuser,main', got %q", searchPath)
		}
	})
}
