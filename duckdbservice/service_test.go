package duckdbservice

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

type exitPanic struct {
	code int
}

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
		if searchPath != "main,memory.main" {
			t.Errorf("expected search_path 'main,memory.main', got %q", searchPath)
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
		if searchPath != "myuser,main,memory.main" {
			t.Errorf("expected search_path 'myuser,main,memory.main', got %q", searchPath)
		}
	})
}

func TestCleanupSessionState(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	t.Run("no temp objects returns true", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		if ok := cleanupSessionState(conn); !ok {
			t.Errorf("cleanupSessionState() = false on a clean connection, want true")
		}
	})

	t.Run("drops user-created temp tables and views", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		if _, err := conn.ExecContext(context.Background(), "CREATE TEMP TABLE t1 (x INTEGER)"); err != nil {
			t.Fatalf("create temp table: %v", err)
		}
		if _, err := conn.ExecContext(context.Background(), "CREATE TEMP VIEW v1 AS SELECT 1 AS x"); err != nil {
			t.Fatalf("create temp view: %v", err)
		}

		// cleanupSessionState may also DROP IF EXISTS many DuckDB-managed system
		// views via the temp schema (most are in other schemas, so the DROPs are
		// no-ops). Only assert that the user-created objects are gone.
		_ = cleanupSessionState(conn)

		var n int
		if err := conn.QueryRowContext(context.Background(),
			"SELECT count(*) FROM duckdb_tables() WHERE temporary = true AND table_name = 't1'",
		).Scan(&n); err != nil {
			t.Fatalf("count t1: %v", err)
		}
		if n != 0 {
			t.Errorf("user temp table t1 not dropped (remaining = %d)", n)
		}

		if err := conn.QueryRowContext(context.Background(),
			"SELECT count(*) FROM duckdb_views() WHERE temporary = true AND view_name = 'v1'",
		).Scan(&n); err != nil {
			t.Fatalf("count v1: %v", err)
		}
		if n != 0 {
			t.Errorf("user temp view v1 not dropped (remaining = %d)", n)
		}
	})

	t.Run("rollback clears aborted transaction state", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// Open a txn and leave it dangling — cleanup should ROLLBACK before
		// running its own statements. Without the rollback, the SELECT against
		// duckdb_tables() would surface the dangling txn's aborted state.
		if _, err := conn.ExecContext(context.Background(), "BEGIN TRANSACTION"); err != nil {
			t.Fatalf("begin txn: %v", err)
		}
		if _, err := conn.ExecContext(context.Background(), "CREATE TEMP TABLE leak (x INTEGER)"); err != nil {
			t.Fatalf("create temp inside txn: %v", err)
		}

		if ok := cleanupSessionState(conn); !ok {
			t.Errorf("cleanupSessionState() = false after rollback path, want true")
		}

		// After cleanup we should be out of the txn — a fresh statement should succeed.
		if _, err := conn.ExecContext(context.Background(), "SELECT 1"); err != nil {
			t.Errorf("post-cleanup SELECT failed: %v", err)
		}
	})
}

func TestRunExitsWhenBundledExtensionBootstrapFails(t *testing.T) {
	prevBootstrap := bootstrapBundledExtensions
	prevExit := exitProcess
	defer func() {
		bootstrapBundledExtensions = prevBootstrap
		exitProcess = prevExit
	}()

	bootstrapBundledExtensions = func(string) error {
		return errors.New("boom")
	}

	exitCode := -1
	exitProcess = func(code int) {
		exitCode = code
		panic(exitPanic{code: code})
	}

	defer func() {
		r := recover()
		p, ok := r.(exitPanic)
		if !ok {
			t.Fatalf("expected exit panic, got %v", r)
		}
		if p.code != 1 {
			t.Fatalf("expected exit code 1, got %d", p.code)
		}
		if exitCode != 1 {
			t.Fatalf("expected exitProcess to be called with 1, got %d", exitCode)
		}
	}()

	Run(ServiceConfig{})
	t.Fatal("expected Run to exit")
}
