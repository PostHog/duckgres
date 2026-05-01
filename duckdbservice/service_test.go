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

func TestDestroySessionClusterModeDiscardsConn(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDB:       db,
		sharedWarmMode: true,
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}

	// Create a temp macro on this conn — current cleanupSessionState does NOT
	// drop temp macros (only tables/views), so without conn discard a macro
	// would leak via the pool to the next session that reuses the conn.
	if _, err := conn.ExecContext(context.Background(),
		"CREATE OR REPLACE TEMP MACRO leak_check() AS 'leaked'",
	); err != nil {
		t.Fatalf("create temp macro: %v", err)
	}

	const token = "tok-cluster"
	pool.sessions[token] = &Session{
		ID:       token,
		DB:       db,
		Conn:     conn,
		Username: "test_user",
	}

	if err := pool.DestroySession(token); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}

	// Open a fresh conn from the same pool and verify the macro is gone.
	// In cluster mode the prior conn was evicted via evictConnFromPool, so
	// this fresh conn opens a new DuckDB connection that never had the
	// macro defined.
	fresh, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire fresh conn: %v", err)
	}
	defer func() { _ = fresh.Close() }()

	var v string
	err = fresh.QueryRowContext(context.Background(), "SELECT leak_check()").Scan(&v)
	if err == nil {
		t.Errorf("temp macro leaked across sessions: got value %q, expected error", v)
	}
}

func TestDestroySessionStandaloneModeRunsCleanup(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		warmupDB:       db,
		sharedWarmMode: false, // standalone mode — cleanup path runs
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}

	// Create a user-level temp table — this is what the standalone cleanup
	// path is responsible for scrubbing before returning the conn to the pool.
	if _, err := conn.ExecContext(context.Background(),
		"CREATE TEMP TABLE standalone_leak (x INTEGER)",
	); err != nil {
		t.Fatalf("create temp table: %v", err)
	}

	const token = "tok-standalone"
	pool.sessions[token] = &Session{
		ID:       token,
		DB:       db,
		Conn:     conn,
		Username: "test_user",
	}

	if err := pool.DestroySession(token); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}

	// In standalone mode the cleanup ran and dropped the temp table,
	// then the conn was returned to the pool (since cleanup succeeded).
	// A fresh conn shouldn't see standalone_leak regardless of which
	// driver conn it gets.
	fresh, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire fresh conn: %v", err)
	}
	defer func() { _ = fresh.Close() }()

	var n int
	if err := fresh.QueryRowContext(context.Background(),
		"SELECT count(*) FROM duckdb_tables() WHERE table_name = 'standalone_leak'",
	).Scan(&n); err != nil {
		t.Fatalf("count standalone_leak: %v", err)
	}
	if n != 0 {
		t.Errorf("standalone_leak survived cleanup: count=%d", n)
	}
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
