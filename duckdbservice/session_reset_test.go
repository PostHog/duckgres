package duckdbservice

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server"
)

func newTestPool(t *testing.T) (*SessionPool, *sql.DB) {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		duckLakeSem: make(chan struct{}, 1),
		cfg: server.Config{
			DataDir: t.TempDir(),
		},
		stopCh:     make(chan struct{}),
		warmupDone: make(chan struct{}),
		warmupDB:   db,
	}
	close(pool.warmupDone)
	return pool, db
}

func TestResetSessionState_ClearsSetVariables(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	// Simulate a session setting a variable.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET default_null_order = 'NULLS_FIRST'"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	// Verify the setting persists on the shared connection.
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var val string
	if err := conn2.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = 'default_null_order'").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(val), "NULLS_FIRST") {
		t.Fatalf("expected NULLS_FIRST, got %s", val)
	}
	_ = conn2.Close()

	// Reset session state.
	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	// Verify the setting was reset.
	conn3, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()

	if err := conn3.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = 'default_null_order'").Scan(&val); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToUpper(val), "NULLS_FIRST") {
		t.Fatalf("setting should have been reset, got %s", val)
	}
}

func TestResetSessionState_ClearsTempTables(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TEMP TABLE tmp_test (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	// Verify temp table is gone.
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'temp' AND table_name = 'tmp_test'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("temp table should have been dropped")
	}
}

func TestResetSessionState_ClearsUserTables(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE user_data (id INTEGER, name VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'memory' AND schema_name = 'main' AND table_name = 'user_data'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("user table should have been dropped")
	}
}

func TestResetSessionState_PreservesSystemTable(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	// Create the system table that warmup would create.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS __duckgres_column_metadata (
		table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR,
		character_maximum_length INTEGER, PRIMARY KEY (table_schema, table_name, column_name)
	)`); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = '__duckgres_column_metadata'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("system table __duckgres_column_metadata should be preserved")
	}
}

func TestResetSessionState_ClearsUserViews(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE VIEW user_view AS SELECT 1 AS x"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_views() WHERE database_name = 'memory' AND schema_name = 'main' AND view_name = 'user_view'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("user view should have been dropped")
	}
}

func TestResetSessionState_PreservesSystemViews(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	// Create a system view (simulating warmup).
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE VIEW pg_database AS SELECT 1 AS oid, 'postgres' AS datname"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_views() WHERE database_name = 'memory' AND schema_name = 'main' AND view_name = 'pg_database'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("system view pg_database should be preserved")
	}
}

func TestResetSessionState_ClearsUserMacros(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE MACRO my_custom_macro(x) AS x * 2"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM duckdb_functions() WHERE database_name = 'memory' AND schema_name = 'main' AND function_name = 'my_custom_macro'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("user macro should have been dropped")
	}
}

func TestResetSessionState_PreservesSystemMacros(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	// Create a system macro (simulating warmup).
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE MACRO pg_backend_pid() AS 0"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM duckdb_functions() WHERE database_name = 'memory' AND schema_name = 'main' AND function_name = 'pg_backend_pid'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("system macro pg_backend_pid should be preserved")
	}
}

func TestResetSessionState_ClearsUserSchemas(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE SCHEMA user_schema"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE user_schema.data (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_schemas() WHERE database_name = 'memory' AND schema_name = 'user_schema'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("user schema should have been dropped")
	}
}

func TestResetSessionState_DetachesUserDatabases(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "ATTACH ':memory:' AS user_db"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'user_db'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("user database should have been detached")
	}
}

func TestResetSessionState_ClearsUserSequences(t *testing.T) {
	pool, db := newTestPool(t)
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE SEQUENCE user_seq START 1"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var count int
	err = conn2.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_sequences() WHERE database_name = 'memory' AND schema_name = 'main' AND sequence_name = 'user_seq'").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("user sequence should have been dropped")
	}
}

func TestResetSessionState_ReappliesSettings(t *testing.T) {
	pool, db := newTestPool(t)
	pool.cfg.Threads = 4
	pool.cfg.MemoryLimit = "512MB"
	ctx := context.Background()

	// Change settings to non-default values.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET threads = 1"); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if err := pool.resetSessionState(db); err != nil {
		t.Fatal(err)
	}

	// Verify warmup settings were re-applied.
	conn2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	var threads string
	if err := conn2.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = 'threads'").Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if threads != "4" {
		t.Fatalf("expected threads=4 after reset, got %s", threads)
	}

	var memLimit string
	if err := conn2.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = 'memory_limit'").Scan(&memLimit); err != nil {
		t.Fatal(err)
	}
	// 512MB (base 10) = 488.2 MiB (base 2)
	if !strings.Contains(memLimit, "488") {
		t.Fatalf("expected memory_limit ~488 MiB (512MB) after reset, got %s", memLimit)
	}
}
