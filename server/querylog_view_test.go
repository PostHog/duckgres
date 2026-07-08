package server

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestEnsureDuckLakeQueryLogViewContextCreatesView(t *testing.T) {
	db := openQueryLogViewTestDB(t)

	if err := ensureDuckLakeQueryLogViewContext(context.Background(), db); err != nil {
		t.Fatalf("ensure query log view: %v", err)
	}
	if err := ensureDuckLakeQueryLogViewContext(context.Background(), db); err != nil {
		t.Fatalf("ensure query log view second time: %v", err)
	}

	var query, userName string
	var cpuTimeS float64
	var peakBufferMemoryBytes int64
	err := db.QueryRow(`
SELECT query, user_name, cpu_time_s, peak_buffer_memory_bytes
FROM ducklake.system.query_log
`).Scan(&query, &userName, &cpuTimeS, &peakBufferMemoryBytes)
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	if query != "SELECT 1" || userName != "alice" || cpuTimeS != 1.25 || peakBufferMemoryBytes != 4096 {
		t.Fatalf("unexpected view row: query=%q user=%q cpu=%v peak=%d", query, userName, cpuTimeS, peakBufferMemoryBytes)
	}

	viewExists, err := duckLakeQueryLogViewExistsContext(context.Background(), db)
	if err != nil {
		t.Fatalf("check view exists: %v", err)
	}
	if !viewExists {
		t.Fatal("expected ducklake.system.query_log view to exist")
	}
}

func TestEnsureDuckLakeQueryLogSurfaceFastPathSkipsPostgresDSN(t *testing.T) {
	resetQueryLogSurfaceCacheForTest()
	t.Cleanup(resetQueryLogSurfaceCacheForTest)
	db := openQueryLogViewTestDB(t)

	if err := ensureDuckLakeQueryLogViewContext(context.Background(), db); err != nil {
		t.Fatalf("ensure query log view: %v", err)
	}

	err := ensureDuckLakeQueryLogSurface(context.Background(), db, Config{
		DuckLake: DuckLakeConfig{
			MetadataStore: "not-a-postgres-metadata-store",
		},
		QueryLog: QueryLogConfig{
			Enabled: true,
		},
	})
	if err != nil {
		t.Fatalf("existing view should skip native Postgres setup: %v", err)
	}
}

func TestEnsureDuckLakeQueryLogViewContextRenamesLegacyTable(t *testing.T) {
	db := openQueryLogViewTestDB(t)

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ducklake.system`); err != nil {
		t.Fatalf("create ducklake system schema: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ducklake.system.query_log (event_time TIMESTAMP, query VARCHAR)`); err != nil {
		t.Fatalf("create legacy query_log table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO ducklake.system.query_log VALUES (TIMESTAMP '2026-07-01 00:00:00', 'legacy row')`); err != nil {
		t.Fatalf("insert legacy query_log row: %v", err)
	}

	if err := ensureDuckLakeQueryLogViewContext(context.Background(), db); err != nil {
		t.Fatalf("ensure query log view: %v", err)
	}

	legacyExists, err := duckLakeQueryLogTableExistsContext(context.Background(), db, duckLakeLegacyQueryLogTable)
	if err != nil {
		t.Fatalf("check legacy table exists: %v", err)
	}
	if !legacyExists {
		t.Fatalf("expected legacy table %s to exist", duckLakeLegacyQueryLogFullName)
	}

	var legacyQuery string
	if err := db.QueryRow(`SELECT query FROM ducklake.system.query_log_ducklake_legacy`).Scan(&legacyQuery); err != nil {
		t.Fatalf("query legacy table: %v", err)
	}
	if legacyQuery != "legacy row" {
		t.Fatalf("legacy row mismatch: got %q", legacyQuery)
	}

	viewExists, err := duckLakeQueryLogViewExistsContext(context.Background(), db)
	if err != nil {
		t.Fatalf("check view exists: %v", err)
	}
	if !viewExists {
		t.Fatal("expected ducklake.system.query_log view to exist")
	}
}

func TestEnsureDuckLakeQueryLogViewContextPreflightsBeforeRenamingLegacyTable(t *testing.T) {
	db := openQueryLogViewTestDBWithoutHiddenSource(t)

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ducklake.system`); err != nil {
		t.Fatalf("create ducklake system schema: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ducklake.system.query_log (event_time TIMESTAMP, query VARCHAR)`); err != nil {
		t.Fatalf("create legacy query_log table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO ducklake.system.query_log VALUES (TIMESTAMP '2026-07-01 00:00:00', 'legacy row')`); err != nil {
		t.Fatalf("insert legacy query_log row: %v", err)
	}

	err := ensureDuckLakeQueryLogViewContext(context.Background(), db)
	if err == nil {
		t.Fatal("expected hidden-source preflight error")
	}
	if !strings.Contains(err.Error(), "preflight ducklake query_log source") {
		t.Fatalf("expected preflight error, got %v", err)
	}

	var legacyQuery string
	if err := db.QueryRow(`SELECT query FROM ducklake.system.query_log`).Scan(&legacyQuery); err != nil {
		t.Fatalf("legacy query_log table should remain in place: %v", err)
	}
	if legacyQuery != "legacy row" {
		t.Fatalf("legacy row mismatch: got %q", legacyQuery)
	}
	legacyBackupExists, err := duckLakeQueryLogTableExistsContext(context.Background(), db, duckLakeLegacyQueryLogTable)
	if err != nil {
		t.Fatalf("check legacy backup table exists: %v", err)
	}
	if legacyBackupExists {
		t.Fatalf("legacy backup table should not be created before source preflight succeeds")
	}
}

func TestEnsureDuckLakeQueryLogViewContextErrorsWhenLegacyNameExists(t *testing.T) {
	db := openQueryLogViewTestDB(t)

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ducklake.system`); err != nil {
		t.Fatalf("create ducklake system schema: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ducklake.system.query_log (event_time TIMESTAMP, query VARCHAR)`); err != nil {
		t.Fatalf("create legacy query_log table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ducklake.system.query_log_ducklake_legacy (event_time TIMESTAMP, query VARCHAR)`); err != nil {
		t.Fatalf("create conflicting legacy query_log table: %v", err)
	}

	if err := ensureDuckLakeQueryLogViewContext(context.Background(), db); err == nil {
		t.Fatal("expected legacy-name conflict error")
	}
}

func openQueryLogViewTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := openQueryLogViewTestDBWithoutHiddenSource(t)

	if _, err := db.Exec(`CREATE SCHEMA "__ducklake_metadata_ducklake".querylog`); err != nil {
		t.Fatalf("create hidden querylog schema: %v", err)
	}
	if _, err := db.Exec(queryLogViewHiddenTableTestSQL()); err != nil {
		t.Fatalf("create hidden querylog table: %v", err)
	}
	if _, err := db.Exec(queryLogViewHiddenRowTestSQL()); err != nil {
		t.Fatalf("insert hidden querylog row: %v", err)
	}

	return db
}

func openQueryLogViewTestDBWithoutHiddenSource(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`ATTACH ':memory:' AS ducklake`); err != nil {
		t.Fatalf("attach ducklake catalog: %v", err)
	}
	if _, err := db.Exec(`ATTACH ':memory:' AS __ducklake_metadata_ducklake`); err != nil {
		t.Fatalf("attach hidden metadata catalog: %v", err)
	}

	return db
}

func queryLogViewHiddenTableTestSQL() string {
	return `CREATE TABLE "__ducklake_metadata_ducklake".querylog.query_log_entries (
	id BIGINT,
	event_time TIMESTAMPTZ,
	query_duration_ms BIGINT,
	type TEXT,
	query TEXT,
	transpiled_query TEXT,
	query_kind TEXT,
	normalized_query_hash BIGINT,
	result_rows BIGINT,
	written_rows BIGINT,
	exception_code TEXT,
	exception TEXT,
	user_name TEXT,
	org_id TEXT,
	current_database TEXT,
	client_address TEXT,
	client_port INTEGER,
	application_name TEXT,
	pid INTEGER,
	worker_id INTEGER,
	is_transpiled BOOLEAN,
	protocol TEXT,
	trace_id TEXT,
	span_id TEXT,
	postgres_scan_ms BIGINT,
	cpu_time_s DOUBLE,
	peak_buffer_memory_bytes BIGINT
)`
}

func queryLogViewHiddenRowTestSQL() string {
	return `INSERT INTO "__ducklake_metadata_ducklake".querylog.query_log_entries (
	id,
	event_time,
	query_duration_ms,
	type,
	query,
	transpiled_query,
	query_kind,
	normalized_query_hash,
	result_rows,
	written_rows,
	exception_code,
	exception,
	user_name,
	org_id,
	current_database,
	client_address,
	client_port,
	application_name,
	pid,
	worker_id,
	is_transpiled,
	protocol,
	trace_id,
	span_id,
	postgres_scan_ms,
	cpu_time_s,
	peak_buffer_memory_bytes
) VALUES (
	1,
	TIMESTAMPTZ '2026-07-01 00:00:00+00',
	12,
	'Select',
	'SELECT 1',
	NULL,
	'Select',
	42,
	1,
	0,
	NULL,
	NULL,
	'alice',
	'org_1',
	'ducklake',
	'127.0.0.1',
	5432,
	'psql',
	123,
	7,
	true,
	'pgwire',
	'trace-1',
	'span-1',
	3,
	1.25,
	4096
)`
}
