package server

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

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

// TestEnsureDuckLakeQueryLogViewContextReplacesDriftedView covers the upgrade
// path for an existing tenant: the view was created by an older build and is
// missing a column the registry has since gained. CREATE VIEW IF NOT EXISTS is
// a no-op against it, so without replace-on-drift the new column would never
// become visible in DuckLake.
func TestEnsureDuckLakeQueryLogViewContextReplacesDriftedView(t *testing.T) {
	db := openQueryLogViewTestDB(t)

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ducklake.system`); err != nil {
		t.Fatalf("create ducklake system schema: %v", err)
	}
	staleView := `CREATE VIEW ducklake.system.query_log AS SELECT event_time, query, user_name FROM ` + queryLogViewHiddenTable
	if _, err := db.Exec(staleView); err != nil {
		t.Fatalf("create stale view: %v", err)
	}

	ready, err := duckLakeQueryLogViewReadyContext(context.Background(), db)
	if err != nil {
		t.Fatalf("check drifted view readiness: %v", err)
	}
	if ready {
		t.Fatal("a view missing registry columns must not report ready")
	}

	if err := ensureDuckLakeQueryLogViewContext(context.Background(), db); err != nil {
		t.Fatalf("ensure query log view: %v", err)
	}

	// Every registry column must now be selectable, and the pre-existing row
	// must still be readable through the replaced view.
	var queryID, query string
	if err := db.QueryRow(`SELECT query_id, query FROM ducklake.system.query_log`).Scan(&queryID, &query); err != nil {
		t.Fatalf("query replaced view: %v", err)
	}
	if query != "SELECT 1" || queryID != queryLogViewTestEntry().QueryID {
		t.Fatalf("unexpected replaced-view row: query=%q query_id=%q", query, queryID)
	}

	ready, err = duckLakeQueryLogViewReadyContext(context.Background(), db)
	if err != nil {
		t.Fatalf("check replaced view readiness: %v", err)
	}
	if !ready {
		t.Fatal("replaced view should report ready")
	}
}

// TestEnsureDuckLakeQueryLogViewContextKeepsDriftedViewWhenSourceLags guards the
// deploy ordering: if the view has drifted but the source table has not been
// migrated yet, replacing would swap a working view for a broken one.
func TestEnsureDuckLakeQueryLogViewContextKeepsDriftedViewWhenSourceLags(t *testing.T) {
	db := openQueryLogViewTestDBWithoutHiddenSource(t)

	if _, err := db.Exec(`CREATE SCHEMA "__ducklake_metadata_ducklake".querylog`); err != nil {
		t.Fatalf("create hidden querylog schema: %v", err)
	}
	// Source table predates the newest registry column.
	if _, err := db.Exec(`CREATE TABLE ` + queryLogViewHiddenTable + ` (event_time TIMESTAMPTZ, query VARCHAR, user_name VARCHAR)`); err != nil {
		t.Fatalf("create lagging hidden table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO ` + queryLogViewHiddenTable + ` VALUES (TIMESTAMPTZ '2026-07-01 00:00:00+00', 'SELECT 1', 'alice')`); err != nil {
		t.Fatalf("seed lagging hidden table: %v", err)
	}
	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS ducklake.system`); err != nil {
		t.Fatalf("create ducklake system schema: %v", err)
	}
	if _, err := db.Exec(`CREATE VIEW ducklake.system.query_log AS SELECT event_time, query, user_name FROM ` + queryLogViewHiddenTable); err != nil {
		t.Fatalf("create stale view: %v", err)
	}

	if err := ensureDuckLakeQueryLogViewContext(context.Background(), db); err == nil {
		t.Fatal("expected preflight failure against the lagging source")
	}

	var query string
	if err := db.QueryRow(`SELECT query FROM ducklake.system.query_log`).Scan(&query); err != nil {
		t.Fatalf("existing view must survive a failed replace: %v", err)
	}
	if query != "SELECT 1" {
		t.Fatalf("unexpected row from preserved view: %q", query)
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

func TestEnsureDuckLakeQueryLogSurfaceDoesNotCreateStorageAfterAttach(t *testing.T) {
	resetQueryLogSurfaceCacheForTest()
	t.Cleanup(resetQueryLogSurfaceCacheForTest)
	db := openQueryLogViewTestDBWithoutHiddenSource(t)

	err := ensureDuckLakeQueryLogSurface(context.Background(), db, Config{
		DuckLake: DuckLakeConfig{
			MetadataStore: "not-a-postgres-metadata-store",
		},
		QueryLog: QueryLogConfig{
			Enabled: true,
		},
	})
	if err == nil {
		t.Fatal("expected hidden-source preflight error")
	}
	if !strings.Contains(err.Error(), "preflight ducklake query_log source") {
		t.Fatalf("expected post-attach view preflight error, got %v", err)
	}
}

func TestEnsurePostgresQueryLogStorageForDuckLakeAttachRetriesAfterFailure(t *testing.T) {
	resetQueryLogSurfaceCacheForTest()
	t.Cleanup(resetQueryLogSurfaceCacheForTest)

	cfg := Config{
		DuckLake: DuckLakeConfig{
			MetadataStore: "not-a-postgres-metadata-store",
		},
		QueryLog: QueryLogConfig{
			Enabled: true,
		},
	}

	err := ensurePostgresQueryLogStorageForDuckLakeAttach(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected first invalid metadata store failure")
	}
	err = ensurePostgresQueryLogStorageForDuckLakeAttach(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected second invalid metadata store failure; pre-attach storage failures must not be cached")
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
	insertQueryLogFixtureRow(t, db, queryLogViewHiddenTable, queryLogViewTestEntry())

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

const queryLogViewHiddenTable = `"__ducklake_metadata_ducklake".querylog.query_log_entries`

// queryLogViewHiddenTableTestSQL mirrors the tenant Postgres table in DuckDB,
// generated from the column registry so a new column needs no fixture edit.
func queryLogViewHiddenTableTestSQL() string {
	return duckDBQueryLogTableSQL(queryLogViewHiddenTable, true)
}

// queryLogViewTestEntry is the single row the view tests read back.
func queryLogViewTestEntry() QueryLogEntry {
	return QueryLogEntry{
		EventTime:             time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		QueryDurationMs:       12,
		Type:                  "QueryFinish",
		Query:                 "SELECT 1",
		QueryKind:             "Select",
		NormalizedHash:        42,
		ResultRows:            1,
		UserName:              "alice",
		OrgID:                 "org_1",
		CurrentDatabase:       "ducklake",
		ClientAddress:         "127.0.0.1",
		ClientPort:            5432,
		ApplicationName:       "psql",
		PID:                   123,
		WorkerID:              7,
		IsTranspiled:          true,
		Protocol:              "pgwire",
		TraceID:               "trace-1",
		SpanID:                "span-1",
		PostgresScanMs:        3,
		CPUTimeSeconds:        1.25,
		PeakBufferMemoryBytes: 4096,
		QueryID:               "0192f0aa-0000-7000-8000-000000000001",
	}
}
