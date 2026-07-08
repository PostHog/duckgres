package server

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresQueryLogDSN(t *testing.T) {
	tests := []struct {
		name            string
		metadataStore   string
		applicationName string
		want            string
		wantErr         bool
	}{
		{
			name:          "ducklake postgres prefix",
			metadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
			want:          "host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5 default_query_exec_mode=simple_protocol",
		},
		{
			name:            "application name",
			metadataStore:   "postgres:host=metadata.internal dbname=ducklake",
			applicationName: "duckgres/acme",
			want:            "host=metadata.internal dbname=ducklake keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5 application_name='duckgres/acme' default_query_exec_mode=simple_protocol",
		},
		{
			name:          "preserve caller options",
			metadataStore: "postgres:host=metadata.internal dbname=ducklake keepalives=1 application_name=custom default_query_exec_mode=exec",
			want:          "host=metadata.internal dbname=ducklake keepalives=1 application_name=custom default_query_exec_mode=exec",
		},
		{
			name:          "trim whitespace",
			metadataStore: "  postgres:host=metadata.internal dbname=ducklake  ",
			want:          "host=metadata.internal dbname=ducklake keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5 default_query_exec_mode=simple_protocol",
		},
		{
			name:          "empty",
			metadataStore: "",
			wantErr:       true,
		},
		{
			name:          "non postgres",
			metadataStore: "metadata.db",
			wantErr:       true,
		},
		{
			name:          "empty postgres dsn",
			metadataStore: "postgres:",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := postgresQueryLogDSN(DuckLakeConfig{
				MetadataStore:   tt.metadataStore,
				ApplicationName: tt.applicationName,
			})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("postgresQueryLogDSN: %v", err)
			}
			if got != tt.want {
				t.Fatalf("DSN = %q, want %q", got, tt.want)
			}
			db, err := openPostgresQueryLogDB(got)
			if err != nil {
				t.Fatalf("openPostgresQueryLogDB with generated DSN: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
		})
	}
}

func TestOpenPostgresQueryLogDBUsesSingleConnectionPool(t *testing.T) {
	db, err := openPostgresQueryLogDB("host=metadata.internal dbname=ducklake")
	if err != nil {
		t.Fatalf("openPostgresQueryLogDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1", got)
	}
}

func TestPostgresQueryLogPGXConfigStripsUnsupportedRuntimeParams(t *testing.T) {
	cfg, err := postgresQueryLogPGXConfig("host=metadata.internal dbname=ducklake keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5 application_name=custom default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("postgresQueryLogPGXConfig: %v", err)
	}
	for _, param := range postgresQueryLogUnsupportedRuntimeParams {
		if _, ok := cfg.RuntimeParams[param]; ok {
			t.Fatalf("RuntimeParams[%q] should be stripped before pgx startup", param)
		}
	}
	if got := cfg.RuntimeParams["application_name"]; got != "custom" {
		t.Fatalf("application_name runtime param = %q, want custom", got)
	}
}

func TestPostgresQueryLogSchemaSQL(t *testing.T) {
	sql := strings.Join(postgresQueryLogSchemaSQLForTime(time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)), "\n")

	for _, want := range []string{
		"CREATE SCHEMA IF NOT EXISTS querylog",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries",
		"PARTITION BY RANGE (event_time)",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202606 PARTITION OF querylog.query_log_entries FOR VALUES FROM ('2026-06-01') TO ('2026-07-01')",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202607 PARTITION OF querylog.query_log_entries FOR VALUES FROM ('2026-07-01') TO ('2026-08-01')",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202608 PARTITION OF querylog.query_log_entries FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_default PARTITION OF querylog.query_log_entries DEFAULT",
		"idx_query_log_entries_event_time",
		"idx_query_log_entries_user_time",
		"idx_query_log_entries_hash_time",
		"cpu_time_s DOUBLE PRECISION DEFAULT 0",
		"peak_buffer_memory_bytes BIGINT DEFAULT 0",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("schema SQL missing %q:\n%s", want, sql)
		}
	}

	if strings.Contains(sql, "event_id") {
		t.Fatalf("Postgres query-log schema must not include event_id:\n%s", sql)
	}
	if strings.Contains(sql, "(org_id,") {
		t.Fatalf("Postgres query-log indexes should not lead with org_id:\n%s", sql)
	}
	defaultPartition := strings.Index(sql, "querylog.query_log_entries_default")
	firstIndex := strings.Index(sql, "idx_query_log_entries_event_time")
	if defaultPartition < 0 || firstIndex < 0 || defaultPartition > firstIndex {
		t.Fatalf("default partition should be created before parent indexes:\n%s", sql)
	}
}

func TestPostgresQueryLogBatchPartitionStarts(t *testing.T) {
	batch := []QueryLogEntry{
		{EventTime: time.Date(2026, 7, 7, 12, 0, 0, 0, time.FixedZone("offset", -4*60*60))},
		{EventTime: time.Date(2026, 7, 31, 23, 59, 0, 0, time.UTC)},
		{EventTime: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)},
		{EventTime: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)},
	}

	got := postgresQueryLogBatchPartitionStarts(batch)
	want := []time.Time{
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("partition starts = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("partition start %d = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestPostgresQueryLogRepairPartitionSQL(t *testing.T) {
	start := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	create := postgresQueryLogCreateStandaloneMonthPartitionSQL(start)
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202610",
		"LIKE querylog.query_log_entries",
		"INCLUDING IDENTITY",
	} {
		if !strings.Contains(create, want) {
			t.Fatalf("standalone partition SQL missing %q:\n%s", want, create)
		}
	}

	move := postgresQueryLogMoveDefaultRowsSQL(start)
	for _, want := range []string{
		"INSERT INTO querylog.query_log_entries_202610",
		"SELECT " + postgresQueryLogColumns + " FROM querylog.query_log_entries_default",
		"WHERE event_time >= $1 AND event_time < $2",
	} {
		if !strings.Contains(move, want) {
			t.Fatalf("move default rows SQL missing %q:\n%s", want, move)
		}
	}

	deleteRows := postgresQueryLogDeleteDefaultRowsSQL()
	if !strings.Contains(deleteRows, "DELETE FROM querylog.query_log_entries_default WHERE event_time >= $1 AND event_time < $2") {
		t.Fatalf("delete default rows SQL is wrong:\n%s", deleteRows)
	}

	attach := postgresQueryLogAttachMonthPartitionSQL(start)
	for _, want := range []string{
		"ALTER TABLE querylog.query_log_entries ATTACH PARTITION querylog.query_log_entries_202610",
		"FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')",
	} {
		if !strings.Contains(attach, want) {
			t.Fatalf("attach partition SQL missing %q:\n%s", want, attach)
		}
	}
}

func TestPostgresQueryLogCreateMonthPartitionSQLNormalizesMonth(t *testing.T) {
	got := postgresQueryLogCreateMonthPartitionSQL(time.Date(2026, 9, 30, 23, 59, 0, 0, time.FixedZone("offset", -4*60*60)))
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202610",
		"FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("partition SQL missing %q:\n%s", want, got)
		}
	}
}

func TestPostgresQueryLogRepairsDirtyDefaultPartitionIntegration(t *testing.T) {
	ctx := context.Background()
	db := openLocalQueryLogIntegrationDB(t)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS querylog CASCADE") })
	if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS querylog CASCADE"); err != nil {
		t.Fatalf("drop querylog schema: %v", err)
	}
	if err := ensurePostgresQueryLogTableContext(ctx, db); err != nil {
		t.Fatalf("ensure query-log table: %v", err)
	}

	eventTime := time.Date(2099, 10, 15, 12, 0, 0, 0, time.UTC)
	dirtyEntry := QueryLogEntry{
		EventTime:       eventTime,
		QueryDurationMs: 1,
		Type:            "QueryFinish",
		Query:           "SELECT dirty_default",
		UserName:        "postgres",
	}
	if err := insertQueryLogEntries(ctx, db, postgresQueryLogTable, []QueryLogEntry{dirtyEntry}); err != nil {
		t.Fatalf("seed default partition through parent: %v", err)
	}
	assertQueryLogTableOID(t, db, dirtyEntry.Query, "querylog.query_log_entries_default")

	if err := ensurePostgresQueryLogPartitionsForBatchContext(ctx, db, []QueryLogEntry{{EventTime: eventTime}}); err != nil {
		t.Fatalf("repair dirty default partition: %v", err)
	}
	assertQueryLogTableOID(t, db, dirtyEntry.Query, "querylog.query_log_entries_209910")
	assertQueryLogDefaultRowsForMonth(t, db, eventTime, 0)

	nextEntry := dirtyEntry
	nextEntry.Query = "SELECT after_repair"
	if err := insertQueryLogEntries(ctx, db, postgresQueryLogTable, []QueryLogEntry{nextEntry}); err != nil {
		t.Fatalf("insert after partition repair: %v", err)
	}
	assertQueryLogTableOID(t, db, nextEntry.Query, "querylog.query_log_entries_209910")
}

func TestPostgresQueryLogInitRepairsDirtyDefaultPartitionIntegration(t *testing.T) {
	ctx := context.Background()
	db := openLocalQueryLogIntegrationDB(t)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS querylog CASCADE") })
	if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS querylog CASCADE"); err != nil {
		t.Fatalf("drop querylog schema: %v", err)
	}
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS querylog`,
		postgresQueryLogCreateTableSQL(),
		`CREATE TABLE IF NOT EXISTS querylog.query_log_entries_default PARTITION OF querylog.query_log_entries DEFAULT`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("seed pre-repair schema: %v", err)
		}
	}

	eventTime := time.Now().UTC()
	dirtyEntry := QueryLogEntry{
		EventTime:       eventTime,
		QueryDurationMs: 1,
		Type:            "QueryFinish",
		Query:           "SELECT dirty_init_default",
		UserName:        "postgres",
	}
	if err := insertQueryLogEntries(ctx, db, postgresQueryLogTable, []QueryLogEntry{dirtyEntry}); err != nil {
		t.Fatalf("seed default partition through parent: %v", err)
	}
	assertQueryLogTableOID(t, db, dirtyEntry.Query, "querylog.query_log_entries_default")

	if err := ensurePostgresQueryLogTableContext(ctx, db); err != nil {
		t.Fatalf("init should repair dirty default partition: %v", err)
	}
	assertQueryLogTableOID(t, db, dirtyEntry.Query, postgresQueryLogPartitionName(eventTime))
	assertQueryLogDefaultRowsForMonth(t, db, eventTime, 0)
}

func TestPostgresQueryLogAttachesExistingStandalonePartitionIntegration(t *testing.T) {
	ctx := context.Background()
	db := openLocalQueryLogIntegrationDB(t)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS querylog CASCADE") })
	if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS querylog CASCADE"); err != nil {
		t.Fatalf("drop querylog schema: %v", err)
	}
	if err := ensurePostgresQueryLogTableContext(ctx, db); err != nil {
		t.Fatalf("ensure query-log table: %v", err)
	}

	eventTime := time.Date(2099, 11, 15, 12, 0, 0, 0, time.UTC)
	if _, err := db.ExecContext(ctx, postgresQueryLogCreateStandaloneMonthPartitionSQL(eventTime)); err != nil {
		t.Fatalf("seed standalone partition table: %v", err)
	}
	if err := ensurePostgresQueryLogPartitionsForBatchContext(ctx, db, []QueryLogEntry{{EventTime: eventTime}}); err != nil {
		t.Fatalf("attach standalone partition table: %v", err)
	}

	entry := QueryLogEntry{
		EventTime:       eventTime,
		QueryDurationMs: 1,
		Type:            "QueryFinish",
		Query:           "SELECT standalone_attach",
		UserName:        "postgres",
	}
	if err := insertQueryLogEntries(ctx, db, postgresQueryLogTable, []QueryLogEntry{entry}); err != nil {
		t.Fatalf("insert after standalone partition attach: %v", err)
	}
	assertQueryLogTableOID(t, db, entry.Query, "querylog.query_log_entries_209911")
}

func openLocalQueryLogIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DUCKGRES_TEST_QUERYLOG_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DUCKGRES_TEST_QUERYLOG_POSTGRES_DSN not set")
	}
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DUCKGRES_TEST_QUERYLOG_POSTGRES_DSN: %v", err)
	}
	if cfg.Host != "127.0.0.1" && cfg.Host != "localhost" && cfg.Host != "::1" {
		t.Skipf("DUCKGRES_TEST_QUERYLOG_POSTGRES_DSN host %q is not local; test drops schema querylog", cfg.Host)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func assertQueryLogTableOID(t *testing.T, db *sql.DB, query, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow(
		"SELECT tableoid::regclass::text FROM querylog.query_log_entries WHERE query = $1",
		query,
	).Scan(&got); err != nil {
		t.Fatalf("query tableoid for %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("tableoid for %q = %q, want %q", query, got, want)
	}
}

func assertQueryLogDefaultRowsForMonth(t *testing.T, db *sql.DB, eventTime time.Time, want int) {
	t.Helper()
	start := postgresQueryLogMonthStart(eventTime)
	end := start.AddDate(0, 1, 0)
	var got int
	if err := db.QueryRow(
		"SELECT count(*) FROM querylog.query_log_entries_default WHERE event_time >= $1 AND event_time < $2",
		start,
		end,
	).Scan(&got); err != nil {
		t.Fatalf("count default rows: %v", err)
	}
	if got != want {
		t.Fatalf("default rows for %s = %d, want %d", postgresQueryLogPartitionKey(eventTime), got, want)
	}
}
