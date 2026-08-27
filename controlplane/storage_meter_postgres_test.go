package controlplane

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestQueryQueryLogHotStatsIntegration(t *testing.T) {
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

	ctx := context.Background()
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS querylog CASCADE") })
	if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS querylog CASCADE"); err != nil {
		t.Fatalf("drop querylog schema: %v", err)
	}

	hotBytes, hotRows, err := queryQueryLogHotStats(ctx, dsn)
	if err != nil {
		t.Fatalf("query absent query log: %v", err)
	}
	if hotBytes != 0 || hotRows != 0 {
		t.Fatalf("absent query log = (%d bytes, %v rows), want zeros", hotBytes, hotRows)
	}

	const fixtureSQL = `
CREATE SCHEMA querylog;
CREATE TABLE querylog.query_log_entries (id integer) PARTITION BY RANGE (id);
CREATE TABLE querylog.query_log_entries_first
    PARTITION OF querylog.query_log_entries FOR VALUES FROM (0) TO (100);
CREATE TABLE querylog.query_log_entries_unanalyzed
    PARTITION OF querylog.query_log_entries FOR VALUES FROM (100) TO (200);
CREATE TABLE querylog.query_log_entries_default
    PARTITION OF querylog.query_log_entries DEFAULT;
INSERT INTO querylog.query_log_entries SELECT generate_series(1, 12);
INSERT INTO querylog.query_log_entries SELECT generate_series(1000, 1006);
ANALYZE querylog.query_log_entries_first;
ANALYZE querylog.query_log_entries_default;`
	if _, err := db.ExecContext(ctx, fixtureSQL); err != nil {
		t.Fatalf("create query-log fixture: %v", err)
	}

	var unanalyzedRows float64
	if err := db.QueryRowContext(ctx, `
SELECT reltuples::DOUBLE PRECISION
FROM pg_class
WHERE oid = 'querylog.query_log_entries_unanalyzed'::regclass`).Scan(&unanalyzedRows); err != nil {
		t.Fatalf("read unanalyzed partition estimate: %v", err)
	}
	if unanalyzedRows >= 0 {
		t.Fatalf("fresh partition reltuples = %v, want negative to exercise clamp", unanalyzedRows)
	}

	hotBytes, hotRows, err = queryQueryLogHotStats(ctx, dsn)
	if err != nil {
		t.Fatalf("query partitioned query log: %v", err)
	}
	if hotBytes <= 0 {
		t.Fatalf("hot bytes = %d, want positive", hotBytes)
	}
	if hotRows != 19 {
		t.Fatalf("hot rows = %v, want 19 (negative estimate contributes zero)", hotRows)
	}
}
