package server

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	duckLakeMetadataCatalog        = "__ducklake_metadata_ducklake"
	duckLakeQueryLogView           = "ducklake.system.query_log"
	duckLakeLegacyQueryLogTable    = "query_log_ducklake_legacy"
	duckLakeLegacyQueryLogFullName = "ducklake.system." + duckLakeLegacyQueryLogTable
)

func ensureDuckLakeQueryLogSurface(ctx context.Context, db *sql.DB, cfg Config) error {
	if !cfg.QueryLog.Enabled || cfg.DuckLake.MetadataStore == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	connStr, err := postgresQueryLogDSN(cfg.DuckLake)
	if err != nil {
		return err
	}
	pgDB, err := openPostgresQueryLogDB(connStr)
	if err != nil {
		return err
	}
	defer func() { _ = pgDB.Close() }()

	if err := ensurePostgresQueryLogTableContext(ctx, pgDB); err != nil {
		return err
	}
	if err := ensureDuckLakeQueryLogViewContext(ctx, db); err != nil {
		return fmt.Errorf("querylog: ensure ducklake view: %w", err)
	}
	return nil
}

func ensureDuckLakeQueryLogViewContext(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return fmt.Errorf("duckdb db is nil")
	}

	if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS ducklake.system"); err != nil {
		return fmt.Errorf("create ducklake system schema: %w", err)
	}

	viewExists, err := duckLakeQueryLogViewExistsContext(ctx, db)
	if err != nil {
		return err
	}
	if viewExists {
		return nil
	}

	tableExists, err := duckLakeQueryLogTableExistsContext(ctx, db, "query_log")
	if err != nil {
		return err
	}
	if tableExists {
		legacyExists, err := duckLakeQueryLogTableExistsContext(ctx, db, duckLakeLegacyQueryLogTable)
		if err != nil {
			return err
		}
		if legacyExists {
			return fmt.Errorf("%s exists and %s already exists", duckLakeQueryLogView, duckLakeLegacyQueryLogFullName)
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE ducklake.system.query_log RENAME TO "+duckLakeLegacyQueryLogTable); err != nil {
			return fmt.Errorf("rename legacy ducklake query_log table: %w", err)
		}
	}

	if _, err := db.ExecContext(ctx, duckLakeQueryLogViewSQL()); err != nil {
		return fmt.Errorf("create ducklake query_log view: %w", err)
	}
	return nil
}

func duckLakeQueryLogViewExistsContext(ctx context.Context, db *sql.DB) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM duckdb_views()
WHERE database_name = 'ducklake'
  AND schema_name = 'system'
  AND view_name = 'query_log'
`).Scan(&count); err != nil {
		return false, fmt.Errorf("check ducklake query_log view: %w", err)
	}
	return count > 0, nil
}

func duckLakeQueryLogTableExistsContext(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM duckdb_tables()
WHERE database_name = 'ducklake'
  AND schema_name = 'system'
  AND table_name = $1
`, table).Scan(&count); err != nil {
		return false, fmt.Errorf("check ducklake query_log table %q: %w", table, err)
	}
	return count > 0, nil
}

func duckLakeQueryLogViewSQL() string {
	return fmt.Sprintf(`CREATE VIEW IF NOT EXISTS ducklake.system.query_log AS
SELECT
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
FROM "%s".querylog.query_log_entries`, duckLakeMetadataCatalog)
}
