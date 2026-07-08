package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/posthog/duckgres/server/ducklake"
)

const (
	postgresQueryLogSchema           = "querylog"
	postgresQueryLogParent           = "query_log_entries"
	postgresQueryLogTable            = postgresQueryLogSchema + "." + postgresQueryLogParent
	postgresQueryLogDefaultPartition = postgresQueryLogSchema + ".query_log_entries_default"
	postgresQueryLogColumns          = "id, event_time, query_duration_ms, type, query, transpiled_query, query_kind, normalized_query_hash, result_rows, written_rows, exception_code, exception, user_name, org_id, current_database, client_address, client_port, application_name, pid, worker_id, is_transpiled, protocol, trace_id, span_id, postgres_scan_ms, cpu_time_s, peak_buffer_memory_bytes"
)

var postgresQueryLogUnsupportedRuntimeParams = []string{
	"keepalives",
	"keepalives_idle",
	"keepalives_interval",
	"keepalives_count",
}

// NewPostgresQueryLoggerContext creates a worker-local query-log sink backed by
// the tenant metadata Postgres database. The caller owns query-log routing; this
// sink only owns a small native pgx pool and batched append-only inserts.
func NewPostgresQueryLoggerContext(ctx context.Context, dlCfg DuckLakeConfig, cfg QueryLogConfig) (*QueryLogger, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !cfg.Enabled {
		return nil, nil
	}
	connStr, err := postgresQueryLogDSN(dlCfg)
	if err != nil {
		return nil, err
	}
	db, err := openPostgresQueryLogDB(connStr)
	if err != nil {
		return nil, err
	}

	if err := ensurePostgresQueryLogTableContext(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	loggerCtx, cancel := context.WithCancel(context.Background())
	ql := &QueryLogger{
		db:     db,
		cfg:    cfg,
		table:  postgresQueryLogTable,
		ch:     make(chan QueryLogEntry, queryLogChannelSize),
		done:   make(chan struct{}),
		ctx:    loggerCtx,
		cancel: cancel,
		prepareBatch: func(ctx context.Context, db *sql.DB, batch []QueryLogEntry) error {
			return ensurePostgresQueryLogPartitionsForBatchContext(ctx, db, batch)
		},
		closeDB: true,
	}
	go ql.flushLoop()
	return ql, nil
}

func openPostgresQueryLogDB(connStr string) (*sql.DB, error) {
	config, err := postgresQueryLogPGXConfig(connStr)
	if err != nil {
		return nil, err
	}
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func postgresQueryLogPGXConfig(connStr string) (*pgx.ConnConfig, error) {
	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("querylog: parse postgres config: %w", err)
	}
	for _, param := range postgresQueryLogUnsupportedRuntimeParams {
		delete(config.RuntimeParams, param)
	}
	return config, nil
}

func postgresQueryLogDSN(dlCfg DuckLakeConfig) (string, error) {
	connStr, err := ducklake.PostgresMetadataStoreConnString(dlCfg.MetadataStore, dlCfg.ApplicationName)
	if err != nil {
		return "", fmt.Errorf("querylog: %w", err)
	}
	return connStr, nil
}

func ensurePostgresQueryLogTableContext(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return errors.New("querylog: postgres db is nil")
	}
	for _, stmt := range postgresQueryLogBaseSchemaSQL() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("querylog: ensure postgres storage: %w", err)
		}
	}
	for _, start := range postgresQueryLogInitialMonthStarts(time.Now().UTC()) {
		if err := ensurePostgresQueryLogMonthPartitionContext(ctx, db, start); err != nil {
			return fmt.Errorf("querylog: ensure postgres storage: %w", err)
		}
	}
	return nil
}

func postgresQueryLogSchemaSQLForTime(now time.Time) []string {
	stmts := postgresQueryLogBaseSchemaSQL()
	for _, start := range postgresQueryLogInitialMonthStarts(now) {
		stmts = append(stmts, postgresQueryLogCreateMonthPartitionSQL(start))
	}
	return stmts
}

func postgresQueryLogBaseSchemaSQL() []string {
	return []string{
		`CREATE SCHEMA IF NOT EXISTS querylog`,
		postgresQueryLogCreateTableSQL(),
		`CREATE TABLE IF NOT EXISTS querylog.query_log_entries_default PARTITION OF querylog.query_log_entries DEFAULT`,
		`CREATE INDEX IF NOT EXISTS idx_query_log_entries_event_time ON querylog.query_log_entries (event_time DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_query_log_entries_user_time ON querylog.query_log_entries (user_name, event_time DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_query_log_entries_hash_time ON querylog.query_log_entries (normalized_query_hash, event_time DESC)`,
	}
}

func postgresQueryLogInitialMonthStarts(now time.Time) []time.Time {
	monthStart := time.Date(now.UTC().Year(), now.UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	starts := make([]time.Time, 0, 3)
	for _, monthOffset := range []int{-1, 0, 1} {
		starts = append(starts, monthStart.AddDate(0, monthOffset, 0))
	}
	return starts
}

func ensurePostgresQueryLogPartitionsForBatchContext(ctx context.Context, db *sql.DB, batch []QueryLogEntry) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return errors.New("querylog: postgres db is nil")
	}
	for _, start := range postgresQueryLogBatchPartitionStarts(batch) {
		if err := ensurePostgresQueryLogMonthPartitionContext(ctx, db, start); err != nil {
			return err
		}
	}
	return nil
}

func ensurePostgresQueryLogMonthPartitionContext(ctx context.Context, db *sql.DB, start time.Time) error {
	start = postgresQueryLogMonthStart(start)
	key := postgresQueryLogPartitionKey(start)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("querylog: begin partition ensure %s: %w", key, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := lockPostgresQueryLogMonthPartitionContext(ctx, tx, start); err != nil {
		return err
	}
	attached, err := postgresQueryLogMonthPartitionAttachedContext(ctx, tx, start)
	if err != nil {
		return err
	}
	if attached {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("querylog: commit partition ensure %s: %w", key, err)
		}
		committed = true
		return nil
	}

	if err := lockPostgresQueryLogDefaultPartitionContext(ctx, tx); err != nil {
		return err
	}
	partitionExists, err := postgresQueryLogRelationExistsContext(ctx, tx, postgresQueryLogPartitionName(start))
	if err != nil {
		return err
	}
	hasDefaultRows, err := postgresQueryLogDefaultHasRowsContext(ctx, tx, start)
	if err != nil {
		return err
	}
	if hasDefaultRows || partitionExists {
		if err := repairPostgresQueryLogMonthPartitionContext(ctx, tx, start); err != nil {
			return err
		}
	} else if _, err := tx.ExecContext(ctx, postgresQueryLogCreateMonthPartitionSQL(start)); err != nil {
		return fmt.Errorf("querylog: create postgres month partition %s: %w", key, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("querylog: commit partition ensure %s: %w", key, err)
	}
	committed = true
	return nil
}

func lockPostgresQueryLogMonthPartitionContext(ctx context.Context, tx *sql.Tx, start time.Time) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresQueryLogPartitionAdvisoryLockID(start)); err != nil {
		return fmt.Errorf("querylog: lock postgres month partition %s: %w", postgresQueryLogPartitionKey(start), err)
	}
	return nil
}

func postgresQueryLogMonthPartitionAttachedContext(ctx context.Context, tx *sql.Tx, start time.Time) (bool, error) {
	var attached bool
	err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	JOIN pg_inherits i ON i.inhrelid = c.oid
	JOIN pg_class p ON p.oid = i.inhparent
	JOIN pg_namespace pn ON pn.oid = p.relnamespace
	WHERE n.nspname = $1
	  AND c.relname = $2
	  AND pn.nspname = $1
	  AND p.relname = $3
)`, postgresQueryLogSchema, postgresQueryLogPartitionRelName(start), postgresQueryLogParent).Scan(&attached)
	if err != nil {
		return false, fmt.Errorf("querylog: check postgres month partition %s: %w", postgresQueryLogPartitionKey(start), err)
	}
	return attached, nil
}

func lockPostgresQueryLogDefaultPartitionContext(ctx context.Context, tx *sql.Tx) error {
	exists, err := postgresQueryLogRelationExistsContext(ctx, tx, postgresQueryLogDefaultPartition)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if _, err := tx.ExecContext(ctx, "LOCK TABLE "+postgresQueryLogDefaultPartition+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return fmt.Errorf("querylog: lock postgres default partition: %w", err)
	}
	return nil
}

func postgresQueryLogRelationExistsContext(ctx context.Context, tx *sql.Tx, relation string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, relation).Scan(&exists); err != nil {
		return false, fmt.Errorf("querylog: check postgres relation %s: %w", relation, err)
	}
	return exists, nil
}

func postgresQueryLogDefaultHasRowsContext(ctx context.Context, tx *sql.Tx, start time.Time) (bool, error) {
	exists, err := postgresQueryLogRelationExistsContext(ctx, tx, postgresQueryLogDefaultPartition)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	var hasRows bool
	end := postgresQueryLogMonthStart(start).AddDate(0, 1, 0)
	err = tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM querylog.query_log_entries_default
	WHERE event_time >= $1 AND event_time < $2
)`, postgresQueryLogMonthStart(start), end).Scan(&hasRows)
	if err != nil {
		return false, fmt.Errorf("querylog: check postgres default rows for partition %s: %w", postgresQueryLogPartitionKey(start), err)
	}
	return hasRows, nil
}

func repairPostgresQueryLogMonthPartitionContext(ctx context.Context, tx *sql.Tx, start time.Time) error {
	key := postgresQueryLogPartitionKey(start)
	start = postgresQueryLogMonthStart(start)
	end := start.AddDate(0, 1, 0)

	if _, err := tx.ExecContext(ctx, postgresQueryLogCreateStandaloneMonthPartitionSQL(start)); err != nil {
		return fmt.Errorf("querylog: repair postgres month partition %s: %w", key, err)
	}
	if _, err := tx.ExecContext(ctx, postgresQueryLogMoveDefaultRowsSQL(start), start, end); err != nil {
		return fmt.Errorf("querylog: repair postgres month partition %s: %w", key, err)
	}
	if _, err := tx.ExecContext(ctx, postgresQueryLogDeleteDefaultRowsSQL(), start, end); err != nil {
		return fmt.Errorf("querylog: repair postgres month partition %s: %w", key, err)
	}
	if _, err := tx.ExecContext(ctx, postgresQueryLogAttachMonthPartitionSQL(start)); err != nil {
		return fmt.Errorf("querylog: repair postgres month partition %s: %w", key, err)
	}
	return nil
}

func postgresQueryLogBatchPartitionStarts(batch []QueryLogEntry) []time.Time {
	seen := make(map[string]struct{})
	starts := make([]time.Time, 0, len(batch))
	for _, entry := range batch {
		start := postgresQueryLogMonthStart(entry.EventTime)
		key := postgresQueryLogPartitionKey(start)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		starts = append(starts, start)
	}
	return starts
}

func postgresQueryLogCreateMonthPartitionSQL(start time.Time) string {
	start = postgresQueryLogMonthStart(start)
	end := start.AddDate(0, 1, 0)
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF querylog.query_log_entries FOR VALUES FROM ('%s') TO ('%s')",
		postgresQueryLogPartitionName(start),
		postgresQueryLogPartitionBound(start),
		postgresQueryLogPartitionBound(end),
	)
}

func postgresQueryLogCreateStandaloneMonthPartitionSQL(start time.Time) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (LIKE querylog.query_log_entries INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY INCLUDING CONSTRAINTS)",
		postgresQueryLogPartitionName(start),
	)
}

func postgresQueryLogMoveDefaultRowsSQL(start time.Time) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM querylog.query_log_entries_default WHERE event_time >= $1 AND event_time < $2",
		postgresQueryLogPartitionName(start),
		postgresQueryLogColumns,
		postgresQueryLogColumns,
	)
}

func postgresQueryLogDeleteDefaultRowsSQL() string {
	return "DELETE FROM querylog.query_log_entries_default WHERE event_time >= $1 AND event_time < $2"
}

func postgresQueryLogAttachMonthPartitionSQL(start time.Time) string {
	start = postgresQueryLogMonthStart(start)
	end := start.AddDate(0, 1, 0)
	return fmt.Sprintf(
		"ALTER TABLE querylog.query_log_entries ATTACH PARTITION %s FOR VALUES FROM ('%s') TO ('%s')",
		postgresQueryLogPartitionName(start),
		postgresQueryLogPartitionBound(start),
		postgresQueryLogPartitionBound(end),
	)
}

func postgresQueryLogPartitionBound(t time.Time) string {
	return postgresQueryLogMonthStart(t).Format("2006-01-02 15:04:05") + "+00"
}

func postgresQueryLogPartitionRelName(start time.Time) string {
	start = postgresQueryLogMonthStart(start)
	return fmt.Sprintf("query_log_entries_%04d%02d", start.Year(), int(start.Month()))
}

func postgresQueryLogPartitionName(start time.Time) string {
	start = postgresQueryLogMonthStart(start)
	return postgresQueryLogSchema + "." + postgresQueryLogPartitionRelName(start)
}

func postgresQueryLogPartitionKey(start time.Time) string {
	start = postgresQueryLogMonthStart(start)
	return start.Format("200601")
}

func postgresQueryLogMonthStart(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func postgresQueryLogPartitionAdvisoryLockID(start time.Time) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("duckgres:querylog:partition:" + postgresQueryLogPartitionKey(start)))
	return int64(h.Sum64())
}

func postgresQueryLogCreateTableSQL() string {
	return `CREATE TABLE IF NOT EXISTS querylog.query_log_entries (
	id BIGINT GENERATED BY DEFAULT AS IDENTITY,
	event_time TIMESTAMPTZ NOT NULL,
	query_duration_ms BIGINT NOT NULL,
	type TEXT NOT NULL,
	query TEXT NOT NULL,
	transpiled_query TEXT,
	query_kind TEXT,
	normalized_query_hash BIGINT,
	result_rows BIGINT,
	written_rows BIGINT,
	exception_code TEXT,
	exception TEXT,
	user_name TEXT NOT NULL,
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
	postgres_scan_ms BIGINT DEFAULT 0,
	cpu_time_s DOUBLE PRECISION DEFAULT 0,
	peak_buffer_memory_bytes BIGINT DEFAULT 0
) PARTITION BY RANGE (event_time)`
}
