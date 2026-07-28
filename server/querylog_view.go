package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	duckLakeMetadataCatalog        = "__ducklake_metadata_ducklake"
	duckLakeQueryLogView           = "ducklake.system.query_log"
	duckLakeLegacyQueryLogTable    = "query_log_ducklake_legacy"
	duckLakeLegacyQueryLogFullName = "ducklake.system." + duckLakeLegacyQueryLogTable
	queryLogSurfaceFailureCooldown = 30 * time.Second
)

var queryLogSurfaceCache = newQueryLogSurfaceCache()
var queryLogStorageCache = newQueryLogSurfaceCache()

// ensurePostgresQueryLogStorageForDuckLakeAttach must run before DuckLake
// ATTACH. DuckLake's hidden metadata catalog may not see Postgres schemas
// created after attach, so the storage schema must exist before the hidden
// catalog is bound.
func ensurePostgresQueryLogStorageForDuckLakeAttach(ctx context.Context, cfg Config) error {
	if !cfg.QueryLog.Enabled || cfg.DuckLake.MetadataStore == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cacheKey := cfg.DuckLake.MetadataStore
	// Cache successes only. A transient pre-attach failure should be retried by
	// the next activation/connect attempt because this is the only point where
	// the querylog schema can become visible to DuckLake's hidden metadata catalog.
	if queryLogStorageCache.ready(cacheKey) {
		return nil
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
	queryLogStorageCache.recordSuccess(cacheKey)
	return nil
}

func ensureDuckLakeQueryLogSurface(ctx context.Context, db *sql.DB, cfg Config) error {
	if !cfg.QueryLog.Enabled || cfg.DuckLake.MetadataStore == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cacheKey := cfg.DuckLake.MetadataStore
	if queryLogSurfaceCache.ready(cacheKey) || queryLogSurfaceCache.failureCoolingDown(cacheKey, time.Now()) {
		return nil
	}

	ready, err := duckLakeQueryLogViewReadyContext(ctx, db)
	if err != nil {
		queryLogSurfaceCache.recordFailure(cacheKey, time.Now())
		return err
	}
	if ready {
		queryLogSurfaceCache.recordSuccess(cacheKey)
		return nil
	}

	if err := ensureDuckLakeQueryLogViewContext(ctx, db); err != nil {
		queryLogSurfaceCache.recordFailure(cacheKey, time.Now())
		return fmt.Errorf("querylog: ensure ducklake view: %w", err)
	}
	queryLogSurfaceCache.recordSuccess(cacheKey)
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
		current, err := duckLakeQueryLogViewColumnsCurrentContext(ctx, db)
		if err != nil {
			return err
		}
		if current {
			return verifyDuckLakeQueryLogViewContext(ctx, db)
		}
		// The view predates a column the registry has since gained. Preflight
		// the new SELECT before replacing, so a source table that has not been
		// migrated yet leaves the working view in place.
		if err := preflightDuckLakeQueryLogViewSourceContext(ctx, db); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, duckLakeQueryLogReplaceViewSQL()); err != nil {
			return fmt.Errorf("replace ducklake query_log view: %w", err)
		}
		return verifyDuckLakeQueryLogViewContext(ctx, db)
	}

	if err := preflightDuckLakeQueryLogViewSourceContext(ctx, db); err != nil {
		return err
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
	return verifyDuckLakeQueryLogViewContext(ctx, db)
}

func duckLakeQueryLogViewReadyContext(ctx context.Context, db *sql.DB) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return false, fmt.Errorf("duckdb db is nil")
	}
	viewExists, err := duckLakeQueryLogViewExistsContext(ctx, db)
	if err != nil || !viewExists {
		return false, err
	}
	current, err := duckLakeQueryLogViewColumnsCurrentContext(ctx, db)
	if err != nil || !current {
		return false, err
	}
	if err := verifyDuckLakeQueryLogViewContext(ctx, db); err != nil {
		return false, err
	}
	return true, nil
}

// duckLakeQueryLogViewColumnsCurrentContext reports whether the live view
// exposes every column the registry expects. A view created by an older build
// is missing appended columns; CREATE VIEW IF NOT EXISTS would silently leave
// it that way, so the caller replaces it instead.
func duckLakeQueryLogViewColumnsCurrentContext(ctx context.Context, db *sql.DB) (bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT * FROM ducklake.system.query_log LIMIT 0")
	if err != nil {
		return false, fmt.Errorf("inspect ducklake query_log view columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return false, fmt.Errorf("inspect ducklake query_log view columns: %w", err)
	}
	present := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		present[strings.ToLower(column)] = struct{}{}
	}
	for _, want := range queryLogEntryColumnNames() {
		if _, ok := present[want]; !ok {
			return false, nil
		}
	}
	return true, rows.Err()
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

func preflightDuckLakeQueryLogViewSourceContext(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, duckLakeQueryLogViewSelectSQL()+" LIMIT 0")
	if err != nil {
		return fmt.Errorf("preflight ducklake query_log source: %w", err)
	}
	return rows.Close()
}

func verifyDuckLakeQueryLogViewContext(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT * FROM ducklake.system.query_log LIMIT 0")
	if err != nil {
		return fmt.Errorf("verify ducklake query_log view: %w", err)
	}
	return rows.Close()
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
	return "CREATE VIEW IF NOT EXISTS ducklake.system.query_log AS\n" + duckLakeQueryLogViewSelectSQL()
}

// duckLakeQueryLogReplaceViewSQL rebuilds an existing view whose column set has
// drifted from the registry. CREATE VIEW IF NOT EXISTS is a no-op against a
// stale view, so a column added to the table would never become visible in
// DuckLake without this.
func duckLakeQueryLogReplaceViewSQL() string {
	return "CREATE OR REPLACE VIEW ducklake.system.query_log AS\n" + duckLakeQueryLogViewSelectSQL()
}

func duckLakeQueryLogViewSelectSQL() string {
	var sb strings.Builder
	sb.WriteString("SELECT")
	for i, name := range queryLogEntryColumnNames() {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("\n\t")
		sb.WriteString(name)
	}
	fmt.Fprintf(&sb, "\nFROM \"%s\".querylog.query_log_entries", duckLakeMetadataCatalog)
	return sb.String()
}

type queryLogSurfaceStateCache struct {
	mu        sync.Mutex
	successes map[string]struct{}
	failures  map[string]time.Time
}

func newQueryLogSurfaceCache() *queryLogSurfaceStateCache {
	return &queryLogSurfaceStateCache{
		successes: make(map[string]struct{}),
		failures:  make(map[string]time.Time),
	}
}

func (c *queryLogSurfaceStateCache) ready(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.successes[key]
	return ok
}

func (c *queryLogSurfaceStateCache) failureCoolingDown(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	until, ok := c.failures[key]
	return ok && now.Before(until)
}

func (c *queryLogSurfaceStateCache) recordSuccess(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.successes[key] = struct{}{}
	delete(c.failures, key)
}

func (c *queryLogSurfaceStateCache) recordFailure(key string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.successes[key]; ok {
		return
	}
	c.failures[key] = now.Add(queryLogSurfaceFailureCooldown)
}

func resetQueryLogSurfaceCacheForTest() {
	queryLogSurfaceCache = newQueryLogSurfaceCache()
	queryLogStorageCache = newQueryLogSurfaceCache()
}
