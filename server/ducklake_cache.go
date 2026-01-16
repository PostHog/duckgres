package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// DuckLakeCacheDuration defines how long to cache DuckLake table metadata
// before refreshing from the catalog. This prevents excessive catalog queries
// during long-running operations like Metabase syncs that can cause SSL timeouts.
const DuckLakeCacheDuration = 15 * time.Minute

// DuckLakeCache manages cached DuckLake table and column metadata.
// It's initialized per-connection since each connection has its own DuckDB instance.
type DuckLakeCache struct {
	db              *sql.DB
	catalogName     string
	lastRefresh     time.Time
	refreshMu       sync.Mutex
	maxRetries      int
}

// NewDuckLakeCache creates a new cache manager for the given database connection.
func NewDuckLakeCache(db *sql.DB, catalogName string) *DuckLakeCache {
	return &DuckLakeCache{
		db:          db,
		catalogName: catalogName,
		maxRetries:  3,
	}
}

// CreateCacheTables creates the cache tables if they don't exist.
// Should be called during pg_catalog initialization.
// Note: Indexes are NOT created here - they're created in Refresh() after the swap.
// This avoids DuckDB dependency issues that would block table renames.
func CreateCacheTables(db *sql.DB) error {
	tablesSQL := `
		CREATE TABLE IF NOT EXISTS main.ducklake_tables_cache (
			schema_id BIGINT NOT NULL,
			schema_name TEXT NOT NULL,
			table_id BIGINT NOT NULL,
			table_name TEXT NOT NULL
		)
	`
	if _, err := db.Exec(tablesSQL); err != nil {
		return fmt.Errorf("failed to create ducklake_tables_cache: %w", err)
	}

	columnsSQL := `
		CREATE TABLE IF NOT EXISTS main.ducklake_columns_cache (
			schema_id BIGINT NOT NULL,
			schema_name TEXT NOT NULL,
			table_id BIGINT NOT NULL,
			table_name TEXT NOT NULL,
			column_index INTEGER NOT NULL,
			column_name TEXT NOT NULL,
			data_type TEXT NOT NULL,
			is_nullable BOOLEAN NOT NULL
		)
	`
	if _, err := db.Exec(columnsSQL); err != nil {
		return fmt.Errorf("failed to create ducklake_columns_cache: %w", err)
	}

	return nil
}

// Refresh updates the cache tables with fresh data from DuckLake metadata.
// Returns early if cache is still valid (within DuckLakeCacheDuration).
// Uses staging tables to ensure atomicity - if refresh fails, existing cache remains intact.
func (c *DuckLakeCache) Refresh(ctx context.Context) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	// Check if cache is still valid
	if time.Since(c.lastRefresh) < DuckLakeCacheDuration {
		return nil
	}

	log.Printf("Refreshing DuckLake cache (catalog: %s)", c.catalogName)
	start := time.Now()

	// Query ducklake metadata tables directly instead of using ducklake_table_info()
	// to avoid potential segfault issues with the SQLite catalog implementation
	metadataDb := fmt.Sprintf("__ducklake_metadata_%s", c.catalogName)

	// Create staging tables with fresh data
	// If this succeeds, we swap them with the live tables
	// If this fails, the existing cache remains intact

	// Clean up any leftover staging tables from previous failed attempts
	c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_tables_cache_staging")
	c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_columns_cache_staging")

	// Create tables staging table with retry
	tablesStagingSQL := fmt.Sprintf(`
		CREATE TABLE main.ducklake_tables_cache_staging AS
		SELECT s.schema_id, s.schema_name, t.table_id, t.table_name
		FROM %s.ducklake_schema s
		JOIN %s.ducklake_table t ON t.schema_id = s.schema_id
		WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog')
		  AND s.end_snapshot IS NULL
		  AND t.end_snapshot IS NULL
	`, metadataDb, metadataDb)

	err := RetryWithBackoff(ctx, c.maxRetries, func() error {
		_, err := c.db.ExecContext(ctx, tablesStagingSQL)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to create tables staging cache: %w", err)
	}

	// Create columns staging table with retry
	columnsStagingSQL := fmt.Sprintf(`
		CREATE TABLE main.ducklake_columns_cache_staging AS
		SELECT
			s.schema_id,
			s.schema_name,
			t.table_id,
			t.table_name,
			c.column_order AS column_index,
			c.column_name,
			LOWER(COALESCE(
				-- Handle nested types (STRUCT, LIST, MAP)
				CASE
					WHEN c.column_type LIKE 'STRUCT%%' THEN 'json'
					WHEN c.column_type LIKE 'MAP%%' THEN 'json'
					WHEN c.column_type LIKE 'LIST%%' THEN 'ARRAY'
					ELSE NULL
				END,
				-- Map DuckDB types to PostgreSQL types
				CASE
					WHEN UPPER(c.column_type) = 'BOOLEAN' THEN 'boolean'
					WHEN UPPER(c.column_type) = 'TINYINT' THEN 'smallint'
					WHEN UPPER(c.column_type) = 'SMALLINT' THEN 'smallint'
					WHEN UPPER(c.column_type) = 'INTEGER' THEN 'integer'
					WHEN UPPER(c.column_type) = 'BIGINT' THEN 'bigint'
					WHEN UPPER(c.column_type) = 'HUGEINT' THEN 'numeric'
					WHEN UPPER(c.column_type) = 'REAL' OR UPPER(c.column_type) = 'FLOAT4' THEN 'real'
					WHEN UPPER(c.column_type) = 'DOUBLE' OR UPPER(c.column_type) = 'FLOAT8' THEN 'double precision'
					WHEN UPPER(c.column_type) LIKE 'DECIMAL%%' THEN 'numeric'
					WHEN UPPER(c.column_type) LIKE 'NUMERIC%%' THEN 'numeric'
					WHEN UPPER(c.column_type) = 'VARCHAR' OR UPPER(c.column_type) LIKE 'VARCHAR(%%' THEN 'text'
					WHEN UPPER(c.column_type) = 'TEXT' THEN 'text'
					WHEN UPPER(c.column_type) = 'DATE' THEN 'date'
					WHEN UPPER(c.column_type) = 'TIME' THEN 'time without time zone'
					WHEN UPPER(c.column_type) = 'TIMESTAMP' THEN 'timestamp without time zone'
					WHEN UPPER(c.column_type) = 'TIMESTAMPTZ' OR UPPER(c.column_type) = 'TIMESTAMP WITH TIME ZONE' THEN 'timestamp with time zone'
					WHEN UPPER(c.column_type) = 'INTERVAL' THEN 'interval'
					WHEN UPPER(c.column_type) = 'UUID' THEN 'uuid'
					WHEN UPPER(c.column_type) = 'BLOB' OR UPPER(c.column_type) = 'BYTEA' THEN 'bytea'
					WHEN UPPER(c.column_type) = 'JSON' THEN 'json'
					WHEN UPPER(c.column_type) LIKE '%%[]' THEN 'ARRAY'
					ELSE c.column_type
				END
			)) AS data_type,
			COALESCE(c.nulls_allowed, true) AS is_nullable
		FROM %s.ducklake_schema s
		JOIN %s.ducklake_table t ON t.schema_id = s.schema_id
		JOIN %s.ducklake_column c ON c.table_id = t.table_id
		WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog')
		  AND s.end_snapshot IS NULL
		  AND t.end_snapshot IS NULL
		  AND c.end_snapshot IS NULL
	`, metadataDb, metadataDb, metadataDb)

	err = RetryWithBackoff(ctx, c.maxRetries, func() error {
		_, err := c.db.ExecContext(ctx, columnsStagingSQL)
		return err
	})
	if err != nil {
		// Clean up tables staging since columns failed
		c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_tables_cache_staging")
		return fmt.Errorf("failed to create columns staging cache: %w", err)
	}

	// Both staging tables created successfully - now swap them safely
	// Use backup tables to ensure we can restore if rename fails

	// Step 1: Clean up any leftover backup tables from previous failed swaps
	c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_tables_cache_backup")
	c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_columns_cache_backup")

	// Step 2: Drop indexes on current tables (DuckDB blocks rename if indexes exist)
	c.db.ExecContext(ctx, "DROP INDEX IF EXISTS main.idx_ducklake_tables_cache_schema")
	c.db.ExecContext(ctx, "DROP INDEX IF EXISTS main.idx_ducklake_tables_cache_table_id")
	c.db.ExecContext(ctx, "DROP INDEX IF EXISTS main.idx_ducklake_columns_cache_table")
	c.db.ExecContext(ctx, "DROP INDEX IF EXISTS main.idx_ducklake_columns_cache_table_id")

	// Step 3: Rename current tables to backup (if they exist)
	// These may fail if tables don't exist yet (first run) - that's OK
	c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_tables_cache RENAME TO ducklake_tables_cache_backup")
	c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_columns_cache RENAME TO ducklake_columns_cache_backup")

	// Step 4: Rename staging tables to live
	if _, err := c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_tables_cache_staging RENAME TO ducklake_tables_cache"); err != nil {
		// Restore from backup
		log.Printf("Error renaming tables staging, restoring backup: %v", err)
		c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_tables_cache_backup RENAME TO ducklake_tables_cache")
		c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_tables_cache_staging")
		c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_columns_cache_staging")
		return fmt.Errorf("failed to rename tables staging: %w", err)
	}

	if _, err := c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_columns_cache_staging RENAME TO ducklake_columns_cache"); err != nil {
		// Restore from backup - need to restore both tables
		log.Printf("Error renaming columns staging, restoring backup: %v", err)
		c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_tables_cache RENAME TO ducklake_tables_cache_staging") // undo tables rename
		c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_tables_cache_backup RENAME TO ducklake_tables_cache")
		c.db.ExecContext(ctx, "ALTER TABLE main.ducklake_columns_cache_backup RENAME TO ducklake_columns_cache")
		c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_tables_cache_staging")
		c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_columns_cache_staging")
		return fmt.Errorf("failed to rename columns staging: %w", err)
	}

	// Step 5: Create indexes on the final tables
	c.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_ducklake_tables_cache_schema ON main.ducklake_tables_cache(schema_name)")
	c.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_ducklake_tables_cache_table_id ON main.ducklake_tables_cache(table_id)")
	c.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_ducklake_columns_cache_table ON main.ducklake_columns_cache(schema_name, table_name)")
	c.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_ducklake_columns_cache_table_id ON main.ducklake_columns_cache(table_id)")

	// Step 6: Clean up backup tables only after successful swap
	c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_tables_cache_backup")
	c.db.ExecContext(ctx, "DROP TABLE IF EXISTS main.ducklake_columns_cache_backup")

	c.lastRefresh = time.Now()
	log.Printf("DuckLake cache refreshed in %v", time.Since(start))
	return nil
}

// RefreshSync is a synchronous wrapper around Refresh that uses a background context.
func (c *DuckLakeCache) RefreshSync() error {
	return c.Refresh(context.Background())
}

// ForceRefresh clears the cache timestamp and forces a refresh on next call.
func (c *DuckLakeCache) ForceRefresh() {
	c.refreshMu.Lock()
	c.lastRefresh = time.Time{}
	c.refreshMu.Unlock()
}
