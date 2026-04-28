package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultDuckLakeSpecVersion is the DuckLake spec version that this build of duckgres expects.
// When the metadata store is at an older version, we backup and migrate automatically.
// This must match the DuckLake version bundled with the current DuckDB driver.
const DefaultDuckLakeSpecVersion = "1.0"

// migrationState holds the result of a single migration check.
type migrationState struct {
	done     bool
	needed   bool
	err      error
	checkedV string
}

// dlMigrations caches per-metadata-store migration check results.
// This is critical for the multi-tenant Control Plane to avoid cross-tenant
// cache contamination.
var dlMigrations sync.Map // connStr (string) -> *migrationState

// dlMigrationMu synchronizes concurrent checks for the same connection string.
var dlMigrationMu sync.Map // connStr (string) -> *sync.Mutex

// ensureDuckLakeMigrationCheck runs the migration check, retrying on transient errors.
// Once the check succeeds (regardless of whether migration is needed), the result
// is locked in and subsequent calls are no-ops. If the check fails, the error is
// stored but the next call will retry — this prevents a transient failure (e.g.,
// metadata store not yet reachable during pod startup) from permanently blocking
// all connections.
//
// The backup file is written to dataDir.
// This should be called BEFORE acquiring the DuckLake attachment semaphore,
// since the backup can take minutes for large metadata stores.
func ensureDuckLakeMigrationCheck(dlCfg DuckLakeConfig, dataDir string) {
	if dlCfg.MetadataStore == "" {
		return
	}
	connStr := dlCfg.MetadataStore

	// Get or create a mutex for this specific connection string.
	muAny, _ := dlMigrationMu.LoadOrStore(connStr, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	// Check if we already have a successful result for this connection string.
	if val, ok := dlMigrations.Load(connStr); ok {
		state := val.(*migrationState)
		if state.done {
			return
		}
	}

	targetVersion := dlCfg.SpecVersion
	if targetVersion == "" {
		targetVersion = DefaultDuckLakeSpecVersion
	}

	needed, ver, err := checkAndBackupIfNeeded(dlCfg, dataDir, targetVersion)
	dlMigrations.Store(connStr, &migrationState{
		needed:   needed,
		checkedV: ver,
		err:      err,
		done:     err == nil,
	})
}

// duckLakeMigrationNeeded returns whether the ATTACH statement should include
// AUTOMATIC_MIGRATION TRUE. Safe to call after ensureDuckLakeMigrationCheck.
func duckLakeMigrationNeeded(connStr string) bool {
	if val, ok := dlMigrations.Load(connStr); ok {
		state := val.(*migrationState)
		return state.needed && state.err == nil
	}
	return false
}

// duckLakeMigrationCheckedVersion returns the version found in the metadata store.
// Returns "" if the check has not run or the metadata store had no version.
func duckLakeMigrationCheckedVersion(connStr string) string {
	if val, ok := dlMigrations.Load(connStr); ok {
		state := val.(*migrationState)
		return state.checkedV
	}
	return ""
}

// DuckLakeMigrationCheckedVersion is an exported accessor for the control plane.
// NOTE: In multi-tenant mode, this only returns a value if called from a worker
// with a single metadata store. Control Plane should use per-org state.
func DuckLakeMigrationCheckedVersion() string {
	// Best-effort: if there is exactly one entry, return it.
	var version string
	dlMigrations.Range(func(_, value any) bool {
		version = value.(*migrationState).checkedV
		return false // stop iteration
	})
	return version
}


// CheckAndBackupDuckLakeMigration runs the migration check for the given
// DuckLake config and returns whether migration is needed. If migration is
// needed, it backs up the metadata store first. This is exported for use by
// the control plane, which runs the check once before activating workers.
func CheckAndBackupDuckLakeMigration(dlCfg DuckLakeConfig, dataDir string, targetVersion string) (bool, error) {
	if dlCfg.MetadataStore == "" {
		return false, nil
	}
	if targetVersion == "" {
		targetVersion = DefaultDuckLakeSpecVersion
	}
	needed, _, err := checkAndBackupIfNeeded(dlCfg, dataDir, targetVersion)
	return needed, err
}

// CheckDuckLakeMigrationVersion checks only whether a DuckLake metadata store
// needs migration, without performing the backup. This is fast (<1s) and safe
// to call during startup without risking timeouts.
func CheckDuckLakeMigrationVersion(dlCfg DuckLakeConfig, targetVersion string) (needed bool, version string, err error) {
	if dlCfg.MetadataStore == "" || !strings.HasPrefix(dlCfg.MetadataStore, "postgres:") {
		return false, "", nil
	}

	if targetVersion == "" {
		targetVersion = DefaultDuckLakeSpecVersion
	}

	connStr := strings.TrimPrefix(dlCfg.MetadataStore, "postgres:")
	pgDB, err := sql.Open("pgx", connStr)
	if err != nil {
		return false, "", fmt.Errorf("open metadata store: %w", err)
	}
	defer func() { _ = pgDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := pgDB.PingContext(ctx); err != nil {
		return false, "", fmt.Errorf("connect to metadata store: %w", err)
	}

	var exists bool
	err = pgDB.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ducklake_metadata')").Scan(&exists)
	if err != nil {
		return false, "", fmt.Errorf("check ducklake_metadata existence: %w", err)
	}
	if !exists {
		return false, "", nil
	}

	var ver string
	err = pgDB.QueryRowContext(ctx,
		`SELECT "value" FROM ducklake_metadata WHERE "key" = 'version'`).Scan(&ver)
	if err != nil {
		return false, "", fmt.Errorf("read DuckLake spec version: %w", err)
	}

	less, err := versionLessThan(ver, targetVersion)
	if err != nil {
		return false, ver, fmt.Errorf("compare DuckLake versions: %w", err)
	}
	return less, ver, nil
}

// BackupDuckLakeMetadata runs only the metadata backup (no version check).
// Exported for use by the control plane to run the backup asynchronously.
func BackupDuckLakeMetadata(dlCfg DuckLakeConfig, dataDir string) error {
	if dlCfg.MetadataStore == "" || !strings.HasPrefix(dlCfg.MetadataStore, "postgres:") {
		return nil
	}

	connStr := strings.TrimPrefix(dlCfg.MetadataStore, "postgres:")
	pgDB, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("open metadata store: %w", err)
	}
	defer func() { _ = pgDB.Close() }()

	var ver string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = pgDB.QueryRowContext(ctx,
		`SELECT "value" FROM ducklake_metadata WHERE "key" = 'version'`).Scan(&ver)
	if err != nil {
		return fmt.Errorf("read DuckLake spec version for backup: %w", err)
	}

	return backupDuckLakeMetadata(pgDB, dataDir, ver)
}

// parseDuckLakeVersion parses a DuckLake version string like "0.3" into
// (major, minor) integers for reliable numeric comparison.
// Returns (0, 0, err) if the string cannot be parsed.
func parseDuckLakeVersion(ver string) (major, minor int, err error) {
	parts := strings.SplitN(ver, ".", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid version format: %q", ver)
	}
	major, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid major version in %q: %w", ver, err)
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minor version in %q: %w", ver, err)
	}
	return major, minor, nil
}

// versionLessThan returns true if version a is strictly less than version b.
// Both must be in "major.minor" format (e.g., "0.3", "0.4", "0.10").
func versionLessThan(a, b string) (bool, error) {
	aMaj, aMin, err := parseDuckLakeVersion(a)
	if err != nil {
		return false, err
	}
	bMaj, bMin, err := parseDuckLakeVersion(b)
	if err != nil {
		return false, err
	}
	return aMaj < bMaj || (aMaj == bMaj && aMin < bMin), nil
}

// checkAndBackupIfNeeded connects to the metadata PostgreSQL store, checks the
// DuckLake spec version, and if migration is required, dumps all ducklake_* tables
// to a SQL backup file before returning.
func checkAndBackupIfNeeded(dlCfg DuckLakeConfig, dataDir string, targetVersion string) (needed bool, version string, err error) {
	if !strings.HasPrefix(dlCfg.MetadataStore, "postgres:") {
		return false, "", nil
	}

	connStr := strings.TrimPrefix(dlCfg.MetadataStore, "postgres:")

	pgDB, err := sql.Open("pgx", connStr)
	if err != nil {
		return false, "", fmt.Errorf("open metadata store: %w", err)
	}
	defer func() { _ = pgDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := pgDB.PingContext(ctx); err != nil {
		return false, "", fmt.Errorf("connect to metadata store: %w", err)
	}

	// Check if ducklake_metadata table exists (fresh install has no tables yet).
	var exists bool
	err = pgDB.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ducklake_metadata')").Scan(&exists)
	if err != nil {
		return false, "", fmt.Errorf("check ducklake_metadata existence: %w", err)
	}
	if !exists {
		slog.Info("DuckLake metadata store has no ducklake_metadata table (fresh install), no migration needed.")
		return false, "", nil
	}

	// Read current spec version.
	var ver string
	err = pgDB.QueryRowContext(ctx,
		`SELECT "value" FROM ducklake_metadata WHERE "key" = 'version'`).Scan(&ver)
	if err != nil {
		return false, "", fmt.Errorf("read DuckLake spec version: %w", err)
	}

	slog.Info("DuckLake metadata store version detected.", "version", ver, "expected", targetVersion)

	less, err := versionLessThan(ver, targetVersion)
	if err != nil {
		return false, ver, fmt.Errorf("compare DuckLake versions: %w", err)
	}
	if !less {
		return false, ver, nil
	}

	// Migration needed — backup first.
	slog.Info("DuckLake metadata migration required. Backing up metadata store before upgrade.",
		"from", ver, "to", targetVersion)

	if err := backupDuckLakeMetadata(pgDB, dataDir, ver); err != nil {
		return true, ver, fmt.Errorf("backup metadata before migration: %w", err)
	}

	return true, ver, nil
}

// backupDuckLakeMetadata dumps all ducklake_* tables from the PostgreSQL metadata
// store to a SQL file. The file contains CREATE TABLE + INSERT statements that can
// be used to restore the metadata if the migration goes wrong.
func backupDuckLakeMetadata(pgDB *sql.DB, dataDir string, version string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Use a REPEATABLE READ transaction to guarantee a consistent snapshot
	// across all tables. Without this, concurrent writes could produce an
	// inconsistent backup.
	tx, err := pgDB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin backup transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Discover all ducklake_* tables.
	rows, err := tx.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'ducklake_%' ORDER BY table_name")
	if err != nil {
		return fmt.Errorf("list ducklake tables: %w", err)
	}

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table names: %w", err)
	}

	if len(tables) == 0 {
		slog.Warn("No ducklake_* tables found in metadata store, nothing to back up.")
		return nil
	}

	// Create backup file.
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	backupPath := filepath.Join(dataDir, fmt.Sprintf("ducklake-backup-%s-v%s.sql", timestamp, version))

	slog.Info("Starting DuckLake metadata backup.", "path", backupPath, "tables", len(tables))

	f, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("create backup file %s: %w", backupPath, err)
	}
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			// Remove partial backup file on error to avoid confusion about
			// whether the backup is complete.
			_ = os.Remove(backupPath)
		}
	}()

	// Write header.
	if _, err := fmt.Fprintf(f, "-- DuckLake metadata backup before migration (v%s → v%s)\n", version, DefaultDuckLakeSpecVersion); err != nil {
		return fmt.Errorf("write backup header: %w", err)
	}
	if _, err := fmt.Fprintf(f, "-- Generated: %s\n", time.Now().UTC().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("write backup header: %w", err)
	}
	if _, err := fmt.Fprintf(f, "-- Tables: %d\n\n", len(tables)); err != nil {
		return fmt.Errorf("write backup header: %w", err)
	}
	if _, err := fmt.Fprintln(f, "BEGIN;"); err != nil {
		return fmt.Errorf("write backup header: %w", err)
	}

	totalRows := 0
	for _, table := range tables {
		count, err := backupTable(ctx, tx, f, table)
		if err != nil {
			return fmt.Errorf("backup table %s: %w", table, err)
		}
		totalRows += count
	}

	if _, err := fmt.Fprintln(f, "\nCOMMIT;"); err != nil {
		return fmt.Errorf("write backup footer: %w", err)
	}

	// Flush to disk before closing — this is a critical safety net file.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync backup file: %w", err)
	}

	success = true

	info, _ := os.Stat(backupPath)
	sizeMB := float64(0)
	if info != nil {
		sizeMB = float64(info.Size()) / 1024 / 1024
	}

	slog.Info("DuckLake metadata backup completed.",
		"path", backupPath,
		"tables", len(tables),
		"rows", totalRows,
		"size_mb", fmt.Sprintf("%.1f", sizeMB))

	return nil
}

// quoteIdent quotes a PostgreSQL identifier with double quotes.
// Any embedded double quotes are doubled per SQL standard.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// backupTable writes CREATE TABLE and INSERT statements for a single table.
// Returns the number of rows backed up.
func backupTable(ctx context.Context, tx *sql.Tx, f *os.File, table string) (int, error) {
	// Get column definitions.
	colRows, err := tx.QueryContext(ctx,
		"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position", table)
	if err != nil {
		return 0, fmt.Errorf("get columns: %w", err)
	}

	type colDef struct {
		name     string
		dataType string
		nullable string
	}
	var cols []colDef
	for colRows.Next() {
		var c colDef
		if err := colRows.Scan(&c.name, &c.dataType, &c.nullable); err != nil {
			_ = colRows.Close()
			return 0, fmt.Errorf("scan column: %w", err)
		}
		cols = append(cols, c)
	}
	_ = colRows.Close()
	if err := colRows.Err(); err != nil {
		return 0, fmt.Errorf("iterate columns: %w", err)
	}

	if len(cols) == 0 {
		return 0, nil
	}

	// Write CREATE TABLE with quoted identifiers.
	quotedTable := quoteIdent(table)
	if _, err := fmt.Fprintf(f, "\n-- Table: %s\n", table); err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}
	if _, err := fmt.Fprintf(f, "CREATE TABLE IF NOT EXISTS %s (\n", quotedTable); err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}
	for i, c := range cols {
		nullStr := ""
		if c.nullable == "NO" {
			nullStr = " NOT NULL"
		}
		comma := ","
		if i == len(cols)-1 {
			comma = ""
		}
		if _, err := fmt.Fprintf(f, "  %s %s%s%s\n", quoteIdent(c.name), c.dataType, nullStr, comma); err != nil {
			return 0, fmt.Errorf("write: %w", err)
		}
	}
	if _, err := fmt.Fprintln(f, ");"); err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}

	// Build quoted column name list for SELECT and INSERT.
	quotedColNames := make([]string, len(cols))
	for i, c := range cols {
		quotedColNames[i] = quoteIdent(c.name)
	}
	quotedColList := strings.Join(quotedColNames, ", ")

	// Query all rows.
	dataRows, err := tx.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s", quotedColList, quotedTable))
	if err != nil {
		return 0, fmt.Errorf("select data: %w", err)
	}
	defer func() { _ = dataRows.Close() }()

	count := 0
	scanDest := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for dataRows.Next() {
		if err := dataRows.Scan(scanPtrs...); err != nil {
			return count, fmt.Errorf("scan row: %w", err)
		}

		vals := make([]string, len(cols))
		for i, v := range scanDest {
			vals[i] = formatSQLValue(v)
		}

		if _, err := fmt.Fprintf(f, "INSERT INTO %s (%s) VALUES (%s);\n",
			quotedTable, quotedColList, strings.Join(vals, ", ")); err != nil {
			return count, fmt.Errorf("write: %w", err)
		}
		count++
	}
	if err := dataRows.Err(); err != nil {
		return count, fmt.Errorf("iterate rows: %w", err)
	}

	return count, nil
}

// formatSQLValue formats a Go value as a SQL literal for INSERT statements.
// Note: []byte is treated as UTF-8 text (fine for DuckLake metadata which stores
// only text, integers, and booleans — no bytea columns).
func formatSQLValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case []byte:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(string(val), "'", "''"))
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format(time.RFC3339Nano))
	default:
		s := fmt.Sprintf("%v", val)
		return fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
	}
}

// injectPostgresKeepalive adds TCP keepalive parameters to a postgres metadata
// store connection string if they are not already present. Without keepalives,
// idle connections through AWS infrastructure (NAT gateways, security group
// connection tracking, NLBs) are silently dropped after ~350 seconds, causing
// "SSL connection has been closed unexpectedly" errors on the next query.
//
// Only modifies strings with the "postgres:" prefix. The keepalive parameters
// are standard libpq options passed through DuckDB's postgres scanner.
func injectPostgresKeepalive(metadataStore string) string {
	if !strings.HasPrefix(metadataStore, "postgres:") {
		return metadataStore
	}
	connPart := metadataStore[len("postgres:"):]
	// Don't override if the user has already set keepalive parameters.
	if strings.Contains(connPart, "keepalives") {
		return metadataStore
	}
	// keepalives_idle=60 sends the first probe after 60s of idle time, well
	// under the 350s AWS idle timeout. keepalives_interval=10 and
	// keepalives_count=5 detect a dead peer within ~110s total.
	return metadataStore + " keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5"
}

// buildDuckLakeAttachStmt builds the ATTACH statement for DuckLake.
// If migrate is true, adds AUTOMATIC_MIGRATION TRUE to the options.
func buildDuckLakeAttachStmt(dlCfg DuckLakeConfig, migrate bool) string {
	connStr := escapeSQLStringLiteral(injectPostgresKeepalive(dlCfg.MetadataStore))
	dataPath := dlCfg.ObjectStore
	if dataPath == "" {
		dataPath = dlCfg.DataPath
	}

	var options []string
	if dataPath != "" {
		options = append(options, fmt.Sprintf("DATA_PATH '%s'", escapeSQLStringLiteral(dataPath)))
	}
	if migrate {
		options = append(options, "AUTOMATIC_MIGRATION TRUE")
	}
	if dlCfg.DataInliningRowLimit != nil {
		options = append(options, fmt.Sprintf("DATA_INLINING_ROW_LIMIT %d", *dlCfg.DataInliningRowLimit))
	}

	if len(options) > 0 {
		return fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake (%s)",
			connStr, strings.Join(options, ", "))
	}
	return fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake", connStr)
}

// DefaultDeltaCatalogPath returns the default Delta Lake catalog location for a
// DuckLake-backed worker as a sibling of the DuckLake prefix at the same parent
// level. For s3://bucket/team/ducklake/ this returns s3://bucket/team/delta/, so
// per-tenant prefixes do not collapse to a shared bucket-root delta/.
func DefaultDeltaCatalogPath(dlCfg DuckLakeConfig) string {
	if dlCfg.ObjectStore != "" {
		return objectStoreParentPrefix(dlCfg.ObjectStore) + "delta/"
	}
	if dlCfg.DataPath != "" {
		return filepath.Join(filepath.Dir(filepath.Clean(dlCfg.DataPath)), "delta")
	}
	return ""
}

// objectStoreParentPrefix returns the parent directory of a URI-style object
// store path, preserving the trailing slash. For s3://bucket/team/ducklake/ it
// returns s3://bucket/team/; for s3://bucket/ducklake/ it returns s3://bucket/.
// A bare bucket (s3://bucket or s3://bucket/) is treated as its own parent.
func objectStoreParentPrefix(path string) string {
	schemeIdx := strings.Index(path, "://")
	var scheme string
	rest := path
	if schemeIdx >= 0 {
		scheme = path[:schemeIdx+len("://")]
		rest = path[schemeIdx+len("://"):]
	}
	rest = strings.TrimRight(rest, "/")
	if rest == "" {
		return scheme
	}
	if idx := strings.LastIndexByte(rest, '/'); idx >= 0 {
		return scheme + rest[:idx+1]
	}
	return scheme + rest + "/"
}

func deltaCatalogPath(dlCfg DuckLakeConfig) string {
	if dlCfg.DeltaCatalogPath != "" {
		return dlCfg.DeltaCatalogPath
	}
	return DefaultDeltaCatalogPath(dlCfg)
}

func buildDeltaCatalogAttachStmt(dlCfg DuckLakeConfig) string {
	return fmt.Sprintf("ATTACH '%s' AS delta (TYPE delta)", escapeSQLStringLiteral(deltaCatalogPath(dlCfg)))
}
