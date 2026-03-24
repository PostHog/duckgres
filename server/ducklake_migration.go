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

// duckLakeSpecVersion is the DuckLake spec version that this build of duckgres expects.
// When the metadata store is at an older version, we backup and migrate automatically.
const duckLakeSpecVersion = "0.4"

// dlMigration holds the result of the one-time migration check.
// The check runs at most once per process (sync.Once).
//
// In multitenant control-plane mode, each worker process serves a single tenant
// with its own metadata store, so the per-process sync.Once is correct.
// If this changes (multiple metadata stores per process), this must be replaced
// with a sync.Map keyed by metadata store connection string.
var dlMigration struct {
	once     sync.Once
	needed   bool   // true if metadata store version < duckLakeSpecVersion
	err      error  // non-nil if the check or backup failed
	checkedV string // the version found in the metadata store
}

// ensureDuckLakeMigrationCheck runs the migration check exactly once.
// If migration is needed, it backs up the metadata store before returning.
// The backup file is written to dataDir.
//
// This should be called BEFORE acquiring the DuckLake attachment semaphore,
// since the backup can take minutes for large metadata stores.
func ensureDuckLakeMigrationCheck(dlCfg DuckLakeConfig, dataDir string) {
	dlMigration.once.Do(func() {
		dlMigration.needed, dlMigration.checkedV, dlMigration.err = checkAndBackupIfNeeded(dlCfg, dataDir)
	})
}

// duckLakeMigrationNeeded returns whether the ATTACH statement should include
// AUTOMATIC_MIGRATION TRUE. Safe to call after ensureDuckLakeMigrationCheck.
func duckLakeMigrationNeeded() bool {
	return dlMigration.needed && dlMigration.err == nil
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
func checkAndBackupIfNeeded(dlCfg DuckLakeConfig, dataDir string) (needed bool, version string, err error) {
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

	slog.Info("DuckLake metadata store version detected.", "version", ver, "expected", duckLakeSpecVersion)

	less, err := versionLessThan(ver, duckLakeSpecVersion)
	if err != nil {
		return false, ver, fmt.Errorf("compare DuckLake versions: %w", err)
	}
	if !less {
		return false, ver, nil
	}

	// Migration needed — backup first.
	slog.Info("DuckLake metadata migration required. Backing up metadata store before upgrade.",
		"from", ver, "to", duckLakeSpecVersion)

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

	// Discover all ducklake_* tables.
	rows, err := pgDB.QueryContext(ctx,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'ducklake_%' ORDER BY table_name")
	if err != nil {
		return fmt.Errorf("list ducklake tables: %w", err)
	}

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	rows.Close()
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
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()

	// Write header.
	fmt.Fprintf(f, "-- DuckLake metadata backup before migration (v%s → v%s)\n", version, duckLakeSpecVersion)
	fmt.Fprintf(f, "-- Generated: %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(f, "-- Tables: %d\n\n", len(tables))
	fmt.Fprintln(f, "BEGIN;")

	totalRows := 0
	for _, table := range tables {
		count, err := backupTable(ctx, pgDB, f, table)
		if err != nil {
			return fmt.Errorf("backup table %s: %w", table, err)
		}
		totalRows += count
	}

	fmt.Fprintln(f, "\nCOMMIT;")

	// Flush to disk before closing — this is a critical safety net file.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync backup file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close backup file: %w", err)
	}
	closed = true

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
func backupTable(ctx context.Context, pgDB *sql.DB, f *os.File, table string) (int, error) {
	// Get column definitions.
	colRows, err := pgDB.QueryContext(ctx,
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
			colRows.Close()
			return 0, fmt.Errorf("scan column: %w", err)
		}
		cols = append(cols, c)
	}
	colRows.Close()
	if err := colRows.Err(); err != nil {
		return 0, fmt.Errorf("iterate columns: %w", err)
	}

	if len(cols) == 0 {
		return 0, nil
	}

	// Write CREATE TABLE with quoted identifiers.
	quotedTable := quoteIdent(table)
	fmt.Fprintf(f, "\n-- Table: %s\n", table)
	fmt.Fprintf(f, "CREATE TABLE IF NOT EXISTS %s (\n", quotedTable)
	for i, c := range cols {
		nullStr := ""
		if c.nullable == "NO" {
			nullStr = " NOT NULL"
		}
		comma := ","
		if i == len(cols)-1 {
			comma = ""
		}
		fmt.Fprintf(f, "  %s %s%s%s\n", quoteIdent(c.name), c.dataType, nullStr, comma)
	}
	fmt.Fprintln(f, ");")

	// Build quoted column name list for SELECT and INSERT.
	quotedColNames := make([]string, len(cols))
	for i, c := range cols {
		quotedColNames[i] = quoteIdent(c.name)
	}
	quotedColList := strings.Join(quotedColNames, ", ")

	// Query all rows.
	dataRows, err := pgDB.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s", quotedColList, quotedTable))
	if err != nil {
		return 0, fmt.Errorf("select data: %w", err)
	}
	defer dataRows.Close()

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

		fmt.Fprintf(f, "INSERT INTO %s (%s) VALUES (%s);\n",
			quotedTable, quotedColList, strings.Join(vals, ", "))
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

// buildDuckLakeAttachStmt builds the ATTACH statement for DuckLake.
// If migrate is true, adds AUTOMATIC_MIGRATION TRUE to the options.
func buildDuckLakeAttachStmt(dlCfg DuckLakeConfig, migrate bool) string {
	connStr := escapeSQLStringLiteral(dlCfg.MetadataStore)
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

	if len(options) > 0 {
		return fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake (%s)",
			connStr, strings.Join(options, ", "))
	}
	return fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake", connStr)
}
