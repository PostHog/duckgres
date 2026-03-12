package duckdbservice

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/posthog/duckgres/server"
)

// Allowlists of objects created during DuckDB warmup (initPgCatalog,
// initClickHouseMacros, initInformationSchema, openBaseDB, AttachDuckLake).
// Objects not in these lists are considered user-created and will be
// dropped during session reset to prevent state leakage between sessions.
//
// All names are lowercase because DuckDB folds unquoted identifiers to lowercase.

var systemMacros = map[string]bool{
	// PostgreSQL compatibility (server/catalog.go initPgCatalog)
	"pg_get_userbyid":                  true,
	"pg_table_is_visible":              true,
	"has_schema_privilege":             true,
	"has_table_privilege":              true,
	"has_any_column_privilege":         true,
	"has_database_privilege":           true,
	"pg_encoding_to_char":             true,
	"format_type":                      true,
	"obj_description":                  true,
	"col_description":                  true,
	"shobj_description":                true,
	"pg_get_indexdef":                  true,
	"pg_get_partkeydef":               true,
	"pg_get_serial_sequence":           true,
	"pg_get_statisticsobjdef_columns": true,
	"pg_relation_is_publishable":       true,
	"current_setting":                  true,
	"pg_is_in_recovery":               true,
	"similar_to_escape":               true,
	"version":                          true,
	"div":                              true,
	"array_remove":                     true,
	"to_number":                        true,
	"pg_backend_pid":                   true,
	"pg_total_relation_size":           true,
	"pg_relation_size":                 true,
	"pg_table_size":                    true,
	"pg_stat_get_numscans":            true,
	"pg_indexes_size":                  true,
	"pg_database_size":                 true,
	"pg_size_pretty":                   true,
	"txid_current":                     true,
	"pg_current_xact_id":              true,
	"quote_ident":                      true,
	"quote_literal":                    true,
	"quote_nullable":                   true,
	// Utility macros (server/catalog.go initUtilityMacros)
	"uptime":                true,
	"worker_uptime":         true,
	"control_plane_version": true,
	"worker_version":        true,
	// ClickHouse compatibility (server/chsql.go)
	"tostring":          true,
	"toint32":           true,
	"toint64":           true,
	"tofloat":           true,
	"toint32ornull":     true,
	"toint32orzero":     true,
	"intdiv":            true,
	"modulo":            true,
	"empty":             true,
	"notempty":          true,
	"splitbychar":       true,
	"lengthutf8":        true,
	"toyear":            true,
	"tomonth":           true,
	"todayofmonth":      true,
	"toyyyymmdd":        true,
	"toyyyymm":          true,
	"protocol":          true,
	"domain":            true,
	"topleveldomain":    true,
	"ipv4numtostring":   true,
	"jsonextractstring": true,
	"jsonhas":           true,
	"generateuuidv4":    true,
	"ifnull":            true,
}

var systemViews = map[string]bool{
	// pg_catalog views (server/catalog.go initPgCatalog)
	"pg_database":           true,
	"pg_class_full":         true,
	"pg_collation":          true,
	"pg_policy":             true,
	"pg_roles":              true,
	"pg_statistic_ext":      true,
	"pg_publication_tables": true,
	"pg_rules":              true,
	"pg_publication":        true,
	"pg_publication_rel":    true,
	"pg_inherits":           true,
	"pg_matviews":           true,
	"pg_stat_statements":    true,
	"pg_partitioned_table":  true,
	"pg_rewrite":            true,
	"pg_stat_user_tables":   true,
	"pg_statio_user_tables": true,
	"pg_stat_activity":      true,
	"pg_namespace":          true,
	"pg_type":               true,
	"pg_attribute":          true,
	"pg_constraint":         true,
	"pg_enum":               true,
	"pg_indexes":            true,
	"pg_shdescription":      true,
	"pg_extension":          true,
	// Stub views
	"pg_auth_members":         true,
	"pg_opclass":              true,
	"pg_conversion":           true,
	"pg_language":             true,
	"pg_foreign_server":       true,
	"pg_foreign_data_wrapper": true,
	"pg_foreign_table":        true,
	"pg_trigger":              true,
	"pg_locks":                true,
	// Information schema wrappers (server/catalog.go initInformationSchema)
	"information_schema_columns_compat":  true,
	"information_schema_tables_compat":   true,
	"information_schema_schemata_compat": true,
	"information_schema_views_compat":    true,
}

var systemTables = map[string]bool{
	"__duckgres_column_metadata": true,
}

var systemDatabases = map[string]bool{
	"memory":   true,
	"system":   true,
	"temp":     true,
	"ducklake": true,
}

var systemSchemas = map[string]bool{
	"main":               true,
	"pg_catalog":         true,
	"information_schema": true,
}

var systemSecrets = map[string]bool{
	"ducklake_s3": true,
}

// resetSessionState performs exhaustive in-place cleanup of the shared DuckDB
// instance after a session ends. It drops all user-created state (tables, views,
// macros, settings, temp objects, attached databases) while preserving the
// warmup-created objects via allowlists. This avoids the ~90ms cost of closing
// and reopening the DuckDB instance.
func (p *SessionPool) resetSessionState(db *sql.DB) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reset session state: %w", err)
	}
	defer func() { _ = conn.Close() }()

	start := time.Now()

	// 1. RESET all DuckDB settings to defaults.
	resetAllSettings(ctx, conn)

	// 2. Drop all temporary objects.
	dropTempObjects(ctx, conn)

	// 3. Drop user-created objects in memory.main not in allowlists.
	dropUserObjects(ctx, conn)

	// 4. Drop user-created schemas in memory.
	dropUserSchemas(ctx, conn)

	// 5. Detach user-attached databases.
	detachUserDatabases(ctx, conn)

	// 6. Drop user-created secrets.
	dropUserSecrets(ctx, conn)

	// 7. Re-apply warmup settings (threads, memory_limit, paths, DuckLake).
	p.reapplySettings(ctx, conn)

	slog.Info("Session state reset.", "duration", time.Since(start))
	return nil
}

// resetAllSettings resets all DuckDB settings to their defaults.
// Read-only settings will fail to reset; errors are silently ignored.
func resetAllSettings(ctx context.Context, conn *sql.Conn) {
	rows, err := conn.QueryContext(ctx, "SELECT name FROM duckdb_settings()")
	if err != nil {
		slog.Warn("Failed to query settings for reset.", "error", err)
		return
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		names = append(names, name)
	}

	for _, name := range names {
		_, _ = conn.ExecContext(ctx, "RESET "+name)
	}
}

// dropTempObjects drops all tables and views in the temp database.
func dropTempObjects(ctx context.Context, conn *sql.Conn) {
	// Views first (they may reference tables).
	for _, v := range queryPairs(ctx, conn,
		"SELECT schema_name, view_name FROM duckdb_views() WHERE database_name = 'temp'") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS temp."%s"."%s" CASCADE`, v[0], v[1])); err != nil {
			slog.Warn("Failed to drop temp view.", "view", v[1], "error", err)
		}
	}
	for _, t := range queryPairs(ctx, conn,
		"SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'temp'") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS temp."%s"."%s" CASCADE`, t[0], t[1])); err != nil {
			slog.Warn("Failed to drop temp table.", "table", t[1], "error", err)
		}
	}
}

// dropUserObjects drops non-allowlisted views, tables, macros, sequences, and
// types in the memory.main schema.
func dropUserObjects(ctx context.Context, conn *sql.Conn) {
	// Views
	for _, v := range queryPairs(ctx, conn,
		"SELECT schema_name, view_name FROM duckdb_views() WHERE database_name = 'memory' AND schema_name = 'main'") {
		if !systemViews[strings.ToLower(v[1])] {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS memory.main."%s" CASCADE`, v[1])); err != nil {
				slog.Warn("Failed to drop user view.", "view", v[1], "error", err)
			}
		}
	}

	// Tables
	for _, t := range queryPairs(ctx, conn,
		"SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'memory' AND schema_name = 'main'") {
		if !systemTables[strings.ToLower(t[1])] {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS memory.main."%s" CASCADE`, t[1])); err != nil {
				slog.Warn("Failed to drop user table.", "table", t[1], "error", err)
			}
		}
	}

	// Macros (scalar and table macros)
	for _, m := range queryMacros(ctx, conn) {
		if !systemMacros[strings.ToLower(m.name)] {
			stmt := fmt.Sprintf(`DROP MACRO IF EXISTS "%s"`, m.name)
			if m.isTable {
				stmt = fmt.Sprintf(`DROP MACRO TABLE IF EXISTS "%s"`, m.name)
			}
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				slog.Warn("Failed to drop user macro.", "macro", m.name, "error", err)
			}
		}
	}

	// Sequences
	for _, s := range queryPairs(ctx, conn,
		"SELECT schema_name, sequence_name FROM duckdb_sequences() WHERE database_name = 'memory' AND schema_name = 'main'") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP SEQUENCE IF EXISTS memory.main."%s"`, s[1])); err != nil {
			slog.Warn("Failed to drop user sequence.", "sequence", s[1], "error", err)
		}
	}

	// User-defined types
	for _, t := range queryPairs(ctx, conn,
		"SELECT schema_name, type_name FROM duckdb_types() WHERE database_name = 'memory' AND schema_name = 'main' AND internal = false") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP TYPE IF EXISTS memory.main."%s"`, t[1])); err != nil {
			slog.Warn("Failed to drop user type.", "type", t[1], "error", err)
		}
	}
}

// dropUserSchemas drops schemas in memory that aren't in the system allowlist.
func dropUserSchemas(ctx context.Context, conn *sql.Conn) {
	for _, s := range queryPairs(ctx, conn,
		"SELECT database_name, schema_name FROM duckdb_schemas() WHERE database_name = 'memory'") {
		if !systemSchemas[strings.ToLower(s[1])] {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS memory."%s" CASCADE`, s[1])); err != nil {
				slog.Warn("Failed to drop user schema.", "schema", s[1], "error", err)
			}
		}
	}
}

// detachUserDatabases detaches databases not in the system allowlist.
func detachUserDatabases(ctx context.Context, conn *sql.Conn) {
	for _, d := range queryPairs(ctx, conn,
		"SELECT database_name, database_name FROM duckdb_databases()") {
		if !systemDatabases[strings.ToLower(d[0])] {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DETACH "%s"`, d[0])); err != nil {
				slog.Warn("Failed to detach user database.", "database", d[0], "error", err)
			}
		}
	}
}

// dropUserSecrets drops secrets not in the system allowlist.
func dropUserSecrets(ctx context.Context, conn *sql.Conn) {
	rows, err := conn.QueryContext(ctx, "SELECT name FROM duckdb_secrets()")
	if err != nil {
		return // duckdb_secrets() may not exist
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		if !systemSecrets[strings.ToLower(name)] {
			names = append(names, name)
		}
	}

	for _, name := range names {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP SECRET IF EXISTS "%s"`, name)); err != nil {
			slog.Warn("Failed to drop user secret.", "secret", name, "error", err)
		}
	}
}

// reapplySettings re-applies the warmup settings that were cleared by RESET.
func (p *SessionPool) reapplySettings(ctx context.Context, conn *sql.Conn) {
	// threads
	threads := p.cfg.Threads
	if threads == 0 {
		threads = runtime.NumCPU() * 2
	}
	_, _ = conn.ExecContext(ctx, fmt.Sprintf("SET threads = %d", threads))

	// memory_limit
	memLimit := p.cfg.MemoryLimit
	if memLimit == "" {
		memLimit = server.AutoMemoryLimit()
	}
	_, _ = conn.ExecContext(ctx, fmt.Sprintf("SET memory_limit = '%s'", memLimit))

	// temp_directory
	tempDir := filepath.Join(p.cfg.DataDir, "tmp")
	_, _ = conn.ExecContext(ctx, fmt.Sprintf("SET temp_directory = '%s'", tempDir))

	// extension_directory
	extDir := filepath.Join(p.cfg.DataDir, "extensions")
	_, _ = conn.ExecContext(ctx, fmt.Sprintf("SET extension_directory = '%s'", extDir))

	// cache_httpfs_cache_directory (if the extension is loaded)
	for _, ext := range p.cfg.Extensions {
		name := ext
		if idx := strings.Index(name, ":"); idx >= 0 {
			name = name[:idx]
		}
		if name == "cache_httpfs" {
			cacheDir := filepath.Join(p.cfg.DataDir, "cache")
			_, _ = conn.ExecContext(ctx, fmt.Sprintf("SET cache_httpfs_cache_directory = '%s/'", cacheDir))
			break
		}
	}

	// DuckLake settings
	if p.cfg.DuckLake.MetadataStore != "" {
		_, _ = conn.ExecContext(ctx, "SET ducklake_max_retry_count = 100")
		_, _ = conn.ExecContext(ctx, "USE ducklake")
	}
}

type macroInfo struct {
	name    string
	isTable bool
}

// queryMacros returns user-created macros (scalar and table) in memory.main.
func queryMacros(ctx context.Context, conn *sql.Conn) []macroInfo {
	rows, err := conn.QueryContext(ctx,
		"SELECT DISTINCT function_name, function_type FROM duckdb_functions() "+
			"WHERE database_name = 'memory' AND schema_name = 'main' "+
			"AND function_type IN ('macro', 'table_macro')")
	if err != nil {
		slog.Warn("Failed to query macros.", "error", err)
		return nil
	}
	defer func() { _ = rows.Close() }()

	var macros []macroInfo
	for rows.Next() {
		var name, ftype string
		if err := rows.Scan(&name, &ftype); err != nil {
			continue
		}
		macros = append(macros, macroInfo{name: name, isTable: ftype == "table_macro"})
	}
	return macros
}

// queryPairs runs a two-column query and returns the results.
func queryPairs(ctx context.Context, conn *sql.Conn, query string) [][2]string {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		slog.Debug("Catalog query returned no results.", "query", query, "error", err)
		return nil
	}
	defer func() { _ = rows.Close() }()

	var results [][2]string
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			continue
		}
		results = append(results, [2]string{a, b})
	}
	return results
}
