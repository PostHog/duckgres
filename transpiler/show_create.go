package transpiler

import (
	"fmt"
	"regexp"
	"strings"
)

// showCreateRe matches SHOW CREATE TABLE/VIEW with flexible whitespace
// after leading comments have been stripped.
var showCreateRe = regexp.MustCompile(`(?i)^SHOW\s+CREATE\s+(TABLE|VIEW)\s+(.+)$`)

// interceptShowCreate rewrites SHOW CREATE TABLE/VIEW [catalog.][schema.]name
// into a DuckDB metadata query. This runs before pg_query parsing because
// these statements are not valid PostgreSQL syntax.
//
// In DuckLake mode, the generated query also joins against DuckLake partition
// metadata to reconstruct the PARTITIONED BY clause (e.g., PARTITIONED BY
// (category, year(created_at))).
//
// Supported forms:
//   - SHOW CREATE TABLE my_table
//   - SHOW CREATE TABLE my_schema.my_table
//   - SHOW CREATE TABLE my_catalog.my_schema.my_table
//   - SHOW CREATE TABLE "MyTable"
//   - SHOW CREATE TABLE "my_schema"."MyTable"
//   - SHOW CREATE VIEW (same forms)
//
// Leading SQL comments (-- and /* */) and flexible whitespace are handled.
func interceptShowCreate(sql string, duckLakeMode bool) (string, bool) {
	// Strip leading comments and whitespace (matching hasAnyPrefix behavior)
	trimmed := stripLeadingSQL(sql)
	// Strip trailing semicolons and whitespace
	trimmed = strings.TrimRight(trimmed, "; \t\n\r")

	m := showCreateRe.FindStringSubmatch(trimmed)
	if m == nil {
		return "", false
	}

	ref := strings.TrimSpace(m[2])
	if ref == "" {
		return "", false
	}

	catalog, schema, table := parseShowCreateRef(ref)
	es, et := escapeStringLiteral(schema), escapeStringLiteral(table)

	// Build WHERE clause, optionally filtering by database
	where := fmt.Sprintf("schema_name = '%s'", es)
	if catalog != "" {
		where += fmt.Sprintf(" AND database_name = '%s'", escapeStringLiteral(catalog))
	}

	if duckLakeMode {
		return buildDuckLakeShowCreate(where, es, et), true
	}

	// Non-DuckLake: simple metadata lookup
	return fmt.Sprintf(
		`SELECT sql AS statement FROM duckdb_tables() WHERE %s AND table_name = '%s' `+
			`UNION ALL `+
			`SELECT sql AS statement FROM duckdb_views() WHERE %s AND view_name = '%s'`,
		where, et, where, et,
	), true
}

// buildDuckLakeShowCreate generates a query that enriches the base CREATE TABLE
// DDL with PARTITIONED BY from DuckLake metadata. The partition info is stored
// in ducklake_partition_info/ducklake_partition_column (inside the
// __ducklake_metadata_ducklake schema), not in duckdb_tables().sql.
//
// Note: the __ducklake_metadata_ducklake schema name derives from the catalog
// being attached as "ducklake" in server.go. If that name changes, update dlm.
//
// TODO: Add SORTED BY reconstruction when DuckLake adds sort metadata tables.
// DuckDB PR #20431 introduces SORTED BY grammar, but the current DuckLake
// extension does not yet expose sort metadata (no ducklake_sort_* tables).
//
// For non-partitioned tables, the base DDL is returned unchanged.
// For views, the duckdb_views() sql is returned as-is (no partition support).
func buildDuckLakeShowCreate(where, schema, table string) string {
	const dlm = `"__ducklake_metadata_ducklake"` // DuckLake metadata schema

	// CTE-based query:
	//   1. base: get raw CREATE TABLE DDL from duckdb_tables()
	//   2. partition_cols: join partition metadata to get ordered column+transform pairs
	//   3. partition_clause: aggregate into a single "col1, year(col2)" string
	//   4. Final SELECT: append PARTITIONED BY (...) if partition clause is non-empty
	//
	// UNION ALL with duckdb_views() for SHOW CREATE VIEW (views have no partitioning).
	return fmt.Sprintf(
		`WITH base AS (`+
			`SELECT sql FROM duckdb_tables() WHERE %s AND table_name = '%s'`+
			`), `+
			`partition_cols AS (`+
			`SELECT pc.partition_key_index, col.column_name, pc.transform `+
			`FROM %s.ducklake_partition_column pc `+
			`JOIN %s.ducklake_partition_info pi `+
			`ON pc.partition_id = pi.partition_id AND pc.table_id = pi.table_id `+
			`JOIN %s.ducklake_table t `+
			`ON pi.table_id = t.table_id `+
			`JOIN %s.ducklake_schema s `+
			`ON t.schema_id = s.schema_id `+
			`JOIN %s.ducklake_column col `+
			`ON pc.column_id = col.column_id AND pc.table_id = col.table_id `+
			`WHERE t.table_name = '%s' AND s.schema_name = '%s' `+
			`AND pi.end_snapshot IS NULL AND col.end_snapshot IS NULL `+
			`ORDER BY pc.partition_key_index`+
			`), `+
			`partition_clause AS (`+
			`SELECT string_agg(`+
			`CASE WHEN transform = 'identity' THEN column_name `+
			`ELSE transform || '(' || column_name || ')' END, `+
			`', ' ORDER BY partition_key_index) AS clause `+
			`FROM partition_cols`+
			`) `+
			`SELECT CASE `+
			`WHEN pc.clause IS NOT NULL AND pc.clause != '' `+
			`THEN regexp_replace(b.sql, ';\s*$', '') || ' PARTITIONED BY (' || pc.clause || ');' `+
			`ELSE b.sql END AS statement `+
			`FROM base b CROSS JOIN partition_clause pc `+
			`UNION ALL `+
			`SELECT sql AS statement FROM duckdb_views() WHERE %s AND view_name = '%s'`,
		// base CTE
		where, table,
		// partition_cols CTE - 5x dlm for the 5 metadata table references
		dlm, dlm, dlm, dlm, dlm,
		// partition_cols WHERE
		table, schema,
		// views UNION
		where, table,
	)
}

// parseShowCreateRef splits a possibly catalog- and schema-qualified, possibly
// double-quoted table reference into (catalog, schema, table).
// Defaults: catalog="" (omitted from filter), schema="main".
func parseShowCreateRef(ref string) (catalog, schema, table string) {
	ref = strings.TrimSpace(ref)
	dots := findAllUnquotedDots(ref)

	switch len(dots) {
	case 0:
		// table
		schema = "main"
		table = unquoteIdent(ref)
	case 1:
		// schema.table
		schema = unquoteIdent(ref[:dots[0]])
		table = unquoteIdent(ref[dots[0]+1:])
	case 2:
		// catalog.schema.table
		catalog = unquoteIdent(ref[:dots[0]])
		schema = unquoteIdent(ref[dots[0]+1 : dots[1]])
		table = unquoteIdent(ref[dots[1]+1:])
	default:
		// Too many parts — treat as plain table name (will likely not match anything)
		schema = "main"
		table = unquoteIdent(ref)
	}

	// Map PostgreSQL "public" schema to DuckDB "main"
	if schema == "public" {
		schema = "main"
	}
	return
}

// stripLeadingSQL strips leading whitespace, line comments (--), and block
// comments (/* ... */) from SQL. Mirrors the logic in hasAnyPrefix.
func stripLeadingSQL(s string) string {
	s = strings.TrimLeft(s, " \t\n\r")
	for {
		if strings.HasPrefix(s, "--") {
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = strings.TrimLeft(s[nl+1:], " \t\n\r")
		} else if strings.HasPrefix(s, "/*") {
			end := strings.Index(s, "*/")
			if end < 0 {
				return ""
			}
			s = strings.TrimLeft(s[end+2:], " \t\n\r")
		} else {
			return s
		}
	}
}

// findAllUnquotedDots returns indices of all dots outside double-quoted identifiers.
func findAllUnquotedDots(s string) []int {
	var dots []int
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case '.':
			if !inQuote {
				dots = append(dots, i)
			}
		}
	}
	return dots
}

// unquoteIdent strips double quotes and preserves case, or lowercases unquoted identifiers.
func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return strings.ToLower(s)
}

// escapeStringLiteral escapes single quotes for safe embedding in SQL string literals.
func escapeStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
