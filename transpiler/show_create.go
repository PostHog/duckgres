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
// Supported forms:
//   - SHOW CREATE TABLE my_table
//   - SHOW CREATE TABLE my_schema.my_table
//   - SHOW CREATE TABLE my_catalog.my_schema.my_table
//   - SHOW CREATE TABLE "MyTable"
//   - SHOW CREATE TABLE "my_schema"."MyTable"
//   - SHOW CREATE VIEW (same forms)
//
// Leading SQL comments (-- and /* */) and flexible whitespace are handled.
func interceptShowCreate(sql string) (string, bool) {
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
	et := escapeStringLiteral(table)

	// Build WHERE clause, optionally filtering by database
	where := fmt.Sprintf("schema_name = '%s'", escapeStringLiteral(schema))
	if catalog != "" {
		where += fmt.Sprintf(" AND database_name = '%s'", escapeStringLiteral(catalog))
	}

	// Query both duckdb_tables() and duckdb_views() so SHOW CREATE TABLE works
	// for views too (matching MySQL/ClickHouse behavior).
	return fmt.Sprintf(
		`SELECT sql AS statement FROM duckdb_tables() WHERE %s AND table_name = '%s' `+
			`UNION ALL `+
			`SELECT sql AS statement FROM duckdb_views() WHERE %s AND view_name = '%s'`,
		where, et, where, et,
	), true
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
