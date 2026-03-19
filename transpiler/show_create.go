package transpiler

import (
	"fmt"
	"strings"
)

// interceptShowCreate rewrites SHOW CREATE TABLE/VIEW [schema.]name into a
// DuckDB metadata query. This runs before pg_query parsing because these
// statements are not valid PostgreSQL syntax.
//
// Supported forms:
//   - SHOW CREATE TABLE my_table
//   - SHOW CREATE TABLE my_schema.my_table
//   - SHOW CREATE TABLE "MyTable"
//   - SHOW CREATE TABLE "my_schema"."MyTable"
//   - SHOW CREATE VIEW (same forms)
func interceptShowCreate(sql string) (string, bool) {
	trimmed := strings.TrimSpace(sql)
	trimmed = strings.TrimRight(trimmed, "; \t\n\r")
	upper := strings.ToUpper(trimmed)

	var rest string
	switch {
	case strings.HasPrefix(upper, "SHOW CREATE TABLE "):
		rest = strings.TrimSpace(trimmed[len("SHOW CREATE TABLE "):])
	case strings.HasPrefix(upper, "SHOW CREATE VIEW "):
		rest = strings.TrimSpace(trimmed[len("SHOW CREATE VIEW "):])
	default:
		return "", false
	}

	if rest == "" {
		return "", false
	}

	schema, table := parseShowCreateRef(rest)
	es, et := escapeStringLiteral(schema), escapeStringLiteral(table)

	// Query both duckdb_tables() and duckdb_views() so SHOW CREATE TABLE works
	// for views too (matching MySQL/ClickHouse behavior).
	return fmt.Sprintf(
		`SELECT sql AS statement FROM duckdb_tables() WHERE schema_name = '%s' AND table_name = '%s' UNION ALL SELECT sql AS statement FROM duckdb_views() WHERE schema_name = '%s' AND view_name = '%s'`,
		es, et, es, et,
	), true
}

// parseShowCreateRef splits a possibly schema-qualified, possibly double-quoted
// table reference into (schema, table). Returns "main" as default schema.
func parseShowCreateRef(ref string) (schema, table string) {
	ref = strings.TrimSpace(ref)

	dotIdx := findUnquotedDot(ref)
	if dotIdx >= 0 {
		schema = unquoteIdent(strings.TrimSpace(ref[:dotIdx]))
		table = unquoteIdent(strings.TrimSpace(ref[dotIdx+1:]))
	} else {
		schema = "main"
		table = unquoteIdent(ref)
	}

	// Map PostgreSQL "public" schema to DuckDB "main"
	if schema == "public" {
		schema = "main"
	}
	return
}

// findUnquotedDot returns the index of the first dot outside double quotes, or -1.
func findUnquotedDot(s string) int {
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case '.':
			if !inQuote {
				return i
			}
		}
	}
	return -1
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
