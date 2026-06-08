package transpiler

import "regexp"

// createOrReplaceTableRe matches a leading
// `CREATE OR REPLACE [TEMP|TEMPORARY|UNLOGGED] TABLE ...` after leading
// comments/whitespace are stripped.
//
// PostgreSQL has no `OR REPLACE` for TABLE (only VIEW/FUNCTION/etc.), so
// pg_query cannot parse this DuckDB-specific form. Without interception the
// transpiler would set FallbackToNative and conn.go would forward the SQL
// raw — skipping every transform, including the logical-catalog rewrite
// (logical database name -> physical catalog, e.g. `analytics` -> `ducklake`).
// On a multi-tenant worker the physical catalog is `ducklake`, so the
// un-rewritten logical name fails to bind ("Catalog \"analytics\" does not
// exist"). dbt-duckdb and SQLMesh both materialize tables with
// `CREATE OR REPLACE TABLE ... AS ...`, so this hit real tenants.
//
// `(?is)`: case-insensitive, and `.` spans newlines (the AS-SELECT body is
// usually multi-line).
var createOrReplaceTableRe = regexp.MustCompile(`(?is)^CREATE\s+OR\s+REPLACE\s+((?:(?:TEMP|TEMPORARY|UNLOGGED)\s+)?TABLE\b.*)$`)

// createPrefixRe captures the leading `CREATE ` keyword (with surrounding
// whitespace) so OR REPLACE can be re-inserted after it.
var createPrefixRe = regexp.MustCompile(`(?is)^(\s*CREATE\s+)(.*)$`)

// stripCreateOrReplaceTable detects `CREATE OR REPLACE TABLE ...`. On a match
// it returns the PostgreSQL-parseable `CREATE TABLE ...` form (with OR REPLACE
// removed) and ok=true; the caller transpiles that through the normal pipeline
// and then restores OR REPLACE via reinjectOrReplace. Returns ok=false for any
// other statement (including the PostgreSQL-valid `CREATE OR REPLACE VIEW`).
func stripCreateOrReplaceTable(sql string) (inner string, ok bool) {
	m := createOrReplaceTableRe.FindStringSubmatch(stripLeadingSQL(sql))
	if m == nil {
		return "", false
	}
	return "CREATE " + m[1], true
}

// reinjectOrReplace inserts `OR REPLACE` after the leading CREATE keyword of a
// transpiled `CREATE [TEMP] TABLE ...` statement, reversing the strip done by
// stripCreateOrReplaceTable.
func reinjectOrReplace(out string) string {
	m := createPrefixRe.FindStringSubmatch(out)
	if m == nil {
		return out
	}
	return m[1] + "OR REPLACE " + m[2]
}
