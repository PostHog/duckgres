package server

import (
	"database/sql"
	"regexp"
	"strings"
)

// initPgCatalog creates PostgreSQL compatibility functions and views in DuckDB
// DuckDB already has a pg_catalog schema with basic views, so we just add missing functions
func initPgCatalog(db *sql.DB) error {
	// Create our own pg_database view that has all the columns psql expects
	// We put it in main schema and rewrite queries to use it
	pgDatabaseSQL := `
		CREATE OR REPLACE VIEW pg_database AS
		SELECT
			1::INTEGER AS oid,
			current_database() AS datname,
			0::INTEGER AS datdba,
			6::INTEGER AS encoding,
			'en_US.UTF-8' AS datcollate,
			'en_US.UTF-8' AS datctype,
			false AS datistemplate,
			true AS datallowconn,
			-1::INTEGER AS datconnlimit,
			NULL AS datacl
	`
	db.Exec(pgDatabaseSQL)

	// Create helper macros/functions that psql expects but DuckDB doesn't have
	// These need to be created without schema prefix so DuckDB finds them
	functions := []string{
		// pg_get_userbyid - returns username for a role OID
		`CREATE OR REPLACE MACRO pg_get_userbyid(id) AS 'duckdb'`,
		// pg_table_is_visible - checks if table is in search path
		`CREATE OR REPLACE MACRO pg_table_is_visible(oid) AS true`,
		// has_schema_privilege - check schema access
		`CREATE OR REPLACE MACRO has_schema_privilege(schema, priv) AS true`,
		`CREATE OR REPLACE MACRO has_schema_privilege(u, schema, priv) AS true`,
		// has_table_privilege - check table access
		`CREATE OR REPLACE MACRO has_table_privilege(table_name, priv) AS true`,
		`CREATE OR REPLACE MACRO has_table_privilege(u, table_name, priv) AS true`,
		// pg_encoding_to_char - convert encoding ID to name
		`CREATE OR REPLACE MACRO pg_encoding_to_char(enc) AS 'UTF8'`,
		// format_type - format a type OID as string
		`CREATE OR REPLACE MACRO format_type(type_oid, typemod) AS
			CASE type_oid
				WHEN 16 THEN 'boolean'
				WHEN 17 THEN 'bytea'
				WHEN 20 THEN 'bigint'
				WHEN 21 THEN 'smallint'
				WHEN 23 THEN 'integer'
				WHEN 25 THEN 'text'
				WHEN 700 THEN 'real'
				WHEN 701 THEN 'double precision'
				WHEN 1042 THEN 'character'
				WHEN 1043 THEN 'character varying'
				WHEN 1082 THEN 'date'
				WHEN 1083 THEN 'time'
				WHEN 1114 THEN 'timestamp'
				WHEN 1184 THEN 'timestamp with time zone'
				WHEN 1700 THEN 'numeric'
				WHEN 2950 THEN 'uuid'
				ELSE 'unknown'
			END`,
		// obj_description - get object comment
		`CREATE OR REPLACE MACRO obj_description(oid, catalog) AS NULL`,
		// col_description - get column comment
		`CREATE OR REPLACE MACRO col_description(table_oid, col_num) AS NULL`,
		// shobj_description - get shared object comment
		`CREATE OR REPLACE MACRO shobj_description(oid, catalog) AS NULL`,
		// pg_get_indexdef - get index definition
		`CREATE OR REPLACE MACRO pg_get_indexdef(index_oid) AS ''`,
		`CREATE OR REPLACE MACRO pg_get_indexdef(index_oid, col, pretty) AS ''`,
		// pg_get_constraintdef - get constraint definition
		`CREATE OR REPLACE MACRO pg_get_constraintdef(constraint_oid) AS ''`,
		`CREATE OR REPLACE MACRO pg_get_constraintdef(constraint_oid, pretty) AS ''`,
		// current_setting - get config setting
		`CREATE OR REPLACE MACRO current_setting(name) AS
			CASE name
				WHEN 'server_version' THEN '15.0'
				WHEN 'server_encoding' THEN 'UTF8'
				ELSE ''
			END`,
		// pg_is_in_recovery - check if in recovery mode
		`CREATE OR REPLACE MACRO pg_is_in_recovery() AS false`,
	}

	for _, f := range functions {
		if _, err := db.Exec(f); err != nil {
			// Log but don't fail - some might already exist or conflict
			continue
		}
	}

	return nil
}

// pgCatalogFunctions is the list of functions we provide that psql calls with pg_catalog. prefix
var pgCatalogFunctions = []string{
	"pg_get_userbyid",
	"pg_table_is_visible",
	"pg_get_expr",
	"pg_encoding_to_char",
	"format_type",
	"obj_description",
	"col_description",
	"shobj_description",
	"pg_get_indexdef",
	"pg_get_constraintdef",
	"current_setting",
	"pg_is_in_recovery",
	"has_schema_privilege",
	"has_table_privilege",
	"array_to_string",
}

// pgCatalogFuncRegex matches pg_catalog.function_name( patterns
var pgCatalogFuncRegex *regexp.Regexp

func init() {
	// Build regex to match pg_catalog.func_name patterns
	funcPattern := strings.Join(pgCatalogFunctions, "|")
	pgCatalogFuncRegex = regexp.MustCompile(`(?i)pg_catalog\.(` + funcPattern + `)\s*\(`)
}

// Regex patterns for query rewriting
var (
	// OPERATOR(pg_catalog.~) -> ~
	operatorRegex = regexp.MustCompile(`(?i)OPERATOR\s*\(\s*pg_catalog\.([~!<>=]+)\s*\)`)
	// COLLATE pg_catalog.default -> (remove)
	collateRegex = regexp.MustCompile(`(?i)\s+COLLATE\s+pg_catalog\."?default"?`)
	// pg_catalog.pg_database -> pg_database (use our view)
	pgDatabaseRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_database`)
	// ::pg_catalog.regtype::pg_catalog.text -> ::VARCHAR (PostgreSQL type cast)
	regtypeTextCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.regtype::pg_catalog\.text`)
	// ::pg_catalog.regtype -> ::VARCHAR
	regtypeCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.regtype`)
	// ::pg_catalog.text -> ::VARCHAR
	textCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.text`)
)

// rewritePgCatalogQuery rewrites PostgreSQL-specific syntax for DuckDB compatibility
func rewritePgCatalogQuery(query string) string {
	// Replace pg_catalog.func_name( with func_name(
	query = pgCatalogFuncRegex.ReplaceAllString(query, "$1(")

	// Replace OPERATOR(pg_catalog.~) with just ~
	query = operatorRegex.ReplaceAllString(query, "$1")

	// Remove COLLATE pg_catalog.default
	query = collateRegex.ReplaceAllString(query, "")

	// Replace pg_catalog.pg_database with pg_database (our view in main schema)
	query = pgDatabaseRegex.ReplaceAllString(query, "pg_database")

	// Replace PostgreSQL type casts (order matters - most specific first)
	query = regtypeTextCastRegex.ReplaceAllString(query, "::VARCHAR")
	query = regtypeCastRegex.ReplaceAllString(query, "::VARCHAR")
	query = textCastRegex.ReplaceAllString(query, "::VARCHAR")

	return query
}

