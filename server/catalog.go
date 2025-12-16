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

	// Create pg_class wrapper that adds missing columns psql expects
	// DuckDB's pg_catalog.pg_class is missing relforcerowsecurity
	pgClassSQL := `
		CREATE OR REPLACE VIEW pg_class_full AS
		SELECT
			oid,
			relname,
			relnamespace,
			reltype,
			reloftype,
			relowner,
			relam,
			relfilenode,
			reltablespace,
			relpages,
			reltuples,
			relallvisible,
			reltoastrelid,
			reltoastidxid,
			relhasindex,
			relisshared,
			relpersistence,
			relkind,
			relnatts,
			relchecks,
			relhasoids,
			relhaspkey,
			relhasrules,
			relhastriggers,
			relhassubclass,
			relrowsecurity,
			false AS relforcerowsecurity,
			relispopulated,
			relreplident,
			relispartition,
			relrewrite,
			relfrozenxid,
			relminmxid,
			relacl,
			reloptions,
			relpartbound
		FROM pg_catalog.pg_class
	`
	db.Exec(pgClassSQL)

	// Create pg_collation view (DuckDB doesn't have this)
	pgCollationSQL := `
		CREATE OR REPLACE VIEW pg_collation AS
		SELECT
			0::BIGINT AS oid,
			'default' AS collname,
			0::BIGINT AS collnamespace,
			0::INTEGER AS collowner,
			'c' AS collprovider,
			true AS collisdeterministic,
			0::INTEGER AS collencoding,
			'C' AS collcollate,
			'C' AS collctype,
			NULL AS collversion
		WHERE false
	`
	db.Exec(pgCollationSQL)

	// Create pg_policy view for row-level security (empty, DuckDB doesn't support RLS)
	pgPolicySQL := `
		CREATE OR REPLACE VIEW pg_policy AS
		SELECT
			0::BIGINT AS oid,
			'' AS polname,
			0::BIGINT AS polrelid,
			'*' AS polcmd,
			true AS polpermissive,
			ARRAY[]::BIGINT[] AS polroles,
			NULL AS polqual,
			NULL AS polwithcheck
		WHERE false
	`
	db.Exec(pgPolicySQL)

	// Create pg_roles view (minimal for psql compatibility)
	pgRolesSQL := `
		CREATE OR REPLACE VIEW pg_roles AS
		SELECT
			0::BIGINT AS oid,
			'duckdb' AS rolname,
			true AS rolsuper,
			true AS rolinherit,
			true AS rolcreaterole,
			true AS rolcreatedb,
			true AS rolcanlogin,
			false AS rolreplication,
			false AS rolbypassrls,
			-1::INTEGER AS rolconnlimit,
			NULL AS rolpassword,
			NULL AS rolvaliduntil,
			ARRAY[]::VARCHAR[] AS rolconfig
	`
	db.Exec(pgRolesSQL)

	// Create pg_statistic_ext view (extended statistics, empty)
	pgStatisticExtSQL := `
		CREATE OR REPLACE VIEW pg_statistic_ext AS
		SELECT
			0::BIGINT AS oid,
			0::BIGINT AS stxrelid,
			0::BIGINT AS stxnamespace,
			'' AS stxname,
			0::INTEGER AS stxowner,
			0::INTEGER AS stxstattarget,
			ARRAY[]::VARCHAR[] AS stxkeys,
			ARRAY[]::VARCHAR[] AS stxkind
		WHERE false
	`
	db.Exec(pgStatisticExtSQL)

	// Create pg_publication_tables view (logical replication, empty)
	pgPublicationTablesSQL := `
		CREATE OR REPLACE VIEW pg_publication_tables AS
		SELECT
			'' AS pubname,
			'' AS schemaname,
			'' AS tablename
		WHERE false
	`
	db.Exec(pgPublicationTablesSQL)

	// Create pg_rules view (empty, DuckDB doesn't have rules)
	pgRulesSQL := `
		CREATE OR REPLACE VIEW pg_rules AS
		SELECT
			'' AS schemaname,
			'' AS tablename,
			'' AS rulename,
			'' AS definition
		WHERE false
	`
	db.Exec(pgRulesSQL)

	// Create pg_publication view (logical replication, empty)
	pgPublicationSQL := `
		CREATE OR REPLACE VIEW pg_publication AS
		SELECT
			0::BIGINT AS oid,
			'' AS pubname,
			0::INTEGER AS pubowner,
			false AS puballtables,
			false AS pubinsert,
			false AS pubupdate,
			false AS pubdelete,
			false AS pubtruncate,
			false AS pubviaroot
		WHERE false
	`
	db.Exec(pgPublicationSQL)

	// Create pg_publication_rel view (publication-relation mapping, empty)
	pgPublicationRelSQL := `
		CREATE OR REPLACE VIEW pg_publication_rel AS
		SELECT
			0::BIGINT AS oid,
			0::BIGINT AS prpubid,
			0::BIGINT AS prrelid
		WHERE false
	`
	db.Exec(pgPublicationRelSQL)

	// Create pg_inherits view (table inheritance, empty - DuckDB doesn't support inheritance)
	pgInheritsSQL := `
		CREATE OR REPLACE VIEW pg_inherits AS
		SELECT
			0::BIGINT AS inhrelid,
			0::BIGINT AS inhparent,
			0::INTEGER AS inhseqno,
			false AS inhdetachpending
		WHERE false
	`
	db.Exec(pgInheritsSQL)

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
		// pg_get_expr - deparse an expression (used for defaults, etc.)
		// Use default parameter so it works with both 2 and 3 args
		`DROP MACRO IF EXISTS pg_get_expr`,
		`CREATE MACRO pg_get_expr(expr, relid, pretty := false) AS NULL`,
		// pg_get_indexdef - get index definition
		`CREATE OR REPLACE MACRO pg_get_indexdef(index_oid) AS ''`,
		`CREATE OR REPLACE MACRO pg_get_indexdef(index_oid, col, pretty) AS ''`,
		// pg_get_constraintdef - get constraint definition
		`CREATE OR REPLACE MACRO pg_get_constraintdef(constraint_oid) AS ''`,
		`CREATE OR REPLACE MACRO pg_get_constraintdef(constraint_oid, pretty) AS ''`,
		// pg_get_statisticsobjdef_columns - get column list for extended statistics
		`CREATE OR REPLACE MACRO pg_get_statisticsobjdef_columns(stat_oid) AS ''`,
		// pg_relation_is_publishable - check if relation can be published
		`CREATE OR REPLACE MACRO pg_relation_is_publishable(rel_oid) AS false`,
		// current_setting - get config setting
		`CREATE OR REPLACE MACRO current_setting(name) AS
			CASE name
				WHEN 'server_version' THEN '15.0'
				WHEN 'server_encoding' THEN 'UTF8'
				ELSE ''
			END`,
		// pg_is_in_recovery - check if in recovery mode
		`CREATE OR REPLACE MACRO pg_is_in_recovery() AS false`,
		// version - return PostgreSQL-compatible version string
		// Fivetran and other tools check this to determine compatibility
		`CREATE OR REPLACE MACRO version() AS 'PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc, 64-bit (Duckgres/DuckDB)'`,
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
	"pg_get_partkeydef",
	"pg_get_statisticsobjdef_columns",
	"pg_relation_is_publishable",
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
	// pg_catalog.pg_class -> pg_class_full (use our wrapper view with extra columns)
	pgClassRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_class\b`)
	// pg_catalog.pg_collation -> pg_collation (use our view)
	pgCollationRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_collation\b`)
	// pg_catalog.pg_policy -> pg_policy (use our view)
	pgPolicyRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_policy\b`)
	// pg_catalog.pg_roles -> pg_roles (use our view)
	pgRolesRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_roles\b`)
	// pg_catalog.pg_statistic_ext -> pg_statistic_ext (use our view)
	pgStatisticExtRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_statistic_ext\b`)
	// pg_catalog.pg_publication_tables -> pg_publication_tables (use our view)
	pgPublicationTablesRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_publication_tables\b`)
	// pg_catalog.pg_rules -> pg_rules (use our view)
	pgRulesRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_rules\b`)
	// pg_catalog.pg_publication -> pg_publication (use our view)
	pgPublicationRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_publication\b`)
	// pg_catalog.pg_publication_rel -> pg_publication_rel (use our view)
	pgPublicationRelRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_publication_rel\b`)
	// pg_catalog.pg_inherits -> pg_inherits (use our view)
	pgInheritsRegex = regexp.MustCompile(`(?i)pg_catalog\.pg_inherits\b`)
	// ::pg_catalog.regtype::pg_catalog.text -> ::VARCHAR (PostgreSQL type cast)
	regtypeTextCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.regtype::pg_catalog\.text`)
	// ::pg_catalog.regtype -> ::VARCHAR
	regtypeCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.regtype`)
	// ::pg_catalog.regclass -> ::VARCHAR
	regclassCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.regclass`)
	// ::pg_catalog.regnamespace::pg_catalog.text -> ::VARCHAR
	regnamespaceTextCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.regnamespace::pg_catalog\.text`)
	// ::pg_catalog.regnamespace -> ::VARCHAR
	regnamespaceCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.regnamespace`)
	// ::pg_catalog.text -> ::VARCHAR
	textCastRegex = regexp.MustCompile(`(?i)::pg_catalog\.text`)
	// SET application_name = 'value' -> SET VARIABLE application_name = 'value'
	setApplicationNameRegex = regexp.MustCompile(`(?i)^SET\s+application_name\s*=`)
	// SET application_name TO 'value' -> SET VARIABLE application_name = 'value'
	setApplicationNameToRegex = regexp.MustCompile(`(?i)^SET\s+application_name\s+TO\s+`)
	// SHOW application_name -> SELECT getvariable('application_name') AS application_name
	showApplicationNameRegex = regexp.MustCompile(`(?i)^SHOW\s+application_name\s*;?\s*$`)
	// Regex to extract SET parameter name
	setParameterRegex = regexp.MustCompile(`(?i)^SET\s+(?:SESSION\s+|LOCAL\s+)?(\w+)`)
	// version() -> PostgreSQL-compatible version string (DuckDB's built-in can't be overridden by macro)
	versionFuncRegex = regexp.MustCompile(`(?i)\bversion\s*\(\s*\)`)
)

// PostgreSQL-specific SET parameters that DuckDB doesn't support.
// These will be silently ignored (return SET success without executing).
var ignoredSetParameters = map[string]bool{
	// SSL/Connection settings
	"ssl_renegotiation_limit": true,

	// Statement/Lock timeouts
	"statement_timeout":                    true,
	"lock_timeout":                         true,
	"idle_in_transaction_session_timeout":  true,
	"idle_session_timeout":                 true,

	// Client connection settings
	"client_min_messages":          true,
	"log_min_messages":             true,
	"log_min_duration_statement":   true,
	"log_statement":                true,
	"extra_float_digits":           true,

	// Transaction settings (DuckDB handles these differently)
	"default_transaction_isolation":  true,
	"default_transaction_read_only":  true,
	"default_transaction_deferrable": true,
	"transaction_isolation":          true,
	"transaction_read_only":          true,
	"transaction_deferrable":         true,

	// Encoding (DuckDB is always UTF-8)
	"client_encoding": true,

	// PostgreSQL-specific features
	"row_security":                  true,
	"check_function_bodies":         true,
	"default_tablespace":            true,
	"temp_tablespaces":              true,
	"session_replication_role":      true,
	"vacuum_freeze_min_age":         true,
	"vacuum_freeze_table_age":       true,
	"bytea_output":                  true,
	"xmlbinary":                     true,
	"xmloption":                     true,
	"gin_pending_list_limit":        true,
	"gin_fuzzy_search_limit":        true,

	// Locale settings
	"lc_messages": true,
	"lc_monetary": true,
	"lc_numeric":  true,
	"lc_time":     true,

	// Constraint settings
	"constraint_exclusion":       true,
	"cursor_tuple_fraction":      true,
	"from_collapse_limit":        true,
	"join_collapse_limit":        true,
	"geqo":                       true,
	"geqo_threshold":             true,

	// Parallel query settings
	"max_parallel_workers_per_gather": true,
	"max_parallel_workers":            true,
	"parallel_leader_participation":   true,
	"parallel_tuple_cost":             true,
	"parallel_setup_cost":             true,
	"min_parallel_table_scan_size":    true,
	"min_parallel_index_scan_size":    true,

	// Planner settings
	"enable_bitmapscan":         true,
	"enable_hashagg":            true,
	"enable_hashjoin":           true,
	"enable_indexscan":          true,
	"enable_indexonlyscan":      true,
	"enable_material":           true,
	"enable_mergejoin":          true,
	"enable_nestloop":           true,
	"enable_parallel_append":    true,
	"enable_parallel_hash":      true,
	"enable_partition_pruning":  true,
	"enable_partitionwise_join": true,
	"enable_partitionwise_aggregate": true,
	"enable_seqscan":            true,
	"enable_sort":               true,
	"enable_tidscan":            true,
	"enable_gathermerge":        true,

	// Cost settings
	"seq_page_cost":             true,
	"random_page_cost":          true,
	"cpu_tuple_cost":            true,
	"cpu_index_tuple_cost":      true,
	"cpu_operator_cost":         true,
	"effective_cache_size":      true,

	// Memory settings
	"work_mem":                  true,
	"maintenance_work_mem":      true,
	"logical_decoding_work_mem": true,
	"temp_buffers":              true,

	// Misc settings that don't apply
	"synchronous_commit":   true,
	"commit_delay":         true,
	"commit_siblings":      true,
	"huge_pages":           true,
	"force_parallel_mode":  true,
	"jit":                  true,
	"jit_above_cost":       true,
	"jit_inline_above_cost": true,
	"jit_optimize_above_cost": true,

	// Replication settings
	"synchronous_standby_names": true,
	"wal_sender_timeout":        true,
	"wal_receiver_timeout":      true,

	// Search path is handled separately but good to have here as fallback
	// "search_path": true,  // This one we might want to handle specially later
}

// isIgnoredSetParameter checks if a SET command targets a PostgreSQL-specific
// parameter that should be silently ignored in DuckDB.
func isIgnoredSetParameter(query string) bool {
	matches := setParameterRegex.FindStringSubmatch(query)
	if len(matches) < 2 {
		return false
	}
	paramName := strings.ToLower(matches[1])
	return ignoredSetParameters[paramName]
}

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

	// Replace pg_catalog.pg_class with pg_class_full (our wrapper view with extra columns)
	query = pgClassRegex.ReplaceAllString(query, "pg_class_full")

	// Replace pg_catalog.pg_collation with pg_collation (our empty view)
	query = pgCollationRegex.ReplaceAllString(query, "pg_collation")

	// Replace pg_catalog.pg_policy with pg_policy (our empty view)
	query = pgPolicyRegex.ReplaceAllString(query, "pg_policy")

	// Replace pg_catalog.pg_roles with pg_roles (our view)
	query = pgRolesRegex.ReplaceAllString(query, "pg_roles")

	// Replace pg_catalog.pg_statistic_ext with pg_statistic_ext (our view)
	query = pgStatisticExtRegex.ReplaceAllString(query, "pg_statistic_ext")

	// Replace pg_catalog.pg_publication_tables with pg_publication_tables (our view)
	query = pgPublicationTablesRegex.ReplaceAllString(query, "pg_publication_tables")

	// Replace pg_catalog.pg_rules with pg_rules (our view)
	query = pgRulesRegex.ReplaceAllString(query, "pg_rules")

	// Replace pg_catalog.pg_publication with pg_publication (our view)
	query = pgPublicationRegex.ReplaceAllString(query, "pg_publication")

	// Replace pg_catalog.pg_publication_rel with pg_publication_rel (our view)
	query = pgPublicationRelRegex.ReplaceAllString(query, "pg_publication_rel")

	// Replace pg_catalog.pg_inherits with pg_inherits (our view)
	query = pgInheritsRegex.ReplaceAllString(query, "pg_inherits")

	// Replace PostgreSQL type casts (order matters - most specific first)
	query = regtypeTextCastRegex.ReplaceAllString(query, "::VARCHAR")
	query = regtypeCastRegex.ReplaceAllString(query, "::VARCHAR")
	query = regclassCastRegex.ReplaceAllString(query, "::VARCHAR")
	query = regnamespaceTextCastRegex.ReplaceAllString(query, "::VARCHAR")
	query = regnamespaceCastRegex.ReplaceAllString(query, "::VARCHAR")
	query = textCastRegex.ReplaceAllString(query, "::VARCHAR")

	// Replace PostgreSQL application_name with DuckDB variable
	query = setApplicationNameRegex.ReplaceAllString(query, "SET VARIABLE application_name =")
	query = setApplicationNameToRegex.ReplaceAllString(query, "SET VARIABLE application_name =")
	query = showApplicationNameRegex.ReplaceAllString(query, "SELECT getvariable('application_name') AS application_name")

	// Replace version() with PostgreSQL-compatible version string
	// DuckDB's built-in version() can't be overridden by macros
	query = versionFuncRegex.ReplaceAllString(query, "'PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc, 64-bit (Duckgres/DuckDB)'")

	return query
}

