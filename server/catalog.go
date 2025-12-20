package server

import (
	"database/sql"
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

		// quote_literal - quote a string literal for SQL
		`CREATE OR REPLACE MACRO quote_literal(val) AS '''' || replace(val::VARCHAR, '''', '''''') || ''''`,
		// quote_ident - quote an identifier
		`CREATE OR REPLACE MACRO quote_ident(val) AS '"' || replace(val::VARCHAR, '"', '""') || '"'`,
		// quote_nullable - like quote_literal but returns 'NULL' for NULL input
		`CREATE OR REPLACE MACRO quote_nullable(val) AS CASE WHEN val IS NULL THEN 'NULL' ELSE '''' || replace(val::VARCHAR, '''', '''''') || '''' END`,

		// initcap - capitalize first letter of each word
		`CREATE OR REPLACE MACRO initcap(str) AS (
			SELECT string_agg(upper(substr(word, 1, 1)) || lower(substr(word, 2)), ' ')
			FROM (SELECT unnest(string_split(str, ' ')) AS word)
		)`,

		// lpad with default space character
		`CREATE OR REPLACE MACRO lpad(str, len) AS lpad(str, len, ' ')`,
		// rpad with default space character
		`CREATE OR REPLACE MACRO rpad(str, len) AS rpad(str, len, ' ')`,

		// overlay - replace substring
		`CREATE OR REPLACE MACRO overlay(str, replacement, start, len) AS
			substr(str, 1, start - 1) || replacement || substr(str, start + len)`,

		// format function for PostgreSQL %s and %I placeholders
		// Note: Limited implementation - only handles 2 args
		`CREATE OR REPLACE MACRO format(fmt, arg1) AS replace(fmt, '%s', arg1::VARCHAR)`,

		// array_length with dimension argument (PostgreSQL has 2 args, DuckDB len has 1)
		`CREATE OR REPLACE MACRO array_length(arr, dim) AS len(arr)`,

		// array_ndims - return number of array dimensions
		`CREATE OR REPLACE MACRO array_ndims(arr) AS 1`,

		// cardinality - total number of elements in array
		`CREATE OR REPLACE MACRO cardinality(arr) AS len(arr)`,

		// octet_length for strings
		`CREATE OR REPLACE MACRO octet_length(str) AS length(str::BLOB)`,

		// encode/decode for binary data - create as wrapper around DuckDB's base64/hex functions
		`CREATE OR REPLACE MACRO encode(data, format) AS CASE
			WHEN lower(format) = 'base64' THEN base64(data)
			WHEN lower(format) = 'hex' THEN hex(data)
			ELSE NULL
		END`,
		`CREATE OR REPLACE MACRO decode(data, format) AS CASE
			WHEN lower(format) = 'base64' THEN from_base64(data)
			WHEN lower(format) = 'hex' THEN unhex(data)
			ELSE NULL
		END`,

		// to_char for dates - limited format string support
		`CREATE OR REPLACE MACRO to_char(val, fmt) AS strftime(val,
			replace(replace(replace(replace(replace(replace(fmt,
				'YYYY', '%Y'),
				'MM', '%m'),
				'DD', '%d'),
				'HH24', '%H'),
				'MI', '%M'),
				'SS', '%S')
		)`,

		// to_date - parse date from string with format
		`CREATE OR REPLACE MACRO to_date(str, fmt) AS strptime(str,
			replace(replace(replace(fmt,
				'YYYY', '%Y'),
				'MM', '%m'),
				'DD', '%d')
		)::DATE`,

		// to_timestamp - parse timestamp from string with format
		`CREATE OR REPLACE MACRO to_timestamp(str, fmt) AS strptime(str,
			replace(replace(replace(replace(replace(replace(fmt,
				'YYYY', '%Y'),
				'MM', '%m'),
				'DD', '%d'),
				'HH24', '%H'),
				'MI', '%M'),
				'SS', '%S')
		)::TIMESTAMP`,

		// make_time - create time from hour, minute, second
		`CREATE OR REPLACE MACRO make_time(hour, minute, second) AS
			(hour::VARCHAR || ':' || minute::VARCHAR || ':' || second::VARCHAR)::TIME`,

		// age with two arguments
		`CREATE OR REPLACE MACRO age(ts1, ts2) AS (ts1 - ts2)`,

		// width_bucket
		`CREATE OR REPLACE MACRO width_bucket(val, lo, hi, buckets) AS
			CASE
				WHEN val < lo THEN 0
				WHEN val >= hi THEN buckets + 1
				ELSE floor((val - lo) / ((hi - lo) / buckets))::INTEGER + 1
			END`,

		// pi constant
		`CREATE OR REPLACE MACRO pi() AS 3.141592653589793`,

		// radians - convert degrees to radians
		`CREATE OR REPLACE MACRO radians(deg) AS deg * 3.141592653589793 / 180`,

		// atan2
		`CREATE OR REPLACE MACRO atan2(y, x) AS atan2(y, x)`,

		// JSON functions that PostgreSQL has
		`CREATE OR REPLACE MACRO json_typeof(j) AS json_type(j)`,
		`CREATE OR REPLACE MACRO jsonb_typeof(j) AS json_type(j)`,

		// json_array_length
		`CREATE OR REPLACE MACRO json_array_length(j) AS json_array_length(j)`,

		// array_agg needs to return array type
		`CREATE OR REPLACE MACRO array_agg(val) AS list(val)`,
	}

	for _, f := range functions {
		if _, err := db.Exec(f); err != nil {
			// Log but don't fail - some might already exist or conflict
			continue
		}
	}

	return nil
}

// initInformationSchema creates the column metadata table and information_schema wrapper views.
// This enables accurate type information (VARCHAR lengths, NUMERIC precision) in information_schema.
func initInformationSchema(db *sql.DB) error {
	// Create metadata table to store column type information that DuckDB doesn't preserve
	metadataTableSQL := `
		CREATE TABLE IF NOT EXISTS __duckgres_column_metadata (
			table_schema VARCHAR NOT NULL,
			table_name VARCHAR NOT NULL,
			column_name VARCHAR NOT NULL,
			character_maximum_length INTEGER,
			numeric_precision INTEGER,
			numeric_scale INTEGER,
			PRIMARY KEY (table_schema, table_name, column_name)
		)
	`
	if _, err := db.Exec(metadataTableSQL); err != nil {
		// Table might already exist, that's OK
		// Ignore errors since PRIMARY KEY might not work in all contexts
	}

	// Create information_schema.columns wrapper view
	// Transforms DuckDB type names to PostgreSQL-compatible names (e.g., VARCHAR -> text)
	// First try with metadata table join, fall back to simple view if table doesn't exist
	columnsViewWithMetaSQL := `
		CREATE OR REPLACE VIEW information_schema_columns_compat AS
		SELECT
			c.table_catalog,
			c.table_schema,
			c.table_name,
			c.column_name,
			c.ordinal_position,
			c.column_default,
			c.is_nullable,
			CASE
				WHEN UPPER(c.data_type) = 'VARCHAR' OR UPPER(c.data_type) LIKE 'VARCHAR(%' THEN 'text'
				ELSE c.data_type
			END AS data_type,
			COALESCE(m.character_maximum_length, c.character_maximum_length) AS character_maximum_length,
			c.character_octet_length,
			COALESCE(m.numeric_precision, c.numeric_precision) AS numeric_precision,
			COALESCE(m.numeric_scale, c.numeric_scale) AS numeric_scale,
			c.datetime_precision,
			NULL AS interval_type,
			NULL AS interval_precision,
			NULL AS character_set_catalog,
			NULL AS character_set_schema,
			NULL AS character_set_name,
			NULL AS collation_catalog,
			NULL AS collation_schema,
			NULL AS collation_name,
			NULL AS domain_catalog,
			NULL AS domain_schema,
			NULL AS domain_name,
			NULL AS udt_catalog,
			NULL AS udt_schema,
			NULL AS udt_name,
			NULL AS scope_catalog,
			NULL AS scope_schema,
			NULL AS scope_name,
			NULL AS maximum_cardinality,
			NULL AS dtd_identifier,
			'NO' AS is_self_referencing,
			'NO' AS is_identity,
			NULL AS identity_generation,
			NULL AS identity_start,
			NULL AS identity_increment,
			NULL AS identity_maximum,
			NULL AS identity_minimum,
			NULL AS identity_cycle,
			'NEVER' AS is_generated,
			NULL AS generation_expression,
			'YES' AS is_updatable
		FROM information_schema.columns c
		LEFT JOIN __duckgres_column_metadata m
			ON c.table_schema = m.table_schema
			AND c.table_name = m.table_name
			AND c.column_name = m.column_name
	`
	// Try with metadata table first
	if _, err := db.Exec(columnsViewWithMetaSQL); err != nil {
		// Metadata table doesn't exist, create simpler view without it
		columnsViewSimpleSQL := `
			CREATE OR REPLACE VIEW information_schema_columns_compat AS
			SELECT
				table_catalog,
				table_schema,
				table_name,
				column_name,
				ordinal_position,
				column_default,
				is_nullable,
				CASE
					WHEN UPPER(data_type) = 'VARCHAR' OR UPPER(data_type) LIKE 'VARCHAR(%' THEN 'text'
					ELSE data_type
				END AS data_type,
				character_maximum_length,
				character_octet_length,
				numeric_precision,
				numeric_scale,
				datetime_precision,
				NULL AS interval_type,
				NULL AS interval_precision,
				NULL AS character_set_catalog,
				NULL AS character_set_schema,
				NULL AS character_set_name,
				NULL AS collation_catalog,
				NULL AS collation_schema,
				NULL AS collation_name,
				NULL AS domain_catalog,
				NULL AS domain_schema,
				NULL AS domain_name,
				NULL AS udt_catalog,
				NULL AS udt_schema,
				NULL AS udt_name,
				NULL AS scope_catalog,
				NULL AS scope_schema,
				NULL AS scope_name,
				NULL AS maximum_cardinality,
				NULL AS dtd_identifier,
				'NO' AS is_self_referencing,
				'NO' AS is_identity,
				NULL AS identity_generation,
				NULL AS identity_start,
				NULL AS identity_increment,
				NULL AS identity_maximum,
				NULL AS identity_minimum,
				NULL AS identity_cycle,
				'NEVER' AS is_generated,
				NULL AS generation_expression,
				'YES' AS is_updatable
			FROM information_schema.columns
		`
		db.Exec(columnsViewSimpleSQL)
	}

	// Create information_schema.tables wrapper view with additional PostgreSQL columns
	tablesViewSQL := `
		CREATE OR REPLACE VIEW information_schema_tables_compat AS
		SELECT
			t.table_catalog,
			t.table_schema,
			t.table_name,
			t.table_type,
			NULL AS self_referencing_column_name,
			NULL AS reference_generation,
			NULL AS user_defined_type_catalog,
			NULL AS user_defined_type_schema,
			NULL AS user_defined_type_name,
			'YES' AS is_insertable_into,
			'NO' AS is_typed,
			NULL AS commit_action
		FROM information_schema.tables t
	`
	db.Exec(tablesViewSQL)

	// Create information_schema.schemata wrapper view
	schemataViewSQL := `
		CREATE OR REPLACE VIEW information_schema_schemata_compat AS
		SELECT
			s.catalog_name,
			s.schema_name,
			'duckdb' AS schema_owner,
			NULL AS default_character_set_catalog,
			NULL AS default_character_set_schema,
			NULL AS default_character_set_name,
			NULL AS sql_path
		FROM information_schema.schemata s
	`
	db.Exec(schemataViewSQL)

	return nil
}
