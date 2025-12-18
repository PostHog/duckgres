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
	}

	for _, f := range functions {
		if _, err := db.Exec(f); err != nil {
			// Log but don't fail - some might already exist or conflict
			continue
		}
	}

	return nil
}
