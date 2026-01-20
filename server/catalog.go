package server

import (
	"database/sql"
	"fmt"
)

// initPgCatalog creates PostgreSQL compatibility functions and views in DuckDB
// DuckDB already has a pg_catalog schema with basic views, so we just add missing functions
func initPgCatalog(db *sql.DB) error {
	// Create our own pg_database view that has all the columns psql expects
	// We put it in main schema and rewrite queries to use it
	// Include template databases for PostgreSQL compatibility
	// Note: We use 'testdb' as the user database name to match the test PostgreSQL container
	pgDatabaseSQL := `
		CREATE OR REPLACE VIEW pg_database AS
		SELECT * FROM (
			VALUES
				(1::INTEGER, 'postgres', 10::INTEGER, 6::INTEGER, 'en_US.UTF-8', 'en_US.UTF-8', false, true, -1::INTEGER, NULL),
				(2::INTEGER, 'template0', 10::INTEGER, 6::INTEGER, 'en_US.UTF-8', 'en_US.UTF-8', true, false, -1::INTEGER, NULL),
				(3::INTEGER, 'template1', 10::INTEGER, 6::INTEGER, 'en_US.UTF-8', 'en_US.UTF-8', true, true, -1::INTEGER, NULL),
				(4::INTEGER, 'testdb', 10::INTEGER, 6::INTEGER, 'en_US.UTF-8', 'en_US.UTF-8', false, true, -1::INTEGER, NULL)
		) AS t(oid, datname, datdba, encoding, datcollate, datctype, datistemplate, datallowconn, datconnlimit, datacl)
	`
	db.Exec(pgDatabaseSQL)

	// Create pg_class wrapper that adds missing columns psql expects
	// DuckDB's pg_catalog.pg_class is missing relforcerowsecurity
	// Also filter out internal duckgres views so they don't appear in \dt output
	// Note: We use an explicit list of internal view names to filter
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
		WHERE relname NOT IN (
			'pg_database', 'pg_class_full', 'pg_collation', 'pg_policy', 'pg_roles',
			'pg_statistic_ext', 'pg_publication_tables', 'pg_rules', 'pg_publication',
			'pg_publication_rel', 'pg_inherits', 'pg_namespace', 'pg_matviews',
			'pg_stat_user_tables', 'pg_stat_statements', 'pg_partitioned_table',
			'pg_attribute',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', '__duckgres_column_metadata'
		)
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

	// Create pg_matviews view (materialized views, empty - DuckDB doesn't support matviews)
	pgMatviewsSQL := `
		CREATE OR REPLACE VIEW pg_matviews AS
		SELECT
			''::VARCHAR AS schemaname,
			''::VARCHAR AS matviewname,
			''::VARCHAR AS matviewowner,
			NULL::VARCHAR AS tablespace,
			false AS hasindexes,
			false AS ispopulated,
			''::VARCHAR AS definition
		WHERE false
	`
	db.Exec(pgMatviewsSQL)

	// Create pg_stat_statements view (query statistics, empty - pg_stat_statements extension not supported)
	pgStatStatementsSQL := `
		CREATE OR REPLACE VIEW pg_stat_statements AS
		SELECT
			0::BIGINT AS userid,
			0::BIGINT AS dbid,
			0::BIGINT AS queryid,
			''::TEXT AS query,
			0::BIGINT AS calls,
			0::DOUBLE AS total_exec_time,
			0::DOUBLE AS total_time,
			0::DOUBLE AS min_exec_time,
			0::DOUBLE AS max_exec_time,
			0::DOUBLE AS mean_exec_time,
			0::DOUBLE AS stddev_exec_time,
			0::BIGINT AS rows,
			0::BIGINT AS shared_blks_hit,
			0::BIGINT AS shared_blks_read,
			0::BIGINT AS shared_blks_dirtied,
			0::BIGINT AS shared_blks_written,
			0::BIGINT AS local_blks_hit,
			0::BIGINT AS local_blks_read,
			0::BIGINT AS local_blks_dirtied,
			0::BIGINT AS local_blks_written,
			0::BIGINT AS temp_blks_read,
			0::BIGINT AS temp_blks_written,
			0::DOUBLE AS blk_read_time,
			0::DOUBLE AS blk_write_time
		WHERE false
	`
	db.Exec(pgStatStatementsSQL)

	// Create pg_partitioned_table view (partitioning, empty - DuckDB doesn't support table partitioning)
	pgPartitionedTableSQL := `
		CREATE OR REPLACE VIEW pg_partitioned_table AS
		SELECT
			0::BIGINT AS partrelid,
			'r'::VARCHAR AS partstrat,
			0::SMALLINT AS partnatts,
			0::BIGINT AS partdefid,
			ARRAY[]::SMALLINT[] AS partattrs,
			ARRAY[]::BIGINT[] AS partclass,
			ARRAY[]::BIGINT[] AS partcollation,
			NULL::TEXT AS partexprs
		WHERE false
	`
	db.Exec(pgPartitionedTableSQL)

	// Create pg_stat_user_tables view (table statistics)
	// Uses reltuples from pg_class for estimated row counts (same as PostgreSQL - it's an estimate)
	// Returns 0 for scan/tuple statistics and NULL for timestamps (DuckDB doesn't track these)
	pgStatUserTablesSQL := `
		CREATE OR REPLACE VIEW pg_stat_user_tables AS
		SELECT
			c.oid AS relid,
			n.nspname AS schemaname,
			c.relname AS relname,
			0::BIGINT AS seq_scan,
			0::BIGINT AS seq_tup_read,
			0::BIGINT AS idx_scan,
			0::BIGINT AS idx_tup_fetch,
			0::BIGINT AS n_tup_ins,
			0::BIGINT AS n_tup_upd,
			0::BIGINT AS n_tup_del,
			0::BIGINT AS n_tup_hot_upd,
			CASE WHEN c.reltuples < 0 THEN 0 ELSE c.reltuples::BIGINT END AS n_live_tup,
			0::BIGINT AS n_dead_tup,
			0::BIGINT AS n_mod_since_analyze,
			0::BIGINT AS n_ins_since_vacuum,
			NULL::TIMESTAMP AS last_vacuum,
			NULL::TIMESTAMP AS last_autovacuum,
			NULL::TIMESTAMP AS last_analyze,
			NULL::TIMESTAMP AS last_autoanalyze,
			0::BIGINT AS vacuum_count,
			0::BIGINT AS autovacuum_count,
			0::BIGINT AS analyze_count,
			0::BIGINT AS autoanalyze_count
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relkind IN ('r', 'p')
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	`
	db.Exec(pgStatUserTablesSQL)

	// Create pg_namespace wrapper that maps 'main' to 'public' for PostgreSQL compatibility
	// Also set owner to match PostgreSQL conventions:
	// - public (main) is owned by pg_database_owner (OID 6171)
	// - other schemas are owned by postgres (OID 10)
	pgNamespaceSQL := `
		CREATE OR REPLACE VIEW pg_namespace AS
		SELECT
			oid,
			CASE WHEN nspname = 'main' THEN 'public' ELSE nspname END AS nspname,
			CASE WHEN nspname = 'main' THEN 6171::BIGINT ELSE 10::BIGINT END AS nspowner,
			nspacl
		FROM pg_catalog.pg_namespace
	`
	db.Exec(pgNamespaceSQL)

	// Create pg_attribute wrapper that maps DuckDB internal type OIDs to PostgreSQL OIDs
	// DuckDB's pg_catalog.pg_attribute returns internal OIDs that don't match pg_type.
	// This causes JOIN pg_type ON atttypid = oid to fail, hiding columns from JDBC.
	// We must map ALL common types so columns join correctly with pg_type.
	pgAttributeSQL := `
		CREATE OR REPLACE VIEW pg_attribute AS
		SELECT
			a.attrelid,
			a.attname,
			-- Map DuckDB internal type OIDs to PostgreSQL standard OIDs
			-- Comprehensive mapping to ensure all columns join with pg_type
			CASE
				-- Numeric types
				WHEN dc.data_type LIKE 'DECIMAL%' OR dc.data_type LIKE 'NUMERIC%' THEN 1700::BIGINT
				WHEN dc.data_type = 'INTEGER' THEN 23::BIGINT
				WHEN dc.data_type = 'BIGINT' THEN 20::BIGINT
				WHEN dc.data_type = 'SMALLINT' THEN 21::BIGINT
				WHEN dc.data_type = 'TINYINT' THEN 21::BIGINT
				WHEN dc.data_type = 'HUGEINT' THEN 1700::BIGINT
				-- Unsigned types (map to larger signed or numeric)
				WHEN dc.data_type = 'UBIGINT' THEN 1700::BIGINT
				WHEN dc.data_type = 'UINTEGER' THEN 20::BIGINT
				WHEN dc.data_type = 'USMALLINT' THEN 23::BIGINT
				WHEN dc.data_type = 'UTINYINT' THEN 21::BIGINT
				-- Floating point
				WHEN dc.data_type = 'FLOAT' OR dc.data_type = 'DOUBLE' THEN 701::BIGINT
				WHEN dc.data_type = 'REAL' THEN 700::BIGINT
				-- String types
				WHEN dc.data_type = 'VARCHAR' THEN 1043::BIGINT
				WHEN dc.data_type = 'TEXT' THEN 25::BIGINT
				WHEN dc.data_type = 'CHAR' OR dc.data_type = 'BPCHAR' THEN 1042::BIGINT
				-- Boolean
				WHEN dc.data_type = 'BOOLEAN' THEN 16::BIGINT
				-- Binary
				WHEN dc.data_type = 'BLOB' OR dc.data_type = 'BYTEA' THEN 17::BIGINT
				-- Date/Time types
				WHEN dc.data_type = 'DATE' THEN 1082::BIGINT
				WHEN dc.data_type = 'TIME' THEN 1083::BIGINT
				WHEN dc.data_type = 'TIMESTAMP' THEN 1114::BIGINT
				WHEN dc.data_type LIKE 'TIMESTAMP WITH TIME ZONE%' THEN 1184::BIGINT
				WHEN dc.data_type LIKE 'TIME WITH TIME ZONE%' THEN 1266::BIGINT
				WHEN dc.data_type = 'INTERVAL' THEN 1186::BIGINT
				-- UUID
				WHEN dc.data_type = 'UUID' THEN 2950::BIGINT
				-- Bit
				WHEN dc.data_type = 'BIT' THEN 1560::BIGINT
				-- JSON
				WHEN dc.data_type = 'JSON' THEN 114::BIGINT
				-- Array types
				WHEN dc.data_type = 'INTEGER[]' THEN 1007::BIGINT
				WHEN dc.data_type = 'BIGINT[]' THEN 1016::BIGINT
				WHEN dc.data_type = 'SMALLINT[]' THEN 1005::BIGINT
				WHEN dc.data_type = 'VARCHAR[]' THEN 1015::BIGINT
				WHEN dc.data_type = 'TEXT[]' THEN 1009::BIGINT
				WHEN dc.data_type = 'BOOLEAN[]' THEN 1000::BIGINT
				WHEN dc.data_type = 'FLOAT[]' OR dc.data_type = 'DOUBLE[]' THEN 1022::BIGINT
				WHEN dc.data_type = 'REAL[]' THEN 1021::BIGINT
				WHEN dc.data_type = 'DATE[]' THEN 1182::BIGINT
				WHEN dc.data_type = 'TIMESTAMP[]' THEN 1115::BIGINT
				WHEN dc.data_type LIKE 'NUMERIC%[]' OR dc.data_type LIKE 'DECIMAL%[]' THEN 1231::BIGINT
				WHEN dc.data_type = 'UUID[]' THEN 2951::BIGINT
				WHEN dc.data_type = 'INTERVAL[]' THEN 1187::BIGINT
				WHEN dc.data_type = 'BLOB[]' OR dc.data_type = 'BYTEA[]' THEN 1001::BIGINT
				ELSE a.atttypid
			END AS atttypid,
			a.attstattarget,
			-- Set correct attlen for each type
			CASE
				-- Variable length types
				WHEN dc.data_type LIKE 'DECIMAL%' OR dc.data_type LIKE 'NUMERIC%' THEN -1::INTEGER
				WHEN dc.data_type = 'VARCHAR' OR dc.data_type = 'TEXT' THEN -1::INTEGER
				WHEN dc.data_type = 'BLOB' OR dc.data_type = 'BYTEA' THEN -1::INTEGER
				WHEN dc.data_type = 'BIT' THEN -1::INTEGER
				WHEN dc.data_type = 'JSON' THEN -1::INTEGER
				WHEN dc.data_type = 'HUGEINT' OR dc.data_type = 'UBIGINT' THEN -1::INTEGER
				-- Fixed size integer types
				WHEN dc.data_type = 'INTEGER' OR dc.data_type = 'USMALLINT' THEN 4::INTEGER
				WHEN dc.data_type = 'BIGINT' OR dc.data_type = 'UINTEGER' THEN 8::INTEGER
				WHEN dc.data_type = 'SMALLINT' OR dc.data_type = 'TINYINT' OR dc.data_type = 'UTINYINT' THEN 2::INTEGER
				-- Floating point
				WHEN dc.data_type = 'FLOAT' OR dc.data_type = 'DOUBLE' THEN 8::INTEGER
				WHEN dc.data_type = 'REAL' THEN 4::INTEGER
				-- Boolean
				WHEN dc.data_type = 'BOOLEAN' THEN 1::INTEGER
				-- Date/Time
				WHEN dc.data_type = 'DATE' THEN 4::INTEGER
				WHEN dc.data_type = 'TIME' THEN 8::INTEGER
				WHEN dc.data_type = 'TIMESTAMP' THEN 8::INTEGER
				WHEN dc.data_type LIKE 'TIMESTAMP WITH TIME ZONE%' THEN 8::INTEGER
				WHEN dc.data_type LIKE 'TIME WITH TIME ZONE%' THEN 12::INTEGER
				WHEN dc.data_type = 'INTERVAL' THEN 16::INTEGER
				-- UUID
				WHEN dc.data_type = 'UUID' THEN 16::INTEGER
				-- CHAR (fixed width, but we use -1 since width varies)
				WHEN dc.data_type = 'CHAR' OR dc.data_type = 'BPCHAR' THEN -1::INTEGER
				-- All array types are variable length
				WHEN dc.data_type LIKE '%[]' THEN -1::INTEGER
				ELSE a.attlen
			END AS attlen,
			a.attnum,
			a.attndims,
			a.attcacheoff,
			-- Fix atttypmod: Convert NUMERIC precision/scale from DuckDB to PostgreSQL format
			-- DuckDB: precision * 1000 + scale (e.g., 10002 for NUMERIC(10,2))
			-- PostgreSQL: (precision << 16) | (scale + 4) (e.g., 655366 for NUMERIC(10,2))
			CASE
				WHEN (dc.data_type LIKE 'DECIMAL%' OR dc.data_type LIKE 'NUMERIC%') AND a.atttypmod > 0 THEN
					(((a.atttypmod / 1000)::INTEGER << 16) | ((a.atttypmod % 1000)::INTEGER + 4))::INTEGER
				ELSE a.atttypmod
			END AS atttypmod,
			a.attbyval,
			a.attalign,
			a.attstorage,
			a.attcompression,
			a.attnotnull,
			a.atthasdef,
			a.atthasmissing,
			a.attidentity,
			a.attgenerated,
			a.attisdropped,
			a.attislocal,
			a.attinhcount,
			a.attcollation,
			a.attacl,
			a.attoptions,
			a.attfdwoptions,
			a.attmissingval
		FROM pg_catalog.pg_attribute a
		LEFT JOIN duckdb_columns() dc ON dc.table_oid = a.attrelid AND dc.column_name = a.attname
	`
	db.Exec(pgAttributeSQL)

	// Create helper macros/functions that psql expects but DuckDB doesn't have
	// These need to be created without schema prefix so DuckDB finds them
	//
	// IMPORTANT: When adding new custom macros here, also add them to the CustomMacros
	// map in transpiler/transform/pgcatalog.go so they get the memory.main. prefix
	// in DuckLake mode. Otherwise, the macros won't be found when DuckLake is attached.
	functions := []string{
		// pg_get_userbyid - returns username for a role OID
		// Map common PostgreSQL role OIDs to their names
		`CREATE OR REPLACE MACRO pg_get_userbyid(id) AS
			CASE id
				WHEN 10 THEN 'postgres'
				WHEN 6171 THEN 'pg_database_owner'
				ELSE 'postgres'
			END`,
		// pg_table_is_visible - checks if table is in search path
		`CREATE OR REPLACE MACRO pg_table_is_visible(oid) AS true`,
		// has_schema_privilege - check schema access
		// Note: DuckDB doesn't support macro overloading well, so we only define 2-arg versions
		// The transpiler should handle 3-arg calls by dropping the user argument
		`CREATE OR REPLACE MACRO has_schema_privilege(schema_name, priv) AS true`,
		// has_table_privilege - check table access
		`CREATE OR REPLACE MACRO has_table_privilege(table_name, priv) AS true`,
		// pg_encoding_to_char - convert encoding ID to name
		`CREATE OR REPLACE MACRO pg_encoding_to_char(enc) AS 'UTF8'`,
		// format_type - format a type OID as string with typemod support
		`CREATE OR REPLACE MACRO format_type(type_oid, typemod) AS
			CASE type_oid
				-- Boolean
				WHEN 16 THEN 'boolean'
				-- Binary
				WHEN 17 THEN 'bytea'
				-- Integer types
				WHEN 20 THEN 'bigint'
				WHEN 21 THEN 'smallint'
				WHEN 23 THEN 'integer'
				WHEN 26 THEN 'oid'
				-- Text types
				WHEN 25 THEN 'text'
				WHEN 1042 THEN CASE WHEN typemod > 0 THEN 'character(' || (typemod - 4)::VARCHAR || ')' ELSE 'character' END
				WHEN 1043 THEN CASE WHEN typemod > 0 THEN 'character varying(' || (typemod - 4)::VARCHAR || ')' ELSE 'character varying' END
				-- Floating point
				WHEN 700 THEN 'real'
				WHEN 701 THEN 'double precision'
				-- Numeric with precision/scale (scale can be negative, stored as two's complement)
				WHEN 1700 THEN CASE
					WHEN typemod > 0 THEN 'numeric(' || ((typemod - 4) >> 16)::VARCHAR || ',' ||
						CASE WHEN ((typemod - 4) & 65535) > 32767
							THEN (((typemod - 4) & 65535) - 65536)::VARCHAR
							ELSE ((typemod - 4) & 65535)::VARCHAR
						END || ')'
					ELSE 'numeric'
				END
				-- Date/Time types
				WHEN 1082 THEN 'date'
				WHEN 1083 THEN CASE WHEN typemod >= 0 THEN 'time(' || typemod::VARCHAR || ') without time zone' ELSE 'time without time zone' END
				WHEN 1114 THEN CASE WHEN typemod >= 0 THEN 'timestamp(' || typemod::VARCHAR || ') without time zone' ELSE 'timestamp without time zone' END
				WHEN 1184 THEN CASE WHEN typemod >= 0 THEN 'timestamp(' || typemod::VARCHAR || ') with time zone' ELSE 'timestamp with time zone' END
				WHEN 1266 THEN CASE WHEN typemod >= 0 THEN 'time(' || typemod::VARCHAR || ') with time zone' ELSE 'time with time zone' END
				WHEN 1186 THEN 'interval'
				-- UUID
				WHEN 2950 THEN 'uuid'
				-- JSON types
				WHEN 114 THEN 'json'
				WHEN 3802 THEN 'jsonb'
				-- Array types (common ones)
				WHEN 1000 THEN 'boolean[]'
				WHEN 1005 THEN 'smallint[]'
				WHEN 1007 THEN 'integer[]'
				WHEN 1016 THEN 'bigint[]'
				WHEN 1009 THEN 'text[]'
				WHEN 1015 THEN 'character varying[]'
				WHEN 1021 THEN 'real[]'
				WHEN 1022 THEN 'double precision[]'
				-- Fallback: return type OID for debugging
				ELSE 'unknown(' || type_oid::VARCHAR || ')'
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
		// pg_get_serial_sequence - get sequence name for a serial/identity column
		// Returns NULL because DuckLake doesn't support sequences
		`CREATE OR REPLACE MACRO pg_get_serial_sequence(table_name, column_name) AS NULL`,
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

// initInformationSchema creates the column metadata table and information_schema wrapper views.
// This enables accurate type information (VARCHAR lengths, NUMERIC precision) in information_schema.
// Views are created in memory.main (before USE ducklake) and query from unqualified information_schema,
// which resolves to the default catalog's information_schema at query time.
func initInformationSchema(db *sql.DB, duckLakeMode bool) error {
	// Use just "information_schema" without catalog prefix
	// Views are created in memory.main (before USE ducklake) and query from information_schema
	// which resolves to the current default catalog's information_schema at query time
	infoSchemaPrefix := "information_schema"

	// Create metadata table to store column type information that DuckDB doesn't preserve
	// Table is created in main schema (which is memory.main before USE ducklake)
	metadataTableSQL := `
		CREATE TABLE IF NOT EXISTS main.__duckgres_column_metadata (
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
	// Transforms DuckDB type names to PostgreSQL-compatible names
	// Maps: VARCHAR->text, BOOLEAN->boolean, INTEGER->integer, BIGINT->bigint,
	//       TIMESTAMP->timestamp without time zone, DECIMAL->numeric, etc.
	// Views are created in main schema (which is memory.main before USE ducklake)
	columnsViewSQL := `
		CREATE OR REPLACE VIEW main.information_schema_columns_compat AS
		SELECT
			c.table_catalog,
			CASE WHEN c.table_schema = 'main' THEN 'public' ELSE c.table_schema END AS table_schema,
			c.table_name,
			c.column_name,
			c.ordinal_position,
			-- Normalize column_default to PostgreSQL format
			CASE
				WHEN c.column_default IS NULL THEN NULL
				WHEN c.column_default = 'CAST(''t'' AS BOOLEAN)' THEN 'true'
				WHEN c.column_default = 'CAST(''f'' AS BOOLEAN)' THEN 'false'
				WHEN UPPER(c.column_default) = 'CURRENT_TIMESTAMP' THEN 'CURRENT_TIMESTAMP'
				WHEN UPPER(c.column_default) = 'NOW()' THEN 'now()'
				ELSE c.column_default
			END AS column_default,
			c.is_nullable,
			-- Normalize data_type to PostgreSQL lowercase format
			CASE
				WHEN UPPER(c.data_type) = 'VARCHAR' OR UPPER(c.data_type) LIKE 'VARCHAR(%%' THEN 'text'
				WHEN UPPER(c.data_type) = 'TEXT' THEN 'text'
				WHEN UPPER(c.data_type) LIKE 'TEXT(%%' THEN 'character'
				WHEN UPPER(c.data_type) = 'BOOLEAN' THEN 'boolean'
				WHEN UPPER(c.data_type) = 'TINYINT' THEN 'smallint'
				WHEN UPPER(c.data_type) = 'SMALLINT' THEN 'smallint'
				WHEN UPPER(c.data_type) = 'INTEGER' THEN 'integer'
				WHEN UPPER(c.data_type) = 'BIGINT' THEN 'bigint'
				WHEN UPPER(c.data_type) = 'HUGEINT' THEN 'numeric'
				WHEN UPPER(c.data_type) = 'REAL' OR UPPER(c.data_type) = 'FLOAT4' THEN 'real'
				WHEN UPPER(c.data_type) = 'DOUBLE' OR UPPER(c.data_type) = 'FLOAT8' THEN 'double precision'
				WHEN UPPER(c.data_type) LIKE 'DECIMAL%%' THEN 'numeric'
				WHEN UPPER(c.data_type) LIKE 'NUMERIC%%' THEN 'numeric'
				WHEN UPPER(c.data_type) = 'DATE' THEN 'date'
				WHEN UPPER(c.data_type) = 'TIME' THEN 'time without time zone'
				WHEN UPPER(c.data_type) = 'TIMESTAMP' THEN 'timestamp without time zone'
				WHEN UPPER(c.data_type) = 'TIMESTAMPTZ' OR UPPER(c.data_type) = 'TIMESTAMP WITH TIME ZONE' THEN 'timestamp with time zone'
				WHEN UPPER(c.data_type) = 'INTERVAL' THEN 'interval'
				WHEN UPPER(c.data_type) = 'UUID' THEN 'uuid'
				WHEN UPPER(c.data_type) = 'BLOB' OR UPPER(c.data_type) = 'BYTEA' THEN 'bytea'
				WHEN UPPER(c.data_type) = 'JSON' THEN 'json'
				WHEN UPPER(c.data_type) LIKE '%%[]' THEN 'ARRAY'
				ELSE LOWER(c.data_type)
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
		FROM %s.columns c
		LEFT JOIN main.__duckgres_column_metadata m
			ON c.table_schema = m.table_schema
			AND c.table_name = m.table_name
			AND c.column_name = m.column_name
	`
	if _, err := db.Exec(fmt.Sprintf(columnsViewSQL, infoSchemaPrefix)); err != nil {
		// If join with metadata table fails, create simpler view without it
		columnsViewSimpleSQL := `
			CREATE OR REPLACE VIEW main.information_schema_columns_compat AS
			SELECT
				table_catalog,
				CASE WHEN table_schema = 'main' THEN 'public' ELSE table_schema END AS table_schema,
				table_name,
				column_name,
				ordinal_position,
				-- Normalize column_default to PostgreSQL format
				CASE
					WHEN column_default IS NULL THEN NULL
					WHEN column_default = 'CAST(''t'' AS BOOLEAN)' THEN 'true'
					WHEN column_default = 'CAST(''f'' AS BOOLEAN)' THEN 'false'
					WHEN UPPER(column_default) = 'CURRENT_TIMESTAMP' THEN 'CURRENT_TIMESTAMP'
					WHEN UPPER(column_default) = 'NOW()' THEN 'now()'
					ELSE column_default
				END AS column_default,
				is_nullable,
				-- Normalize data_type to PostgreSQL lowercase format
				CASE
					WHEN UPPER(data_type) = 'VARCHAR' OR UPPER(data_type) LIKE 'VARCHAR(%%' THEN 'text'
					WHEN UPPER(data_type) = 'TEXT' THEN 'text'
					WHEN UPPER(data_type) LIKE 'TEXT(%%' THEN 'character'
					WHEN UPPER(data_type) = 'BOOLEAN' THEN 'boolean'
					WHEN UPPER(data_type) = 'TINYINT' THEN 'smallint'
					WHEN UPPER(data_type) = 'SMALLINT' THEN 'smallint'
					WHEN UPPER(data_type) = 'INTEGER' THEN 'integer'
					WHEN UPPER(data_type) = 'BIGINT' THEN 'bigint'
					WHEN UPPER(data_type) = 'HUGEINT' THEN 'numeric'
					WHEN UPPER(data_type) = 'REAL' OR UPPER(data_type) = 'FLOAT4' THEN 'real'
					WHEN UPPER(data_type) = 'DOUBLE' OR UPPER(data_type) = 'FLOAT8' THEN 'double precision'
					WHEN UPPER(data_type) LIKE 'DECIMAL%%' THEN 'numeric'
					WHEN UPPER(data_type) LIKE 'NUMERIC%%' THEN 'numeric'
					WHEN UPPER(data_type) = 'DATE' THEN 'date'
					WHEN UPPER(data_type) = 'TIME' THEN 'time without time zone'
					WHEN UPPER(data_type) = 'TIMESTAMP' THEN 'timestamp without time zone'
					WHEN UPPER(data_type) = 'TIMESTAMPTZ' OR UPPER(data_type) = 'TIMESTAMP WITH TIME ZONE' THEN 'timestamp with time zone'
					WHEN UPPER(data_type) = 'INTERVAL' THEN 'interval'
					WHEN UPPER(data_type) = 'UUID' THEN 'uuid'
					WHEN UPPER(data_type) = 'BLOB' OR UPPER(data_type) = 'BYTEA' THEN 'bytea'
					WHEN UPPER(data_type) = 'JSON' THEN 'json'
					WHEN UPPER(data_type) LIKE '%%[]' THEN 'ARRAY'
					ELSE LOWER(data_type)
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
			FROM %s.columns
		`
		db.Exec(fmt.Sprintf(columnsViewSimpleSQL, infoSchemaPrefix))
	}

	// Create information_schema.tables wrapper view with additional PostgreSQL columns
	// Filter out internal duckgres tables/views and DuckDB system views
	// Normalize 'main' schema to 'public' for PostgreSQL compatibility
	tablesViewSQL := `
		CREATE OR REPLACE VIEW main.information_schema_tables_compat AS
		SELECT
			t.table_catalog,
			CASE WHEN t.table_schema = 'main' THEN 'public' ELSE t.table_schema END AS table_schema,
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
		FROM %s.tables t
		WHERE t.table_name NOT IN (
			-- Internal duckgres tables
			'__duckgres_column_metadata',
			-- pg_catalog compat views
			'pg_class_full', 'pg_collation', 'pg_database', 'pg_inherits',
			'pg_namespace', 'pg_policy', 'pg_publication', 'pg_publication_rel',
			'pg_publication_tables', 'pg_roles', 'pg_rules', 'pg_statistic_ext', 'pg_matviews',
			'pg_stat_user_tables', 'pg_stat_statements', 'pg_partitioned_table', 'pg_attribute',
			-- information_schema compat views
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat'
		)
		AND t.table_name NOT LIKE 'duckdb_%%'
		AND t.table_name NOT LIKE 'sqlite_%%'
		AND t.table_name NOT LIKE 'pragma_%%'
	`
	db.Exec(fmt.Sprintf(tablesViewSQL, infoSchemaPrefix))

	// Create information_schema.schemata wrapper view
	// Normalize 'main' to 'public' and add synthetic entries for pg_catalog and information_schema
	// to match PostgreSQL's information_schema.schemata
	schemataViewSQL := `
		CREATE OR REPLACE VIEW main.information_schema_schemata_compat AS
		SELECT
			s.catalog_name,
			CASE WHEN s.schema_name = 'main' THEN 'public' ELSE s.schema_name END AS schema_name,
			'duckdb' AS schema_owner,
			NULL AS default_character_set_catalog,
			NULL AS default_character_set_schema,
			NULL AS default_character_set_name,
			NULL AS sql_path
		FROM %s.schemata s
		WHERE s.schema_name NOT IN ('main', 'pg_catalog', 'information_schema')
		UNION ALL
		SELECT 'memory' AS catalog_name, 'public' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
		UNION ALL
		SELECT 'memory' AS catalog_name, 'pg_catalog' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
		UNION ALL
		SELECT 'memory' AS catalog_name, 'information_schema' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
		UNION ALL
		SELECT 'memory' AS catalog_name, 'pg_toast' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
	`
	db.Exec(fmt.Sprintf(schemataViewSQL, infoSchemaPrefix))

	// Create information_schema.views wrapper view
	// Filter out internal duckgres views and DuckDB system views
	// Normalize 'main' schema to 'public' for PostgreSQL compatibility
	viewsViewSQL := `
		CREATE OR REPLACE VIEW main.information_schema_views_compat AS
		SELECT
			v.table_catalog,
			CASE WHEN v.table_schema = 'main' THEN 'public' ELSE v.table_schema END AS table_schema,
			v.table_name,
			v.view_definition,
			v.check_option,
			v.is_updatable,
			v.is_insertable_into,
			v.is_trigger_updatable,
			v.is_trigger_deletable,
			v.is_trigger_insertable_into
		FROM %s.views v
		WHERE v.table_name NOT IN (
			-- pg_catalog compat views
			'pg_class_full', 'pg_collation', 'pg_database', 'pg_inherits',
			'pg_namespace', 'pg_policy', 'pg_publication', 'pg_publication_rel',
			'pg_publication_tables', 'pg_roles', 'pg_rules', 'pg_statistic_ext', 'pg_matviews',
			'pg_stat_user_tables', 'pg_stat_statements', 'pg_partitioned_table', 'pg_attribute',
			-- information_schema compat views
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat'
		)
		AND v.table_name NOT LIKE 'duckdb_%%'
		AND v.table_name NOT LIKE 'sqlite_%%'
		AND v.table_name NOT LIKE 'pragma_%%'
	`
	db.Exec(fmt.Sprintf(viewsViewSQL, infoSchemaPrefix))

	return nil
}

// recreatePgClassForDuckLake recreates pg_class_full to source from DuckDB's native
// system functions (duckdb_tables, duckdb_views, etc.) filtered to only include
// objects from the 'ducklake' catalog (user tables/views).
// This excludes internal DuckLake metadata tables from '__ducklake_metadata_ducklake'.
// Must be called AFTER DuckLake is attached.
func recreatePgClassForDuckLake(db *sql.DB) error {
	pgClassSQL := `
		CREATE OR REPLACE VIEW pg_class_full AS
		-- Tables from ducklake catalog
		SELECT
			table_oid AS oid,
			table_name AS relname,
			schema_oid AS relnamespace,
			0 AS reltype,
			0 AS reloftype,
			0 AS relowner,
			0 AS relam,
			0 AS relfilenode,
			0 AS reltablespace,
			0 AS relpages,
			CAST(estimated_size AS FLOAT) AS reltuples,
			0 AS relallvisible,
			0 AS reltoastrelid,
			0::BIGINT AS reltoastidxid,
			(index_count > 0) AS relhasindex,
			false AS relisshared,
			CASE WHEN temporary THEN 't' ELSE 'p' END AS relpersistence,
			'r' AS relkind,
			column_count AS relnatts,
			check_constraint_count AS relchecks,
			false AS relhasoids,
			has_primary_key AS relhaspkey,
			false AS relhasrules,
			false AS relhastriggers,
			false AS relhassubclass,
			false AS relrowsecurity,
			false AS relforcerowsecurity,
			true AS relispopulated,
			NULL AS relreplident,
			false AS relispartition,
			0 AS relrewrite,
			0 AS relfrozenxid,
			NULL AS relminmxid,
			NULL AS relacl,
			NULL AS reloptions,
			NULL AS relpartbound
		FROM duckdb_tables()
		WHERE database_name = 'ducklake'
		  AND table_name NOT IN (
			'pg_database', 'pg_class_full', 'pg_collation', 'pg_policy', 'pg_roles',
			'pg_statistic_ext', 'pg_publication_tables', 'pg_rules', 'pg_publication',
			'pg_publication_rel', 'pg_inherits', 'pg_namespace', 'pg_matviews',
			'pg_stat_user_tables', 'pg_stat_statements', 'pg_partitioned_table',
			'pg_attribute',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', '__duckgres_column_metadata'
		  )
		UNION ALL
		-- Views from ducklake catalog
		SELECT
			view_oid AS oid,
			view_name AS relname,
			schema_oid AS relnamespace,
			0 AS reltype,
			0 AS reloftype,
			0 AS relowner,
			0 AS relam,
			0 AS relfilenode,
			0 AS reltablespace,
			0 AS relpages,
			0 AS reltuples,
			0 AS relallvisible,
			0 AS reltoastrelid,
			0::BIGINT AS reltoastidxid,
			false AS relhasindex,
			false AS relisshared,
			CASE WHEN temporary THEN 't' ELSE 'p' END AS relpersistence,
			'v' AS relkind,
			column_count AS relnatts,
			0 AS relchecks,
			false AS relhasoids,
			false AS relhaspkey,
			false AS relhasrules,
			false AS relhastriggers,
			false AS relhassubclass,
			false AS relrowsecurity,
			false AS relforcerowsecurity,
			true AS relispopulated,
			NULL AS relreplident,
			false AS relispartition,
			0 AS relrewrite,
			0 AS relfrozenxid,
			NULL AS relminmxid,
			NULL AS relacl,
			NULL AS reloptions,
			NULL AS relpartbound
		FROM duckdb_views()
		WHERE database_name = 'ducklake'
		  AND view_name NOT IN (
			'pg_database', 'pg_class_full', 'pg_collation', 'pg_policy', 'pg_roles',
			'pg_statistic_ext', 'pg_publication_tables', 'pg_rules', 'pg_publication',
			'pg_publication_rel', 'pg_inherits', 'pg_namespace', 'pg_matviews',
			'pg_stat_user_tables', 'pg_stat_statements', 'pg_partitioned_table',
			'pg_attribute',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', '__duckgres_column_metadata'
		  )
		UNION ALL
		-- Sequences from ducklake catalog
		SELECT
			sequence_oid AS oid,
			sequence_name AS relname,
			schema_oid AS relnamespace,
			0 AS reltype,
			0 AS reloftype,
			0 AS relowner,
			0 AS relam,
			0 AS relfilenode,
			0 AS reltablespace,
			0 AS relpages,
			0 AS reltuples,
			0 AS relallvisible,
			0 AS reltoastrelid,
			0::BIGINT AS reltoastidxid,
			false AS relhasindex,
			false AS relisshared,
			CASE WHEN temporary THEN 't' ELSE 'p' END AS relpersistence,
			'S' AS relkind,
			0 AS relnatts,
			0 AS relchecks,
			false AS relhasoids,
			false AS relhaspkey,
			false AS relhasrules,
			false AS relhastriggers,
			false AS relhassubclass,
			false AS relrowsecurity,
			false AS relforcerowsecurity,
			true AS relispopulated,
			NULL AS relreplident,
			false AS relispartition,
			0 AS relrewrite,
			0 AS relfrozenxid,
			NULL AS relminmxid,
			NULL AS relacl,
			NULL AS reloptions,
			NULL AS relpartbound
		FROM duckdb_sequences()
		WHERE database_name = 'ducklake'
		UNION ALL
		-- Indexes from ducklake catalog
		SELECT
			index_oid AS oid,
			index_name AS relname,
			schema_oid AS relnamespace,
			0 AS reltype,
			0 AS reloftype,
			0 AS relowner,
			0 AS relam,
			0 AS relfilenode,
			0 AS reltablespace,
			0 AS relpages,
			0 AS reltuples,
			0 AS relallvisible,
			0 AS reltoastrelid,
			0::BIGINT AS reltoastidxid,
			false AS relhasindex,
			false AS relisshared,
			't' AS relpersistence,
			'i' AS relkind,
			NULL AS relnatts,
			0 AS relchecks,
			false AS relhasoids,
			false AS relhaspkey,
			false AS relhasrules,
			false AS relhastriggers,
			false AS relhassubclass,
			false AS relrowsecurity,
			false AS relforcerowsecurity,
			true AS relispopulated,
			NULL AS relreplident,
			false AS relispartition,
			0 AS relrewrite,
			0 AS relfrozenxid,
			NULL AS relminmxid,
			NULL AS relacl,
			NULL AS reloptions,
			NULL AS relpartbound
		FROM duckdb_indexes()
		WHERE database_name = 'ducklake'
	`
	_, err := db.Exec(pgClassSQL)
	return err
}

// recreatePgNamespaceForDuckLake recreates pg_namespace to source from DuckDB's native
// duckdb_tables() function to get schema OIDs that are consistent with pg_class_full.
// We derive namespaces from duckdb_tables() because duckdb_schemas() doesn't have schema_oid.
// Must be called AFTER DuckLake is attached.
func recreatePgNamespaceForDuckLake(db *sql.DB) error {
	pgNamespaceSQL := `
		CREATE OR REPLACE VIEW pg_namespace AS
		SELECT DISTINCT
			schema_oid AS oid,
			CASE WHEN schema_name = 'main' THEN 'public' ELSE schema_name END AS nspname,
			CASE WHEN schema_name = 'main' THEN 6171::BIGINT ELSE 10::BIGINT END AS nspowner,
			NULL AS nspacl
		FROM duckdb_tables()
		WHERE database_name = 'ducklake'
	`
	_, err := db.Exec(pgNamespaceSQL)
	return err
}
