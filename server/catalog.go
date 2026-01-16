package server

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
)

// getTableLimit returns the table limit from DUCKGRES_TABLE_LIMIT env var, or 0 for no limit
func getTableLimit() int {
	if limitStr := os.Getenv("DUCKGRES_TABLE_LIMIT"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 {
			return limit
		}
	}
	return 0
}

// initPgCatalog creates PostgreSQL compatibility functions and views in DuckDB
// DuckDB already has a pg_catalog schema with basic views, so we just add missing functions
func initPgCatalog(db *sql.DB) error {
	tableLimit := getTableLimit()
	if tableLimit > 0 {
		log.Printf("DUCKGRES_TABLE_LIMIT=%d: limiting tables for faster testing", tableLimit)
	}
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
			'pg_stat_user_tables', 'pg_attribute', 'pg_type', 'pg_index', 'pg_constraint',
			'pg_description', 'pg_attrdef', 'pg_proc', 'pg_tables', 'pg_views',
			'pg_shadow', 'pg_user', 'pg_stat_activity', 'pg_extension',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat',
			'information_schema_table_constraints_compat', 'information_schema_key_column_usage_compat',
			'__duckgres_column_metadata'
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

	// Create pg_shadow view for user password/credential information
	// Used by psql and Metabase connection validation
	pgShadowSQL := `
		CREATE OR REPLACE VIEW pg_shadow AS
		SELECT
			'postgres'::VARCHAR AS usename,
			10::BIGINT AS usesysid,
			true AS usecreatedb,
			true AS usesuper,
			false AS userepl,
			false AS usebypassrls,
			'********'::VARCHAR AS passwd,
			NULL::TIMESTAMPTZ AS valuntil,
			NULL::VARCHAR[] AS useconfig
	`
	db.Exec(pgShadowSQL)

	// Create pg_user view (simplified view of pg_shadow)
	// Used by psql \du command and Metabase
	pgUserSQL := `
		CREATE OR REPLACE VIEW pg_user AS
		SELECT
			usename,
			usesysid,
			usecreatedb,
			usesuper,
			userepl,
			usebypassrls,
			'********'::VARCHAR AS passwd,
			valuntil,
			useconfig
		FROM pg_shadow
	`
	db.Exec(pgUserSQL)

	// Create pg_stat_activity view (database connection monitoring)
	// Used by Grafana monitoring dashboards and Metabase admin
	// Returns empty result set with correct schema
	pgStatActivitySQL := `
		CREATE OR REPLACE VIEW pg_stat_activity AS
		SELECT
			NULL::OID AS datid,
			NULL::TEXT AS datname,
			NULL::INTEGER AS pid,
			NULL::INTEGER AS leader_pid,
			NULL::OID AS usesysid,
			NULL::TEXT AS usename,
			NULL::TEXT AS application_name,
			NULL::TEXT AS client_addr,
			NULL::TEXT AS client_hostname,
			NULL::INTEGER AS client_port,
			NULL::TIMESTAMPTZ AS backend_start,
			NULL::TIMESTAMPTZ AS xact_start,
			NULL::TIMESTAMPTZ AS query_start,
			NULL::TIMESTAMPTZ AS state_change,
			NULL::TEXT AS wait_event_type,
			NULL::TEXT AS wait_event,
			NULL::TEXT AS state,
			NULL::OID AS backend_xid,
			NULL::TEXT AS backend_xmin,
			NULL::BIGINT AS query_id,
			NULL::TEXT AS query,
			NULL::TEXT AS backend_type
		WHERE false
	`
	db.Exec(pgStatActivitySQL)

	// Create pg_extension view (installed extensions metadata)
	// Used by Metabase feature detection
	// Returns plpgsql as a default extension for compatibility
	pgExtensionSQL := `
		CREATE OR REPLACE VIEW pg_extension AS
		SELECT
			13823::OID AS oid,
			'plpgsql' AS extname,
			10::OID AS extowner,
			11::OID AS extnamespace,
			true AS extrelocatable,
			'1.0' AS extversion,
			NULL::OID[] AS extconfig,
			NULL::TEXT[] AS extcondition
	`
	db.Exec(pgExtensionSQL)

	// Create pg_attribute wrapper view
	// Wraps DuckDB's pg_catalog.pg_attribute for compatibility
	// Filter out internal duckgres views
	pgAttributeSQL := `
		CREATE OR REPLACE VIEW pg_attribute AS
		SELECT
			a.attrelid,
			a.attname,
			a.atttypid,
			a.attstattarget,
			a.attlen,
			a.attnum,
			a.attndims,
			a.attcacheoff,
			a.atttypmod,
			a.attbyval,
			a.attstorage,
			a.attalign,
			a.attnotnull,
			a.atthasdef,
			a.atthasmissing,
			a.attidentity,
			a.attgenerated,
			a.attisdropped,
			a.attislocal,
			a.attinhcount,
			a.attcollation,
			a.attcompression,
			a.attacl,
			a.attoptions,
			a.attfdwoptions,
			a.attmissingval
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
		WHERE c.relname NOT IN (
			'pg_database', 'pg_class_full', 'pg_collation', 'pg_policy', 'pg_roles',
			'pg_statistic_ext', 'pg_publication_tables', 'pg_rules', 'pg_publication',
			'pg_publication_rel', 'pg_inherits', 'pg_namespace', 'pg_matviews',
			'pg_stat_user_tables', 'pg_attribute', 'pg_type', 'pg_index', 'pg_constraint',
			'pg_description', 'pg_attrdef', 'pg_proc', 'pg_tables', 'pg_views',
			'pg_shadow', 'pg_user',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat',
			'information_schema_table_constraints_compat', 'information_schema_key_column_usage_compat',
			'__duckgres_column_metadata'
		)
	`
	db.Exec(pgAttributeSQL)

	// Create pg_type wrapper view with comprehensive PostgreSQL type OID mappings
	// DuckDB has pg_catalog.pg_type but with different OIDs
	// We create a comprehensive mapping for PostgreSQL compatibility
	pgTypeSQL := `
		CREATE OR REPLACE VIEW pg_type AS
		SELECT * FROM pg_catalog.pg_type
		UNION ALL
		SELECT * FROM (VALUES
			-- Additional PostgreSQL types not in DuckDB's pg_type
			(16::BIGINT, 'bool', 2527::BIGINT, 0, 1::BIGINT, true, 'b', 'B', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'c', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(21::BIGINT, 'int2', 2527::BIGINT, 0, 2::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 's', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(23::BIGINT, 'int4', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(25::BIGINT, 'text', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'S', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(700::BIGINT, 'float4', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(701::BIGINT, 'float8', 2527::BIGINT, 0, 8::BIGINT, true, 'b', 'N', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1042::BIGINT, 'bpchar', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1043::BIGINT, 'varchar', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1082::BIGINT, 'date', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'D', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1083::BIGINT, 'time', 2527::BIGINT, 0, 8::BIGINT, true, 'b', 'D', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1114::BIGINT, 'timestamp', 2527::BIGINT, 0, 8::BIGINT, true, 'b', 'D', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1184::BIGINT, 'timestamptz', 2527::BIGINT, 0, 8::BIGINT, true, 'b', 'D', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1186::BIGINT, 'interval', 2527::BIGINT, 0, 16::BIGINT, false, 'b', 'T', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1700::BIGINT, 'numeric', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'm', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2950::BIGINT, 'uuid', 2527::BIGINT, 0, 16::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'c', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(114::BIGINT, 'json', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3802::BIGINT, 'jsonb', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(26::BIGINT, 'oid', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2205::BIGINT, 'regclass', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2206::BIGINT, 'regtype', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1005::BIGINT, '_int2', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 21, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1007::BIGINT, '_int4', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 23, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1016::BIGINT, '_int8', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 20, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1009::BIGINT, '_text', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 25, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1021::BIGINT, '_float4', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 700, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1022::BIGINT, '_float8', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 701, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1000::BIGINT, '_bool', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1115::BIGINT, '_timestamp', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1114, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2951::BIGINT, '_uuid', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2950, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(199::BIGINT, '_json', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 114, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3807::BIGINT, '_jsonb', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3802, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- String types
			(18::BIGINT, 'char', 2527::BIGINT, 0, 1::BIGINT, true, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'c', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(19::BIGINT, 'name', 2527::BIGINT, 0, 64::BIGINT, false, 'b', 'S', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'c', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- System types
			(22::BIGINT, 'int2vector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 21, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(24::BIGINT, 'regproc', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(27::BIGINT, 'tid', 2527::BIGINT, 0, 6::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 's', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(28::BIGINT, 'xid', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(29::BIGINT, 'cid', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(30::BIGINT, 'oidvector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 26, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Network types
			(650::BIGINT, 'cidr', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'I', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'm', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(651::BIGINT, '_cidr', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 650, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(829::BIGINT, 'macaddr', 2527::BIGINT, 0, 6::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(869::BIGINT, 'inet', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'I', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'm', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Numeric types
			(790::BIGINT, 'money', 2527::BIGINT, 0, 8::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(791::BIGINT, '_money', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 790, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Geometric types
			(600::BIGINT, 'point', 2527::BIGINT, 0, 16::BIGINT, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(601::BIGINT, 'lseg', 2527::BIGINT, 0, 32::BIGINT, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(602::BIGINT, 'path', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(603::BIGINT, 'box', 2527::BIGINT, 0, 32::BIGINT, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(604::BIGINT, 'polygon', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(628::BIGINT, 'line', 2527::BIGINT, 0, 24::BIGINT, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(718::BIGINT, 'circle', 2527::BIGINT, 0, 24::BIGINT, false, 'b', 'G', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- XML and text search types
			(142::BIGINT, 'xml', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3614::BIGINT, 'tsvector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3615::BIGINT, 'tsquery', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Range types
			(3904::BIGINT, 'int4range', 2527::BIGINT, 0, -1::BIGINT, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3906::BIGINT, 'numrange', 2527::BIGINT, 0, -1::BIGINT, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3908::BIGINT, 'tsrange', 2527::BIGINT, 0, -1::BIGINT, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3910::BIGINT, 'tstzrange', 2527::BIGINT, 0, -1::BIGINT, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3912::BIGINT, 'daterange', 2527::BIGINT, 0, -1::BIGINT, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3926::BIGINT, 'int8range', 2527::BIGINT, 0, -1::BIGINT, false, 'r', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Pseudo types
			(2249::BIGINT, 'record', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2276::BIGINT, 'any', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2277::BIGINT, 'anyarray', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2278::BIGINT, 'void', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2279::BIGINT, 'trigger', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Additional arrays
			(1014::BIGINT, '_bpchar', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1042, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1015::BIGINT, '_varchar', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1043, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1182::BIGINT, '_date', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1082, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1183::BIGINT, '_time', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1083, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1185::BIGINT, '_timestamptz', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1184, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1187::BIGINT, '_interval', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1186, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1231::BIGINT, '_numeric', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1700, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1028::BIGINT, '_oid', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 26, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Internal and composite types
			(32::BIGINT, 'pg_ddl_command', 2527::BIGINT, 0, 8::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(71::BIGINT, 'pg_type', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(75::BIGINT, 'pg_attribute', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(81::BIGINT, 'pg_proc', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(83::BIGINT, 'pg_class', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(143::BIGINT, '_xml', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 142, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(194::BIGINT, 'pg_node_tree', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(210::BIGINT, '_pg_type', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 71, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(269::BIGINT, 'table_am_handler', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(270::BIGINT, '_pg_attribute', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 75, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(271::BIGINT, '_xid8', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 5069, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(272::BIGINT, '_pg_proc', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 81, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(273::BIGINT, '_pg_class', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 83, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(325::BIGINT, 'index_am_handler', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(629::BIGINT, '_line', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 628, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(705::BIGINT, 'unknown', 2527::BIGINT, 0, -2::BIGINT, false, 'p', 'X', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(719::BIGINT, '_circle', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 718, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(774::BIGINT, 'macaddr8', 2527::BIGINT, 0, 8::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(775::BIGINT, '_macaddr8', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 774, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Additional array types
			(1001::BIGINT, '_bytea', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1002::BIGINT, '_char', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 18, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1003::BIGINT, '_name', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 19, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1006::BIGINT, '_int2vector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 22, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1008::BIGINT, '_regproc', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 24, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1010::BIGINT, '_tid', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 27, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1011::BIGINT, '_xid', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 28, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1012::BIGINT, '_cid', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 29, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1013::BIGINT, '_oidvector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 30, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1017::BIGINT, '_point', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 600, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1018::BIGINT, '_lseg', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 601, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1019::BIGINT, '_path', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 602, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1020::BIGINT, '_box', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 603, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1027::BIGINT, '_polygon', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 604, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1033::BIGINT, 'aclitem', 2527::BIGINT, 0, 16::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1034::BIGINT, '_aclitem', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1033, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1040::BIGINT, '_macaddr', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 829, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1041::BIGINT, '_inet', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 869, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1248::BIGINT, 'pg_database', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1263::BIGINT, '_cstring', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2275, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1270::BIGINT, '_timetz', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1266, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Bit types
			(1560::BIGINT, 'bit', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'V', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1561::BIGINT, '_bit', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1560, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1562::BIGINT, 'varbit', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'V', true, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'i', 'x', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(1563::BIGINT, '_varbit', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1562, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Cursor types
			(1790::BIGINT, 'refcursor', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2201::BIGINT, '_refcursor', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 1790, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Reg* types
			(2202::BIGINT, 'regprocedure', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2203::BIGINT, 'regoper', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2204::BIGINT, 'regoperator', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2207::BIGINT, '_regprocedure', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2202, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2208::BIGINT, '_regoper', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2203, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2209::BIGINT, '_regoperator', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2204, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2210::BIGINT, '_regclass', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2205, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2211::BIGINT, '_regtype', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2206, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Handler and pseudo types
			(2275::BIGINT, 'cstring', 2527::BIGINT, 0, -2::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2280::BIGINT, 'language_handler', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2281::BIGINT, 'internal', 2527::BIGINT, 0, 8::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2283::BIGINT, 'anyelement', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2287::BIGINT, '_record', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, 2249, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2776::BIGINT, 'anynonarray', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Auth types
			(2842::BIGINT, 'pg_authid', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2843::BIGINT, 'pg_auth_members', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Transaction and snapshot types
			(2949::BIGINT, '_txid_snapshot', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 2970, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(2970::BIGINT, 'txid_snapshot', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3115::BIGINT, 'fdw_handler', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3220::BIGINT, 'pg_lsn', 2527::BIGINT, 0, 8::BIGINT, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3221::BIGINT, '_pg_lsn', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3220, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3310::BIGINT, 'tsm_handler', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3361::BIGINT, 'pg_ndistinct', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3402::BIGINT, 'pg_dependencies', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Text search types (gtsvector and arrays)
			(3500::BIGINT, 'anyenum', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3642::BIGINT, 'gtsvector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3643::BIGINT, '_tsvector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3614, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3644::BIGINT, '_gtsvector', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3642, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3645::BIGINT, '_tsquery', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3615, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3734::BIGINT, 'regconfig', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3735::BIGINT, '_regconfig', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3734, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3769::BIGINT, 'regdictionary', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3770::BIGINT, '_regdictionary', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3769, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Range/event types
			(3831::BIGINT, 'anyrange', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3838::BIGINT, 'event_trigger', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3905::BIGINT, '_int4range', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3904, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3907::BIGINT, '_numrange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3906, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3909::BIGINT, '_tsrange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3908, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3911::BIGINT, '_tstzrange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3910, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3913::BIGINT, '_daterange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3912, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(3927::BIGINT, '_int8range', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 3926, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- JSON/namespace/role types
			(4066::BIGINT, 'pg_shseclabel', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4072::BIGINT, 'jsonpath', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4073::BIGINT, '_jsonpath', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4072, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4089::BIGINT, 'regnamespace', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4090::BIGINT, '_regnamespace', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4089, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4096::BIGINT, 'regrole', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4097::BIGINT, '_regrole', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4096, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4191::BIGINT, 'regcollation', 2527::BIGINT, 0, 4::BIGINT, true, 'b', 'N', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4192::BIGINT, '_regcollation', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4191, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Multirange types (PG14+)
			(4451::BIGINT, 'int4multirange', 2527::BIGINT, 0, -1::BIGINT, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4532::BIGINT, 'nummultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4533::BIGINT, 'tsmultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4534::BIGINT, 'tstzmultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4535::BIGINT, 'datemultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4536::BIGINT, 'int8multirange', 2527::BIGINT, 0, -1::BIGINT, false, 'm', 'R', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4537::BIGINT, 'anymultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4538::BIGINT, 'anycompatiblemultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Internal stats and summary types
			(4600::BIGINT, 'pg_brin_bloom_summary', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(4601::BIGINT, 'pg_brin_minmax_multi_summary', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(5017::BIGINT, 'pg_mcv_list', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'Z', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(5038::BIGINT, 'pg_snapshot', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(5039::BIGINT, '_pg_snapshot', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 5038, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(5069::BIGINT, 'xid8', 2527::BIGINT, 0, 8::BIGINT, true, 'b', 'U', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Any-compatible types
			(5077::BIGINT, 'anycompatible', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(5078::BIGINT, 'anycompatiblearray', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(5079::BIGINT, 'anycompatiblenonarray', 2527::BIGINT, 0, 4::BIGINT, true, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(5080::BIGINT, 'anycompatiblerange', 2527::BIGINT, 0, -1::BIGINT, false, 'p', 'P', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Subscription type
			(6101::BIGINT, 'pg_subscription', 2527::BIGINT, 0, -1::BIGINT, false, 'c', 'C', false, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			-- Multirange arrays
			(6150::BIGINT, '_int4multirange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4451, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(6151::BIGINT, '_nummultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4532, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(6152::BIGINT, '_tsmultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4533, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(6153::BIGINT, '_tstzmultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4534, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(6155::BIGINT, '_datemultirange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4535, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(6157::BIGINT, '_int8multirange', 2527::BIGINT, 0, -1::BIGINT, false, 'b', 'A', false, true, NULL, NULL, NULL, 4536, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'd', 'p', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
		) AS t(oid, typname, typnamespace, typowner, typlen, typbyval, typtype, typcategory, typispreferred, typisdefined, typdelim, typrelid, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, typalign, typstorage, typnotnull, typbasetype, typtypmod, typndims, typcollation, typdefaultbin, typdefault, typacl)
		WHERE t.oid NOT IN (SELECT oid FROM pg_catalog.pg_type WHERE oid IS NOT NULL)
	`
	db.Exec(pgTypeSQL)

	// Materialize pg_type VIEW into TABLE for sub-second JOIN performance
	// The VIEW with 170+ UNION ALL adds ~1.5s to queries; TABLE with index is instant
	// Use safe swap pattern: rename VIEW to backup, rename TABLE to pg_type, then drop backup
	if _, err := db.Exec("CREATE TABLE pg_type_materialized AS SELECT * FROM pg_type"); err != nil {
		log.Printf("Warning: failed to materialize pg_type, keeping VIEW: %v", err)
	} else {
		// Rename VIEW to backup first (so we can restore if RENAME TABLE fails)
		if _, err := db.Exec("ALTER VIEW pg_type RENAME TO pg_type_view_backup"); err != nil {
			log.Printf("Warning: failed to rename pg_type view to backup, keeping VIEW: %v", err)
			db.Exec("DROP TABLE IF EXISTS pg_type_materialized")
		} else {
			// Rename materialized table to pg_type
			if _, err := db.Exec("ALTER TABLE pg_type_materialized RENAME TO pg_type"); err != nil {
				log.Printf("Warning: failed to rename pg_type_materialized, restoring VIEW: %v", err)
				db.Exec("ALTER VIEW pg_type_view_backup RENAME TO pg_type")
				db.Exec("DROP TABLE IF EXISTS pg_type_materialized")
			} else {
				// Success - create index and drop backup
				db.Exec("CREATE INDEX idx_pg_type_oid ON main.pg_type(oid)")
				db.Exec("DROP VIEW IF EXISTS pg_type_view_backup")
			}
		}
	}

	// Create pg_index wrapper view
	// DuckDB has pg_catalog.pg_index with real index metadata
	pgIndexSQL := `
		CREATE OR REPLACE VIEW pg_index AS
		SELECT * FROM pg_catalog.pg_index
	`
	db.Exec(pgIndexSQL)

	// Create pg_constraint wrapper view
	// DuckDB has pg_catalog.pg_constraint with constraint metadata
	pgConstraintSQL := `
		CREATE OR REPLACE VIEW pg_constraint AS
		SELECT * FROM pg_catalog.pg_constraint
	`
	db.Exec(pgConstraintSQL)

	// Create pg_description view (object comments - empty, DuckDB doesn't store comments this way)
	pgDescriptionSQL := `
		CREATE OR REPLACE VIEW pg_description AS
		SELECT
			0::BIGINT AS objoid,
			0::BIGINT AS classoid,
			0::INTEGER AS objsubid,
			''::VARCHAR AS description
		WHERE false
	`
	db.Exec(pgDescriptionSQL)

	// Create pg_attrdef view (column defaults)
	pgAttrdefSQL := `
		CREATE OR REPLACE VIEW pg_attrdef AS
		SELECT
			0::BIGINT AS oid,
			0::BIGINT AS adrelid,
			0::INTEGER AS adnum,
			''::VARCHAR AS adbin
		WHERE false
	`
	db.Exec(pgAttrdefSQL)

	// Create pg_proc view (functions/procedures - stub for now)
	pgProcSQL := `
		CREATE OR REPLACE VIEW pg_proc AS
		SELECT
			0::BIGINT AS oid,
			''::VARCHAR AS proname,
			0::BIGINT AS pronamespace,
			0::INTEGER AS proowner,
			0::BIGINT AS prolang,
			0::REAL AS procost,
			0::REAL AS prorows,
			0::BIGINT AS provariadic,
			''::VARCHAR AS prosupport,
			''::VARCHAR AS prokind,
			false AS prosecdef,
			false AS proleakproof,
			false AS proisstrict,
			false AS proretset,
			''::VARCHAR AS provolatile,
			''::VARCHAR AS proparallel,
			0::INTEGER AS pronargs,
			0::INTEGER AS pronargdefaults,
			0::BIGINT AS prorettype,
			ARRAY[]::BIGINT[] AS proargtypes,
			ARRAY[]::BIGINT[] AS proallargtypes,
			ARRAY[]::VARCHAR[] AS proargmodes,
			ARRAY[]::VARCHAR[] AS proargnames,
			NULL::VARCHAR AS proargdefaults,
			ARRAY[]::BIGINT[] AS protrftypes,
			''::VARCHAR AS prosrc,
			''::VARCHAR AS probin,
			NULL::VARCHAR AS prosqlbody,
			ARRAY[]::VARCHAR[] AS proconfig,
			NULL::VARCHAR AS proacl
		WHERE false
	`
	db.Exec(pgProcSQL)

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

	// Create pg_tables wrapper that maps 'main' to 'public' for PostgreSQL compatibility
	// Apply DUCKGRES_TABLE_LIMIT if set for faster testing (only limits user tables)
	var pgTablesSQL string
	if tableLimit > 0 {
		pgTablesSQL = fmt.Sprintf(`
		CREATE OR REPLACE VIEW pg_tables AS
		-- System tables (no limit)
		SELECT schemaname, tablename, schemaname AS tableowner, tablespace, hasindexes, hasrules, hastriggers
		FROM pg_catalog.pg_tables
		WHERE schemaname NOT IN ('main', 'public')
		UNION ALL
		-- User tables (with limit for testing)
		SELECT 'public' AS schemaname, tablename, 'public' AS tableowner, tablespace, hasindexes, hasrules, hastriggers
		FROM pg_catalog.pg_tables
		WHERE schemaname = 'main'
		LIMIT %d
	`, tableLimit)
	} else {
		pgTablesSQL = `
		CREATE OR REPLACE VIEW pg_tables AS
		SELECT
			CASE WHEN schemaname = 'main' THEN 'public' ELSE schemaname END AS schemaname,
			tablename,
			CASE WHEN schemaname = 'main' THEN 'public' ELSE schemaname END AS tableowner,
			tablespace,
			hasindexes,
			hasrules,
			hastriggers
		FROM pg_catalog.pg_tables
	`
	}
	db.Exec(pgTablesSQL)

	// Create pg_views wrapper that maps 'main' to 'public' for PostgreSQL compatibility
	pgViewsSQL := `
		CREATE OR REPLACE VIEW pg_views AS
		SELECT
			CASE WHEN schemaname = 'main' THEN 'public' ELSE schemaname END AS schemaname,
			viewname,
			CASE WHEN schemaname = 'main' THEN 'public' ELSE schemaname END AS viewowner,
			definition
		FROM pg_catalog.pg_views
	`
	db.Exec(pgViewsSQL)

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
		// has_any_column_privilege - check column access
		`CREATE OR REPLACE MACRO has_any_column_privilege(table_name, priv) AS true`,
		// _pg_expandarray - fallback macro for cases not handled by transpiler.
		// The transpiler's expandarray.go provides the correct LATERAL join
		// transformation for most queries. This macro handles edge cases.
		// Returns a struct with x (value) and n (1-based index).
		`CREATE OR REPLACE MACRO _pg_expandarray(arr) AS
			STRUCT_PACK(x := unnest(arr), n := unnest(range(1, len(arr) + 1)))`,
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
		// current_schemas - return list of schemas in search path
		// Grafana uses this for table discovery filtering
		`CREATE OR REPLACE MACRO current_schemas(include_implicit) AS
			CASE WHEN include_implicit THEN ['pg_catalog', 'public'] ELSE ['public'] END`,
		// array_upper - return upper bound of array dimension
		// Used in various catalog queries
		`CREATE OR REPLACE MACRO array_upper(arr, dim) AS
			CASE WHEN dim = 1 THEN len(arr) ELSE NULL END`,
		// unnest compatibility - DuckDB has unnest but we need to ensure it works
		// This is a pass-through since DuckDB supports unnest natively

		// pg_get_viewdef - get view definition SQL
		// Used by Metabase view introspection
		// Returns NULL as stub - proper OID lookup is complex and not critical for compatibility
		`DROP MACRO IF EXISTS pg_get_viewdef`,
		`CREATE MACRO pg_get_viewdef(view_oid, pretty_bool := false) AS NULL`,

		// Size functions - return 0 as stubs since DuckDB doesn't track relation sizes the same way
		// Used by various PostgreSQL clients for storage monitoring
		// Use DROP before CREATE to ensure clean state (DuckDB doesn't support CREATE OR REPLACE for overloaded macros)
		`DROP MACRO IF EXISTS pg_total_relation_size`,
		`CREATE MACRO pg_total_relation_size(rel) AS 0::BIGINT`,
		`DROP MACRO IF EXISTS pg_table_size`,
		`CREATE MACRO pg_table_size(rel) AS 0::BIGINT`,
		`DROP MACRO IF EXISTS pg_indexes_size`,
		`CREATE MACRO pg_indexes_size(rel) AS 0::BIGINT`,
		`DROP MACRO IF EXISTS pg_relation_size`,
		`CREATE MACRO pg_relation_size(rel, fork := 'main') AS 0::BIGINT`,

		// pg_backend_pid - return backend process ID
		// Used by connection monitoring
		`CREATE OR REPLACE MACRO pg_backend_pid() AS 1`,

		// quote_ident - quote identifier if needed
		// Used in dynamic SQL generation
		`CREATE OR REPLACE MACRO quote_ident(ident) AS
			CASE WHEN ident ~ '^[a-z_][a-z0-9_]*$' THEN ident
			ELSE '"' || replace(ident, '"', '""') || '"' END`,

		// row_to_json - convert record/row to JSON
		// Used in application queries for JSON serialization
		`CREATE OR REPLACE MACRO row_to_json(record) AS to_json(record)`,

		// set_config - set configuration value (stub)
		// Returns the new_value parameter to satisfy queries that use it
		`CREATE OR REPLACE MACRO set_config(setting_name, new_value, is_local) AS new_value`,

		// pg_date_trunc - safe date_trunc wrapper for JDBC NULL handling
		// Prevents errors when JDBC passes NULL timestamp values
		`CREATE OR REPLACE MACRO pg_date_trunc(date_part, timestamp_val) AS
			CASE WHEN timestamp_val IS NULL THEN CAST(NULL AS TIMESTAMP)
			ELSE date_trunc(date_part, timestamp_val) END`,
	}

	for _, f := range functions {
		if _, err := db.Exec(f); err != nil {
			// Log but don't fail - some might already exist or conflict
			continue
		}
	}

	// Create DuckLake cache tables (used when DuckLake is attached for faster catalog queries)
	// These tables cache table/column metadata to avoid SSL timeouts during long Metabase syncs
	if err := CreateCacheTables(db); err != nil {
		log.Printf("Warning: failed to create DuckLake cache tables: %v", err)
		// Non-fatal - cache is optional optimization
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
			'pg_stat_user_tables', 'pg_attribute', 'pg_type', 'pg_index', 'pg_constraint',
			'pg_description', 'pg_attrdef', 'pg_proc',
			-- information_schema compat views
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat',
			'information_schema_table_constraints_compat', 'information_schema_key_column_usage_compat'
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
			'pg_stat_user_tables', 'pg_attribute', 'pg_type', 'pg_index', 'pg_constraint',
			'pg_description', 'pg_attrdef', 'pg_proc',
			-- information_schema compat views
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat',
			'information_schema_table_constraints_compat', 'information_schema_key_column_usage_compat'
		)
		AND v.table_name NOT LIKE 'duckdb_%%'
		AND v.table_name NOT LIKE 'sqlite_%%'
		AND v.table_name NOT LIKE 'pragma_%%'
	`
	db.Exec(fmt.Sprintf(viewsViewSQL, infoSchemaPrefix))

	// Create information_schema.table_constraints wrapper view
	// Returns constraints from pg_constraint - primarily for Metabase PK/FK discovery
	// Normalize 'main' schema to 'public' for PostgreSQL compatibility
	tableConstraintsViewSQL := `
		CREATE OR REPLACE VIEW main.information_schema_table_constraints_compat AS
		SELECT
			'memory' AS constraint_catalog,
			CASE WHEN n.nspname = 'main' THEN 'public' ELSE n.nspname END AS constraint_schema,
			c.conname AS constraint_name,
			'memory' AS table_catalog,
			CASE WHEN n.nspname = 'main' THEN 'public' ELSE n.nspname END AS table_schema,
			cl.relname AS table_name,
			CASE c.contype
				WHEN 'p' THEN 'PRIMARY KEY'
				WHEN 'u' THEN 'UNIQUE'
				WHEN 'f' THEN 'FOREIGN KEY'
				WHEN 'c' THEN 'CHECK'
				WHEN 'x' THEN 'EXCLUDE'
				ELSE 'UNKNOWN'
			END AS constraint_type,
			CASE WHEN c.condeferrable THEN 'YES' ELSE 'NO' END AS is_deferrable,
			CASE WHEN c.condeferred THEN 'YES' ELSE 'NO' END AS initially_deferred,
			'YES' AS enforced
		FROM pg_catalog.pg_constraint c
		JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
		JOIN pg_catalog.pg_namespace n ON c.connamespace = n.oid
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	`
	db.Exec(tableConstraintsViewSQL)

	// Create information_schema.key_column_usage wrapper view
	// Returns columns that are part of primary key or unique constraints
	// Used by Metabase to join with table_constraints for PK discovery
	keyColumnUsageViewSQL := `
		CREATE OR REPLACE VIEW main.information_schema_key_column_usage_compat AS
		SELECT
			'memory' AS constraint_catalog,
			CASE WHEN n.nspname = 'main' THEN 'public' ELSE n.nspname END AS constraint_schema,
			c.conname AS constraint_name,
			'memory' AS table_catalog,
			CASE WHEN n.nspname = 'main' THEN 'public' ELSE n.nspname END AS table_schema,
			cl.relname AS table_name,
			a.attname AS column_name,
			a.attnum AS ordinal_position,
			NULL AS position_in_unique_constraint
		FROM pg_catalog.pg_constraint c
		JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
		JOIN pg_catalog.pg_namespace n ON c.connamespace = n.oid
		JOIN pg_catalog.pg_attribute a ON a.attrelid = cl.oid AND a.attnum = ANY(c.conkey)
		WHERE c.contype IN ('p', 'u')
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	`
	db.Exec(keyColumnUsageViewSQL)

	return nil
}

// recreatePgClassForDuckLake creates pg_class_full from cached DuckLake metadata.
//
// This differs from using duckdb_tables() directly because:
// 1. Long-running Metabase syncs can cause SSL timeouts when repeatedly querying
//    the DuckLake metadata database over the network
// 2. Caching the metadata locally allows syncs to complete without timeouts
// 3. The cache duration (DuckLakeCacheDuration) balances freshness vs stability
//
// Trade-off: Tables created/dropped during a sync may not appear until cache refresh.
// Must be called AFTER DuckLake is attached AND cache is refreshed.
func recreatePgClassForDuckLake(db *sql.DB) error {
	// DuckLake user tables are cached in ducklake_tables_cache
	// We synthesize pg_class entries from cache and UNION with internal tables
	pgClassSQL := `
		CREATE OR REPLACE VIEW pg_class_full AS
		-- User tables from DuckLake cache
		SELECT
			(16384 + t.table_id)::BIGINT AS oid,
			t.table_name AS relname,
			(16384 + t.schema_id)::BIGINT AS relnamespace,
			0::BIGINT AS reltype,
			0::BIGINT AS reloftype,
			6171::BIGINT AS relowner,
			0::BIGINT AS relam,
			0::BIGINT AS relfilenode,
			0::BIGINT AS reltablespace,
			0::BIGINT AS relpages,
			0::DOUBLE AS reltuples,
			0::BIGINT AS relallvisible,
			0::BIGINT AS reltoastrelid,
			0::BIGINT AS reltoastidxid,
			false AS relhasindex,
			false AS relisshared,
			'p'::VARCHAR AS relpersistence,
			'r'::VARCHAR AS relkind,
			(SELECT COUNT(*)::SMALLINT FROM main.ducklake_columns_cache c
			 WHERE c.table_id = t.table_id)::SMALLINT AS relnatts,
			0::SMALLINT AS relchecks,
			false AS relhasoids,
			false AS relhaspkey,
			false AS relhasrules,
			false AS relhastriggers,
			false AS relhassubclass,
			false AS relrowsecurity,
			false AS relforcerowsecurity,
			true AS relispopulated,
			'd'::VARCHAR AS relreplident,
			false AS relispartition,
			0::BIGINT AS relrewrite,
			0::BIGINT AS relfrozenxid,
			0::BIGINT AS relminmxid,
			NULL AS relacl,
			NULL AS reloptions,
			NULL AS relpartbound
		FROM main.ducklake_tables_cache t
		WHERE t.table_name NOT LIKE 'ducklake_%'

		UNION ALL

		-- DuckLake internal metadata tables from pg_catalog
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
			0::BIGINT AS reltoastidxid,
			relhasindex,
			relisshared,
			relpersistence,
			relkind,
			relnatts,
			relchecks,
			false AS relhasoids,
			false AS relhaspkey,
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
		FROM "__ducklake_metadata_ducklake".pg_catalog.pg_class
		WHERE relname LIKE 'ducklake_%'
		  AND relkind = 'r'
		  AND relname NOT IN (
			'pg_database', 'pg_class_full', 'pg_collation', 'pg_policy', 'pg_roles',
			'pg_statistic_ext', 'pg_publication_tables', 'pg_rules', 'pg_publication',
			'pg_publication_rel', 'pg_inherits', 'pg_namespace', 'pg_matviews',
			'pg_stat_user_tables', 'pg_attribute', 'pg_type', 'pg_index', 'pg_constraint',
			'pg_description', 'pg_attrdef', 'pg_proc', 'pg_tables', 'pg_views',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat',
			'information_schema_table_constraints_compat', 'information_schema_key_column_usage_compat',
			'__duckgres_column_metadata'
		)
	`
	_, err := db.Exec(pgClassSQL)
	return err
}

// recreatePgNamespaceForDuckLake recreates pg_namespace to source from cached DuckLake metadata.
// Uses cache tables to avoid SSL timeouts during long Metabase syncs.
// Must be called AFTER DuckLake is attached AND cache is refreshed.
func recreatePgNamespaceForDuckLake(db *sql.DB) error {
	// Include schemas from ducklake_tables_cache (distinct schema_id/schema_name)
	pgNamespaceSQL := `
		CREATE OR REPLACE VIEW pg_namespace AS
		-- DuckLake user schemas from cache
		SELECT DISTINCT
			(16384 + t.schema_id)::BIGINT AS oid,
			CASE WHEN t.schema_name = 'main' THEN 'public' ELSE t.schema_name END AS nspname,
			6171::BIGINT AS nspowner,
			NULL AS nspacl
		FROM main.ducklake_tables_cache t

		UNION ALL

		-- System schemas from pg_catalog
		SELECT
			oid,
			CASE WHEN nspname = 'main' THEN 'public' ELSE nspname END AS nspname,
			CASE WHEN nspname = 'main' THEN 6171::BIGINT ELSE 10::BIGINT END AS nspowner,
			nspacl
		FROM "__ducklake_metadata_ducklake".pg_catalog.pg_namespace
		WHERE nspname IN ('pg_catalog', 'information_schema', 'pg_temp')
	`
	_, err := db.Exec(pgNamespaceSQL)
	return err
}

// recreatePgAttributeForDuckLake recreates pg_attribute to source from cached DuckLake metadata.
// Uses cache tables to avoid SSL timeouts during long Metabase syncs.
// Must be called AFTER DuckLake is attached AND cache is refreshed.
func recreatePgAttributeForDuckLake(db *sql.DB) error {
	// Synthesize pg_attribute from ducklake_columns_cache
	pgAttributeSQL := `
		CREATE OR REPLACE VIEW pg_attribute AS
		-- User table columns from DuckLake cache
		SELECT
			(16384 + c.table_id)::BIGINT AS attrelid,
			c.column_name AS attname,
			CASE c.data_type
				WHEN 'boolean' THEN 16::BIGINT
				WHEN 'smallint' THEN 21::BIGINT
				WHEN 'integer' THEN 23::BIGINT
				WHEN 'bigint' THEN 20::BIGINT
				WHEN 'real' THEN 700::BIGINT
				WHEN 'double precision' THEN 701::BIGINT
				WHEN 'numeric' THEN 1700::BIGINT
				WHEN 'text' THEN 25::BIGINT
				WHEN 'character varying' THEN 1043::BIGINT
				WHEN 'character' THEN 1042::BIGINT
				WHEN 'bytea' THEN 17::BIGINT
				WHEN 'date' THEN 1082::BIGINT
				WHEN 'time without time zone' THEN 1083::BIGINT
				WHEN 'timestamp without time zone' THEN 1114::BIGINT
				WHEN 'timestamp with time zone' THEN 1184::BIGINT
				WHEN 'interval' THEN 1186::BIGINT
				WHEN 'json' THEN 114::BIGINT
				WHEN 'uuid' THEN 2950::BIGINT
				WHEN 'array' THEN 1009::BIGINT  -- _text as default array type
				ELSE 25::BIGINT  -- default to text
			END AS atttypid,
			0::INTEGER AS attstattarget,
			-1::SMALLINT AS attlen,
			(c.column_index + 1)::SMALLINT AS attnum,
			0::INTEGER AS attndims,
			-1::INTEGER AS attcacheoff,
			-1::INTEGER AS atttypmod,
			false AS attbyval,
			'x'::VARCHAR AS attstorage,
			'i'::VARCHAR AS attalign,
			NOT c.is_nullable AS attnotnull,
			false AS atthasdef,
			false AS atthasmissing,
			''::VARCHAR AS attidentity,
			''::VARCHAR AS attgenerated,
			false AS attisdropped,
			true AS attislocal,
			0::INTEGER AS attinhcount,
			0::BIGINT AS attcollation,
			''::VARCHAR AS attcompression,
			NULL AS attacl,
			NULL AS attoptions,
			NULL AS attfdwoptions,
			NULL AS attmissingval
		FROM main.ducklake_columns_cache c
		WHERE c.table_name NOT LIKE 'ducklake_%'

		UNION ALL

		-- Internal table columns from pg_catalog
		SELECT
			a.attrelid,
			a.attname,
			a.atttypid,
			a.attstattarget,
			a.attlen,
			a.attnum,
			a.attndims,
			a.attcacheoff,
			a.atttypmod,
			a.attbyval,
			a.attstorage,
			a.attalign,
			a.attnotnull,
			a.atthasdef,
			a.atthasmissing,
			a.attidentity,
			a.attgenerated,
			a.attisdropped,
			a.attislocal,
			a.attinhcount,
			a.attcollation,
			a.attcompression,
			a.attacl,
			a.attoptions,
			a.attfdwoptions,
			a.attmissingval
		FROM "__ducklake_metadata_ducklake".pg_catalog.pg_attribute a
		JOIN "__ducklake_metadata_ducklake".pg_catalog.pg_class c ON a.attrelid = c.oid
		WHERE c.relname LIKE 'ducklake_%'
	`
	_, err := db.Exec(pgAttributeSQL)
	return err
}

// recreatePgIndexForDuckLake recreates pg_index with synthetic primary keys for DuckLake.
// Since DuckLake doesn't have real indexes/constraints, we synthesize primary keys
// based on column naming conventions:
// 1. Column named exactly 'id' gets priority
// 2. First column ending with '_id' as fallback
// Uses cache tables to avoid SSL timeouts during long Metabase syncs.
// Must be called AFTER DuckLake is attached AND cache is refreshed.
func recreatePgIndexForDuckLake(db *sql.DB) error {
	// Synthesize pg_index with primary keys from ducklake cache tables
	// Note: We don't UNION with DuckDB's pg_catalog.pg_index because it has type compatibility issues
	// with indkey column (stored as VARCHAR but needs INTEGER[])
	pgIndexSQL := `
		CREATE OR REPLACE VIEW pg_index AS
		-- Synthetic primary keys for user tables from DuckLake cache
		SELECT
			(hash(t.table_name || '_pkey') % 2147483647)::BIGINT AS indexrelid,
			(16384 + t.table_id)::BIGINT AS indrelid,
			1::INTEGER AS indnatts,
			1::INTEGER AS indnkeyatts,
			true AS indisunique,
			true AS indisprimary,
			false AS indisexclusion,
			true AS indimmediate,
			false AS indisclustered,
			true AS indisvalid,
			false AS indcheckxmin,
			true AS indisready,
			true AS indislive,
			false AS indisreplident,
			-- Find the PK column: prefer 'id', then first '*_id' column
			[COALESCE(
				(SELECT (c.column_index + 1)::SMALLINT FROM main.ducklake_columns_cache c
				 WHERE c.table_id = t.table_id AND c.column_name = 'id' LIMIT 1),
				(SELECT (c.column_index + 1)::SMALLINT FROM main.ducklake_columns_cache c
				 WHERE c.table_id = t.table_id AND c.column_name LIKE '%%_id'
				 ORDER BY c.column_index LIMIT 1)
			)]::INTEGER[] AS indkey,
			[]::BIGINT[] AS indcollation,
			[]::BIGINT[] AS indclass,
			[]::INTEGER[] AS indoption,
			[]::VARCHAR AS indexprs,
			NULL::VARCHAR AS indpred
		FROM main.ducklake_tables_cache t
		WHERE t.table_name NOT LIKE 'ducklake_%'
		  -- Only for tables that have an id or *_id column
		  AND EXISTS (
			SELECT 1 FROM main.ducklake_columns_cache c
			WHERE c.table_id = t.table_id
			  AND (c.column_name = 'id' OR c.column_name LIKE '%%_id')
		  )
	`
	_, err := db.Exec(pgIndexSQL)
	return err
}

// recreatePgConstraintForDuckLake recreates pg_constraint with synthetic primary keys for DuckLake.
// Uses cache tables to avoid SSL timeouts during long Metabase syncs.
// Must be called AFTER DuckLake is attached AND cache is refreshed.
func recreatePgConstraintForDuckLake(db *sql.DB) error {
	// Synthesize pg_constraint with primary keys from ducklake cache tables
	// Note: We don't UNION with DuckDB's pg_catalog.pg_constraint to avoid type compatibility issues
	pgConstraintSQL := `
		CREATE OR REPLACE VIEW pg_constraint AS
		-- Synthetic primary key constraints for user tables from DuckLake cache
		SELECT
			(hash(t.table_name || '_pkey') % 2147483647)::BIGINT AS oid,
			(t.table_name || '_pkey')::VARCHAR AS conname,
			(16384 + t.schema_id)::BIGINT AS connamespace,
			'p'::VARCHAR AS contype,
			false AS condeferrable,
			false AS condeferred,
			true AS convalidated,
			(16384 + t.table_id)::BIGINT AS conrelid,
			0::BIGINT AS contypid,
			(hash(t.table_name || '_pkey') % 2147483647)::BIGINT AS conindid,
			0::BIGINT AS conparentid,
			0::BIGINT AS confrelid,
			''::VARCHAR AS confupdtype,
			''::VARCHAR AS confdeltype,
			''::VARCHAR AS confmatchtype,
			true AS conislocal,
			0::INTEGER AS coninhcount,
			false AS connoinherit,
			-- Find the PK column: prefer 'id', then first '*_id' column
			[COALESCE(
				(SELECT (c.column_index + 1)::SMALLINT FROM main.ducklake_columns_cache c
				 WHERE c.table_id = t.table_id AND c.column_name = 'id' LIMIT 1),
				(SELECT (c.column_index + 1)::SMALLINT FROM main.ducklake_columns_cache c
				 WHERE c.table_id = t.table_id AND c.column_name LIKE '%%_id'
				 ORDER BY c.column_index LIMIT 1)
			)]::INTEGER[] AS conkey,
			[]::INTEGER[] AS confkey,
			[]::BIGINT[] AS conpfeqop,
			[]::BIGINT[] AS conppeqop,
			[]::BIGINT[] AS conffeqop,
			[]::BIGINT[] AS conexclop,
			NULL::VARCHAR AS conbin
		FROM main.ducklake_tables_cache t
		WHERE t.table_name NOT LIKE 'ducklake_%'
		  -- Only for tables that have an id or *_id column
		  AND EXISTS (
			SELECT 1 FROM main.ducklake_columns_cache c
			WHERE c.table_id = t.table_id
			  AND (c.column_name = 'id' OR c.column_name LIKE '%%_id')
		  )
	`
	_, err := db.Exec(pgConstraintSQL)
	return err
}

// recreatePgTablesForDuckLake recreates pg_tables to source from cached DuckLake metadata.
// Uses cache tables to avoid SSL timeouts during long Metabase syncs.
// Must be called AFTER DuckLake is attached AND cache is refreshed.
func recreatePgTablesForDuckLake(db *sql.DB) error {
	tableLimit := getTableLimit()
	log.Printf("Recreating pg_tables for DuckLake (limit=%d)", tableLimit)

	var pgTablesSQL string
	if tableLimit > 0 {
		// With limit: only show limited user tables + all system tables
		// Use explicit memory.main. prefix to ensure view is in correct schema
		// Note: LIMIT must be in a subquery to only apply to user tables
		pgTablesSQL = fmt.Sprintf(`
		CREATE OR REPLACE VIEW memory.main.pg_tables AS
		-- System tables (no limit)
		SELECT schemaname, tablename, schemaname AS tableowner,
			NULL::VARCHAR AS tablespace, false AS hasindexes, false AS hasrules, false AS hastriggers
		FROM "__ducklake_metadata_ducklake".pg_catalog.pg_tables
		WHERE schemaname NOT IN ('main', 'public')

		UNION ALL

		-- User tables from DuckLake cache (with limit for testing)
		SELECT * FROM (
			SELECT
				CASE WHEN t.schema_name = 'main' THEN 'public' ELSE t.schema_name END AS schemaname,
				t.table_name AS tablename,
				'public' AS tableowner,
				NULL::VARCHAR AS tablespace,
				false AS hasindexes,
				false AS hasrules,
				false AS hastriggers
			FROM main.ducklake_tables_cache t
			WHERE t.table_name NOT LIKE 'ducklake_%%'
			LIMIT %d
		)
	`, tableLimit)
	} else {
		// No limit: show all tables
		// Use explicit memory.main. prefix to ensure view is in correct schema
		pgTablesSQL = `
		CREATE OR REPLACE VIEW memory.main.pg_tables AS
		-- System tables
		SELECT schemaname, tablename, schemaname AS tableowner,
			NULL::VARCHAR AS tablespace, false AS hasindexes, false AS hasrules, false AS hastriggers
		FROM "__ducklake_metadata_ducklake".pg_catalog.pg_tables
		WHERE schemaname NOT IN ('main', 'public')

		UNION ALL

		-- User tables from DuckLake cache
		SELECT
			CASE WHEN t.schema_name = 'main' THEN 'public' ELSE t.schema_name END AS schemaname,
			t.table_name AS tablename,
			'public' AS tableowner,
			NULL::VARCHAR AS tablespace,
			false AS hasindexes,
			false AS hasrules,
			false AS hastriggers
		FROM main.ducklake_tables_cache t
		WHERE t.table_name NOT LIKE 'ducklake_%%'
	`
	}
	_, err := db.Exec(pgTablesSQL)
	if err != nil {
		log.Printf("Error creating pg_tables view: %v", err)
	} else {
		log.Printf("Successfully created pg_tables view for DuckLake")
	}
	return err
}
