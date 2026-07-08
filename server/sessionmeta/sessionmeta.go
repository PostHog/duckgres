// Package sessionmeta installs session-local catalog/metadata overrides on
// a duckgres connection (current_database, pg_database, information_schema
// views) so they reflect the catalog the session defaults to on the PG wire.
//
// The catalog name passed in is the real, attached catalog (e.g. "ducklake")
// the session uses — duckgres no longer masks a logical database
// name onto a physical catalog, so current_database() reports the truth.
//
// Pure helpers — no dependency on github.com/duckdb/duckdb-go. The control
// plane and other duckdb-free callers use this package without linking
// libduckdb.
package sessionmeta

import (
	"context"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/server/sqlcore"
)

// InitSessionDatabaseMetadata installs session-local overrides for metadata
// surfaces (current_database, pg_database, information_schema views) so they
// reflect `catalog` — the real, attached catalog the session defaults to. The
// caller resolves `catalog` to "ducklake" (the name the catalog is actually
// attached as); there is no logical→physical masking.
func InitSessionDatabaseMetadata(ctx context.Context, executor sqlcore.QueryExecutor, catalog string) error {
	if executor == nil {
		return fmt.Errorf("session executor is required")
	}

	catalog = strings.TrimSpace(catalog)
	if catalog == "" {
		return nil
	}

	if _, err := executor.ExecContext(ctx, fmt.Sprintf(
		"CREATE OR REPLACE TEMP MACRO current_database() AS %s",
		quoteSQLStringLiteral(catalog),
	)); err != nil {
		return fmt.Errorf("create current_database() macro: %w", err)
	}

	duckLakeAttached, err := HasAttachedCatalog(ctx, executor, "ducklake")
	if err != nil {
		return fmt.Errorf("detect ducklake attachment: %w", err)
	}

	if _, err := executor.ExecContext(ctx, "USE memory"); err != nil {
		return fmt.Errorf("switch to memory catalog: %w", err)
	}
	defer func() {
		// Leave the session in a real catalog (we entered `memory` to install the
		// compat views there). For DuckLake sessions, restore `ducklake` here. Keep
		// memory.main on the search_path so the pg_catalog compat macros stay
		// resolvable after the switch.
		if duckLakeAttached {
			_, _ = executor.ExecContext(context.Background(), "USE ducklake")
			_, _ = executor.ExecContext(context.Background(), "SET search_path = 'main,memory.main'")
		}
	}()

	if _, err := executor.ExecContext(ctx, buildSessionMetadataSQL(catalog)); err != nil {
		return fmt.Errorf("apply session metadata override: %w", err)
	}

	return nil
}

func HasAttachedCatalog(ctx context.Context, executor sqlcore.QueryExecutor, catalog string) (bool, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = %s",
		quoteSQLStringLiteral(catalog),
	)
	rows, err := executor.QueryContext(
		ctx,
		query,
	)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	var count int
	if !rows.Next() {
		if rows.Err() != nil {
			return false, rows.Err()
		}
		return false, fmt.Errorf("duckdb_databases() returned no rows")
	}
	var rawCount any
	if err := rows.Scan(&rawCount); err != nil {
		return false, err
	}
	switch v := rawCount.(type) {
	case int:
		count = v
	case int8:
		count = int(v)
	case int16:
		count = int(v)
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	case uint:
		count = int(v)
	case uint8:
		count = int(v)
	case uint16:
		count = int(v)
	case uint32:
		count = int(v)
	case uint64:
		count = int(v)
	default:
		return false, fmt.Errorf("duckdb_databases() count has unsupported type %T", rawCount)
	}
	return count > 0, rows.Err()
}

// buildSessionMetadataSQL returns a single SQL script containing all the
// per-session catalog/metadata setup statements concatenated with semicolons.
// The DuckDB driver executes the script as a multi-statement batch in one
// ExecContext call (one Flight RPC from the control plane to the worker
// instead of N), which materially cuts session establishment latency on
// remote-worker setups where each round-trip is non-trivial.
//
// All statements are independent: each is CREATE OR REPLACE VIEW or
// CREATE TABLE IF NOT EXISTS, with no cross-statement dependencies that would
// require ordering beyond the order they appear in the script.
func buildSessionMetadataSQL(database string) string {
	parts := []string{
		sessionColumnMetadataTableSQL(),
		buildSessionPgDatabaseViewSQL(database),
		buildSessionPgClassViewSQL(),
		buildSessionPgNamespaceViewSQL(),
		buildSessionPgAttributeViewSQL(),
		buildSessionPgTablesViewSQL(),
		buildSessionPgViewsViewSQL(),
		buildSessionPgSequencesViewSQL(),
		buildSessionInformationSchemaColumnsViewSQL(),
		buildSessionInformationSchemaTablesViewSQL(),
		buildSessionInformationSchemaSchemataViewSQL(),
		buildSessionInformationSchemaViewsViewSQL(),
	}
	return strings.Join(parts, ";\n") + ";"
}

func sessionColumnMetadataTableSQL() string {
	return `
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
}

func buildSessionPgDatabaseViewSQL(database string) string {
	lit := quoteSQLStringLiteral(database)
	return fmt.Sprintf(`
		CREATE OR REPLACE VIEW main.pg_database AS
		WITH base AS (
			SELECT * FROM (
				VALUES
					(1::INTEGER, 'postgres', 10::INTEGER, 6::INTEGER, 'c', false, true, -1::INTEGER, 0::INTEGER, 0::INTEGER, 1663::INTEGER, 'en_US.UTF-8', 'en_US.UTF-8', NULL, NULL, NULL, NULL),
					(2::INTEGER, 'template0', 10::INTEGER, 6::INTEGER, 'c', true, false, -1::INTEGER, 0::INTEGER, 0::INTEGER, 1663::INTEGER, 'en_US.UTF-8', 'en_US.UTF-8', NULL, NULL, NULL, NULL),
					(3::INTEGER, 'template1', 10::INTEGER, 6::INTEGER, 'c', true, true, -1::INTEGER, 0::INTEGER, 0::INTEGER, 1663::INTEGER, 'en_US.UTF-8', 'en_US.UTF-8', NULL, NULL, NULL, NULL)
			) AS rows(
				oid, datname, datdba, encoding, datlocprovider, datistemplate, datallowconn,
				datconnlimit, datfrozenxid, datminmxid, dattablespace, datcollate, datctype,
				daticulocale, daticurules, datcollversion, datacl
			)
		),
		requested AS (
			SELECT
				CASE
					WHEN %s = 'postgres' THEN 1
					WHEN %s = 'template0' THEN 2
					WHEN %s = 'template1' THEN 3
					ELSE 4
				END::INTEGER AS oid,
				%s AS datname,
				10::INTEGER AS datdba,
				6::INTEGER AS encoding,
				'c' AS datlocprovider,
				(%s IN ('template0', 'template1')) AS datistemplate,
				(%s != 'template0') AS datallowconn,
				-1::INTEGER AS datconnlimit,
				0::INTEGER AS datfrozenxid,
				0::INTEGER AS datminmxid,
				1663::INTEGER AS dattablespace,
				'en_US.UTF-8' AS datcollate,
				'en_US.UTF-8' AS datctype,
				NULL AS daticulocale,
				NULL AS daticurules,
				NULL AS datcollversion,
				NULL AS datacl
		)
		SELECT
			oid, datname, datdba, encoding, datlocprovider, datistemplate, datallowconn,
			datconnlimit, datfrozenxid, datminmxid, dattablespace, datcollate, datctype,
			daticulocale, daticurules, datcollversion, datacl
		FROM base
		WHERE datname <> %s
		UNION ALL
		SELECT
			oid, datname, datdba, encoding, datlocprovider, datistemplate, datallowconn,
			datconnlimit, datfrozenxid, datminmxid, dattablespace, datcollate, datctype,
			daticulocale, daticurules, datcollversion, datacl
		FROM requested
	`, lit, lit, lit, lit, lit, lit, lit)
}

func internalCompatRelationNamesSQL() string {
	return `
		'pg_database', 'pg_class_full', 'pg_collation', 'pg_policy', 'pg_roles',
		'pg_statistic_ext', 'pg_publication_tables', 'pg_rules', 'pg_publication',
		'pg_publication_rel', 'pg_inherits', 'pg_namespace', 'pg_matviews',
		'pg_stat_user_tables', 'pg_statio_user_tables', 'pg_stat_statements', 'pg_stat_activity',
		'pg_partitioned_table', 'pg_rewrite', 'pg_type', 'pg_attribute',
		'pg_tables', 'pg_views', 'pg_sequences',
		'information_schema_columns_compat', 'information_schema_tables_compat',
		'information_schema_schemata_compat', 'information_schema_views_compat',
		'information_schema_sequences_compat', 'information_schema_routines_compat',
		'__duckgres_column_metadata',
		-- Legacy: created by pre-Iceberg-removal versions (CREATE TABLE IF NOT
		-- EXISTS, instance-lifetime). Keep excluded so a hot-idle worker warmed
		-- by an older binary doesn't surface it as a user table during a
		-- rolling deploy; drop once no pre-removal workers remain.
		'__duckgres_iceberg_column_metadata'
	`
}

func buildSessionPgClassViewSQL() string {
	internalNames := internalCompatRelationNamesSQL()
	return fmt.Sprintf(`
		CREATE OR REPLACE VIEW main.pg_class_full AS
		WITH active_catalog AS (
			SELECT current_database() AS catalog
		),
		relations AS (
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
				NULL AS relpartbound,
				database_name
			FROM duckdb_tables()
			WHERE table_name NOT IN (%s)
			UNION ALL
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
				NULL AS relpartbound,
				database_name
			FROM duckdb_views()
			WHERE view_name NOT IN (%s)
			UNION ALL
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
				NULL AS relpartbound,
				database_name
			FROM duckdb_sequences()
			WHERE sequence_name NOT IN (%s)
			UNION ALL
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
				NULL AS relpartbound,
				database_name
			FROM duckdb_indexes()
			WHERE index_name NOT IN (%s)
		)
		SELECT
			oid, relname, relnamespace, reltype, reloftype, relowner, relam,
			relfilenode, reltablespace, relpages, reltuples, relallvisible,
			reltoastrelid, reltoastidxid, relhasindex, relisshared,
			relpersistence, relkind, relnatts, relchecks, relhasoids,
			relhaspkey, relhasrules, relhastriggers, relhassubclass,
			relrowsecurity, relforcerowsecurity, relispopulated, relreplident,
			relispartition, relrewrite, relfrozenxid, relminmxid, relacl,
			reloptions, relpartbound
		FROM relations r
		CROSS JOIN active_catalog ac
		WHERE r.database_name = ac.catalog
	`, internalNames, internalNames, internalNames, internalNames)
}

func buildSessionPgNamespaceViewSQL() string {
	return `
		CREATE OR REPLACE VIEW main.pg_namespace AS
		WITH active_catalog AS (
			SELECT current_database() AS catalog
		),
		user_namespaces AS (
			SELECT schema_oid AS oid, schema_name, database_name FROM duckdb_tables()
			UNION
			SELECT schema_oid AS oid, schema_name, database_name FROM duckdb_views()
			UNION
			SELECT schema_oid AS oid, schema_name, database_name FROM duckdb_sequences()
			UNION
			SELECT schema_oid AS oid, schema_name, database_name FROM duckdb_indexes()
		),
		display_namespaces AS (
			SELECT
				oid,
				CASE WHEN schema_name = 'main' THEN 'public' ELSE schema_name END AS nspname
			FROM user_namespaces n
			CROSS JOIN active_catalog ac
			WHERE n.database_name = ac.catalog
				AND n.schema_name NOT LIKE '__ducklake_metadata_%'
				AND n.schema_name <> 'system'
		)
		SELECT
			MIN(oid) AS oid,
			nspname,
			CASE WHEN nspname = 'public' THEN 6171::BIGINT ELSE 10::BIGINT END AS nspowner,
			NULL AS nspacl
		FROM display_namespaces
		GROUP BY nspname
		UNION ALL
		SELECT 11::BIGINT AS oid, 'pg_catalog' AS nspname, 10::BIGINT AS nspowner, NULL AS nspacl
		UNION ALL
		SELECT 12::BIGINT AS oid, 'information_schema' AS nspname, 10::BIGINT AS nspowner, NULL AS nspacl
		UNION ALL
		SELECT 99::BIGINT AS oid, 'pg_toast' AS nspname, 10::BIGINT AS nspowner, NULL AS nspacl
	`
}

func pgTypeOIDCaseSQL(dataTypeExpr, fallbackExpr string) string {
	template := `CASE
		WHEN {dt} LIKE 'DECIMAL%' OR {dt} LIKE 'NUMERIC%' THEN 1700::UINTEGER
		WHEN {dt} = 'INTEGER' THEN 23::UINTEGER
		WHEN {dt} = 'BIGINT' THEN 20::UINTEGER
		WHEN {dt} = 'SMALLINT' THEN 21::UINTEGER
		WHEN {dt} = 'TINYINT' THEN 21::UINTEGER
		WHEN {dt} = 'HUGEINT' THEN 1700::UINTEGER
		WHEN {dt} = 'UBIGINT' THEN 1700::UINTEGER
		WHEN {dt} = 'UINTEGER' THEN 20::UINTEGER
		WHEN {dt} = 'USMALLINT' THEN 23::UINTEGER
		WHEN {dt} = 'UTINYINT' THEN 21::UINTEGER
		WHEN {dt} = 'FLOAT' OR {dt} = 'DOUBLE' THEN 701::UINTEGER
		WHEN {dt} = 'REAL' THEN 700::UINTEGER
		WHEN {dt} = 'VARCHAR' THEN 1043::UINTEGER
		WHEN {dt} = 'TEXT' THEN 25::UINTEGER
		WHEN {dt} = 'CHAR' OR {dt} = 'BPCHAR' THEN 1042::UINTEGER
		WHEN {dt} = 'BOOLEAN' THEN 16::UINTEGER
		WHEN {dt} = 'BLOB' OR {dt} = 'BYTEA' THEN 17::UINTEGER
		WHEN {dt} = 'DATE' THEN 1082::UINTEGER
		WHEN {dt} = 'TIME' THEN 1083::UINTEGER
		WHEN {dt} = 'TIMESTAMP' THEN 1114::UINTEGER
		WHEN {dt} LIKE 'TIMESTAMP WITH TIME ZONE%' THEN 1184::UINTEGER
		WHEN {dt} LIKE 'TIME WITH TIME ZONE%' THEN 1266::UINTEGER
		WHEN {dt} = 'INTERVAL' THEN 1186::UINTEGER
		WHEN {dt} = 'UUID' THEN 2950::UINTEGER
		WHEN {dt} = 'BIT' THEN 1560::UINTEGER
		WHEN {dt} = 'JSON' THEN 114::UINTEGER
		WHEN {dt} = 'INTEGER[]' THEN 1007::UINTEGER
		WHEN {dt} = 'BIGINT[]' THEN 1016::UINTEGER
		WHEN {dt} = 'SMALLINT[]' THEN 1005::UINTEGER
		WHEN {dt} = 'VARCHAR[]' THEN 1015::UINTEGER
		WHEN {dt} = 'TEXT[]' THEN 1009::UINTEGER
		WHEN {dt} = 'BOOLEAN[]' THEN 1000::UINTEGER
		WHEN {dt} = 'FLOAT[]' OR {dt} = 'DOUBLE[]' THEN 1022::UINTEGER
		WHEN {dt} = 'REAL[]' THEN 1021::UINTEGER
		WHEN {dt} = 'DATE[]' THEN 1182::UINTEGER
		WHEN {dt} = 'TIMESTAMP[]' THEN 1115::UINTEGER
		WHEN {dt} LIKE 'NUMERIC%[]' OR {dt} LIKE 'DECIMAL%[]' THEN 1231::UINTEGER
		WHEN {dt} = 'UUID[]' THEN 2951::UINTEGER
		WHEN {dt} = 'INTERVAL[]' THEN 1187::UINTEGER
		WHEN {dt} = 'BLOB[]' OR {dt} = 'BYTEA[]' THEN 1001::UINTEGER
		WHEN {dt} LIKE 'STRUCT%' THEN 2249::UINTEGER
		ELSE {fallback}::UINTEGER
	END`
	return strings.NewReplacer("{dt}", dataTypeExpr, "{fallback}", fallbackExpr).Replace(template)
}

func buildSessionPgAttributeViewSQL() string {
	nativeTypeOID := pgTypeOIDCaseSQL("UPPER(dc.data_type)", "a.atttypid")
	return fmt.Sprintf(`
		CREATE OR REPLACE VIEW main.pg_attribute AS
		SELECT
				a.attrelid::UINTEGER AS attrelid,
				a.attname,
				%s AS atttypid,
				a.attstattarget,
				a.attlen,
				a.attnum::SMALLINT AS attnum,
				a.attndims::SMALLINT AS attndims,
				a.attcacheoff,
				CASE
					WHEN (dc.data_type LIKE 'DECIMAL%%' OR dc.data_type LIKE 'NUMERIC%%') AND a.atttypmod > 0 THEN
						(((a.atttypmod / 1000)::INTEGER << 16) | ((a.atttypmod %% 1000)::INTEGER + 4))::INTEGER
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
				a.attinhcount::INTEGER AS attinhcount,
				a.attcollation::UINTEGER AS attcollation,
				a.attacl,
				a.attoptions,
				a.attfdwoptions,
				a.attmissingval
		FROM pg_catalog.pg_attribute a
		JOIN main.pg_class_full c ON c.oid = a.attrelid
		JOIN main.pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN duckdb_columns() dc ON dc.table_oid = a.attrelid AND dc.column_name = a.attname
	`, nativeTypeOID)
}

// buildSessionPgTablesViewSQL builds the catalog-scoped pg_catalog.pg_tables
// compat view. DuckDB's native pg_tables spans EVERY attached catalog and
// ignores current_database(), so on a multi-catalog worker a bare pg_tables
// would leak another catalog's table names into the session. This view sources
// duckdb_tables() filtered to current_database() so a session only sees its
// own catalog's tables. Column shape mirrors DuckDB's
// native pg_tables (schemaname, tablename, tableowner, tablespace, hasindexes,
// hasrules, hastriggers) so clients see no change beyond the scoping.
func buildSessionPgTablesViewSQL() string {
	internalNames := internalCompatRelationNamesSQL()
	return fmt.Sprintf(`
		CREATE OR REPLACE VIEW main.pg_tables AS
		WITH active_catalog AS (
			SELECT current_database() AS catalog
		)
		SELECT
			CASE WHEN t.schema_name = 'main' THEN 'public' ELSE t.schema_name END AS schemaname,
			t.table_name AS tablename,
			'duckdb' AS tableowner,
			NULL::INTEGER AS tablespace,
			(t.index_count > 0) AS hasindexes,
			false AS hasrules,
			false AS hastriggers
		FROM duckdb_tables() t
		CROSS JOIN active_catalog ac
		WHERE t.database_name = ac.catalog
			AND t.schema_name NOT LIKE '__ducklake_metadata_%%'
			AND t.table_name NOT IN (%s)
	`, internalNames)
}

// buildSessionPgViewsViewSQL builds the catalog-scoped pg_catalog.pg_views
// compat view (same cross-catalog-leak rationale as buildSessionPgTablesViewSQL).
// Sources duckdb_views() filtered to current_database(); the compat views
// themselves live in memory.main and are excluded by name so a memory-catalog
// session does not surface them as user views.
func buildSessionPgViewsViewSQL() string {
	internalNames := internalCompatRelationNamesSQL()
	return fmt.Sprintf(`
		CREATE OR REPLACE VIEW main.pg_views AS
		WITH active_catalog AS (
			SELECT current_database() AS catalog
		)
		SELECT
			CASE WHEN v.schema_name = 'main' THEN 'public' ELSE v.schema_name END AS schemaname,
			v.view_name AS viewname,
			'duckdb' AS viewowner,
			v.sql AS definition
		FROM duckdb_views() v
		CROSS JOIN active_catalog ac
		WHERE v.database_name = ac.catalog
			AND v.schema_name NOT LIKE '__ducklake_metadata_%%'
			AND v.view_name NOT IN (%s)
	`, internalNames)
}

// buildSessionPgSequencesViewSQL builds the catalog-scoped pg_catalog.pg_sequences
// compat view (same cross-catalog-leak rationale as buildSessionPgTablesViewSQL).
// Sources duckdb_sequences() filtered to current_database(). Column names and
// types mirror DuckDB's native pg_sequences (the view these queries resolved to
// before scoping) so the only behavior change is the catalog filter: data_type
// and cache_size are INTEGER (DuckDB does not expose real values, so NULL), and
// the value columns are BIGINT.
func buildSessionPgSequencesViewSQL() string {
	return `
		CREATE OR REPLACE VIEW main.pg_sequences AS
		WITH active_catalog AS (
			SELECT current_database() AS catalog
		)
		SELECT
			CASE WHEN s.schema_name = 'main' THEN 'public' ELSE s.schema_name END AS schemaname,
			s.sequence_name AS sequencename,
			'duckdb' AS sequenceowner,
			NULL::INTEGER AS data_type,
			s.start_value::BIGINT AS start_value,
			s.min_value::BIGINT AS min_value,
			s.max_value::BIGINT AS max_value,
			s.increment_by::BIGINT AS increment_by,
			s.cycle AS cycle,
			NULL::INTEGER AS cache_size,
			s.last_value::BIGINT AS last_value
		FROM duckdb_sequences() s
		CROSS JOIN active_catalog ac
		WHERE s.database_name = ac.catalog
			AND s.schema_name NOT LIKE '__ducklake_metadata_%'
	`
}

func buildSessionInformationSchemaColumnsViewSQL() string {
	return `
		CREATE OR REPLACE VIEW main.information_schema_columns_compat AS
		WITH all_columns AS (
			SELECT
				c.table_catalog AS source_catalog,
				CASE WHEN c.table_catalog IN ('ducklake', 'memory') THEN current_database() ELSE c.table_catalog END AS table_catalog,
				CASE WHEN c.table_schema = 'main' THEN 'public' ELSE c.table_schema END AS table_schema,
				c.table_name,
				c.column_name,
				c.ordinal_position,
				c.column_default,
				c.is_nullable,
				c.data_type,
				c.udt_name,
				COALESCE(m.character_maximum_length, c.character_maximum_length) AS character_maximum_length,
				c.character_octet_length,
				COALESCE(m.numeric_precision, c.numeric_precision) AS numeric_precision,
				COALESCE(m.numeric_scale, c.numeric_scale) AS numeric_scale,
				c.datetime_precision
			FROM information_schema.columns c
			LEFT JOIN main.__duckgres_column_metadata m
				ON c.table_schema = m.table_schema
				AND c.table_name = m.table_name
				AND c.column_name = m.column_name
		),
		active_catalog AS (
			SELECT current_database() AS catalog
		),
		filtered_columns AS (
			SELECT c.*
			FROM all_columns c
			CROSS JOIN active_catalog ac
			WHERE c.source_catalog = ac.catalog
			OR c.source_catalog IN ('ducklake', 'memory')
		),
		active_search_path AS (
			SELECT
				',' || COALESCE(
					(
						SELECT lower(regexp_replace(value, '\s+', '', 'g'))
						FROM duckdb_settings()
						WHERE name = 'search_path'
					),
					''
				) || ',' AS search_path,
				COALESCE(
					(
						SELECT lower(regexp_extract(regexp_replace(value, '\s+', '', 'g'), '^([A-Za-z0-9_]+)\.', 1))
						FROM duckdb_settings()
						WHERE name = 'search_path'
					),
					''
				) AS default_catalog
		),
		ranked_columns AS (
			SELECT
				c.*,
				ROW_NUMBER() OVER (
					PARTITION BY c.table_schema, c.table_name, c.column_name
					ORDER BY
						COALESCE(
							NULLIF(strpos(sp.search_path, ',' || lower(c.source_catalog || '.' || c.table_schema) || ','), 0),
							CASE
								WHEN c.source_catalog IN ('ducklake', 'memory') THEN NULLIF(strpos(sp.search_path, ',' || lower(c.table_schema) || ','), 0)
								ELSE NULL
							END,
							CASE
								WHEN lower(c.source_catalog) = sp.default_catalog THEN 500000
								ELSE NULL
							END,
							1000000
						),
						CASE
							WHEN c.source_catalog IN ('ducklake', 'memory') THEN 0
							ELSE 1
						END,
						c.source_catalog
				) AS search_path_rank
			FROM filtered_columns c
			CROSS JOIN active_search_path sp
		)
		SELECT
			c.table_catalog,
			c.table_schema,
			c.table_name,
			c.column_name,
			c.ordinal_position,
			CASE
				WHEN c.column_default IS NULL THEN NULL
				WHEN c.column_default = 'CAST(''t'' AS BOOLEAN)' THEN 'true'
				WHEN c.column_default = 'CAST(''f'' AS BOOLEAN)' THEN 'false'
				WHEN UPPER(c.column_default) = 'CURRENT_TIMESTAMP' THEN 'CURRENT_TIMESTAMP'
				WHEN UPPER(c.column_default) = 'NOW()' THEN 'now()'
				ELSE c.column_default
			END AS column_default,
			c.is_nullable,
			CASE
				WHEN UPPER(c.data_type) = 'VARCHAR' OR UPPER(c.data_type) LIKE 'VARCHAR(%' THEN 'text'
				WHEN UPPER(c.data_type) = 'TEXT' THEN 'text'
				WHEN UPPER(c.data_type) LIKE 'TEXT(%' THEN 'character'
				WHEN UPPER(c.data_type) = 'STRING' THEN 'text'
				WHEN UPPER(c.data_type) = 'BOOLEAN' OR UPPER(c.data_type) = 'BOOL' THEN 'boolean'
				WHEN UPPER(c.data_type) = 'TINYINT' THEN 'smallint'
				WHEN UPPER(c.data_type) = 'SMALLINT' THEN 'smallint'
				WHEN UPPER(c.data_type) = 'INTEGER' OR UPPER(c.data_type) = 'INT' THEN 'integer'
				WHEN UPPER(c.data_type) = 'BIGINT' OR UPPER(c.data_type) = 'LONG' THEN 'bigint'
				WHEN UPPER(c.data_type) = 'HUGEINT' THEN 'numeric'
				WHEN UPPER(c.data_type) = 'REAL' OR UPPER(c.data_type) = 'FLOAT4' THEN 'real'
				WHEN UPPER(c.data_type) = 'DOUBLE' OR UPPER(c.data_type) = 'FLOAT8' OR UPPER(c.data_type) = 'DOUBLE PRECISION' THEN 'double precision'
				WHEN UPPER(c.data_type) LIKE 'DECIMAL%' THEN 'numeric'
				WHEN UPPER(c.data_type) LIKE 'NUMERIC%' THEN 'numeric'
				WHEN UPPER(c.data_type) = 'DATE' THEN 'date'
				WHEN UPPER(c.data_type) = 'TIME' THEN 'time without time zone'
				WHEN UPPER(c.data_type) = 'TIMESTAMP' THEN 'timestamp without time zone'
				WHEN UPPER(c.data_type) = 'TIMESTAMPTZ' OR UPPER(c.data_type) = 'TIMESTAMP WITH TIME ZONE' THEN 'timestamp with time zone'
				WHEN UPPER(c.data_type) = 'INTERVAL' THEN 'interval'
				WHEN UPPER(c.data_type) = 'UUID' THEN 'uuid'
				WHEN UPPER(c.data_type) = 'BLOB' OR UPPER(c.data_type) = 'BYTEA' OR UPPER(c.data_type) = 'BINARY' OR UPPER(c.data_type) = 'FIXED' THEN 'bytea'
				WHEN UPPER(c.data_type) = 'JSON' THEN 'json'
				WHEN UPPER(c.data_type) LIKE '%[]' OR UPPER(c.data_type) LIKE 'LIST%' THEN 'ARRAY'
				WHEN UPPER(c.data_type) LIKE 'STRUCT%' OR UPPER(c.data_type) LIKE 'MAP%' OR c.data_type LIKE '{%' THEN 'json'
				ELSE LOWER(c.data_type)
			END AS data_type,
			c.character_maximum_length,
			c.character_octet_length,
			c.numeric_precision,
			c.numeric_scale,
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
			c.udt_name,
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
		FROM ranked_columns c
		WHERE c.search_path_rank = 1
	`
}

func buildSessionInformationSchemaTablesViewSQL() string {
	return `
		CREATE OR REPLACE VIEW main.information_schema_tables_compat AS
		SELECT
			CASE WHEN t.table_catalog IN ('ducklake', 'memory') THEN current_database() ELSE t.table_catalog END AS table_catalog,
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
		FROM information_schema.tables t
		WHERE t.table_name NOT IN (
			'__duckgres_column_metadata', '__duckgres_iceberg_column_metadata',
			'pg_class_full', 'pg_collation', 'pg_database', 'pg_inherits',
			'pg_namespace', 'pg_policy', 'pg_publication', 'pg_publication_rel',
			'pg_publication_tables', 'pg_roles', 'pg_rules', 'pg_statistic_ext', 'pg_matviews',
			'pg_stat_user_tables', 'pg_statio_user_tables', 'pg_stat_statements', 'pg_stat_activity',
			'pg_partitioned_table', 'pg_rewrite', 'pg_attribute',
			'pg_tables', 'pg_views', 'pg_sequences',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat',
			'information_schema_sequences_compat', 'information_schema_routines_compat'
		)
		AND (
			t.table_catalog = current_database()
			OR t.table_catalog IN ('ducklake', 'memory')
		)
		AND t.table_name NOT LIKE 'duckdb_%'
		AND t.table_name NOT LIKE 'sqlite_%'
		AND t.table_name NOT LIKE 'pragma_%'
	`
}

func buildSessionInformationSchemaSchemataViewSQL() string {
	return `
		CREATE OR REPLACE VIEW main.information_schema_schemata_compat AS
		SELECT
			CASE WHEN s.catalog_name IN ('ducklake', 'memory') THEN current_database() ELSE s.catalog_name END AS catalog_name,
			CASE WHEN s.schema_name = 'main' THEN 'public' ELSE s.schema_name END AS schema_name,
			'duckdb' AS schema_owner,
			NULL AS default_character_set_catalog,
			NULL AS default_character_set_schema,
			NULL AS default_character_set_name,
			NULL AS sql_path
		FROM information_schema.schemata s
		WHERE s.schema_name NOT IN ('main', 'pg_catalog', 'information_schema')
		AND s.catalog_name NOT LIKE '__ducklake_metadata_%'
		AND (
			s.catalog_name = current_database()
			OR s.catalog_name IN ('ducklake', 'memory')
		)
		UNION ALL
		SELECT current_database() AS catalog_name, 'public' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
		UNION ALL
		SELECT current_database() AS catalog_name, 'pg_catalog' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
		UNION ALL
		SELECT current_database() AS catalog_name, 'information_schema' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
		UNION ALL
		SELECT current_database() AS catalog_name, 'pg_toast' AS schema_name, 'duckdb' AS schema_owner,
			NULL, NULL, NULL, NULL
	`
}

func buildSessionInformationSchemaViewsViewSQL() string {
	return `
		CREATE OR REPLACE VIEW main.information_schema_views_compat AS
		SELECT
			CASE WHEN v.table_catalog IN ('ducklake', 'memory') THEN current_database() ELSE v.table_catalog END AS table_catalog,
			CASE WHEN v.table_schema = 'main' THEN 'public' ELSE v.table_schema END AS table_schema,
			v.table_name,
			v.view_definition,
			v.check_option,
			v.is_updatable,
			v.is_insertable_into,
			v.is_trigger_updatable,
			v.is_trigger_deletable,
			v.is_trigger_insertable_into
		FROM information_schema.views v
		WHERE v.table_name NOT IN (
			'pg_class_full', 'pg_collation', 'pg_database', 'pg_inherits',
			'pg_namespace', 'pg_policy', 'pg_publication', 'pg_publication_rel',
			'pg_publication_tables', 'pg_roles', 'pg_rules', 'pg_statistic_ext', 'pg_matviews',
			'pg_stat_user_tables', 'pg_statio_user_tables', 'pg_stat_statements', 'pg_stat_activity',
			'pg_partitioned_table', 'pg_rewrite', 'pg_attribute',
			'pg_tables', 'pg_views', 'pg_sequences',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat',
			'information_schema_sequences_compat', 'information_schema_routines_compat'
		)
		AND (
			v.table_catalog = current_database()
			OR v.table_catalog IN ('ducklake', 'memory')
		)
		AND v.table_name NOT LIKE 'duckdb_%'
		AND v.table_name NOT LIKE 'sqlite_%'
		AND v.table_name NOT LIKE 'pragma_%'
	`
}

func quoteSQLStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
