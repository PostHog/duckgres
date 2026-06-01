// Package sessionmeta installs session-local catalog/metadata overrides on
// a duckgres connection (current_database, pg_database, information_schema
// views) so they reflect the catalog the session defaults to on the PG wire.
//
// The catalog name passed in is the real, attached catalog (e.g. "ducklake" or
// "iceberg") the session uses — duckgres no longer masks a logical database
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

	"github.com/posthog/duckgres/server/icebergmeta"
	"github.com/posthog/duckgres/server/sqlcore"
)

// InitSessionDatabaseMetadata installs session-local overrides for metadata
// surfaces (current_database, pg_database, information_schema views) so they
// reflect `catalog` — the real, attached catalog the session defaults to. The
// caller resolves `catalog` to "ducklake"/"iceberg" (the names the catalogs are
// actually attached as); there is no logical→physical masking.
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
		// compat views there). For DuckLake sessions, restore `ducklake` here; for
		// Iceberg the caller issues `USE iceberg.public` after this returns. Keep
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
		sessionIcebergColumnMetadataTableSQL(),
		buildSessionPgDatabaseViewSQL(database),
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

func sessionIcebergColumnMetadataTableSQL() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS main.%s (
			table_schema VARCHAR NOT NULL,
			table_name VARCHAR NOT NULL,
			column_name VARCHAR NOT NULL,
			ordinal_position INTEGER NOT NULL,
			is_nullable VARCHAR NOT NULL,
			data_type VARCHAR NOT NULL,
			character_maximum_length INTEGER,
			character_octet_length INTEGER,
			numeric_precision INTEGER,
			numeric_scale INTEGER,
			datetime_precision INTEGER,
			PRIMARY KEY (table_schema, table_name, column_name)
		)
	`, icebergmeta.ColumnMetadataTable)
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
			WHERE NOT (
				c.table_catalog = 'iceberg'
				AND c.column_name = '__'
				AND UPPER(c.data_type) = 'UNKNOWN'
			)
			AND NOT (
				c.table_catalog = 'iceberg'
				AND EXISTS (
					SELECT 1
					FROM main.__duckgres_iceberg_column_metadata im
					WHERE im.table_schema = c.table_schema
					AND im.table_name = c.table_name
					AND im.column_name = c.column_name
				)
			)
			UNION ALL
			SELECT
				'iceberg' AS source_catalog,
				'iceberg' AS table_catalog,
				table_schema,
				table_name,
				column_name,
				ordinal_position,
				NULL AS column_default,
				is_nullable,
				data_type,
				character_maximum_length,
				character_octet_length,
				numeric_precision,
				numeric_scale,
				datetime_precision
			FROM main.__duckgres_iceberg_column_metadata
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
							WHEN c.source_catalog = 'iceberg' THEN 1
							ELSE 2
						END,
						c.source_catalog
				) AS search_path_rank
			FROM all_columns c
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
			'__duckgres_column_metadata',
			'pg_class_full', 'pg_collation', 'pg_database', 'pg_inherits',
			'pg_namespace', 'pg_policy', 'pg_publication', 'pg_publication_rel',
			'pg_publication_tables', 'pg_roles', 'pg_rules', 'pg_statistic_ext', 'pg_matviews',
			'pg_stat_user_tables', 'pg_statio_user_tables', 'pg_stat_statements', 'pg_stat_activity',
			'pg_partitioned_table', 'pg_rewrite', 'pg_attribute',
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat'
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
			'information_schema_columns_compat', 'information_schema_tables_compat',
			'information_schema_schemata_compat', 'information_schema_views_compat'
		)
		AND v.table_name NOT LIKE 'duckdb_%'
		AND v.table_name NOT LIKE 'sqlite_%'
		AND v.table_name NOT LIKE 'pragma_%'
	`
}

func quoteSQLStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
