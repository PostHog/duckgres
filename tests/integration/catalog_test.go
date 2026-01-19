package integration

import (
	"fmt"
	"testing"
)

// TestCatalogPgClass tests pg_catalog.pg_class
func TestCatalogPgClass(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "pg_class_tables",
			Query:        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r' LIMIT 10",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_class_views",
			Query:        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'v' LIMIT 10",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_class_indexes",
			Query:        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'i' LIMIT 10",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_class_count",
			Query:        "SELECT COUNT(*) FROM pg_catalog.pg_class",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPgNamespace tests pg_catalog.pg_namespace
func TestCatalogPgNamespace(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "pg_namespace_all",
			Query:        "SELECT nspname FROM pg_catalog.pg_namespace",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_namespace_public",
			Query:        "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema' LIMIT 10",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPgAttribute tests pg_catalog.pg_attribute
func TestCatalogPgAttribute(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "pg_attribute_columns",
			Query:        "SELECT attname, attnum, atttypid FROM pg_catalog.pg_attribute WHERE attnum > 0 LIMIT 20",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPgType tests pg_catalog.pg_type
func TestCatalogPgType(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "pg_type_all",
			Query:        "SELECT typname, typtype FROM pg_catalog.pg_type LIMIT 20",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_type_base",
			Query:        "SELECT typname FROM pg_catalog.pg_type WHERE typtype = 'b' LIMIT 10",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPgDatabase tests pg_catalog.pg_database
func TestCatalogPgDatabase(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "pg_database_all",
			Query:        "SELECT datname FROM pg_catalog.pg_database",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPgRoles tests pg_catalog.pg_roles
func TestCatalogPgRoles(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "pg_roles_all",
			Query:        "SELECT rolname, rolsuper FROM pg_catalog.pg_roles",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPgSettings tests pg_catalog.pg_settings
func TestCatalogPgSettings(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "pg_settings_sample",
			Query:        "SELECT name, setting FROM pg_catalog.pg_settings LIMIT 10",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogInformationSchemaTables tests information_schema.tables
func TestCatalogInformationSchemaTables(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "info_schema_tables_all",
			Query:        "SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema') LIMIT 20",
			DuckgresOnly: false,
		},
		{
			Name:         "info_schema_tables_public",
			Query:        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' OR table_schema = 'public' LIMIT 20",
			DuckgresOnly: false,
		},
		{
			Name:         "info_schema_tables_base",
			Query:        "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_name LIMIT 20",
			DuckgresOnly: false,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogInformationSchemaColumns tests information_schema.columns
func TestCatalogInformationSchemaColumns(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "info_schema_columns_users",
			Query:        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'users' ORDER BY ordinal_position",
			DuckgresOnly: false,
		},
		{
			Name:         "info_schema_columns_with_default",
			Query:        "SELECT column_name, column_default FROM information_schema.columns WHERE column_default IS NOT NULL LIMIT 10",
			DuckgresOnly: false,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogInformationSchemaViews tests information_schema.views
func TestCatalogInformationSchemaViews(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "info_schema_views_all",
			Query:        "SELECT table_schema, table_name FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_name LIMIT 10",
			DuckgresOnly: false,
		},
		{
			// view_definition format differs between PostgreSQL and DuckDB - test separately
			Name:         "info_schema_views_definition",
			Query:        "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog', 'information_schema') LIMIT 10",
			DuckgresOnly: true, // view_definition format differs
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogInformationSchemaSchemata tests information_schema.schemata
func TestCatalogInformationSchemaSchemata(t *testing.T) {
	tests := []QueryTest{
		{
			Name:         "info_schema_schemata_all",
			Query:        "SELECT schema_name FROM information_schema.schemata",
			DuckgresOnly: false,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPsqlCommands tests queries generated by psql meta-commands
func TestCatalogPsqlCommands(t *testing.T) {
	tests := []QueryTest{
		// \dt - list tables
		{
			Name: "psql_dt",
			Query: `
				SELECT n.nspname as "Schema",
					c.relname as "Name",
					CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END as "Type",
					pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
				FROM pg_catalog.pg_class c
				LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind IN ('r','v')
				AND n.nspname NOT IN ('pg_catalog', 'information_schema')
				ORDER BY 1, 2
				LIMIT 20
			`,
		},

		// \dn - list schemas
		{
			Name: "psql_dn",
			Query: `
				SELECT n.nspname AS "Name",
					pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
				FROM pg_catalog.pg_namespace n
				WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
				ORDER BY 1
			`,
		},

		// \l - list databases
		{
			Name: "psql_l",
			Query: `
				SELECT d.datname as "Name",
					pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
					pg_catalog.pg_encoding_to_char(d.encoding) as "Encoding"
				FROM pg_catalog.pg_database d
				ORDER BY 1
			`,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogSystemFunctions tests pg_catalog system functions
func TestCatalogSystemFunctions(t *testing.T) {
	tests := []QueryTest{
		// pg_get_userbyid
		{
			Name:         "pg_get_userbyid",
			Query:        "SELECT pg_catalog.pg_get_userbyid(1)",
			DuckgresOnly: true,
		},

		// pg_table_is_visible
		{
			Name:         "pg_table_is_visible",
			Query:        "SELECT pg_catalog.pg_table_is_visible(c.oid) FROM pg_catalog.pg_class c LIMIT 5",
			DuckgresOnly: true,
		},

		// format_type - basic types
		{
			Name:         "format_type_integer",
			Query:        "SELECT format_type(23, -1)",
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_text",
			Query:        "SELECT format_type(25, -1)",
			DuckgresOnly: true,
		},
		// format_type - types with typemod
		{
			Name:         "format_type_varchar_with_length",
			Query:        "SELECT format_type(1043, 54)", // typemod 54 = length 50 + 4
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_varchar_no_length",
			Query:        "SELECT format_type(1043, -1)",
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_numeric_with_precision",
			Query:        "SELECT format_type(1700, 655366)", // (10 << 16) | 2 + 4 = 655366
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_numeric_negative_scale",
			Query:        "SELECT format_type(1700, 393218)", // (5 << 16) | 65534 + 4 = 393218, scale -2 as two's complement
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_numeric_no_precision",
			Query:        "SELECT format_type(1700, -1)",
			DuckgresOnly: true,
		},
		// format_type - JSON types
		{
			Name:         "format_type_json",
			Query:        "SELECT format_type(114, -1)",
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_jsonb",
			Query:        "SELECT format_type(3802, -1)",
			DuckgresOnly: true,
		},
		// format_type - date/time types
		{
			Name:         "format_type_interval",
			Query:        "SELECT format_type(1186, -1)",
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_timestamp",
			Query:        "SELECT format_type(1114, -1)",
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_timestamptz",
			Query:        "SELECT format_type(1184, -1)",
			DuckgresOnly: true,
		},
		// format_type - array types
		{
			Name:         "format_type_int_array",
			Query:        "SELECT format_type(1007, -1)",
			DuckgresOnly: true,
		},
		{
			Name:         "format_type_text_array",
			Query:        "SELECT format_type(1009, -1)",
			DuckgresOnly: true,
		},
		// format_type - unknown type fallback
		{
			Name:         "format_type_unknown",
			Query:        "SELECT format_type(99999, -1)",
			DuckgresOnly: true,
		},

		// pg_encoding_to_char
		{
			Name:         "pg_encoding_to_char",
			Query:        "SELECT pg_encoding_to_char(6)",
			DuckgresOnly: true,
		},

		// current_setting
		{
			Name:         "current_setting",
			Query:        "SELECT current_setting('server_version') IS NOT NULL",
			DuckgresOnly: true,
		},

		// obj_description (stub, returns null)
		{
			Name:         "obj_description",
			Query:        "SELECT obj_description(1, 'pg_class')",
			DuckgresOnly: true,
		},

		// col_description (stub, returns null)
		{
			Name:         "col_description",
			Query:        "SELECT col_description(1, 1)",
			DuckgresOnly: true,
		},

		// has_schema_privilege
		{
			Name:         "has_schema_privilege",
			Query:        "SELECT has_schema_privilege('public', 'USAGE')",
			DuckgresOnly: true,
		},

		// has_table_privilege
		{
			Name:         "has_table_privilege",
			Query:        "SELECT has_table_privilege('users', 'SELECT')",
			DuckgresOnly: true,
		},

		// pg_is_in_recovery
		{
			Name:         "pg_is_in_recovery",
			Query:        "SELECT pg_is_in_recovery()",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogPgGetSerialSequence tests pg_get_serial_sequence function
// In DuckLake mode, sequences are not supported, so pg_get_serial_sequence
// should always return NULL (which is correct behavior - no sequences exist)
func TestCatalogPgGetSerialSequence(t *testing.T) {
	tests := []QueryTest{
		// Test that pg_get_serial_sequence returns NULL for non-serial columns
		// In DuckLake mode, this is always NULL since sequences aren't supported
		{
			Name:         "pg_get_serial_sequence_returns_null",
			Query:        "SELECT pg_get_serial_sequence('users', 'id') IS NULL",
			DuckgresOnly: true,
		},
		// Test with non-existent table (should return NULL, not error)
		{
			Name:         "pg_get_serial_sequence_nonexistent_table",
			Query:        "SELECT pg_get_serial_sequence('nonexistent_table', 'id') IS NULL",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogCombinedQueries tests more complex catalog queries
func TestCatalogCombinedQueries(t *testing.T) {
	tests := []QueryTest{
		// Join pg_class with pg_namespace
		{
			Name: "pg_class_with_schema",
			Query: `
				SELECT n.nspname, c.relname, c.relkind
				FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
				WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
				ORDER BY n.nspname, c.relname
				LIMIT 20
			`,
			DuckgresOnly: true,
		},

		// Get table columns with types
		{
			Name: "table_columns_with_types",
			Query: `
				SELECT
					c.relname as table_name,
					a.attname as column_name,
					pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
					a.attnotnull as not_null
				FROM pg_catalog.pg_attribute a
				JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
				JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
				WHERE a.attnum > 0
				AND NOT a.attisdropped
				AND c.relname = 'users'
				AND n.nspname NOT IN ('pg_catalog', 'information_schema')
				ORDER BY a.attnum
			`,
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestCatalogStubs tests stub tables that return empty results
func TestCatalogStubs(t *testing.T) {
	tests := []QueryTest{
		// These should work but may return empty results
		{
			Name:         "pg_policy",
			Query:        "SELECT * FROM pg_catalog.pg_policy LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_collation",
			Query:        "SELECT collname FROM pg_catalog.pg_collation LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_statistic_ext",
			Query:        "SELECT * FROM pg_catalog.pg_statistic_ext LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_publication",
			Query:        "SELECT * FROM pg_catalog.pg_publication LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_publication_rel",
			Query:        "SELECT * FROM pg_catalog.pg_publication_rel LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_inherits",
			Query:        "SELECT * FROM pg_catalog.pg_inherits LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_rules",
			Query:        "SELECT * FROM pg_catalog.pg_rules LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_matviews",
			Query:        "SELECT * FROM pg_catalog.pg_matviews LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_matviews_unqualified",
			Query:        "SELECT * FROM pg_matviews LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_stat_statements",
			Query:        "SELECT * FROM pg_catalog.pg_stat_statements LIMIT 5",
			DuckgresOnly: true,
		},
		{
			Name:         "pg_partitioned_table",
			Query:        "SELECT * FROM pg_catalog.pg_partitioned_table LIMIT 5",
			DuckgresOnly: true,
		},
	}
	runQueryTests(t, tests)
}

// TestFormatTypeTimePrecision tests that format_type correctly handles time type precision
func TestFormatTypeTimePrecision(t *testing.T) {
	db := dgOnly(t)

	tests := []struct {
		name     string
		oid      int
		typemod  int
		expected string
	}{
		// time without time zone (OID 1083)
		{"time_no_precision", 1083, -1, "time without time zone"},
		{"time_precision_0", 1083, 0, "time(0) without time zone"},
		{"time_precision_3", 1083, 3, "time(3) without time zone"},
		{"time_precision_6", 1083, 6, "time(6) without time zone"},
		// time with time zone (OID 1266)
		{"timetz_no_precision", 1266, -1, "time with time zone"},
		{"timetz_precision_0", 1266, 0, "time(0) with time zone"},
		{"timetz_precision_3", 1266, 3, "time(3) with time zone"},
		{"timetz_precision_6", 1266, 6, "time(6) with time zone"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var result string
			query := fmt.Sprintf("SELECT pg_catalog.format_type(%d, %d)", tc.oid, tc.typemod)
			err := db.QueryRow(query).Scan(&result)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if result != tc.expected {
				t.Errorf("format_type(%d, %d) = %q, want %q", tc.oid, tc.typemod, result, tc.expected)
			}
		})
	}
}
