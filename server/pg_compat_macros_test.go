package server

import (
	"database/sql"
	"testing"
)

// macroCase exercises a PostgreSQL-compatibility macro through a query that
// yields a single VARCHAR (or NULL) column, so all cases scan uniformly.
type macroCase struct {
	name     string
	query    string
	want     string
	wantNull bool
}

func runMacroCases(t *testing.T, cases []macroCase) {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("initPgCatalog: %v", err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got sql.NullString
			if err := db.QueryRow(tc.query).Scan(&got); err != nil {
				t.Fatalf("query %q failed: %v", tc.query, err)
			}
			if tc.wantNull {
				if got.Valid {
					t.Fatalf("query %q = %q, want NULL", tc.query, got.String)
				}
				return
			}
			if !got.Valid {
				t.Fatalf("query %q = NULL, want %q", tc.query, tc.want)
			}
			if got.String != tc.want {
				t.Fatalf("query %q = %q, want %q", tc.query, got.String, tc.want)
			}
		})
	}
}

func TestCompatMacros_BatchA(t *testing.T) {
	runMacroCases(t, []macroCase{
		// set_config — value-returning session-setting writer (connection-startup unblocker)
		{"set_config_returns_value", `SELECT set_config('search_path','main',false)`, "main", false},
		{"set_config_null_passthrough", `SELECT (set_config('x', NULL, false) IS NULL)::VARCHAR`, "true", false},

		// uuid_generate_v4 — uuid-ossp alias
		{"uuid_generate_v4_len", `SELECT length(uuid_generate_v4()::VARCHAR)::VARCHAR`, "36", false},
		{"uuid_generate_v4_unique", `SELECT (uuid_generate_v4() <> uuid_generate_v4())::VARCHAR`, "true", false},
		{"uuid_generate_v4_version_nibble", `SELECT substr(uuid_generate_v4()::VARCHAR, 15, 1)`, "4", false},

		// statement_timestamp — now() alias
		{"statement_timestamp_not_null", `SELECT (statement_timestamp() IS NOT NULL)::VARCHAR`, "true", false},

		// pg_get_function_* — \df stubs (empty string, not NULL, so the join completes)
		{"pg_get_function_arguments_empty", `SELECT pg_get_function_arguments(0)`, "", false},
		{"pg_get_function_result_empty", `SELECT pg_get_function_result(0)`, "", false},
		{"pg_get_function_identity_arguments_empty", `SELECT pg_get_function_identity_arguments(0)`, "", false},

		// pg_get_triggerdef — DuckDB has no triggers; always NULL (1- and 2-arg forms)
		{"pg_get_triggerdef_1arg_null", `SELECT (pg_get_triggerdef(0) IS NULL)::VARCHAR`, "true", false},
		{"pg_get_triggerdef_2arg_null", `SELECT (pg_get_triggerdef(0, true) IS NULL)::VARCHAR`, "true", false},

		// pg_jit_available / row_security_active — capability stubs
		{"pg_jit_available_false", `SELECT pg_jit_available()::VARCHAR`, "false", false},
		{"row_security_active_false", `SELECT row_security_active('any_table')::VARCHAR`, "false", false},

		// pg_collation_for — effective-collation stub
		{"pg_collation_for_default", `SELECT pg_collation_for('abc')`, `"default"`, false},

		// pg_input_is_valid — bounded type-set validator
		{"pg_input_is_valid_int_ok", `SELECT pg_input_is_valid('123','integer')::VARCHAR`, "true", false},
		{"pg_input_is_valid_int_bad", `SELECT pg_input_is_valid('abc','integer')::VARCHAR`, "false", false},
		{"pg_input_is_valid_numeric_ok", `SELECT pg_input_is_valid('1.5','numeric')::VARCHAR`, "true", false},

		// to_regclass / to_regtype / to_regproc — NULL-safe name->oid probes
		{"to_regclass_hit", `SELECT (to_regclass('pg_class') IS NOT NULL)::VARCHAR`, "true", false},
		{"to_regclass_miss", `SELECT to_regclass('definitely_missing_xyz')::VARCHAR`, "", true},
		{"to_regtype_hit", `SELECT (to_regtype('integer') IS NOT NULL)::VARCHAR`, "true", false},
		{"to_regtype_miss", `SELECT to_regtype('no_such_type_xyz')::VARCHAR`, "", true},
		{"to_regproc_hit", `SELECT (to_regproc('upper') IS NOT NULL)::VARCHAR`, "true", false},
		{"to_regproc_miss", `SELECT to_regproc('no_such_fn_xyz')::VARCHAR`, "", true},

		// jsonb_pretty — indented JSON (assert it inserts newlines)
		{"jsonb_pretty_multiline", `SELECT (jsonb_pretty('{"a":1,"b":2}'::json) LIKE '%' || chr(10) || '%')::VARCHAR`, "true", false},
		{"jsonb_pretty_null", `SELECT (jsonb_pretty(NULL::json) IS NULL)::VARCHAR`, "true", false},

		// to_ascii — accent-stripping transliteration
		{"to_ascii_accents", `SELECT to_ascii('Mötley')`, "Motley", false},
		{"to_ascii_plain", `SELECT to_ascii('abc')`, "abc", false},

		// convert_from — bytea->text (UTF8)
		{"convert_from_utf8", `SELECT convert_from(unhex('48656c6c6f'),'UTF8')`, "Hello", false},

		// width_bucket — equi-width histogram bucketing (below-range=0, at/above=count+1)
		{"width_bucket_mid", `SELECT width_bucket(5.35, 0.024, 10.06, 5)::VARCHAR`, "3", false},
		{"width_bucket_below", `SELECT width_bucket(-1, 0, 10, 5)::VARCHAR`, "0", false},
		{"width_bucket_above", `SELECT width_bucket(10, 0, 10, 5)::VARCHAR`, "6", false},
		{"width_bucket_first", `SELECT width_bucket(0, 0, 10, 5)::VARCHAR`, "1", false},

		// scale / min_scale — numeric fractional-digit counts
		{"scale_trailing_zeros", `SELECT scale(8.4100)::VARCHAR`, "4", false},
		{"scale_integer", `SELECT scale(5)::VARCHAR`, "0", false},
		{"min_scale_strips_zeros", `SELECT min_scale(8.4100)::VARCHAR`, "2", false},

		// inet helpers
		{"masklen_cidr", `SELECT masklen('192.168.1.5/24'::inet)::VARCHAR`, "24", false},
		{"masklen_bare_ipv4", `SELECT masklen('192.168.1.5'::inet)::VARCHAR`, "32", false},
		{"hostmask_24", `SELECT hostmask('192.168.1.5/24'::inet)::VARCHAR`, "0.0.0.255", false},
		{"set_masklen_16", `SELECT set_masklen('192.168.1.5/24'::inet, 16)::VARCHAR`, "192.168.1.5/16", false},
		{"inet_same_family_mixed", `SELECT inet_same_family('192.168.1.5'::inet,'::1'::inet)::VARCHAR`, "false", false},
	})
}
