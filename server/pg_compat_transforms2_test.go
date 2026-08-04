package server

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/posthog/duckgres/transpiler"
)

// These tests cover the wiring-gap fixes for functions.go / catalog.go:
// variadic json_extract_path*, regexp_match(es) flags, precision-form
// SQLValueFunctions, 3-arg string_to_array, 3-arg array_position, the
// timeofday() macro, and PG-vocabulary json_typeof/jsonb_typeof macros.
// Same transpile-and-execute pattern as pg_compat_transforms_test.go.

func TestCompatTransforms_JSONExtractPathVariadic(t *testing.T) {
	runTransformCases(t, []transformCase{
		// >2 path elements must become a left-nested chain of 2-arg calls —
		// DuckDB has no 3+-arg json_extract overload.
		{"json_extract_path_2elem", `SELECT json_extract_path('{"a":{"b":2}}', 'a', 'b')::text`, "2", false},
		{"json_extract_path_text_3elem", `SELECT json_extract_path_text('{"a":{"b":{"c":2}}}', 'a', 'b', 'c')`, "2", false},
		{"jsonb_extract_path_2elem", `SELECT jsonb_extract_path('{"a":{"b":"x"}}', 'a', 'b')::text`, `"x"`, false},
		{"jsonb_extract_path_text_2elem", `SELECT jsonb_extract_path_text('{"a":{"b":"x"}}', 'a', 'b')`, "x", false},
		// 1-path regression: plain rename must keep working.
		{"json_extract_path_1elem", `SELECT json_extract_path('{"a":1}', 'a')::text`, "1", false},
		{"json_extract_path_text_1elem", `SELECT json_extract_path_text('{"a":"v"}', 'a')`, "v", false},
	})
}

func TestCompatTransforms_RegexpMatchFlags(t *testing.T) {
	runTransformCases(t, []transformCase{
		// PG flags string must land in DuckDB's 4th (options) slot, not the
		// 3rd (group) slot.
		{"regexp_match_i_flag", `SELECT regexp_match('aBc', 'b', 'i')`, "B", false},
		{"regexp_match_2arg", `SELECT regexp_match('abc', 'b')`, "b", false},
		// regexp_extract_all is already global: 'g' must be stripped, the
		// rest of the flags preserved.
		{"regexp_matches_gi", `SELECT regexp_matches('Abc', 'b', 'gi')::text`, "[b]", false},
		{"regexp_matches_g", `SELECT regexp_matches('AbcAbc', 'b', 'g')::text`, "[b, b]", false},
		{"regexp_matches_i", `SELECT regexp_matches('aBc', 'b', 'i')::text`, "[B]", false},
		{"regexp_matches_empty_flags", `SELECT regexp_matches('abc', 'b', '')::text`, "[b]", false},
		// Non-literal flags: shifted to the options slot with 'g' stripped at
		// runtime (DuckDB constant-folds the replace()).
		{"regexp_matches_expr_flags", `SELECT regexp_matches('aBc', 'b', lower('GI'))::text`, "[B]", false},
		{"regexp_matches_2arg", `SELECT regexp_matches('abc', 'b')::text`, "[b]", false},
	})
}

// Precision forms LOCALTIME(p)/LOCALTIMESTAMP(p)/CURRENT_TIME(p)/CURRENT_TIMESTAMP(p)
// parse as SQLValueFunction nodes; they must be rewritten to the bare same-family
// keyword (DuckDB has no TIME(p) typmod and no callable function of that name).
func TestCompatTransforms_PrecisionSQLValueFunctions(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("initPgCatalog: %v", err)
	}
	tr := transpiler.New(transpiler.Config{})
	for _, q := range []string{
		`SELECT LOCALTIME(2)`,
		`SELECT LOCALTIMESTAMP(3)`,
		`SELECT CURRENT_TIME(2)`,
		`SELECT CURRENT_TIMESTAMP(3)`,
	} {
		res, err := tr.Transpile(q)
		if err != nil {
			t.Fatalf("transpile %q: %v", q, err)
		}
		if strings.Contains(res.SQL, "(") {
			t.Errorf("transpile %q = %q, want bare keyword form (no parens)", q, res.SQL)
		}
		var got any
		if err := db.QueryRow(res.SQL).Scan(&got); err != nil {
			t.Errorf("exec %q (transpiled %q): %v", q, res.SQL, err)
		} else if got == nil {
			t.Errorf("exec %q (transpiled %q) = NULL, want a value", q, res.SQL)
		}
	}
	// Bare keyword forms must remain untouched passthroughs.
	for _, q := range []string{`SELECT LOCALTIME`, `SELECT CURRENT_TIMESTAMP`} {
		res, err := tr.Transpile(q)
		if err != nil {
			t.Fatalf("transpile %q: %v", q, err)
		}
		if res.SQL != q {
			t.Errorf("transpile %q = %q, want passthrough", q, res.SQL)
		}
	}
}

func TestCompatTransforms_StringToArrayNullStr(t *testing.T) {
	runTransformCases(t, []transformCase{
		// 3-arg form: elements equal to nullstr become NULL (DuckDB has no
		// 3-arg string_split — routed to the duckgres_string_to_array3 macro).
		{"string_to_array_nullstr", `SELECT string_to_array('a,b,,c', ',', '')::text`, "[a, b, NULL, c]", false},
		{"string_to_array_nullstr_nomatch", `SELECT string_to_array('a,b', ',', 'x')::text`, "[a, b]", false},
		// 2-arg regression: stays on string_split.
		{"string_to_array_2arg", `SELECT string_to_array('a,b', ',')::text`, "[a, b]", false},
	})
}

func TestCompatTransforms_ArrayPositionStart(t *testing.T) {
	runTransformCases(t, []transformCase{
		// 3-arg form: search starts at `start` but positions stay relative to
		// the whole array (DuckDB list_position has no 3-arg overload).
		{"array_position_start", `SELECT array_position(ARRAY[1,2,3], 2, 2)::text`, "2", false},
		{"array_position_dup", `SELECT array_position(ARRAY[1,2,2,3], 2, 3)::text`, "3", false},
		{"array_position_miss", `SELECT array_position(ARRAY[1,2,3], 9, 2)::text`, "", true},
		{"array_position_miss_past_start", `SELECT array_position(ARRAY[1,2,3], 1, 2)::text`, "", true},
		{"array_position_start_zero", `SELECT array_position(ARRAY[1,2,3], 2, 0)::text`, "2", false},
		// 2-arg regression: plain list_position rename.
		{"array_position_2arg", `SELECT array_position(ARRAY[1,2,3], 2)::text`, "2", false},
	})
}

// timeofday() must return PG-shaped text like
// "Tue Jun 09 13:17:57.627882 2026 America/Los_Angeles" — the old mapping to a
// nonexistent scalar "current_timestamp"() was a hard Catalog Error.
func TestCompatTransforms_Timeofday(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := initPgCatalog(db, processStartTime, processStartTime, "dev", "dev"); err != nil {
		t.Fatalf("initPgCatalog: %v", err)
	}
	tr := transpiler.New(transpiler.Config{})
	// PG abbreviates the zone; DuckDB %Z yields the zone name — accepted
	// approximation, so the trailing zone field is just "non-empty".
	shape := regexp.MustCompile(`^[A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2}\.\d{6} \d{4} \S+`)
	for _, q := range []string{`SELECT timeofday()`, `SELECT pg_catalog.timeofday()`} {
		res, err := tr.Transpile(q)
		if err != nil {
			t.Fatalf("transpile %q: %v", q, err)
		}
		var got string
		if err := db.QueryRow(res.SQL).Scan(&got); err != nil {
			t.Fatalf("exec %q (transpiled %q): %v", q, res.SQL, err)
		}
		if !shape.MatchString(got) {
			t.Errorf("%q = %q, want PG timeofday() shape", q, got)
		}
	}
	// DuckLake mode: the macro lives in memory.main and must be qualified.
	dlRes, err := transpiler.New(transpiler.Config{DuckLakeMode: true}).Transpile(`SELECT timeofday()`)
	if err != nil {
		t.Fatalf("transpile DuckLake timeofday: %v", err)
	}
	if !strings.Contains(dlRes.SQL, "memory.main.timeofday") {
		t.Errorf("DuckLake transpile = %q, want memory.main.timeofday qualification", dlRes.SQL)
	}
}

func TestCompatTransforms_JSONTypeofVocabulary(t *testing.T) {
	runTransformCases(t, []transformCase{
		// PG vocabulary (number/string/...), not DuckDB's (UBIGINT/VARCHAR/...).
		{"json_typeof_uint", `SELECT json_typeof('1')`, "number", false},
		{"json_typeof_negint", `SELECT json_typeof('-1')`, "number", false},
		{"json_typeof_double", `SELECT json_typeof('1.5')`, "number", false},
		{"json_typeof_string", `SELECT json_typeof('"hi"')`, "string", false},
		{"json_typeof_object", `SELECT json_typeof('{"a":1}')`, "object", false},
		{"json_typeof_array", `SELECT json_typeof('[1,2]')`, "array", false},
		{"json_typeof_null_literal", `SELECT json_typeof('null')`, "null", false},
		// PG json_typeof is strict: SQL NULL in, SQL NULL out (not 'number').
		{"json_typeof_sql_null", `SELECT json_typeof(NULL)`, "", true},
		{"jsonb_typeof_bool", `SELECT jsonb_typeof('true'::jsonb)`, "boolean", false},
		{"jsonb_typeof_number", `SELECT jsonb_typeof('42'::jsonb)`, "number", false},
	})
}
