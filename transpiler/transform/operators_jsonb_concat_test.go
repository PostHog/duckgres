package transform

import (
	"strings"
	"testing"
)

// Regression tests for the jsonb || rewrite (issue #716). The old rewrite to
// json_merge_patch silently corrupted non-flat-object concatenation: Postgres
// `'[1,2]'::jsonb || '[3,4]'::jsonb` is [1,2,3,4] but json_merge_patch yields
// [3,4] (RFC 7396 replaces non-object values), and `'{"a":1}' || '{"a":null}'`
// is {"a":null} in Postgres but {} under merge-patch (null deletes the key).
// The rewrite now emits a json_type()-dispatching CASE; these tests assert its
// shape. The runtime semantics of the emitted expression are verified against
// a live PostgreSQL by tests/integration/functions_test.go (TestFunctionsJSON,
// jsonb_concat_* cases), which compares Duckgres output with real Postgres for
// the full matrix: object merge, null-value kept, nested-object shallow merge,
// array concat, array append/prepend, scalar pairing, and NULL operands.

// concatShapeMarkers are the load-bearing pieces of the emitted CASE: NULL
// guard, object||object shallow merge (map_concat, NOT json_merge_patch), and
// the array-concat fallback with non-arrays wrapped as one-element lists.
var concatShapeMarkers = []string{
	"is null",
	"json_type",
	"map_concat",
	`json_transform`,
	`"map(varchar, json)"`,
	"list_concat",
	`["json"]`,
	"list_value",
}

func assertJSONConcatShape(t *testing.T, sql string) {
	t.Helper()
	tr := NewOperatorTransform()
	out := strings.ToLower(deparseAfter(t, tr, sql))
	for _, marker := range concatShapeMarkers {
		if !strings.Contains(out, marker) {
			t.Errorf("transpiled %q missing %q: %q", sql, marker, out)
		}
	}
	if strings.Contains(out, "json_merge_patch") {
		t.Errorf("jsonb || must not use json_merge_patch (wrong for arrays/nulls), got %q", out)
	}
}

func TestOperatorTransform_JSONBConcatObjects(t *testing.T) {
	assertJSONConcatShape(t, `SELECT '{"a":1}'::jsonb || '{"b":2}'::jsonb`)
}

func TestOperatorTransform_JSONBConcatArrays(t *testing.T) {
	// The regression that motivated #716: array || array must concatenate,
	// not merge-patch (which replaces the left array wholesale).
	assertJSONConcatShape(t, `SELECT '[1,2]'::jsonb || '[3,4]'::jsonb`)
}

func TestOperatorTransform_JSONBConcatUpdateSet(t *testing.T) {
	// The write-path idiom that silently corrupted data: jsonb array append
	// inside UPDATE ... SET.
	assertJSONConcatShape(t, `UPDATE t SET col = col || '["x"]'::jsonb`)
}

func TestOperatorTransform_JSONBConcatMixedOperand(t *testing.T) {
	// Only one operand is provably JSON; the rewrite must still fire (the
	// other side is implicitly JSON in Postgres) and keep full semantics.
	assertJSONConcatShape(t, `SELECT json_extract(data, '$.tags') || '["new"]'::jsonb FROM t`)
}

func TestOperatorTransform_JSONBConcatChainedArrowRewritten(t *testing.T) {
	tr := NewOperatorTransform()
	out := strings.ToLower(deparseAfter(t, tr, `SELECT (data -> 'tags') || '["new"]'::jsonb FROM t`))
	// The arrow inside the left operand must still be rewritten to
	// json_extract (the operand pre-pass), and the || must dispatch.
	if !strings.Contains(out, "json_extract") {
		t.Errorf("nested -> not rewritten inside || operand: %q", out)
	}
	if !strings.Contains(out, "map_concat") || !strings.Contains(out, "list_concat") {
		t.Errorf("|| with JSON operand not rewritten to dispatching CASE: %q", out)
	}
}

func TestOperatorTransform_StringConcatUnchanged(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT 'a' || 'b'`)
	low := strings.ToLower(out)
	if strings.Contains(low, "json_merge_patch") || strings.Contains(low, "map_concat") {
		t.Errorf("plain string concat should not be rewritten, got %q", out)
	}
	if !strings.Contains(out, "||") {
		t.Errorf("string concat || should be preserved, got %q", out)
	}
}

func TestOperatorTransform_TextReturningJSONFuncConcatUnchanged(t *testing.T) {
	tr := NewOperatorTransform()
	// json_extract_string / *_text / *_length return non-JSON values, so ||
	// on their results is plain text concat in Postgres and must stay ||.
	for _, sql := range []string{
		`SELECT json_extract_string(data, '$.name') || '-suffix' FROM t`,
		`SELECT json_extract_path_text(data, 'name') || '-suffix' FROM t`,
		`SELECT json_array_length(data) || ' items' FROM t`,
		`SELECT json_type(data) || '!' FROM t`,
	} {
		out := deparseAfter(t, tr, sql)
		low := strings.ToLower(out)
		if strings.Contains(low, "json_merge_patch") || strings.Contains(low, "map_concat") {
			t.Errorf("text-returning json func concat was rewritten: %q -> %q", sql, out)
		}
		if !strings.Contains(out, "||") {
			t.Errorf("|| should be preserved for %q, got %q", sql, out)
		}
	}
}

func TestOperatorTransform_JSONBConcatToJSONFuncOperands(t *testing.T) {
	// to_json/to_jsonb/row_to_json/array_to_json return JSON but their names
	// don't start with "json"; without explicit looksJSON coverage
	// `to_jsonb(a) || to_jsonb(b)` fell through to DuckDB string concat,
	// producing invalid JSON. (FunctionTransform maps to_jsonb -> to_json
	// earlier in the pipeline, but this transform must recognize both
	// spellings since it also runs standalone.)
	for _, sql := range []string{
		`SELECT to_jsonb(a) || to_jsonb(b) FROM t`,
		`SELECT to_json(a) || '[1]'::jsonb FROM t`,
		`SELECT row_to_json(r) || '{"x":1}'::jsonb FROM t`,
		`SELECT array_to_json(arr) || '[1]'::jsonb FROM t`,
	} {
		assertJSONConcatShape(t, sql)
	}
}

func TestOperatorTransform_JSONBConcatBareArrowOperands(t *testing.T) {
	// Neither side carries a cast: both operands are raw `->` A_Exprs, which
	// this transform deterministically rewrites to json_extract (JSON-
	// returning). The || gate runs before the operand rewrite, so the raw
	// arrow must count as JSON-looking or this would silently become string
	// concat of two JSON values.
	assertJSONConcatShape(t, `SELECT (d -> 'a') || (d -> 'b') FROM t`)
}

func TestOperatorTransform_DoubleArrowConcatUnchanged(t *testing.T) {
	// ->> yields text, so || on its bare result stays plain text concat.
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT (d ->> 'a') || '-x' FROM t`)
	low := strings.ToLower(out)
	if strings.Contains(low, "map_concat") || strings.Contains(low, "list_concat") {
		t.Errorf("->> result concat should stay text concat, got %q", out)
	}
	if !strings.Contains(out, "||") {
		t.Errorf("|| should be preserved, got %q", out)
	}
}

func TestOperatorTransform_JSONBConcatDeparses(t *testing.T) {
	// The emitted CASE must survive a pg_query deparse round-trip for every
	// shape the issue calls out; deparseAfter fails the test on error.
	tr := NewOperatorTransform()
	for _, sql := range []string{
		`SELECT '{"a":1}'::jsonb || '{"a":null}'::jsonb`,
		`SELECT '{"a":{"x":1}}'::jsonb || '{"a":{"y":2}}'::jsonb`,
		`SELECT '[1,2]'::jsonb || '3'::jsonb`,
		`SELECT '3'::jsonb || '[1,2]'::jsonb`,
		`SELECT '1'::jsonb || '2'::jsonb`,
		`SELECT NULL::jsonb || '[1]'::jsonb`,
		`INSERT INTO t (col) SELECT a || b::jsonb FROM s`,
	} {
		_ = deparseAfter(t, tr, sql)
	}
}
