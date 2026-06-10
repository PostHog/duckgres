package transform

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func deparseAfter(t *testing.T, tr Transform, sql string) string {
	t.Helper()
	tree, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	if _, err := tr.Transform(tree, &Result{}); err != nil {
		t.Fatalf("Transform(%q): %v", sql, err)
	}
	out, err := pg_query.Deparse(tree)
	if err != nil {
		t.Fatalf("Deparse(%q): %v", sql, err)
	}
	return out
}

func TestLiteralTransform_ByteaHex(t *testing.T) {
	tr := NewLiteralTransform()
	out := deparseAfter(t, tr, `SELECT '\xDEADBEEF'::bytea`)
	if !strings.Contains(strings.ToLower(out), "unhex('deadbeef')") {
		t.Errorf("expected unhex('DEADBEEF'), got %q", out)
	}
	if strings.Contains(out, `\x`) {
		t.Errorf("hex prefix should be gone, got %q", out)
	}
}

func TestLiteralTransform_ByteaEscapeLeftAlone(t *testing.T) {
	tr := NewLiteralTransform()
	// Non-hex bytea literal must be left untouched (no unhex rewrite).
	out := deparseAfter(t, tr, `SELECT 'plain'::bytea`)
	if strings.Contains(strings.ToLower(out), "unhex") {
		t.Errorf("escape-format bytea should not be rewritten, got %q", out)
	}
}

func TestLiteralTransform_BitString(t *testing.T) {
	tr := NewLiteralTransform()
	out := deparseAfter(t, tr, `SELECT B'101'`)
	up := strings.ToUpper(out)
	if !strings.Contains(up, "'101'") || !strings.Contains(up, "BIT") {
		t.Errorf("expected '101'::BIT, got %q", out)
	}
	if strings.Contains(strings.ToLower(out), "b101") {
		t.Errorf("bit literal should not deparse to the string b101, got %q", out)
	}
}

func TestLiteralTransform_NonLiteralUnchanged(t *testing.T) {
	tr := NewLiteralTransform()
	out := deparseAfter(t, tr, `SELECT 1 + 2`)
	if !strings.Contains(out, "1 + 2") {
		t.Errorf("unrelated query changed: %q", out)
	}
}

// jsonb || rewrite coverage lives in operators_jsonb_concat_test.go.

func TestOperatorTransform_JSONBContains(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT '{"a":1,"b":2}'::jsonb @> '{"a":1}'::jsonb`)
	if !strings.Contains(strings.ToLower(out), "json_contains") {
		t.Errorf("expected json_contains, got %q", out)
	}
}

func TestOperatorTransform_ArrayContainsUnchanged(t *testing.T) {
	tr := NewOperatorTransform()
	// Array @> array is native in DuckDB; must NOT be rewritten to json_contains.
	out := deparseAfter(t, tr, `SELECT ARRAY[1,2,3] @> ARRAY[2]`)
	if strings.Contains(strings.ToLower(out), "json_contains") {
		t.Errorf("array containment should stay native @>, got %q", out)
	}
	if !strings.Contains(out, "@>") {
		t.Errorf("array @> should be preserved, got %q", out)
	}
}

func TestOperatorTransform_JSONPathExtractText(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT '{"a":{"b":1}}'::json #>> '{a,b}'`)
	low := strings.ToLower(out)
	if !strings.Contains(low, "json_extract_string") {
		t.Errorf("expected json_extract_string, got %q", out)
	}
	if !strings.Contains(out, `$."a"."b"`) {
		t.Errorf("expected JSONPath $.\"a\".\"b\", got %q", out)
	}
}

func TestTypeCastTransform_ArrayLiteralInInsertValues(t *testing.T) {
	tr := NewTypeCastTransform()
	// PG array-literal casts inside an INSERT ... VALUES list must be rewritten
	// (the VALUES rows live in SelectStmt.ValuesLists, not the target list).
	out := deparseAfter(t, tr, `INSERT INTO t VALUES (1, '{a,b}'::text[])`)
	if !strings.Contains(out, "ARRAY[") {
		t.Errorf("array literal in VALUES not rewritten to ARRAY[...]: %q", out)
	}
	if strings.Contains(out, "'{a,b}'") {
		t.Errorf("raw curly array literal should be gone: %q", out)
	}
}

func TestTypeCastTransform_ArrayLiteral(t *testing.T) {
	tr := NewTypeCastTransform()
	cases := []struct {
		name    string
		in      string
		wantSub []string // substrings that must be present
		wantNo  []string // substrings that must be absent
	}{
		{"int_array", `SELECT '{1,2,3}'::integer[]`, []string{"ARRAY[", "'1'", "'2'", "'3'"}, []string{"{1,2,3}"}},
		{"text_array", `SELECT '{a,b}'::text[]`, []string{"ARRAY[", "'a'", "'b'"}, []string{"{a,b}"}},
		{"empty_array", `SELECT '{}'::integer[]`, []string{"ARRAY["}, []string{"'{}'"}},
		{"null_element", `SELECT '{1,NULL,3}'::integer[]`, []string{"ARRAY[", "NULL"}, []string{"{1,NULL,3}"}},
		{"quoted_embedded_comma", `SELECT '{"a,b",c}'::text[]`, []string{"ARRAY[", "'a,b'", "'c'"}, nil},
		// Unquoted backslash escapes (PG accepts '{a\,b}' as one element) are NOT
		// handled — the literal must be left untouched (honest hard error at the
		// cast) rather than silently mis-split into two elements.
		{"unquoted_backslash_left_alone", `SELECT e'{a\\,b}'::text[]`, []string{`{a`}, []string{"ARRAY["}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := deparseAfter(t, tr, tc.in)
			for _, sub := range tc.wantSub {
				if !strings.Contains(out, sub) {
					t.Errorf("%s: expected %q in %q", tc.name, sub, out)
				}
			}
			for _, no := range tc.wantNo {
				if strings.Contains(out, no) {
					t.Errorf("%s: did not expect %q in %q", tc.name, no, out)
				}
			}
		})
	}
}
