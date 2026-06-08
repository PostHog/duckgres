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

func TestOperatorTransform_JSONBMerge(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT '{"a":1}'::jsonb || '{"b":2}'::jsonb`)
	if !strings.Contains(strings.ToLower(out), "json_merge_patch") {
		t.Errorf("expected json_merge_patch, got %q", out)
	}
}

func TestOperatorTransform_StringConcatUnchanged(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT 'a' || 'b'`)
	if strings.Contains(strings.ToLower(out), "json_merge_patch") {
		t.Errorf("plain string concat should not become json_merge_patch, got %q", out)
	}
	if !strings.Contains(out, "||") {
		t.Errorf("string concat || should be preserved, got %q", out)
	}
}
