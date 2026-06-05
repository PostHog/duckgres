package transform

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func transpileCTE(t *testing.T, sql string) *Result {
	t.Helper()
	tree, err := pg_query.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	result := &Result{}
	if _, err := NewWritableCTETransform().Transform(tree, result); err != nil {
		t.Fatalf("Transform(%q): %v", sql, err)
	}
	return result
}

func sqlState(err error) string {
	if s, ok := err.(interface{ SQLState() string }); ok {
		return s.SQLState()
	}
	return ""
}

// UPDATE ... RETURNING a modified column inside a writable CTE must be rejected
// (the pre-update capture would return a stale value).
func TestWritableCTE_UpdateReturningModifiedColumnRejected(t *testing.T) {
	r := transpileCTE(t, `WITH u AS (UPDATE t SET val = val + 1 WHERE id = 1 RETURNING id, val) SELECT * FROM u`)
	if r.Error == nil {
		t.Fatalf("expected rejection, got statements=%v", r.Statements)
	}
	if sqlState(r.Error) != "0A000" {
		t.Errorf("SQLSTATE = %q, want 0A000", sqlState(r.Error))
	}
}

// RETURNING * is exempt from the guard (common row-identification idiom; callers
// typically read only the key). It must still rewrite, not error.
func TestWritableCTE_UpdateReturningStarAllowed(t *testing.T) {
	r := transpileCTE(t, `WITH u AS (UPDATE t SET val = 5 WHERE id = 1 RETURNING *) SELECT * FROM u`)
	if r.Error != nil {
		t.Fatalf("RETURNING * should be allowed, got %v", r.Error)
	}
	if len(r.Statements) == 0 {
		t.Errorf("expected a multi-statement rewrite, got none")
	}
}

// The Airbyte-style pattern — RETURNING only an unmodified key — must keep working.
func TestWritableCTE_UpdateReturningUnmodifiedKeyAllowed(t *testing.T) {
	r := transpileCTE(t, `WITH u AS (UPDATE t SET val = val + 1 WHERE id = 1 RETURNING id) SELECT * FROM u`)
	if r.Error != nil {
		t.Fatalf("RETURNING an unmodified key should be allowed, got %v", r.Error)
	}
	if len(r.Statements) == 0 {
		t.Errorf("expected a multi-statement rewrite, got none")
	}
}

// DELETE ... RETURNING in a CTE is correct (OLD == the deleted rows) and allowed.
func TestWritableCTE_DeleteReturningAllowed(t *testing.T) {
	r := transpileCTE(t, `WITH d AS (DELETE FROM t WHERE id = 1 RETURNING id, val) SELECT * FROM d`)
	if r.Error != nil {
		t.Fatalf("DELETE ... RETURNING should be allowed, got %v", r.Error)
	}
	if len(r.Statements) == 0 {
		t.Errorf("expected a multi-statement rewrite, got none")
	}
}
