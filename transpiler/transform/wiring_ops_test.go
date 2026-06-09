package transform

import (
	"strings"
	"testing"
)

// Regression tests for expression positions the OperatorTransform manual walk
// used to miss. An untransformed `~` binds to DuckDB's regexp_full_match,
// silently flipping match semantics, so each position below must rewrite to
// regexp_matches (or json_extract_string for ->>).

func TestOperatorTransform_ValuesLists(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `INSERT INTO t VALUES ('abc' ~ 'b')`)
	if !strings.Contains(out, "regexp_matches('abc', 'b')") {
		t.Errorf("expected regexp_matches in VALUES list, got %q", out)
	}
}

func TestOperatorTransform_InsertReturning(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `INSERT INTO t (s) VALUES ('x') RETURNING s ~ 'b'`)
	if !strings.Contains(out, "regexp_matches(s, 'b')") {
		t.Errorf("expected regexp_matches in INSERT RETURNING, got %q", out)
	}
}

func TestOperatorTransform_UpdateReturning(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `UPDATE t SET a = 1 RETURNING s ~ 'b'`)
	if !strings.Contains(out, "regexp_matches(s, 'b')") {
		t.Errorf("expected regexp_matches in UPDATE RETURNING, got %q", out)
	}
}

func TestOperatorTransform_DeleteReturning(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `DELETE FROM t RETURNING s ~ 'b'`)
	if !strings.Contains(out, "regexp_matches(s, 'b')") {
		t.Errorf("expected regexp_matches in DELETE RETURNING, got %q", out)
	}
}

func TestOperatorTransform_DeleteUsing(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `DELETE FROM t USING (SELECT 'abc' ~ 'b' AS m) u WHERE u.m`)
	if !strings.Contains(out, "regexp_matches('abc', 'b')") {
		t.Errorf("expected regexp_matches in DELETE USING subselect, got %q", out)
	}
}

func TestOperatorTransform_AggFilter(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT count(*) FILTER (WHERE s ~ 'b') FROM t`)
	if !strings.Contains(out, "regexp_matches(s, 'b')") {
		t.Errorf("expected regexp_matches in FILTER clause, got %q", out)
	}
}

func TestOperatorTransform_AggOrder(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT string_agg(s, ',' ORDER BY s ~ 'b') FROM t`)
	if !strings.Contains(out, "regexp_matches(s, 'b')") {
		t.Errorf("expected regexp_matches in aggregate ORDER BY, got %q", out)
	}
}

func TestOperatorTransform_InlineWindowDef(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT count(*) OVER (PARTITION BY j ->> 'k' ORDER BY s ~ 'b') FROM t`)
	if !strings.Contains(out, "json_extract_string(j, 'k')") {
		t.Errorf("expected json_extract_string in OVER PARTITION BY, got %q", out)
	}
	if !strings.Contains(out, "regexp_matches(s, 'b')") {
		t.Errorf("expected regexp_matches in OVER ORDER BY, got %q", out)
	}
}

func TestOperatorTransform_NamedWindowClause(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT count(*) OVER w FROM t WINDOW w AS (PARTITION BY j ->> 'k')`)
	if !strings.Contains(out, "json_extract_string(j, 'k')") {
		t.Errorf("expected json_extract_string in WINDOW clause, got %q", out)
	}
}

func TestOperatorTransform_ArrayExpr(t *testing.T) {
	tr := NewOperatorTransform()
	// Semantic regression: untransformed, the raw `~` binds to DuckDB's
	// regexp_full_match and ARRAY['abc' ~ 'b'] evaluates to [false] instead
	// of [true]. The rewrite to regexp_matches preserves Postgres semantics.
	out := deparseAfter(t, tr, `SELECT ARRAY['abc' ~ 'b']`)
	if !strings.Contains(out, "regexp_matches('abc', 'b')") {
		t.Errorf("expected regexp_matches inside ARRAY[...], got %q", out)
	}
}

func TestOperatorTransform_DistinctOn(t *testing.T) {
	tr := NewOperatorTransform()
	out := deparseAfter(t, tr, `SELECT DISTINCT ON (j ->> 'k') s FROM t`)
	if !strings.Contains(out, "json_extract_string(j, 'k')") {
		t.Errorf("expected json_extract_string in DISTINCT ON, got %q", out)
	}
}

func TestOperatorTransform_PlainDistinctUntouched(t *testing.T) {
	tr := NewOperatorTransform()
	// Plain DISTINCT is represented as a single empty node in DistinctClause;
	// it must survive the transform without panicking or being rewritten.
	out := deparseAfter(t, tr, `SELECT DISTINCT s FROM t`)
	if !strings.Contains(out, "DISTINCT") {
		t.Errorf("expected plain DISTINCT to be preserved, got %q", out)
	}
}

// Regression tests for LockingTransform: FOR UPDATE/FOR SHARE must be stripped
// at any depth, not just on the top-level statement, or DuckDB fails with
// "SELECT locking clause is not supported!".

func TestLockingTransform_TopLevel(t *testing.T) {
	tr := NewLockingTransform()
	out := deparseAfter(t, tr, `SELECT 1 AS a FOR UPDATE`)
	if strings.Contains(out, "FOR UPDATE") {
		t.Errorf("expected top-level FOR UPDATE stripped, got %q", out)
	}
}

func TestLockingTransform_Subquery(t *testing.T) {
	tr := NewLockingTransform()
	out := deparseAfter(t, tr, `SELECT * FROM (SELECT 1 AS a FOR UPDATE) sub`)
	if strings.Contains(out, "FOR UPDATE") {
		t.Errorf("expected subquery FOR UPDATE stripped, got %q", out)
	}
}

func TestLockingTransform_CTE(t *testing.T) {
	tr := NewLockingTransform()
	out := deparseAfter(t, tr, `WITH c AS (SELECT 1 AS a FOR UPDATE) SELECT * FROM c`)
	if strings.Contains(out, "FOR UPDATE") {
		t.Errorf("expected CTE FOR UPDATE stripped, got %q", out)
	}
}

func TestLockingTransform_ExistsSublink(t *testing.T) {
	tr := NewLockingTransform()
	out := deparseAfter(t, tr, `SELECT EXISTS (SELECT 1 FROM t FOR SHARE)`)
	if strings.Contains(out, "FOR SHARE") {
		t.Errorf("expected EXISTS sublink FOR SHARE stripped, got %q", out)
	}
}

func TestLockingTransform_SetOpArms(t *testing.T) {
	tr := NewLockingTransform()
	out := deparseAfter(t, tr, `(SELECT 1 AS a FOR UPDATE) UNION ALL (SELECT 2 FOR UPDATE)`)
	if strings.Contains(out, "FOR UPDATE") {
		t.Errorf("expected FOR UPDATE stripped from both UNION arms, got %q", out)
	}
}
