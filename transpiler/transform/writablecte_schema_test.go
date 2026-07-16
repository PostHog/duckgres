package transform

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func TestWritableCTESchemaQueryUsesReadOnlyCTEsForReturningStar(t *testing.T) {
	r := transpileCTE(t, `WITH changed AS (UPDATE widgets SET title = 'new' WHERE id = 7 RETURNING *) SELECT * FROM changed`)
	if r.SchemaQuery == "" {
		t.Fatal("expected a schema query")
	}

	tree, err := pg_query.Parse(r.SchemaQuery)
	if err != nil {
		t.Fatalf("Parse schema query: %v\n%s", err, r.SchemaQuery)
	}
	if len(tree.Stmts) != 1 {
		t.Fatalf("schema query has %d statements, want 1", len(tree.Stmts))
	}

	// Schema discovery must be able to PREPARE this query without executing the
	// writable CTE. The rewritten CTE must therefore be a SELECT, while the
	// final SELECT still consumes the CTE's output shape.
	final := tree.Stmts[0].Stmt.GetSelectStmt()
	if final == nil {
		t.Fatalf("schema query final statement = %T, want SELECT", tree.Stmts[0].Stmt.Node)
	}
	if final.WithClause == nil || len(final.WithClause.Ctes) != 1 {
		t.Fatalf("schema query WITH clause = %#v, want one CTE", final.WithClause)
	}
	cte := final.WithClause.Ctes[0].GetCommonTableExpr()
	if cte == nil || cte.Ctequery.GetSelectStmt() == nil {
		t.Fatalf("schema CTE = %#v, want generated SELECT", cte)
	}
	if got := final.FromClause[0].GetRangeVar().Relname; got != "changed" {
		t.Fatalf("schema final FROM = %q, want changed", got)
	}
	if got := cte.Ctequery.GetSelectStmt().FromClause[0].GetRangeVar().Relname; got != "widgets" {
		t.Fatalf("generated CTE source = %q, want widgets", got)
	}
	if !selectReturnsStar(final) || !selectReturnsStar(cte.Ctequery.GetSelectStmt()) {
		t.Fatalf("schema query did not preserve RETURNING * output shape: %s", r.SchemaQuery)
	}

	var hasDML bool
	WalkFunc(tree, func(node *pg_query.Node) bool {
		if node.GetUpdateStmt() != nil || node.GetInsertStmt() != nil || node.GetDeleteStmt() != nil {
			hasDML = true
			return false
		}
		return true
	})
	if hasDML {
		t.Fatalf("schema query contains DML: %s", r.SchemaQuery)
	}
	if strings.Contains(strings.ToUpper(r.SchemaQuery), "CREATE TEMP") || strings.Contains(strings.ToUpper(r.SchemaQuery), "BEGIN") {
		t.Fatalf("schema query contains setup SQL: %s", r.SchemaQuery)
	}

	// The execution rewrite remains responsible for the one real mutation.
	if !strings.Contains(strings.ToUpper(strings.Join(r.Statements, "\n")), "UPDATE") {
		t.Fatalf("execution rewrite lost the UPDATE: %v", r.Statements)
	}
}

func selectReturnsStar(stmt *pg_query.SelectStmt) bool {
	if stmt == nil || len(stmt.TargetList) != 1 {
		return false
	}
	target := stmt.TargetList[0].GetResTarget()
	if target == nil || target.Val == nil {
		return false
	}
	ref := target.Val.GetColumnRef()
	if ref == nil || len(ref.Fields) != 1 {
		return false
	}
	return ref.Fields[0].GetAStar() != nil
}
