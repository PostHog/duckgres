package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// TempTableTransform qualifies temporary table references with the temp schema.
// In DuckLake mode, the default catalog is "ducklake", so unqualified table references
// look in ducklake.main rather than the temp schema where temporary tables live.
// This transform detects Fivetran-style staging tables (containing "-staging-") and
// qualifies them with "temp." to ensure they're found.
type TempTableTransform struct{}

// NewTempTableTransform creates a new TempTableTransform.
func NewTempTableTransform() *TempTableTransform {
	return &TempTableTransform{}
}

func (t *TempTableTransform) Name() string {
	return "temptable"
}

func (t *TempTableTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		t.walkAndTransform(stmt.Stmt, &changed)
	}

	return changed, nil
}

func (t *TempTableTransform) walkAndTransform(node *pg_query.Node, changed *bool) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		// Check if this is an unqualified table reference that looks like a staging table
		if n.RangeVar != nil && n.RangeVar.Schemaname == "" && t.isStagingTable(n.RangeVar.Relname) {
			// Qualify with temp schema so DuckDB finds it in the temp tables
			n.RangeVar.Schemaname = "temp"
			*changed = true
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			t.walkSelectStmt(n.SelectStmt, changed)
		}

	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			// Don't transform the target relation for INSERT - only FROM clause references
			t.walkAndTransform(n.InsertStmt.SelectStmt, changed)
		}

	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			for _, from := range n.UpdateStmt.FromClause {
				t.walkAndTransform(from, changed)
			}
			t.walkAndTransform(n.UpdateStmt.WhereClause, changed)
		}

	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			// Check USING clause for staging tables
			for _, using := range n.DeleteStmt.UsingClause {
				t.walkAndTransform(using, changed)
			}
			t.walkAndTransform(n.DeleteStmt.WhereClause, changed)
		}

	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			t.walkAndTransform(n.JoinExpr.Larg, changed)
			t.walkAndTransform(n.JoinExpr.Rarg, changed)
			t.walkAndTransform(n.JoinExpr.Quals, changed)
		}

	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			t.walkAndTransform(n.SubLink.Subselect, changed)
		}

	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			t.walkAndTransform(n.RangeSubselect.Subquery, changed)
		}

	case *pg_query.Node_CommonTableExpr:
		if n.CommonTableExpr != nil {
			t.walkAndTransform(n.CommonTableExpr.Ctequery, changed)
		}

	case *pg_query.Node_WithClause:
		if n.WithClause != nil {
			for _, cte := range n.WithClause.Ctes {
				t.walkAndTransform(cte, changed)
			}
		}
	}
}

func (t *TempTableTransform) walkSelectStmt(stmt *pg_query.SelectStmt, changed *bool) {
	if stmt == nil {
		return
	}

	for _, from := range stmt.FromClause {
		t.walkAndTransform(from, changed)
	}
	t.walkAndTransform(stmt.WhereClause, changed)

	if stmt.WithClause != nil {
		t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_WithClause{WithClause: stmt.WithClause}}, changed)
	}

	// Handle UNION/INTERSECT/EXCEPT
	if stmt.Larg != nil {
		t.walkSelectStmt(stmt.Larg, changed)
	}
	if stmt.Rarg != nil {
		t.walkSelectStmt(stmt.Rarg, changed)
	}
}

// isStagingTable checks if a table name matches the Fivetran staging table pattern.
// Fivetran creates temporary staging tables with names like:
// "schema_table-staging-uuid" (e.g., "stripe_test_upcomi-staging-62de57c1-39ce-45c4-93f3-34e0a6dd72f4")
func (t *TempTableTransform) isStagingTable(name string) bool {
	return strings.Contains(name, "-staging-")
}
