package transform

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// PlaceholderTransform converts PostgreSQL $1, $2 placeholders to ? for database/sql.
// This is needed for the extended query protocol (prepared statements).
type PlaceholderTransform struct{}

// NewPlaceholderTransform creates a new PlaceholderTransform.
func NewPlaceholderTransform() *PlaceholderTransform {
	return &PlaceholderTransform{}
}

func (t *PlaceholderTransform) Name() string {
	return "placeholder"
}

func (t *PlaceholderTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false
	paramCount := 0

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		t.walkAndCount(stmt.Stmt, &paramCount, &changed)
	}

	result.ParamCount = paramCount
	return changed, nil
}

func (t *PlaceholderTransform) walkAndCount(node *pg_query.Node, paramCount *int, changed *bool) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_ParamRef:
		// Count parameters
		if n.ParamRef != nil {
			num := int(n.ParamRef.Number)
			if num > *paramCount {
				*paramCount = num
			}
			// Note: We don't need to change the AST here because
			// DuckDB's driver accepts both $N and ? syntax.
			// The paramCount is what we need for the extended protocol.
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			t.walkSelectStmt(n.SelectStmt, paramCount, changed)
		}

	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			for _, col := range n.InsertStmt.Cols {
				t.walkAndCount(col, paramCount, changed)
			}
			t.walkAndCount(n.InsertStmt.SelectStmt, paramCount, changed)
			for _, target := range n.InsertStmt.ReturningList {
				t.walkAndCount(target, paramCount, changed)
			}
		}

	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			for _, target := range n.UpdateStmt.TargetList {
				t.walkAndCount(target, paramCount, changed)
			}
			t.walkAndCount(n.UpdateStmt.WhereClause, paramCount, changed)
			for _, from := range n.UpdateStmt.FromClause {
				t.walkAndCount(from, paramCount, changed)
			}
			for _, target := range n.UpdateStmt.ReturningList {
				t.walkAndCount(target, paramCount, changed)
			}
		}

	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			t.walkAndCount(n.DeleteStmt.WhereClause, paramCount, changed)
			for _, target := range n.DeleteStmt.ReturningList {
				t.walkAndCount(target, paramCount, changed)
			}
		}

	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			for _, arg := range n.FuncCall.Args {
				t.walkAndCount(arg, paramCount, changed)
			}
		}

	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			t.walkAndCount(n.TypeCast.Arg, paramCount, changed)
		}

	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			t.walkAndCount(n.AExpr.Lexpr, paramCount, changed)
			t.walkAndCount(n.AExpr.Rexpr, paramCount, changed)
		}

	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.walkAndCount(arg, paramCount, changed)
			}
		}

	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			t.walkAndCount(n.SubLink.Subselect, paramCount, changed)
			t.walkAndCount(n.SubLink.Testexpr, paramCount, changed)
		}

	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			t.walkAndCount(n.ResTarget.Val, paramCount, changed)
		}

	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			t.walkAndCount(n.JoinExpr.Larg, paramCount, changed)
			t.walkAndCount(n.JoinExpr.Rarg, paramCount, changed)
			t.walkAndCount(n.JoinExpr.Quals, paramCount, changed)
		}

	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			t.walkAndCount(n.CaseExpr.Arg, paramCount, changed)
			for _, when := range n.CaseExpr.Args {
				t.walkAndCount(when, paramCount, changed)
			}
			t.walkAndCount(n.CaseExpr.Defresult, paramCount, changed)
		}

	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			t.walkAndCount(n.CaseWhen.Expr, paramCount, changed)
			t.walkAndCount(n.CaseWhen.Result, paramCount, changed)
		}

	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, arg := range n.CoalesceExpr.Args {
				t.walkAndCount(arg, paramCount, changed)
			}
		}

	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			t.walkAndCount(n.NullTest.Arg, paramCount, changed)
		}

	case *pg_query.Node_CommonTableExpr:
		if n.CommonTableExpr != nil {
			t.walkAndCount(n.CommonTableExpr.Ctequery, paramCount, changed)
		}

	case *pg_query.Node_WithClause:
		if n.WithClause != nil {
			for _, cte := range n.WithClause.Ctes {
				t.walkAndCount(cte, paramCount, changed)
			}
		}

	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			t.walkAndCount(n.RangeSubselect.Subquery, paramCount, changed)
		}

	case *pg_query.Node_List:
		if n.List != nil {
			for _, item := range n.List.Items {
				t.walkAndCount(item, paramCount, changed)
			}
		}

	case *pg_query.Node_ArrayExpr:
		if n.ArrayExpr != nil {
			for _, elem := range n.ArrayExpr.Elements {
				t.walkAndCount(elem, paramCount, changed)
			}
		}

	case *pg_query.Node_RowExpr:
		if n.RowExpr != nil {
			for _, arg := range n.RowExpr.Args {
				t.walkAndCount(arg, paramCount, changed)
			}
		}
	}
}

func (t *PlaceholderTransform) walkSelectStmt(stmt *pg_query.SelectStmt, paramCount *int, changed *bool) {
	if stmt == nil {
		return
	}

	for _, target := range stmt.TargetList {
		t.walkAndCount(target, paramCount, changed)
	}
	for _, from := range stmt.FromClause {
		t.walkAndCount(from, paramCount, changed)
	}
	t.walkAndCount(stmt.WhereClause, paramCount, changed)
	for _, group := range stmt.GroupClause {
		t.walkAndCount(group, paramCount, changed)
	}
	t.walkAndCount(stmt.HavingClause, paramCount, changed)
	for _, sort := range stmt.SortClause {
		if sortBy := sort.GetSortBy(); sortBy != nil {
			t.walkAndCount(sortBy.Node, paramCount, changed)
		}
	}
	t.walkAndCount(stmt.LimitCount, paramCount, changed)
	t.walkAndCount(stmt.LimitOffset, paramCount, changed)

	// Handle VALUES lists (used in INSERT ... VALUES statements)
	for _, valuesList := range stmt.ValuesLists {
		t.walkAndCount(valuesList, paramCount, changed)
	}

	if stmt.WithClause != nil {
		t.walkAndCount(&pg_query.Node{Node: &pg_query.Node_WithClause{WithClause: stmt.WithClause}}, paramCount, changed)
	}

	if stmt.Larg != nil {
		t.walkSelectStmt(stmt.Larg, paramCount, changed)
	}
	if stmt.Rarg != nil {
		t.walkSelectStmt(stmt.Rarg, paramCount, changed)
	}
}
