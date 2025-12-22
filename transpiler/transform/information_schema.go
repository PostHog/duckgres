package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// InformationSchemaTransform redirects information_schema table references to our
// compatibility views that provide PostgreSQL-compatible type information.
type InformationSchemaTransform struct {
	// ViewMappings maps information_schema table names to our compatibility views
	ViewMappings map[string]string

	// DuckLakeMode indicates whether we're running with DuckLake attached
	DuckLakeMode bool
}

// NewInformationSchemaTransform creates a new InformationSchemaTransform with default mappings.
func NewInformationSchemaTransform() *InformationSchemaTransform {
	return NewInformationSchemaTransformWithConfig(false)
}

// NewInformationSchemaTransformWithConfig creates a new InformationSchemaTransform with configuration.
func NewInformationSchemaTransformWithConfig(duckLakeMode bool) *InformationSchemaTransform {
	return &InformationSchemaTransform{
		DuckLakeMode: duckLakeMode,
		ViewMappings: map[string]string{
			"columns":  "information_schema_columns_compat",
			"tables":   "information_schema_tables_compat",
			"schemata": "information_schema_schemata_compat",
			"views":    "information_schema_views_compat",
		},
	}
}

func (t *InformationSchemaTransform) Name() string {
	return "information_schema"
}

func (t *InformationSchemaTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		// Walk the entire statement looking for nodes to transform
		t.walkAndTransform(stmt.Stmt, &changed)
	}

	return changed, nil
}

func (t *InformationSchemaTransform) walkAndTransform(node *pg_query.Node, changed *bool) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		// Table references: information_schema.columns -> memory.main.information_schema_columns_compat
		// Views are created in the memory.main schema so they're always accessible regardless of default catalog
		// Skip transformation if catalog is explicitly "ducklake" - those are queries from
		// within the compat views that need to access the actual DuckLake information_schema
		if n.RangeVar != nil && strings.EqualFold(n.RangeVar.Schemaname, "information_schema") &&
			!strings.EqualFold(n.RangeVar.Catalogname, "ducklake") {
			relname := strings.ToLower(n.RangeVar.Relname)
			if newName, ok := t.ViewMappings[relname]; ok {
				n.RangeVar.Relname = newName
				// Views are in memory.main schema - always use explicit catalog for DuckLake compatibility
				n.RangeVar.Catalogname = "memory"
				n.RangeVar.Schemaname = "main"
				*changed = true
			}
			// If no mapping, leave as-is (DuckDB will handle it)
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			t.walkSelectStmt(n.SelectStmt, changed)
		}

	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			if n.InsertStmt.Relation != nil {
				t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: n.InsertStmt.Relation}}, changed)
			}
			for _, col := range n.InsertStmt.Cols {
				t.walkAndTransform(col, changed)
			}
			t.walkAndTransform(n.InsertStmt.SelectStmt, changed)
		}

	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			if n.UpdateStmt.Relation != nil {
				t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: n.UpdateStmt.Relation}}, changed)
			}
			for _, target := range n.UpdateStmt.TargetList {
				t.walkAndTransform(target, changed)
			}
			t.walkAndTransform(n.UpdateStmt.WhereClause, changed)
			for _, from := range n.UpdateStmt.FromClause {
				t.walkAndTransform(from, changed)
			}
		}

	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			if n.DeleteStmt.Relation != nil {
				t.walkAndTransform(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: n.DeleteStmt.Relation}}, changed)
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
			t.walkAndTransform(n.SubLink.Testexpr, changed)
		}

	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			t.walkAndTransform(n.ResTarget.Val, changed)
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

	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			t.walkAndTransform(n.RangeSubselect.Subquery, changed)
		}

	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			t.walkAndTransform(n.TypeCast.Arg, changed)
		}

	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			for _, arg := range n.FuncCall.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			t.walkAndTransform(n.AExpr.Lexpr, changed)
			t.walkAndTransform(n.AExpr.Rexpr, changed)
		}

	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			t.walkAndTransform(n.CaseExpr.Arg, changed)
			for _, when := range n.CaseExpr.Args {
				t.walkAndTransform(when, changed)
			}
			t.walkAndTransform(n.CaseExpr.Defresult, changed)
		}

	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			t.walkAndTransform(n.CaseWhen.Expr, changed)
			t.walkAndTransform(n.CaseWhen.Result, changed)
		}

	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, arg := range n.CoalesceExpr.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			t.walkAndTransform(n.NullTest.Arg, changed)
		}

	case *pg_query.Node_List:
		if n.List != nil {
			for _, item := range n.List.Items {
				t.walkAndTransform(item, changed)
			}
		}
	}
}

func (t *InformationSchemaTransform) walkSelectStmt(stmt *pg_query.SelectStmt, changed *bool) {
	if stmt == nil {
		return
	}

	for _, target := range stmt.TargetList {
		t.walkAndTransform(target, changed)
	}
	for _, from := range stmt.FromClause {
		t.walkAndTransform(from, changed)
	}
	t.walkAndTransform(stmt.WhereClause, changed)
	for _, group := range stmt.GroupClause {
		t.walkAndTransform(group, changed)
	}
	t.walkAndTransform(stmt.HavingClause, changed)
	for _, sort := range stmt.SortClause {
		t.walkAndTransform(sort, changed)
	}
	t.walkAndTransform(stmt.LimitCount, changed)
	t.walkAndTransform(stmt.LimitOffset, changed)

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
