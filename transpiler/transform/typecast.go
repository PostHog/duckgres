package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// TypeCastTransform converts PostgreSQL-specific type casts to DuckDB equivalents.
// For example: ::pg_catalog.regtype -> ::VARCHAR
type TypeCastTransform struct {
	// TypeMappings maps pg_catalog types to DuckDB types
	TypeMappings map[string]string
}

// NewTypeCastTransform creates a new TypeCastTransform with default mappings.
func NewTypeCastTransform() *TypeCastTransform {
	return &TypeCastTransform{
		TypeMappings: map[string]string{
			"regtype":      "varchar",
			"regclass":     "varchar",
			"regnamespace": "varchar",
			"regproc":      "varchar",
			"regoper":      "varchar",
			"regoperator":  "varchar",
			"regprocedure": "varchar",
			"regconfig":    "varchar",
			"regdictionary": "varchar",
			"text":         "varchar",
		},
	}
}

func (t *TypeCastTransform) Name() string {
	return "typecast"
}

func (t *TypeCastTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		t.walkAndTransform(stmt.Stmt, &changed)
	}

	return changed, nil
}

func (t *TypeCastTransform) walkAndTransform(node *pg_query.Node, changed *bool) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil && n.TypeCast.TypeName != nil {
			// Check if this is a pg_catalog type cast
			typeName := n.TypeCast.TypeName
			if len(typeName.Names) >= 2 {
				// Check for pg_catalog.typename pattern
				if first := typeName.Names[0]; first != nil {
					if str := first.GetString_(); str != nil && strings.EqualFold(str.Sval, "pg_catalog") {
						if second := typeName.Names[1]; second != nil {
							if typeStr := second.GetString_(); typeStr != nil {
								typeLower := strings.ToLower(typeStr.Sval)
								if newType, ok := t.TypeMappings[typeLower]; ok {
									// Replace with just the DuckDB type name
									typeName.Names = []*pg_query.Node{
										{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: newType}}},
									}
									*changed = true
								}
							}
						}
					}
				}
			} else if len(typeName.Names) == 1 {
				// Single name, check if it needs mapping
				if first := typeName.Names[0]; first != nil {
					if str := first.GetString_(); str != nil {
						typeLower := strings.ToLower(str.Sval)
						if newType, ok := t.TypeMappings[typeLower]; ok {
							str.Sval = newType
							*changed = true
						}
					}
				}
			}
			// Recurse into the argument
			t.walkAndTransform(n.TypeCast.Arg, changed)
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			t.walkSelectStmt(n.SelectStmt, changed)
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

	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			t.walkAndTransform(n.SubLink.Subselect, changed)
			t.walkAndTransform(n.SubLink.Testexpr, changed)
		}

	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			t.walkAndTransform(n.ResTarget.Val, changed)
		}

	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			t.walkAndTransform(n.JoinExpr.Larg, changed)
			t.walkAndTransform(n.JoinExpr.Rarg, changed)
			t.walkAndTransform(n.JoinExpr.Quals, changed)
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

	case *pg_query.Node_List:
		if n.List != nil {
			for _, item := range n.List.Items {
				t.walkAndTransform(item, changed)
			}
		}

	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			for _, col := range n.InsertStmt.Cols {
				t.walkAndTransform(col, changed)
			}
			t.walkAndTransform(n.InsertStmt.SelectStmt, changed)
		}

	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
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
			t.walkAndTransform(n.DeleteStmt.WhereClause, changed)
		}
	}
}

func (t *TypeCastTransform) walkSelectStmt(stmt *pg_query.SelectStmt, changed *bool) {
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

	if stmt.Larg != nil {
		t.walkSelectStmt(stmt.Larg, changed)
	}
	if stmt.Rarg != nil {
		t.walkSelectStmt(stmt.Rarg, changed)
	}
}
