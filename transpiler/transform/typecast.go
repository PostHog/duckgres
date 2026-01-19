package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// TypeCastTransform converts PostgreSQL-specific type casts to DuckDB equivalents.
// For example: ::pg_catalog.regtype -> ::VARCHAR
// Special case: 'tablename'::regclass -> (SELECT oid FROM pg_class WHERE relname = 'tablename')
type TypeCastTransform struct {
	// TypeMappings maps pg_catalog types to DuckDB types
	TypeMappings map[string]string
}

// NewTypeCastTransform creates a new TypeCastTransform with default mappings.
func NewTypeCastTransform() *TypeCastTransform {
	return &TypeCastTransform{
		TypeMappings: map[string]string{
			"regtype":       "varchar",
			"regnamespace":  "varchar",
			"regproc":       "varchar",
			"regoper":       "varchar",
			"regoperator":   "varchar",
			"regprocedure":  "varchar",
			"regconfig":     "varchar",
			"regdictionary": "varchar",
			"text":          "varchar",
			// Note: regclass is handled specially - converted to subquery lookup
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
			typeLower := ""

			if len(typeName.Names) >= 2 {
				// Check for pg_catalog.typename pattern
				if first := typeName.Names[0]; first != nil {
					if str := first.GetString_(); str != nil && strings.EqualFold(str.Sval, "pg_catalog") {
						if second := typeName.Names[1]; second != nil {
							if typeStr := second.GetString_(); typeStr != nil {
								typeLower = strings.ToLower(typeStr.Sval)
							}
						}
					}
				}
			} else if len(typeName.Names) == 1 {
				// Single name
				if first := typeName.Names[0]; first != nil {
					if str := first.GetString_(); str != nil {
						typeLower = strings.ToLower(str.Sval)
					}
				}
			}

			// Special handling for ::regclass casts
			// String literal: 'tablename'::regclass -> (SELECT oid FROM pg_class WHERE relname = 'tablename')
			// OID/integer: oid_val::regclass -> (SELECT relname FROM pg_class WHERE oid = oid_val)
			if typeLower == "regclass" {
				// First check if argument is a string literal (table name lookup -> OID)
				if sublink := t.createRegclassFromNameSubquery(n.TypeCast.Arg); sublink != nil {
					node.Node = &pg_query.Node_SubLink{SubLink: sublink}
					*changed = true
					return
				}
				// Check if argument is an integer/OID (OID lookup -> table name, for ::regclass::text pattern)
				if sublink := t.createRegclassFromOidSubquery(n.TypeCast.Arg); sublink != nil {
					node.Node = &pg_query.Node_SubLink{SubLink: sublink}
					*changed = true
					return
				}
				// For other expressions, convert to varchar as fallback
				typeLower = "regclass_fallback"
			}

			// Standard type mapping for other types
			if typeLower == "regclass_fallback" {
				// Fallback for regclass with non-string arguments
				if len(typeName.Names) >= 2 {
					typeName.Names = []*pg_query.Node{
						{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "varchar"}}},
					}
				} else if len(typeName.Names) == 1 {
					if first := typeName.Names[0]; first != nil {
						if str := first.GetString_(); str != nil {
							str.Sval = "varchar"
						}
					}
				}
				*changed = true
			} else if newType, ok := t.TypeMappings[typeLower]; ok {
				if len(typeName.Names) >= 2 {
					// Replace pg_catalog.typename with just the new type
					typeName.Names = []*pg_query.Node{
						{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: newType}}},
					}
				} else if len(typeName.Names) == 1 {
					if first := typeName.Names[0]; first != nil {
						if str := first.GetString_(); str != nil {
							str.Sval = newType
						}
					}
				}
				*changed = true
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

// createRegclassFromNameSubquery creates a SubLink for 'tablename'::regclass
// This converts: 'users'::regclass
// To: (SELECT oid FROM pg_class_full WHERE relname = 'users')
// In DuckLake mode, pg_class_full sources from DuckLake metadata for consistent PostgreSQL OIDs.
func (t *TypeCastTransform) createRegclassFromNameSubquery(arg *pg_query.Node) *pg_query.SubLink {
	if arg == nil {
		return nil
	}

	// Get the string value from the argument
	var tableName string
	if aConst := arg.GetAConst(); aConst != nil {
		if sval := aConst.GetSval(); sval != nil {
			tableName = sval.Sval
		}
	}

	if tableName == "" {
		return nil
	}

	// Build: SELECT oid FROM pg_class WHERE relname = 'tablename' LIMIT 1
	// Create the SELECT target: oid
	targetList := []*pg_query.Node{
		{
			Node: &pg_query.Node_ResTarget{
				ResTarget: &pg_query.ResTarget{
					Val: &pg_query.Node{
						Node: &pg_query.Node_ColumnRef{
							ColumnRef: &pg_query.ColumnRef{
								Fields: []*pg_query.Node{
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "oid"}}},
								},
							},
						},
					},
				},
			},
		},
	}

	// Create the FROM clause: memory.main.pg_class_full
	// We reference pg_class_full directly since TypeCastTransform runs after PgCatalogTransform.
	// In DuckLake mode, pg_class_full sources from DuckLake metadata for consistent PostgreSQL OIDs.
	fromClause := []*pg_query.Node{
		{
			Node: &pg_query.Node_RangeVar{
				RangeVar: &pg_query.RangeVar{
					Catalogname:    "memory",
					Schemaname:     "main",
					Relname:        "pg_class_full",
					Inh:            true,
					Relpersistence: "p",
				},
			},
		},
	}

	// Create the WHERE clause: relname = 'tablename'
	whereClause := &pg_query.Node{
		Node: &pg_query.Node_AExpr{
			AExpr: &pg_query.A_Expr{
				Kind: pg_query.A_Expr_Kind_AEXPR_OP,
				Name: []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "="}}},
				},
				Lexpr: &pg_query.Node{
					Node: &pg_query.Node_ColumnRef{
						ColumnRef: &pg_query.ColumnRef{
							Fields: []*pg_query.Node{
								{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "relname"}}},
							},
						},
					},
				},
				Rexpr: &pg_query.Node{
					Node: &pg_query.Node_AConst{
						AConst: &pg_query.A_Const{
							Val: &pg_query.A_Const_Sval{
								Sval: &pg_query.String{Sval: tableName},
							},
						},
					},
				},
			},
		},
	}

	// Build the SelectStmt
	// Note: If multiple tables with the same name exist in different schemas,
	// the subquery may return multiple rows and fail. This matches PostgreSQL's
	// behavior where ::regclass uses search_path to resolve ambiguity.
	selectStmt := &pg_query.SelectStmt{
		TargetList:  targetList,
		FromClause:  fromClause,
		WhereClause: whereClause,
	}

	// Create the SubLink (scalar subquery)
	return &pg_query.SubLink{
		SubLinkType: pg_query.SubLinkType_EXPR_SUBLINK,
		Subselect: &pg_query.Node{
			Node: &pg_query.Node_SelectStmt{
				SelectStmt: selectStmt,
			},
		},
	}
}

// createRegclassFromOidSubquery creates a SubLink for oid::regclass
// This converts: 12345::regclass
// To: (SELECT relname FROM pg_class_full WHERE oid = 12345)
// This is used when regclass is then cast to text (::regclass::text pattern)
func (t *TypeCastTransform) createRegclassFromOidSubquery(arg *pg_query.Node) *pg_query.SubLink {
	if arg == nil {
		return nil
	}

	// Check if argument is an integer constant or a column reference (likely an OID)
	var isOidLike bool
	var argCopy *pg_query.Node

	if aConst := arg.GetAConst(); aConst != nil {
		if aConst.GetIval() != nil {
			isOidLike = true
			argCopy = arg
		}
	} else if colRef := arg.GetColumnRef(); colRef != nil {
		// Only accept QUALIFIED column references (e.g., a.attrelid with 2+ fields)
		// Unqualified refs like 'oid' would be shadowed by pg_class_full.oid in the subquery
		if len(colRef.Fields) >= 2 {
			isOidLike = true
			argCopy = arg
		}
	} else if typeCast := arg.GetTypeCast(); typeCast != nil {
		// Nested type cast - could be something like foo::oid::regclass
		isOidLike = true
		argCopy = arg
	}

	if !isOidLike {
		return nil
	}

	// Build: SELECT relname FROM pg_class_full WHERE oid = arg
	// Create the SELECT target: relname
	targetList := []*pg_query.Node{
		{
			Node: &pg_query.Node_ResTarget{
				ResTarget: &pg_query.ResTarget{
					Val: &pg_query.Node{
						Node: &pg_query.Node_ColumnRef{
							ColumnRef: &pg_query.ColumnRef{
								Fields: []*pg_query.Node{
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "relname"}}},
								},
							},
						},
					},
				},
			},
		},
	}

	// Create the FROM clause: memory.main.pg_class_full
	fromClause := []*pg_query.Node{
		{
			Node: &pg_query.Node_RangeVar{
				RangeVar: &pg_query.RangeVar{
					Catalogname:    "memory",
					Schemaname:     "main",
					Relname:        "pg_class_full",
					Inh:            true,
					Relpersistence: "p",
				},
			},
		},
	}

	// Create the WHERE clause: oid = arg
	whereClause := &pg_query.Node{
		Node: &pg_query.Node_AExpr{
			AExpr: &pg_query.A_Expr{
				Kind: pg_query.A_Expr_Kind_AEXPR_OP,
				Name: []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "="}}},
				},
				Lexpr: &pg_query.Node{
					Node: &pg_query.Node_ColumnRef{
						ColumnRef: &pg_query.ColumnRef{
							Fields: []*pg_query.Node{
								{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "oid"}}},
							},
						},
					},
				},
				Rexpr: argCopy,
			},
		},
	}

	selectStmt := &pg_query.SelectStmt{
		TargetList:  targetList,
		FromClause:  fromClause,
		WhereClause: whereClause,
	}

	return &pg_query.SubLink{
		SubLinkType: pg_query.SubLinkType_EXPR_SUBLINK,
		Subselect: &pg_query.Node{
			Node: &pg_query.Node_SelectStmt{
				SelectStmt: selectStmt,
			},
		},
	}
}
