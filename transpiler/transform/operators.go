package transform

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// OperatorTransform converts PostgreSQL operators to DuckDB equivalents.
// Handles JSON operators, regex operators, and other PostgreSQL-specific operators.
type OperatorTransform struct{}

func NewOperatorTransform() *OperatorTransform {
	return &OperatorTransform{}
}

func (t *OperatorTransform) Name() string {
	return "operators"
}

func (t *OperatorTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	// Need to walk and potentially replace nodes, so we use a custom walker
	// that can modify parent nodes when replacing A_Expr with FuncCall
	for _, stmt := range tree.Stmts {
		if stmt.Stmt != nil {
			if t.walkAndTransform(stmt.Stmt, &changed) {
				changed = true
			}
		}
	}

	return changed, nil
}

// walkAndTransform recursively walks the AST and transforms regex operators.
// It needs to replace A_Expr nodes with FuncCall nodes, which requires access
// to the parent node, so we handle this by modifying the Node's inner type.
func (t *OperatorTransform) walkAndTransform(node *pg_query.Node, changed *bool) bool {
	if node == nil {
		return false
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			// First recursively process child nodes
			t.walkAndTransform(n.AExpr.Lexpr, changed)
			t.walkAndTransform(n.AExpr.Rexpr, changed)

			// Check if this is a regex operator that needs conversion
			if result := t.convertRegexToFunc(n.AExpr); result != nil {
				// Replace the A_Expr node with the converted node
				switch r := result.(type) {
				case *pg_query.Node_FuncCall:
					node.Node = r
				case *pg_query.Node_BoolExpr:
					node.Node = r
				case *pg_query.Node_AExpr:
					node.Node = r
				}
				*changed = true
				return true
			}
		}

	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			// Process target list
			for _, target := range n.SelectStmt.TargetList {
				t.walkAndTransform(target, changed)
			}
			// Process FROM clause
			for _, from := range n.SelectStmt.FromClause {
				t.walkAndTransform(from, changed)
			}
			// Process WHERE clause
			t.walkAndTransform(n.SelectStmt.WhereClause, changed)
			// Process HAVING clause
			t.walkAndTransform(n.SelectStmt.HavingClause, changed)
			// Process GROUP BY
			for _, group := range n.SelectStmt.GroupClause {
				t.walkAndTransform(group, changed)
			}
			// Process ORDER BY
			for _, sort := range n.SelectStmt.SortClause {
				t.walkAndTransform(sort, changed)
			}
			// Process CTEs
			if n.SelectStmt.WithClause != nil {
				for _, cte := range n.SelectStmt.WithClause.Ctes {
					t.walkAndTransform(cte, changed)
				}
			}
			// Process set operations (UNION, etc.)
			// Larg and Rarg are *pg_query.SelectStmt, need to wrap in Node
			if n.SelectStmt.Larg != nil {
				t.walkAndTransform(&pg_query.Node{
					Node: &pg_query.Node_SelectStmt{SelectStmt: n.SelectStmt.Larg},
				}, changed)
			}
			if n.SelectStmt.Rarg != nil {
				t.walkAndTransform(&pg_query.Node{
					Node: &pg_query.Node_SelectStmt{SelectStmt: n.SelectStmt.Rarg},
				}, changed)
			}
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

	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			t.walkAndTransform(n.SubLink.Subselect, changed)
			t.walkAndTransform(n.SubLink.Testexpr, changed)
		}

	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			for _, arg := range n.FuncCall.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, arg := range n.CoalesceExpr.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			t.walkAndTransform(n.CaseExpr.Arg, changed)
			t.walkAndTransform(n.CaseExpr.Defresult, changed)
			for _, when := range n.CaseExpr.Args {
				t.walkAndTransform(when, changed)
			}
		}

	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			t.walkAndTransform(n.CaseWhen.Expr, changed)
			t.walkAndTransform(n.CaseWhen.Result, changed)
		}

	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			t.walkAndTransform(n.TypeCast.Arg, changed)
		}

	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			t.walkAndTransform(n.NullTest.Arg, changed)
		}

	case *pg_query.Node_CommonTableExpr:
		if n.CommonTableExpr != nil {
			t.walkAndTransform(n.CommonTableExpr.Ctequery, changed)
		}

	case *pg_query.Node_SortBy:
		if n.SortBy != nil {
			t.walkAndTransform(n.SortBy.Node, changed)
		}

	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			t.walkAndTransform(n.RangeSubselect.Subquery, changed)
		}
	}

	return false
}

// convertRegexToFunc checks if an A_Expr is a regex operator and converts it
// to the appropriate node. Returns nil if not a regex operator.
// For positive matches (~, ~*), returns a FuncCall.
// For negative matches (!~, !~*), returns a BoolExpr with NOT.
func (t *OperatorTransform) convertRegexToFunc(aexpr *pg_query.A_Expr) interface{} {
	if aexpr == nil || len(aexpr.Name) == 0 {
		return nil
	}

	// Get operator name
	var opName string
	for _, name := range aexpr.Name {
		if str := name.GetString_(); str != nil {
			opName = str.Sval
			break
		}
	}

	if opName == "" {
		return nil
	}

	// Check if this is a regex operator
	var negate bool
	var caseInsensitive bool

	switch opName {
	case "~":
		// col ~ pattern -> regexp_matches(col, pattern)
		negate = false
		caseInsensitive = false
	case "~*":
		// col ~* pattern -> regexp_matches(col, pattern, 'i')
		negate = false
		caseInsensitive = true
	case "!~":
		// col !~ pattern -> NOT regexp_matches(col, pattern)
		negate = true
		caseInsensitive = false
	case "!~*":
		// col !~* pattern -> NOT regexp_matches(col, pattern, 'i')
		negate = true
		caseInsensitive = true
	default:
		return nil
	}

	// Build: length(regexp_extract(col, pattern)) > 0 for match
	// Build: length(regexp_extract(col, pattern)) = 0 for no match
	// For case-insensitive, we prepend (?i) to the pattern

	// For case-insensitive matching, we need to modify the pattern
	// by prepending (?i). This requires wrapping the pattern in a concat.
	patternArg := aexpr.Rexpr
	if caseInsensitive {
		// Build: '(?i)' || pattern
		patternArg = &pg_query.Node{
			Node: &pg_query.Node_AExpr{
				AExpr: &pg_query.A_Expr{
					Kind: pg_query.A_Expr_Kind_AEXPR_OP,
					Name: []*pg_query.Node{
						{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "||"}}},
					},
					Lexpr: &pg_query.Node{
						Node: &pg_query.Node_AConst{
							AConst: &pg_query.A_Const{
								Val: &pg_query.A_Const_Sval{
									Sval: &pg_query.String{Sval: "(?i)"},
								},
							},
						},
					},
					Rexpr:    aexpr.Rexpr,
					Location: aexpr.Location,
				},
			},
		}
	}

	// Build: regexp_extract(col, pattern)
	regexpExtract := &pg_query.FuncCall{
		Funcname: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "regexp_extract"}}},
		},
		Args:     []*pg_query.Node{aexpr.Lexpr, patternArg},
		Location: aexpr.Location,
	}

	// Build: length(regexp_extract(...))
	lengthCall := &pg_query.FuncCall{
		Funcname: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "length"}}},
		},
		Args: []*pg_query.Node{
			{Node: &pg_query.Node_FuncCall{FuncCall: regexpExtract}},
		},
		Location: aexpr.Location,
	}

	// Build comparison: length(...) > 0 or length(...) = 0
	compareOp := ">"
	if negate {
		compareOp = "="
	}

	comparison := &pg_query.A_Expr{
		Kind: pg_query.A_Expr_Kind_AEXPR_OP,
		Name: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: compareOp}}},
		},
		Lexpr: &pg_query.Node{
			Node: &pg_query.Node_FuncCall{FuncCall: lengthCall},
		},
		Rexpr: &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{
						Ival: &pg_query.Integer{Ival: 0},
					},
				},
			},
		},
		Location: aexpr.Location,
	}

	return &pg_query.Node_AExpr{AExpr: comparison}
}

// OperatorMappingNote documents the operator mappings for reference:
//
// JSON Operators (PostgreSQL -> DuckDB):
//   -> : Same (extract JSON object field)
//   ->> : Same (extract JSON object field as text)
//   #> : Not directly supported (use json_extract with path)
//   #>> : Not directly supported (use json_extract_string with path)
//   @> : json_contains() or manual check
//   <@ : Reverse of @>
//   ? : json_exists()
//   ?| : Manual check with OR
//   ?& : Manual check with AND
//
// Regex Operators (PostgreSQL -> DuckDB):
//   ~ : regexp_matches(text, pattern)
//   ~* : regexp_matches(text, pattern, 'i')
//   !~ : NOT regexp_matches(text, pattern)
//   !~* : NOT regexp_matches(text, pattern, 'i')
//
// Array Operators:
//   && : list_has_any()
//   @> : list_has_all()
//   <@ : Reverse containment
//   || : list_concat() for arrays, || for strings
