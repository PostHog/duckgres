package transform

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ExpandArrayTransform handles PostgreSQL's _pg_expandarray function.
// This function expands an array into rows with (x, n) where x is the element
// and n is the 1-based index. It's commonly used by JDBC drivers for primary key discovery.
//
// The transform rewrites:
//   (information_schema._pg_expandarray(arr)).n AS KEY_SEQ
// To:
//   _pg_exp_N.n AS KEY_SEQ
// And adds a LATERAL join:
//   LATERAL (SELECT unnest(arr) AS x, generate_subscripts(arr, 1) AS n) AS _pg_exp_N
type ExpandArrayTransform struct {
	lateralCounter int
}

func NewExpandArrayTransform() *ExpandArrayTransform {
	return &ExpandArrayTransform{}
}

func (t *ExpandArrayTransform) Name() string {
	return "expandarray"
}

func (t *ExpandArrayTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false
	t.lateralCounter = 0

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		if selectStmt := stmt.Stmt.GetSelectStmt(); selectStmt != nil {
			if t.transformSelectStmt(selectStmt) {
				changed = true
			}
		}
	}

	return changed, nil
}

func (t *ExpandArrayTransform) transformSelectStmt(stmt *pg_query.SelectStmt) bool {
	if stmt == nil {
		return false
	}

	changed := false

	// Track _pg_expandarray calls and their lateral aliases
	// Key: deparsed array expression, Value: lateral alias name
	arrayExprToAlias := make(map[string]string)

	// First pass: find all _pg_expandarray calls and assign lateral aliases
	for _, target := range stmt.TargetList {
		resTarget := target.GetResTarget()
		if resTarget == nil || resTarget.Val == nil {
			continue
		}
		t.collectExpandArrayCalls(resTarget.Val, arrayExprToAlias)
	}

	// Also check WHERE clause
	if stmt.WhereClause != nil {
		t.collectExpandArrayCalls(stmt.WhereClause, arrayExprToAlias)
	}

	if len(arrayExprToAlias) == 0 {
		// No _pg_expandarray calls found, but still recurse into subqueries
		for _, from := range stmt.FromClause {
			if rangeSubselect := from.GetRangeSubselect(); rangeSubselect != nil {
				if subSelect := rangeSubselect.Subquery.GetSelectStmt(); subSelect != nil {
					if t.transformSelectStmt(subSelect) {
						changed = true
					}
				}
			}
		}

		// Recurse into CTEs
		if stmt.WithClause != nil {
			for _, cte := range stmt.WithClause.Ctes {
				if cteExpr := cte.GetCommonTableExpr(); cteExpr != nil {
					if cteSelect := cteExpr.Ctequery.GetSelectStmt(); cteSelect != nil {
						if t.transformSelectStmt(cteSelect) {
							changed = true
						}
					}
				}
			}
		}

		return changed
	}

	// Second pass: replace _pg_expandarray calls with column references and add LATERAL joins
	for _, target := range stmt.TargetList {
		resTarget := target.GetResTarget()
		if resTarget == nil || resTarget.Val == nil {
			continue
		}
		if t.replaceExpandArrayCalls(resTarget, arrayExprToAlias) {
			changed = true
		}
	}

	// Also replace in WHERE clause
	if stmt.WhereClause != nil {
		newWhere := t.replaceInExpression(stmt.WhereClause, arrayExprToAlias)
		if newWhere != nil {
			stmt.WhereClause = newWhere
			changed = true
		}
	}

	// Add LATERAL joins for each unique array expression
	for arrayExpr, alias := range arrayExprToAlias {
		_ = arrayExpr // Used for deduplication
		// Re-parse the array expression to get a fresh node
		// This is a bit hacky but works for the common cases
		lateralJoin := t.createLateralJoin(alias, arrayExpr)
		if lateralJoin != nil {
			stmt.FromClause = append(stmt.FromClause, lateralJoin)
			changed = true
		}
	}

	// Recurse into subqueries in FROM clause
	for _, from := range stmt.FromClause {
		if rangeSubselect := from.GetRangeSubselect(); rangeSubselect != nil {
			if subSelect := rangeSubselect.Subquery.GetSelectStmt(); subSelect != nil {
				if t.transformSelectStmt(subSelect) {
					changed = true
				}
			}
		}
	}

	// Recurse into CTEs
	if stmt.WithClause != nil {
		for _, cte := range stmt.WithClause.Ctes {
			if cteExpr := cte.GetCommonTableExpr(); cteExpr != nil {
				if cteSelect := cteExpr.Ctequery.GetSelectStmt(); cteSelect != nil {
					if t.transformSelectStmt(cteSelect) {
						changed = true
					}
				}
			}
		}
	}

	return changed
}

// collectExpandArrayCalls finds all _pg_expandarray calls and assigns lateral aliases
func (t *ExpandArrayTransform) collectExpandArrayCalls(node *pg_query.Node, arrayExprToAlias map[string]string) {
	if node == nil {
		return
	}

	// Check for AIndirection with _pg_expandarray
	if aIndirection := node.GetAIndirection(); aIndirection != nil {
		if funcCall := aIndirection.Arg.GetFuncCall(); funcCall != nil {
			if t.isExpandArrayFunc(funcCall) {
				// Get the array argument
				if len(funcCall.Args) > 0 {
					arrayExpr := t.deparseNode(funcCall.Args[0])
					if _, exists := arrayExprToAlias[arrayExpr]; !exists {
						t.lateralCounter++
						arrayExprToAlias[arrayExpr] = fmt.Sprintf("_pg_exp_%d", t.lateralCounter)
					}
				}
			}
		}
		// Also check the arg for nested expressions
		t.collectExpandArrayCalls(aIndirection.Arg, arrayExprToAlias)
		return
	}

	// Check for standalone _pg_expandarray (aliased without field access)
	if funcCall := node.GetFuncCall(); funcCall != nil {
		if t.isExpandArrayFunc(funcCall) {
			if len(funcCall.Args) > 0 {
				arrayExpr := t.deparseNode(funcCall.Args[0])
				if _, exists := arrayExprToAlias[arrayExpr]; !exists {
					t.lateralCounter++
					arrayExprToAlias[arrayExpr] = fmt.Sprintf("_pg_exp_%d", t.lateralCounter)
				}
			}
		}
		// Recurse into function arguments
		for _, arg := range funcCall.Args {
			t.collectExpandArrayCalls(arg, arrayExprToAlias)
		}
		return
	}

	// Recurse into other node types
	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			t.collectExpandArrayCalls(n.AExpr.Lexpr, arrayExprToAlias)
			t.collectExpandArrayCalls(n.AExpr.Rexpr, arrayExprToAlias)
		}
	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				t.collectExpandArrayCalls(arg, arrayExprToAlias)
			}
		}
	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			// Don't add lateral joins from subqueries to parent
			// Just recurse to transform the subquery independently
			// (subselect transformation happens via recursive transformSelectStmt calls)
			t.collectExpandArrayCalls(n.SubLink.Testexpr, arrayExprToAlias)
		}
	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			t.collectExpandArrayCalls(n.TypeCast.Arg, arrayExprToAlias)
		}
	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			t.collectExpandArrayCalls(n.CaseExpr.Arg, arrayExprToAlias)
			for _, when := range n.CaseExpr.Args {
				t.collectExpandArrayCalls(when, arrayExprToAlias)
			}
			t.collectExpandArrayCalls(n.CaseExpr.Defresult, arrayExprToAlias)
		}
	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			t.collectExpandArrayCalls(n.CaseWhen.Expr, arrayExprToAlias)
			t.collectExpandArrayCalls(n.CaseWhen.Result, arrayExprToAlias)
		}
	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, arg := range n.CoalesceExpr.Args {
				t.collectExpandArrayCalls(arg, arrayExprToAlias)
			}
		}
	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			t.collectExpandArrayCalls(n.NullTest.Arg, arrayExprToAlias)
		}
	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			t.collectExpandArrayCalls(n.ResTarget.Val, arrayExprToAlias)
		}
	}
}

// replaceExpandArrayCalls replaces _pg_expandarray calls with column references
func (t *ExpandArrayTransform) replaceExpandArrayCalls(resTarget *pg_query.ResTarget, arrayExprToAlias map[string]string) bool {
	if resTarget == nil || resTarget.Val == nil {
		return false
	}

	newVal := t.replaceInExpression(resTarget.Val, arrayExprToAlias)
	if newVal != nil {
		resTarget.Val = newVal
		return true
	}
	return false
}

// replaceInExpression replaces _pg_expandarray calls in an expression
func (t *ExpandArrayTransform) replaceInExpression(node *pg_query.Node, arrayExprToAlias map[string]string) *pg_query.Node {
	if node == nil {
		return nil
	}

	// Check for AIndirection with _pg_expandarray
	if aIndirection := node.GetAIndirection(); aIndirection != nil {
		if funcCall := aIndirection.Arg.GetFuncCall(); funcCall != nil {
			if t.isExpandArrayFunc(funcCall) {
				// Get the array argument and field name
				if len(funcCall.Args) > 0 && len(aIndirection.Indirection) > 0 {
					arrayExpr := t.deparseNode(funcCall.Args[0])
					alias, exists := arrayExprToAlias[arrayExpr]
					if !exists {
						return nil
					}

					// Get the field name from indirection
					fieldName := ""
					if strNode := aIndirection.Indirection[0].GetString_(); strNode != nil {
						fieldName = strNode.Sval
					}

					if fieldName == "" {
						return nil
					}

					// Replace with alias.fieldname column reference
					return &pg_query.Node{
						Node: &pg_query.Node_ColumnRef{
							ColumnRef: &pg_query.ColumnRef{
								Fields: []*pg_query.Node{
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: alias}}},
									{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: fieldName}}},
								},
							},
						},
					}
				}
			}
		}
	}

	// Check for standalone _pg_expandarray (used as whole row, e.g., aliased)
	if funcCall := node.GetFuncCall(); funcCall != nil {
		if t.isExpandArrayFunc(funcCall) {
			if len(funcCall.Args) > 0 {
				arrayExpr := t.deparseNode(funcCall.Args[0])
				alias, exists := arrayExprToAlias[arrayExpr]
				if !exists {
					return nil
				}

				// Replace with a ROW constructor or struct that mimics the composite type
				// For DuckDB, we create a struct with x and n fields
				return &pg_query.Node{
					Node: &pg_query.Node_FuncCall{
						FuncCall: &pg_query.FuncCall{
							Funcname: []*pg_query.Node{
								{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "struct_pack"}}},
							},
							Args: []*pg_query.Node{
								// x := alias.x
								{
									Node: &pg_query.Node_NamedArgExpr{
										NamedArgExpr: &pg_query.NamedArgExpr{
											Name: "x",
											Arg: &pg_query.Node{
												Node: &pg_query.Node_ColumnRef{
													ColumnRef: &pg_query.ColumnRef{
														Fields: []*pg_query.Node{
															{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: alias}}},
															{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "x"}}},
														},
													},
												},
											},
										},
									},
								},
								// n := alias.n
								{
									Node: &pg_query.Node_NamedArgExpr{
										NamedArgExpr: &pg_query.NamedArgExpr{
											Name: "n",
											Arg: &pg_query.Node{
												Node: &pg_query.Node_ColumnRef{
													ColumnRef: &pg_query.ColumnRef{
														Fields: []*pg_query.Node{
															{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: alias}}},
															{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "n"}}},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}
			}
		}
	}

	// Recurse into other node types and replace children
	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			if newLexpr := t.replaceInExpression(n.AExpr.Lexpr, arrayExprToAlias); newLexpr != nil {
				n.AExpr.Lexpr = newLexpr
				return node
			}
			if newRexpr := t.replaceInExpression(n.AExpr.Rexpr, arrayExprToAlias); newRexpr != nil {
				n.AExpr.Rexpr = newRexpr
				return node
			}
		}
	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			changed := false
			for i, arg := range n.BoolExpr.Args {
				if newArg := t.replaceInExpression(arg, arrayExprToAlias); newArg != nil {
					n.BoolExpr.Args[i] = newArg
					changed = true
				}
			}
			if changed {
				return node
			}
		}
	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			if newArg := t.replaceInExpression(n.TypeCast.Arg, arrayExprToAlias); newArg != nil {
				n.TypeCast.Arg = newArg
				return node
			}
		}
	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			changed := false
			if newArg := t.replaceInExpression(n.CaseExpr.Arg, arrayExprToAlias); newArg != nil {
				n.CaseExpr.Arg = newArg
				changed = true
			}
			for i, when := range n.CaseExpr.Args {
				if newWhen := t.replaceInExpression(when, arrayExprToAlias); newWhen != nil {
					n.CaseExpr.Args[i] = newWhen
					changed = true
				}
			}
			if newDef := t.replaceInExpression(n.CaseExpr.Defresult, arrayExprToAlias); newDef != nil {
				n.CaseExpr.Defresult = newDef
				changed = true
			}
			if changed {
				return node
			}
		}
	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			changed := false
			for i, arg := range n.CoalesceExpr.Args {
				if newArg := t.replaceInExpression(arg, arrayExprToAlias); newArg != nil {
					n.CoalesceExpr.Args[i] = newArg
					changed = true
				}
			}
			if changed {
				return node
			}
		}
	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			if newArg := t.replaceInExpression(n.NullTest.Arg, arrayExprToAlias); newArg != nil {
				n.NullTest.Arg = newArg
				return node
			}
		}
	}

	return nil
}

// isExpandArrayFunc checks if a function call is to _pg_expandarray
func (t *ExpandArrayTransform) isExpandArrayFunc(funcCall *pg_query.FuncCall) bool {
	if funcCall == nil || len(funcCall.Funcname) == 0 {
		return false
	}

	// Check the last element (function name, ignoring schema)
	lastIdx := len(funcCall.Funcname) - 1
	if strNode := funcCall.Funcname[lastIdx].GetString_(); strNode != nil {
		return strings.EqualFold(strNode.Sval, "_pg_expandarray")
	}
	return false
}

// deparseNode converts a node back to SQL string for use as a key
func (t *ExpandArrayTransform) deparseNode(node *pg_query.Node) string {
	if node == nil {
		return ""
	}

	// Create a minimal SELECT statement to deparse the node
	tree := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{
						SelectStmt: &pg_query.SelectStmt{
							TargetList: []*pg_query.Node{
								{
									Node: &pg_query.Node_ResTarget{
										ResTarget: &pg_query.ResTarget{
											Val: node,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	sql, err := pg_query.Deparse(tree)
	if err != nil {
		return ""
	}

	// Extract just the expression part (remove "SELECT " prefix)
	if strings.HasPrefix(strings.ToUpper(sql), "SELECT ") {
		return strings.TrimSpace(sql[7:])
	}
	return sql
}

// createLateralJoin creates a LATERAL subquery for the array expansion
func (t *ExpandArrayTransform) createLateralJoin(alias string, arrayExpr string) *pg_query.Node {
	// Build: LATERAL (SELECT unnest(arrayExpr) AS x, generate_subscripts(arrayExpr, 1) AS n) AS alias
	// We need to parse the array expression to get a proper AST node

	// Build the LATERAL subquery SQL and parse it
	lateralSQL := fmt.Sprintf("SELECT * FROM (SELECT unnest(%s) AS x, generate_subscripts(%s, 1) AS n) AS %s", arrayExpr, arrayExpr, alias)

	tree, err := pg_query.Parse(lateralSQL)
	if err != nil {
		return nil
	}

	if len(tree.Stmts) == 0 {
		return nil
	}

	selectStmt := tree.Stmts[0].Stmt.GetSelectStmt()
	if selectStmt == nil || len(selectStmt.FromClause) == 0 {
		return nil
	}

	// Return the RangeSubselect from the parsed query
	return selectStmt.FromClause[0]
}
