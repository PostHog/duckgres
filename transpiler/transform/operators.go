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

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		if selectStmt := stmt.Stmt.GetSelectStmt(); selectStmt != nil {
			if t.transformSelectStmt(selectStmt) {
				changed = true
			}
		} else if insertStmt := stmt.Stmt.GetInsertStmt(); insertStmt != nil {
			if t.transformInsertStmt(insertStmt) {
				changed = true
			}
		} else if updateStmt := stmt.Stmt.GetUpdateStmt(); updateStmt != nil {
			if t.transformUpdateStmt(updateStmt) {
				changed = true
			}
		} else if deleteStmt := stmt.Stmt.GetDeleteStmt(); deleteStmt != nil {
			if t.transformDeleteStmt(deleteStmt) {
				changed = true
			}
		}
	}

	return changed, nil
}

func (t *OperatorTransform) transformSelectStmt(stmt *pg_query.SelectStmt) bool {
	if stmt == nil {
		return false
	}

	changed := false

	// Transform target list
	for _, target := range stmt.TargetList {
		if resTarget := target.GetResTarget(); resTarget != nil && resTarget.Val != nil {
			if newVal := t.transformExpression(resTarget.Val); newVal != nil {
				resTarget.Val = newVal
				changed = true
			}
		}
	}

	// Transform WHERE clause
	if stmt.WhereClause != nil {
		if newWhere := t.transformExpression(stmt.WhereClause); newWhere != nil {
			stmt.WhereClause = newWhere
			changed = true
		}
	}

	// Transform HAVING clause
	if stmt.HavingClause != nil {
		if newHaving := t.transformExpression(stmt.HavingClause); newHaving != nil {
			stmt.HavingClause = newHaving
			changed = true
		}
	}

	// Transform FROM clause (for JOINs)
	for _, from := range stmt.FromClause {
		if t.transformFromItem(from) {
			changed = true
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

	// Recurse into UNION/INTERSECT/EXCEPT
	if stmt.Larg != nil {
		if t.transformSelectStmt(stmt.Larg) {
			changed = true
		}
	}
	if stmt.Rarg != nil {
		if t.transformSelectStmt(stmt.Rarg) {
			changed = true
		}
	}

	return changed
}

func (t *OperatorTransform) transformInsertStmt(stmt *pg_query.InsertStmt) bool {
	if stmt == nil {
		return false
	}

	changed := false

	if stmt.SelectStmt != nil {
		if selectStmt := stmt.SelectStmt.GetSelectStmt(); selectStmt != nil {
			if t.transformSelectStmt(selectStmt) {
				changed = true
			}
		}
	}

	return changed
}

func (t *OperatorTransform) transformUpdateStmt(stmt *pg_query.UpdateStmt) bool {
	if stmt == nil {
		return false
	}

	changed := false

	// Transform SET clause values
	for _, target := range stmt.TargetList {
		if resTarget := target.GetResTarget(); resTarget != nil && resTarget.Val != nil {
			if newVal := t.transformExpression(resTarget.Val); newVal != nil {
				resTarget.Val = newVal
				changed = true
			}
		}
	}

	// Transform WHERE clause
	if stmt.WhereClause != nil {
		if newWhere := t.transformExpression(stmt.WhereClause); newWhere != nil {
			stmt.WhereClause = newWhere
			changed = true
		}
	}

	// Transform FROM clause
	for _, from := range stmt.FromClause {
		if t.transformFromItem(from) {
			changed = true
		}
	}

	return changed
}

func (t *OperatorTransform) transformDeleteStmt(stmt *pg_query.DeleteStmt) bool {
	if stmt == nil {
		return false
	}

	changed := false

	// Transform WHERE clause
	if stmt.WhereClause != nil {
		if newWhere := t.transformExpression(stmt.WhereClause); newWhere != nil {
			stmt.WhereClause = newWhere
			changed = true
		}
	}

	return changed
}

func (t *OperatorTransform) transformFromItem(node *pg_query.Node) bool {
	if node == nil {
		return false
	}

	changed := false

	// Handle JoinExpr
	if joinExpr := node.GetJoinExpr(); joinExpr != nil {
		if joinExpr.Quals != nil {
			if newQuals := t.transformExpression(joinExpr.Quals); newQuals != nil {
				joinExpr.Quals = newQuals
				changed = true
			}
		}
		if t.transformFromItem(joinExpr.Larg) {
			changed = true
		}
		if t.transformFromItem(joinExpr.Rarg) {
			changed = true
		}
	}

	// Handle subselects
	if rangeSubselect := node.GetRangeSubselect(); rangeSubselect != nil {
		if rangeSubselect.Subquery != nil {
			if subSelect := rangeSubselect.Subquery.GetSelectStmt(); subSelect != nil {
				if t.transformSelectStmt(subSelect) {
					changed = true
				}
			}
		}
	}

	return changed
}

// transformExpression recursively transforms an expression, replacing regex operators
// with function calls. Returns the new node if transformed, nil otherwise.
func (t *OperatorTransform) transformExpression(node *pg_query.Node) *pg_query.Node {
	if node == nil {
		return nil
	}

	// Check if this is an operator A_Expr that needs transformation
	if aexpr := node.GetAExpr(); aexpr != nil {
		opName := t.getOperatorName(aexpr)

		switch opName {
		// JSON operators - convert to function calls to avoid DuckDB precedence issues
		// DuckDB parses "a AND b -> 'key'" as "(a AND b) -> 'key'" instead of "a AND (b -> 'key')"
		case "->":
			return t.createJsonExtractFuncCall(aexpr.Lexpr, aexpr.Rexpr, false)
		case "->>":
			return t.createJsonExtractFuncCall(aexpr.Lexpr, aexpr.Rexpr, true)
		// Regex operators
		case "~":
			return t.createRegexFuncCall(aexpr.Lexpr, aexpr.Rexpr, false, false)
		case "~*":
			return t.createRegexFuncCall(aexpr.Lexpr, aexpr.Rexpr, true, false)
		case "!~":
			return t.createRegexFuncCall(aexpr.Lexpr, aexpr.Rexpr, false, true)
		case "!~*":
			return t.createRegexFuncCall(aexpr.Lexpr, aexpr.Rexpr, true, true)
		}

		// Recursively transform operands for other operators
		leftChanged := false
		rightChanged := false

		if aexpr.Lexpr != nil {
			if newLeft := t.transformExpression(aexpr.Lexpr); newLeft != nil {
				aexpr.Lexpr = newLeft
				leftChanged = true
			}
		}
		if aexpr.Rexpr != nil {
			if newRight := t.transformExpression(aexpr.Rexpr); newRight != nil {
				aexpr.Rexpr = newRight
				rightChanged = true
			}
		}

		if leftChanged || rightChanged {
			return node
		}
		return nil
	}

	// Handle BoolExpr (AND, OR, NOT)
	if boolExpr := node.GetBoolExpr(); boolExpr != nil {
		anyChanged := false
		for i, arg := range boolExpr.Args {
			if newArg := t.transformExpression(arg); newArg != nil {
				boolExpr.Args[i] = newArg
				anyChanged = true
			}
		}
		if anyChanged {
			return node
		}
		return nil
	}

	// Handle function calls (recurse into arguments)
	if funcCall := node.GetFuncCall(); funcCall != nil {
		anyChanged := false
		for i, arg := range funcCall.Args {
			if newArg := t.transformExpression(arg); newArg != nil {
				funcCall.Args[i] = newArg
				anyChanged = true
			}
		}
		if anyChanged {
			return node
		}
		return nil
	}

	// Handle CASE expressions
	if caseExpr := node.GetCaseExpr(); caseExpr != nil {
		anyChanged := false
		if caseExpr.Arg != nil {
			if newArg := t.transformExpression(caseExpr.Arg); newArg != nil {
				caseExpr.Arg = newArg
				anyChanged = true
			}
		}
		for _, when := range caseExpr.Args {
			if caseWhen := when.GetCaseWhen(); caseWhen != nil {
				if caseWhen.Expr != nil {
					if newExpr := t.transformExpression(caseWhen.Expr); newExpr != nil {
						caseWhen.Expr = newExpr
						anyChanged = true
					}
				}
				if caseWhen.Result != nil {
					if newResult := t.transformExpression(caseWhen.Result); newResult != nil {
						caseWhen.Result = newResult
						anyChanged = true
					}
				}
			}
		}
		if caseExpr.Defresult != nil {
			if newDef := t.transformExpression(caseExpr.Defresult); newDef != nil {
				caseExpr.Defresult = newDef
				anyChanged = true
			}
		}
		if anyChanged {
			return node
		}
		return nil
	}

	// Handle COALESCE
	if coalesceExpr := node.GetCoalesceExpr(); coalesceExpr != nil {
		anyChanged := false
		for i, arg := range coalesceExpr.Args {
			if newArg := t.transformExpression(arg); newArg != nil {
				coalesceExpr.Args[i] = newArg
				anyChanged = true
			}
		}
		if anyChanged {
			return node
		}
		return nil
	}

	// Handle subqueries
	if subLink := node.GetSubLink(); subLink != nil {
		anyChanged := false
		if subLink.Testexpr != nil {
			if newTest := t.transformExpression(subLink.Testexpr); newTest != nil {
				subLink.Testexpr = newTest
				anyChanged = true
			}
		}
		if subLink.Subselect != nil {
			if subSelect := subLink.Subselect.GetSelectStmt(); subSelect != nil {
				if t.transformSelectStmt(subSelect) {
					anyChanged = true
				}
			}
		}
		if anyChanged {
			return node
		}
		return nil
	}

	// Handle type casts
	if typeCast := node.GetTypeCast(); typeCast != nil {
		if typeCast.Arg != nil {
			if newArg := t.transformExpression(typeCast.Arg); newArg != nil {
				typeCast.Arg = newArg
				return node
			}
		}
		return nil
	}

	// Handle NullTest
	if nullTest := node.GetNullTest(); nullTest != nil {
		if nullTest.Arg != nil {
			if newArg := t.transformExpression(nullTest.Arg); newArg != nil {
				nullTest.Arg = newArg
				return node
			}
		}
		return nil
	}

	return nil
}

func (t *OperatorTransform) getOperatorName(aexpr *pg_query.A_Expr) string {
	if aexpr == nil || len(aexpr.Name) == 0 {
		return ""
	}

	// Get the last element (operator name, ignoring schema prefix)
	for i := len(aexpr.Name) - 1; i >= 0; i-- {
		if str := aexpr.Name[i].GetString_(); str != nil {
			return str.Sval
		}
	}
	return ""
}

// createJsonExtractFuncCall creates a json_extract or json_extract_string function call node.
// This converts -> and ->> operators to explicit function calls to avoid DuckDB's
// operator precedence issues where "a AND b -> 'key'" is parsed as "(a AND b) -> 'key'".
func (t *OperatorTransform) createJsonExtractFuncCall(left, right *pg_query.Node, asText bool) *pg_query.Node {
	// First, recursively transform the left operand (for chained JSON access like a->'b'->'c')
	if newLeft := t.transformExpression(left); newLeft != nil {
		left = newLeft
	}

	funcName := "json_extract"
	if asText {
		funcName = "json_extract_string"
	}

	return &pg_query.Node{
		Node: &pg_query.Node_FuncCall{
			FuncCall: &pg_query.FuncCall{
				Funcname: []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: funcName}}},
				},
				Args: []*pg_query.Node{left, right},
			},
		},
	}
}

// createRegexFuncCall creates a regexp_matches function call node
// For negated operators, wraps in NOT
func (t *OperatorTransform) createRegexFuncCall(left, right *pg_query.Node, caseInsensitive, negated bool) *pg_query.Node {
	// Build function arguments
	args := []*pg_query.Node{left, right}

	// Add 'i' flag for case-insensitive matching
	if caseInsensitive {
		args = append(args, &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Sval{
						Sval: &pg_query.String{Sval: "i"},
					},
				},
			},
		})
	}

	// Create the function call node
	funcCallNode := &pg_query.Node{
		Node: &pg_query.Node_FuncCall{
			FuncCall: &pg_query.FuncCall{
				Funcname: []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "regexp_matches"}}},
				},
				Args: args,
			},
		},
	}

	// Wrap in NOT for negated operators (!~ and !~*)
	if negated {
		return &pg_query.Node{
			Node: &pg_query.Node_BoolExpr{
				BoolExpr: &pg_query.BoolExpr{
					Boolop: pg_query.BoolExprType_NOT_EXPR,
					Args:   []*pg_query.Node{funcCallNode},
				},
			},
		}
	}

	return funcCallNode
}

// OperatorMappingNote documents the operator mappings for reference:
//
// JSON Operators (PostgreSQL -> DuckDB):
//
//	-> : json_extract() - converted to function call to avoid precedence issues
//	->> : json_extract_string() - converted to function call to avoid precedence issues
//	#> : Not directly supported (use json_extract with path)
//	#>> : Not directly supported (use json_extract_string with path)
//	@> : json_contains() or manual check
//	<@ : Reverse of @>
//	? : json_exists()
//	?| : Manual check with OR
//	?& : Manual check with AND
//
// Regex Operators (PostgreSQL -> DuckDB):
//
//	~ : regexp_matches(text, pattern)
//	~* : regexp_matches(text, pattern, 'i')
//	!~ : NOT regexp_matches(text, pattern)
//	!~* : NOT regexp_matches(text, pattern, 'i')
//
// Array Operators:
//
//	&& : list_has_any()
//	@> : list_has_all()
//	<@ : Reverse containment
//	|| : list_concat() for arrays, || for strings
