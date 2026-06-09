package transform

import (
	"strings"

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
		} else if ctasStmt := stmt.Stmt.GetCreateTableAsStmt(); ctasStmt != nil {
			// CREATE TABLE AS SELECT (and CREATE OR REPLACE TABLE AS, which the
			// transpiler routes here after stripping OR REPLACE; also CREATE
			// MATERIALIZED VIEW AS / SELECT INTO). Without descending into the
			// AS-SELECT body, a chained JSON arrow there is left as a raw
			// operator and hits DuckDB's ->> precedence bug once the parens are
			// normalized away by the pg_query round-trip — e.g. a SQLMesh
			// `CREATE OR REPLACE TABLE ... AS ... CASE WHEN x AND (j -> 'a') ->>
			// 'b' LIKE ... ` materialization fails with a spurious numeric cast.
			// Query is usually a SelectStmt; for `CREATE TABLE t AS EXECUTE plan`
			// it's an ExecuteStmt (no arrows to rewrite), so the nil-check below
			// intentionally skips it.
			if ctasStmt.Query != nil {
				if ctasSelect := ctasStmt.Query.GetSelectStmt(); ctasSelect != nil {
					if t.transformSelectStmt(ctasSelect) {
						changed = true
					}
				}
			}
		} else if viewStmt := stmt.Stmt.GetViewStmt(); viewStmt != nil {
			// CREATE [OR REPLACE] VIEW ... AS SELECT ... — the view body carries
			// the same arrow-precedence risk as a CTAS body.
			if viewStmt.Query != nil {
				if viewSelect := viewStmt.Query.GetSelectStmt(); viewSelect != nil {
					if t.transformSelectStmt(viewSelect) {
						changed = true
					}
				}
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

	// Transform GROUP BY expressions (e.g. GROUP BY data->>'type')
	for i, group := range stmt.GroupClause {
		if newGroup := t.transformExpression(group); newGroup != nil {
			stmt.GroupClause[i] = newGroup
			changed = true
		}
	}

	// Transform ORDER BY expressions. Each SortClause element is a SortBy node
	// wrapping the sort expression, so descend into its Node.
	for _, sort := range stmt.SortClause {
		if sortBy := sort.GetSortBy(); sortBy != nil && sortBy.Node != nil {
			if newNode := t.transformExpression(sortBy.Node); newNode != nil {
				sortBy.Node = newNode
				changed = true
			}
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
		// jsonb || jsonb is an object merge in Postgres, but DuckDB treats || as
		// string concatenation, silently producing invalid JSON. Rewrite to
		// json_merge_patch only when an operand is clearly JSON; otherwise leave
		// || alone (string/array concat is the safe default).
		case "||":
			if looksJSON(aexpr.Lexpr) || looksJSON(aexpr.Rexpr) {
				return t.createJSONMergeFuncCall(aexpr.Lexpr, aexpr.Rexpr)
			}
		// jsonb @> jsonb is containment in Postgres; DuckDB has no @>(JSON,JSON) but does
		// have json_contains(). Only rewrite when an operand is clearly JSON — array @> array
		// is native in DuckDB and must be left alone.
		case "@>":
			if looksJSON(aexpr.Lexpr) || looksJSON(aexpr.Rexpr) {
				return t.createJSONContainsFuncCall(aexpr.Lexpr, aexpr.Rexpr)
			}
		// json #>> '{a,b}' extracts the value at a text[] path as text. DuckDB lacks #>> but
		// json_extract_string(j, '$."a"."b"') is equivalent. Only literal path arrays can be
		// converted at transpile time; a non-literal path is left as-is.
		case "#>>":
			if jsonPath := pgPathArrayToJSONPath(aexpr.Rexpr); jsonPath != "" {
				return t.createJsonExtractFuncCall(aexpr.Lexpr, stringConstNode(jsonPath), true)
			}
		// Regex operators — only match binary ~ (both operands present).
		// Unary ~ (bitwise NOT, e.g. ~id) has Lexpr=nil and must be left as-is;
		// DuckDB supports ~ as bitwise NOT natively. Passing nil into
		// createRegexFuncCall would create a nil AST node that crashes pg_query.Deparse.
		case "~":
			if aexpr.Lexpr != nil {
				return t.createRegexFuncCall(aexpr.Lexpr, aexpr.Rexpr, false, false)
			}
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

// looksJSON reports whether a node is syntactically JSON: a cast to json/jsonb,
// or a json* function call (including the json_extract calls this transform
// produces from -> / ->>). Used to gate the || -> json_merge_patch rewrite so
// genuine string/array concatenation is left untouched.
func looksJSON(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if tc := node.GetTypeCast(); tc != nil && tc.TypeName != nil && len(tc.TypeName.Names) > 0 {
		if last := tc.TypeName.Names[len(tc.TypeName.Names)-1].GetString_(); last != nil {
			switch strings.ToLower(last.Sval) {
			case "json", "jsonb":
				return true
			}
		}
	}
	if fc := node.GetFuncCall(); fc != nil && len(fc.Funcname) > 0 {
		if last := fc.Funcname[len(fc.Funcname)-1].GetString_(); last != nil {
			if strings.HasPrefix(strings.ToLower(last.Sval), "json") {
				return true
			}
		}
	}
	return false
}

// createJSONMergeFuncCall rewrites `a || b` to json_merge_patch(a, b). Note this
// is RFC 7396 merge-patch semantics (recursive, and a null value deletes a key),
// which matches Postgres jsonb || for the common flat-object-merge case but
// diverges for nested objects and explicit nulls.
func (t *OperatorTransform) createJSONMergeFuncCall(left, right *pg_query.Node) *pg_query.Node {
	if newLeft := t.transformExpression(left); newLeft != nil {
		left = newLeft
	}
	if newRight := t.transformExpression(right); newRight != nil {
		right = newRight
	}
	return &pg_query.Node{
		Node: &pg_query.Node_FuncCall{
			FuncCall: &pg_query.FuncCall{
				Funcname: []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "json_merge_patch"}}},
				},
				Args: []*pg_query.Node{left, right},
			},
		},
	}
}

// createJSONContainsFuncCall rewrites `a @> b` to json_contains(a, b), matching
// Postgres jsonb containment for the common object/array cases.
func (t *OperatorTransform) createJSONContainsFuncCall(left, right *pg_query.Node) *pg_query.Node {
	if newLeft := t.transformExpression(left); newLeft != nil {
		left = newLeft
	}
	if newRight := t.transformExpression(right); newRight != nil {
		right = newRight
	}
	return &pg_query.Node{
		Node: &pg_query.Node_FuncCall{
			FuncCall: &pg_query.FuncCall{
				Funcname: []*pg_query.Node{
					{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "json_contains"}}},
				},
				Args: []*pg_query.Node{left, right},
			},
		},
	}
}

// stringConstNode builds an A_Const string node carrying the given value.
func stringConstNode(s string) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_AConst{
			AConst: &pg_query.A_Const{
				Val: &pg_query.A_Const_Sval{Sval: &pg_query.String{Sval: s}},
			},
		},
	}
}

// pgPathArrayToJSONPath converts a literal Postgres text[] path (e.g. '{a,b}' or
// '{a,0,b}') into a DuckDB JSONPath ('$."a"."b"', '$."a"[0]."b"'). Keys are
// double-quoted so dotted keys are handled; all-digit elements become array
// indices. Returns "" for non-literal operands (left untransformed).
//
// Divergence: PG resolves each path element against the container's runtime type
// (a digit element addresses an object key "0" when the container is an object),
// but this static conversion always treats all-digit elements as array indices —
// an object with a literal numeric key returns NULL instead of its value.
func pgPathArrayToJSONPath(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	// Unwrap an explicit cast like '{a,b}'::text[].
	if tc := node.GetTypeCast(); tc != nil {
		node = tc.Arg
	}
	ac := node.GetAConst()
	if ac == nil {
		return ""
	}
	sval := ac.GetSval()
	if sval == nil {
		return ""
	}
	raw := strings.TrimSpace(sval.Sval)
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return ""
	}
	inner := raw[1 : len(raw)-1]
	if strings.TrimSpace(inner) == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString("$")
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		if part != "" && isAllDigits(part) {
			b.WriteString("[")
			b.WriteString(part)
			b.WriteString("]")
			continue
		}
		b.WriteString(`."`)
		b.WriteString(strings.ReplaceAll(part, `"`, `""`))
		b.WriteString(`"`)
	}
	return b.String()
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
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
