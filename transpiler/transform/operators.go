package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
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
		// jsonb || jsonb is concatenation in Postgres (object merge / array
		// concat), but DuckDB treats || as string concatenation, silently
		// producing invalid JSON. Rewrite to a json_type()-dispatching CASE
		// that reproduces Postgres semantics only when an operand is clearly
		// JSON; otherwise leave || alone (string/array concat is the safe
		// default).
		case "||":
			if looksJSON(aexpr.Lexpr) || looksJSON(aexpr.Rexpr) {
				return t.createJSONConcatExpr(aexpr.Lexpr, aexpr.Rexpr)
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
// a JSON-returning json* function call (including the json_extract calls this
// transform produces from -> / ->>), a JSON-producing conversion function
// (to_json/to_jsonb/row_to_json/array_to_json), or a `->` A_Expr (which this
// transform deterministically rewrites to json_extract — the || gate runs
// before the operands' own rewrite, so the raw arrow must count). Used to gate
// the || JSON-concat rewrite so genuine string/array concatenation is left
// untouched. json*-named functions that return text/numbers/booleans
// (json_extract_string, json_array_length, json_type, ...) are excluded:
// `json_extract_string(d,'a') || 'x'` is plain text concat in Postgres and
// must stay that way (likewise `->>`, which yields text, does not count).
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
			name := strings.ToLower(last.Sval)
			if strings.HasPrefix(name, "json") && !jsonFuncReturnsNonJSON(name) {
				return true
			}
			// JSON-producing conversions whose names don't start with "json".
			// FunctionTransform (which runs earlier) maps to_jsonb -> to_json,
			// but cover the Postgres spellings too for direct/standalone use.
			switch name {
			case "to_json", "to_jsonb", "row_to_json", "array_to_json":
				return true
			}
		}
	}
	// A bare `d -> 'a'` operand (no cast, not yet rewritten): the arrow always
	// becomes json_extract, which returns JSON. Without this, `(d->'a') ||
	// (d->'b')` would fall through to DuckDB string concat of two JSON values.
	if ae := node.GetAExpr(); ae != nil && ae.Kind == pg_query.A_Expr_Kind_AEXPR_OP &&
		ae.Lexpr != nil && ae.Rexpr != nil && len(ae.Name) == 1 {
		if s := ae.Name[0].GetString_(); s != nil && s.Sval == "->" {
			return true
		}
	}
	return false
}

// jsonFuncReturnsNonJSON reports whether a json*-named function returns a
// non-JSON value (VARCHAR, BIGINT, BOOLEAN, ...), so its result concatenated
// with || must keep DuckDB string-concat semantics. Covers both the PostgreSQL
// names and the DuckDB names FunctionTransform rewrites them to (function
// mapping runs before this transform in the pipeline).
func jsonFuncReturnsNonJSON(name string) bool {
	for _, suffix := range []string{"_string", "_text", "_length", "_keys", "_valid", "_exists", "_contains"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	switch name {
	case "json_type", "json_typeof", "jsonb_typeof":
		return true
	}
	return false
}

// createJSONConcatExpr rewrites `a || b` (with a JSON-looking operand) to a
// CASE expression reproducing Postgres jsonb || semantics in DuckDB:
//
//	object || object  -> shallow merge, right side wins, explicit nulls are
//	                     KEPT (unlike json_merge_patch, which deep-merges and
//	                     deletes null-valued keys)
//	array  || array   -> element concatenation
//	array  || other   -> append   (non-array side wrapped as a one-element array)
//	other  || array   -> prepend
//	scalar || scalar  -> two-element array
//	NULL   || any     -> NULL (SQL NULL, not JSON null)
//
// Emitted shape (L/R = operands):
//
//	CASE
//	  WHEN L IS NULL OR R IS NULL THEN NULL
//	  WHEN json_type(L) = 'OBJECT' AND json_type(R) = 'OBJECT'
//	    THEN to_json(map_concat(json_transform(L, '"MAP(VARCHAR, JSON)"'),
//	                            json_transform(R, '"MAP(VARCHAR, JSON)"')))
//	  ELSE to_json(list_concat(<L as JSON[]>, <R as JSON[]>))
//	END
//
// where <x as JSON[]> is CASE WHEN json_type(x) = 'ARRAY' THEN
// json_transform(x, '["JSON"]') ELSE list_value(CAST(x AS JSON)) END.
// map_concat is last-wins on key collision and to_json inlines JSON-typed
// values, which is exactly the Postgres shallow-merge behavior (verified
// against PostgreSQL output for all the cases above; key ORDER may differ —
// Postgres sorts jsonb keys, DuckDB preserves left-operand order — but jsonb
// objects are semantically unordered; objects with DUPLICATE keys also differ,
// but that is duckgres's global jsonb-as-JSON divergence — Postgres dedups at
// the ::jsonb cast, DuckDB JSON does not — not something this rewrite adds).
// Operands are cloned per use site so later pipeline transforms never visit a
// shared node twice.
//
// KNOWN COSTS of the per-use-site cloning: each operand appears ~6 times in
// the emitted CASE, so a chain of N `||` grows the transpiled SQL ~6^N (fine
// for the realistic 1-3 operand idioms; ~1MB of SQL by 6 chained operands),
// and a non-trivial operand (scalar subquery, volatile function) is EVALUATED
// up to that many times — a volatile operand could even dispatch differently
// between the json_type() probe and the use site. If this ever bites, the fix
// is linear expansion: register a DuckDB scalar macro (e.g.
// duckgres_jsonb_concat(l, r)) at session/worker init and emit one call per
// ||, binding each operand exactly once.
func (t *OperatorTransform) createJSONConcatExpr(left, right *pg_query.Node) *pg_query.Node {
	if newLeft := t.transformExpression(left); newLeft != nil {
		left = newLeft
	}
	if newRight := t.transformExpression(right); newRight != nil {
		right = newRight
	}

	asObjectMap := func(operand *pg_query.Node) *pg_query.Node {
		return jsonFuncCall("json_transform", cloneNode(operand), strConst(`"MAP(VARCHAR, JSON)"`))
	}
	// CASE WHEN json_type(x) = 'ARRAY' THEN json_transform(x, '["JSON"]')
	// ELSE list_value(CAST(x AS JSON)) END — Postgres treats every non-array
	// operand of a non-object||object concat as a one-element array.
	asElementList := func(operand *pg_query.Node) *pg_query.Node {
		return &pg_query.Node{Node: &pg_query.Node_CaseExpr{CaseExpr: &pg_query.CaseExpr{
			Args: []*pg_query.Node{{Node: &pg_query.Node_CaseWhen{CaseWhen: &pg_query.CaseWhen{
				Expr:   jsonTypeEquals(operand, "ARRAY"),
				Result: jsonFuncCall("json_transform", cloneNode(operand), strConst(`["JSON"]`)),
			}}}},
			Defresult: jsonFuncCall("list_value", castToJSON(cloneNode(operand))),
		}}}
	}

	// The whole CASE is wrapped in an explicit CAST(... AS JSON). DuckDB's
	// to_json()/map_concat()/list_concat() result type is not reported as JSON
	// by every bundled DuckDB version — on some versions the wire layer then
	// sees the column as an untyped list and renders it with Go's %v
	// ("[1 2 3 4]") instead of routing it through the JSON re-serializer
	// ("[1,2,3,4]"). Forcing the column to JSON pins OID 114 so server's
	// encodeJSON path always produces valid JSON text. The cast is idempotent
	// on versions that already type it as JSON. (#716 regression.)
	return castToJSON(&pg_query.Node{Node: &pg_query.Node_CaseExpr{CaseExpr: &pg_query.CaseExpr{
		Args: []*pg_query.Node{
			// WHEN L IS NULL OR R IS NULL THEN NULL — Postgres jsonb || is
			// strict; without this guard the ELSE branch would wrap a SQL
			// NULL operand into [null, ...].
			{Node: &pg_query.Node_CaseWhen{CaseWhen: &pg_query.CaseWhen{
				Expr: &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: &pg_query.BoolExpr{
					Boolop: pg_query.BoolExprType_OR_EXPR,
					Args:   []*pg_query.Node{isNullTest(left), isNullTest(right)},
				}}},
				Result: nullConstNode(0),
			}}},
			// WHEN both objects THEN shallow merge, right side wins.
			{Node: &pg_query.Node_CaseWhen{CaseWhen: &pg_query.CaseWhen{
				Expr: &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: &pg_query.BoolExpr{
					Boolop: pg_query.BoolExprType_AND_EXPR,
					Args:   []*pg_query.Node{jsonTypeEquals(left, "OBJECT"), jsonTypeEquals(right, "OBJECT")},
				}}},
				Result: jsonFuncCall("to_json", jsonFuncCall("map_concat", asObjectMap(left), asObjectMap(right))),
			}}},
		},
		// ELSE array concatenation (with non-arrays wrapped as one element).
		Defresult: jsonFuncCall("to_json", jsonFuncCall("list_concat", asElementList(left), asElementList(right))),
	}}})
}

// jsonFuncCall builds an unqualified function-call node.
func jsonFuncCall(name string, args ...*pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_FuncCall{FuncCall: &pg_query.FuncCall{
		Funcname: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: name}}},
		},
		Args: args,
	}}}
}

// jsonTypeEquals builds `json_type(<operand clone>) = '<typ>'`.
func jsonTypeEquals(operand *pg_query.Node, typ string) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: &pg_query.A_Expr{
		Kind: pg_query.A_Expr_Kind_AEXPR_OP,
		Name: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "="}}},
		},
		Lexpr: jsonFuncCall("json_type", cloneNode(operand)),
		Rexpr: strConst(typ),
	}}}
}

// isNullTest builds `<operand clone> IS NULL`.
func isNullTest(operand *pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_NullTest{NullTest: &pg_query.NullTest{
		Arg:          cloneNode(operand),
		Nulltesttype: pg_query.NullTestType_IS_NULL,
	}}}
}

// castToJSON builds `CAST(<operand> AS json)`.
func castToJSON(operand *pg_query.Node) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: &pg_query.TypeCast{
		Arg: operand,
		TypeName: &pg_query.TypeName{
			Names:   []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "json"}}}},
			Typemod: -1,
		},
	}}}
}

// cloneNode deep-copies an AST node so an operand can appear at several
// positions in the emitted expression without sharing mutable state.
func cloneNode(node *pg_query.Node) *pg_query.Node {
	return proto.Clone(node).(*pg_query.Node)
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
