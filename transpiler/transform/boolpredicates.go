package transform

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// BooleanPredicateTransform normalizes boolean literal comparisons in
// predicate-valued contexts so DuckLake doesn't see `= true/false` or
// `!= true/false` there. Traversal is explicit to avoid descending into
// scalar-expression wrappers where `IS TRUE/FALSE` is not semantics-preserving.
type BooleanPredicateTransform struct{}

func NewBooleanPredicateTransform() *BooleanPredicateTransform {
	return &BooleanPredicateTransform{}
}

func (t *BooleanPredicateTransform) Name() string {
	return "boolean_predicates"
}

func (t *BooleanPredicateTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false
	for _, stmt := range tree.Stmts {
		if stmt.Stmt != nil && t.visitStatement(stmt.Stmt) {
			changed = true
		}
	}
	return changed, nil
}

func (t *BooleanPredicateTransform) visitStatement(node *pg_query.Node) bool {
	if node == nil {
		return false
	}

	switch {
	case node.GetSelectStmt() != nil:
		return t.visitSelect(node.GetSelectStmt())
	case node.GetInsertStmt() != nil:
		return t.visitInsert(node.GetInsertStmt())
	case node.GetUpdateStmt() != nil:
		return t.visitUpdate(node.GetUpdateStmt())
	case node.GetDeleteStmt() != nil:
		return t.visitDelete(node.GetDeleteStmt())
	case node.GetMergeStmt() != nil:
		return t.visitMerge(node.GetMergeStmt())
	default:
		return false
	}
}

func (t *BooleanPredicateTransform) visitSelect(stmt *pg_query.SelectStmt) bool {
	if stmt == nil {
		return false
	}

	changed := t.visitPredicateField(&stmt.WhereClause, false)
	if t.visitPredicateField(&stmt.HavingClause, false) {
		changed = true
	}
	for _, from := range stmt.FromClause {
		if t.visitFromItem(from) {
			changed = true
		}
	}
	if t.visitWithClause(stmt.WithClause) {
		changed = true
	}
	if stmt.Larg != nil && t.visitSelect(stmt.Larg) {
		changed = true
	}
	if stmt.Rarg != nil && t.visitSelect(stmt.Rarg) {
		changed = true
	}

	return changed
}

func (t *BooleanPredicateTransform) visitInsert(stmt *pg_query.InsertStmt) bool {
	if stmt == nil {
		return false
	}

	changed := t.visitWithClause(stmt.WithClause)
	if stmt.SelectStmt != nil && t.visitStatement(stmt.SelectStmt) {
		changed = true
	}
	if stmt.OnConflictClause != nil {
		if t.visitPredicateField(&stmt.OnConflictClause.WhereClause, false) {
			changed = true
		}
		if stmt.OnConflictClause.Infer != nil && t.visitPredicateField(&stmt.OnConflictClause.Infer.WhereClause, false) {
			changed = true
		}
	}
	return changed
}

func (t *BooleanPredicateTransform) visitUpdate(stmt *pg_query.UpdateStmt) bool {
	if stmt == nil {
		return false
	}

	changed := t.visitWithClause(stmt.WithClause)
	if t.visitPredicateField(&stmt.WhereClause, false) {
		changed = true
	}
	for _, from := range stmt.FromClause {
		if t.visitFromItem(from) {
			changed = true
		}
	}
	return changed
}

func (t *BooleanPredicateTransform) visitDelete(stmt *pg_query.DeleteStmt) bool {
	if stmt == nil {
		return false
	}

	changed := t.visitWithClause(stmt.WithClause)
	if t.visitPredicateField(&stmt.WhereClause, false) {
		changed = true
	}
	for _, from := range stmt.UsingClause {
		if t.visitFromItem(from) {
			changed = true
		}
	}
	return changed
}

func (t *BooleanPredicateTransform) visitMerge(stmt *pg_query.MergeStmt) bool {
	if stmt == nil {
		return false
	}

	changed := t.visitWithClause(stmt.WithClause)
	if stmt.SourceRelation != nil && t.visitFromItem(stmt.SourceRelation) {
		changed = true
	}
	if t.visitPredicateField(&stmt.JoinCondition, false) {
		changed = true
	}
	for _, clauseNode := range stmt.MergeWhenClauses {
		clause := clauseNode.GetMergeWhenClause()
		if clause == nil {
			continue
		}
		if t.visitPredicateField(&clause.Condition, false) {
			changed = true
		}
	}
	return changed
}

func (t *BooleanPredicateTransform) visitWithClause(withClause *pg_query.WithClause) bool {
	if withClause == nil {
		return false
	}

	changed := false
	for _, cte := range withClause.Ctes {
		cteExpr := cte.GetCommonTableExpr()
		if cteExpr != nil && t.visitStatement(cteExpr.Ctequery) {
			changed = true
		}
	}
	return changed
}

func (t *BooleanPredicateTransform) visitFromItem(node *pg_query.Node) bool {
	if node == nil {
		return false
	}

	changed := false
	if joinExpr := node.GetJoinExpr(); joinExpr != nil {
		if t.visitPredicateField(&joinExpr.Quals, false) {
			changed = true
		}
		if t.visitFromItem(joinExpr.Larg) {
			changed = true
		}
		if t.visitFromItem(joinExpr.Rarg) {
			changed = true
		}
	}
	if rangeSubselect := node.GetRangeSubselect(); rangeSubselect != nil {
		if t.visitStatement(rangeSubselect.Subquery) {
			changed = true
		}
	}
	return changed
}

func (t *BooleanPredicateTransform) visitPredicateField(field **pg_query.Node, preserveNull bool) bool {
	if field == nil || *field == nil {
		return false
	}
	rewritten, changed := t.rewritePredicate(*field, preserveNull)
	if changed {
		*field = rewritten
	}
	return changed
}

func (t *BooleanPredicateTransform) rewritePredicate(node *pg_query.Node, preserveNull bool) (*pg_query.Node, bool) {
	if node == nil {
		return nil, false
	}

	if replacement := rewriteBooleanLiteralComparison(node.GetAExpr(), preserveNull); replacement != nil {
		return replacement, true
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_BoolExpr:
		changed := false
		nextPreserveNull := preserveNull
		if n.BoolExpr.Boolop == pg_query.BoolExprType_NOT_EXPR {
			nextPreserveNull = true
		}
		for i, arg := range n.BoolExpr.Args {
			rewritten, ok := t.rewritePredicate(arg, nextPreserveNull)
			if ok {
				n.BoolExpr.Args[i] = rewritten
				changed = true
			}
		}
		return node, changed

	case *pg_query.Node_CaseExpr:
		// Only searched CASE has predicate-valued WHEN conditions. The payload
		// expressions remain scalar and must not be rewritten here.
		if n.CaseExpr.Arg != nil {
			return node, false
		}
		changed := false
		for _, whenNode := range n.CaseExpr.Args {
			when := whenNode.GetCaseWhen()
			if when == nil {
				continue
			}
			rewritten, ok := t.rewritePredicate(when.Expr, false)
			if ok {
				when.Expr = rewritten
				changed = true
			}
		}
		return node, changed

	case *pg_query.Node_SubLink:
		changed := t.visitStatement(n.SubLink.Subselect)
		return node, changed

	default:
		return node, false
	}
}

func rewriteBooleanLiteralComparison(aexpr *pg_query.A_Expr, preserveNull bool) *pg_query.Node {
	if aexpr == nil || aexpr.Kind != pg_query.A_Expr_Kind_AEXPR_OP {
		return nil
	}

	op := operatorName(aexpr)
	if op == "" {
		return nil
	}

	boolValue, expr, ok := booleanComparisonArg(aexpr.Lexpr, aexpr.Rexpr)
	if !ok {
		return nil
	}

	boolTestType, ok := boolComparisonRewrite(op, boolValue)
	if !ok {
		return nil
	}

	if preserveNull {
		return wrapBooleanTestWithNullCase(expr, boolTestType, aexpr.Location)
	}
	return booleanTestNode(expr, boolTestType, aexpr.Location)
}

func operatorName(aexpr *pg_query.A_Expr) string {
	if aexpr == nil || len(aexpr.Name) == 0 {
		return ""
	}
	for i := len(aexpr.Name) - 1; i >= 0; i-- {
		if str := aexpr.Name[i].GetString_(); str != nil {
			return str.Sval
		}
	}
	return ""
}

func booleanComparisonArg(left, right *pg_query.Node) (bool, *pg_query.Node, bool) {
	if value, ok := booleanLiteralValue(right); ok && !isBooleanLiteral(left) {
		return value, left, true
	}
	if value, ok := booleanLiteralValue(left); ok && !isBooleanLiteral(right) {
		return value, right, true
	}
	return false, nil, false
}

func isBooleanLiteral(node *pg_query.Node) bool {
	_, ok := booleanLiteralValue(node)
	return ok
}

func booleanLiteralValue(node *pg_query.Node) (bool, bool) {
	if node == nil {
		return false, false
	}
	aConst := node.GetAConst()
	if aConst == nil || aConst.GetBoolval() == nil {
		return false, false
	}
	return aConst.GetBoolval().Boolval, true
}

func boolComparisonRewrite(op string, value bool) (pg_query.BoolTestType, bool) {
	switch op {
	case "=":
		if value {
			return pg_query.BoolTestType_IS_TRUE, true
		}
		return pg_query.BoolTestType_IS_FALSE, true
	case "!=", "<>":
		if value {
			return pg_query.BoolTestType_IS_FALSE, true
		}
		return pg_query.BoolTestType_IS_TRUE, true
	default:
		return pg_query.BoolTestType_BOOL_TEST_TYPE_UNDEFINED, false
	}
}

func booleanTestNode(expr *pg_query.Node, boolTestType pg_query.BoolTestType, location int32) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_BooleanTest{
			BooleanTest: &pg_query.BooleanTest{
				Arg:          expr,
				Booltesttype: boolTestType,
				Location:     location,
			},
		},
	}
}

func wrapBooleanTestWithNullCase(expr *pg_query.Node, boolTestType pg_query.BoolTestType, location int32) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_CaseExpr{
			CaseExpr: &pg_query.CaseExpr{
				Args: []*pg_query.Node{
					{
						Node: &pg_query.Node_CaseWhen{
							CaseWhen: &pg_query.CaseWhen{
								Expr: &pg_query.Node{
									Node: &pg_query.Node_NullTest{
										NullTest: &pg_query.NullTest{
											Arg:          expr,
											Nulltesttype: pg_query.NullTestType_IS_NULL,
											Location:     location,
										},
									},
								},
								Result: nullConstNode(location),
							},
						},
					},
				},
				Defresult: booleanTestNode(expr, boolTestType, location),
				Location:  location,
			},
		},
	}
}

func nullConstNode(location int32) *pg_query.Node {
	return &pg_query.Node{
		Node: &pg_query.Node_AConst{
			AConst: &pg_query.A_Const{
				Isnull:   true,
				Location: location,
			},
		},
	}
}
