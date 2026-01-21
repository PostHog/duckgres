package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// FuncAliasTransform adds column aliases to function calls that PostgreSQL
// would normally display without parentheses. For example, "SELECT current_database()"
// returns column name "current_database" in PostgreSQL, not "current_database()".
type FuncAliasTransform struct{}

func NewFuncAliasTransform() *FuncAliasTransform {
	return &FuncAliasTransform{}
}

func (t *FuncAliasTransform) Name() string {
	return "funcalias"
}

// Functions that should have their parentheses stripped from column names
var funcAliasMapping = map[string]string{
	"current_database": "current_database",
	"current_schema":   "current_schema",
	"current_user":     "current_user",
	"session_user":     "session_user",
	"current_catalog":  "current_catalog",
}

func (t *FuncAliasTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		if selectStmt := stmt.Stmt.GetSelectStmt(); selectStmt != nil {
			if t.transformSelectTargets(selectStmt) {
				changed = true
			}
		}
	}

	return changed, nil
}

func (t *FuncAliasTransform) transformSelectTargets(stmt *pg_query.SelectStmt) bool {
	if stmt == nil {
		return false
	}

	changed := false

	for _, target := range stmt.TargetList {
		if resTarget := target.GetResTarget(); resTarget != nil {
			// Only add alias if one isn't already set
			if resTarget.Name == "" {
				if funcCall := resTarget.Val.GetFuncCall(); funcCall != nil {
					if alias := t.getFuncAlias(funcCall); alias != "" {
						resTarget.Name = alias
						changed = true
					}
				}
			}
		}
	}

	// Recurse into FROM clause subqueries
	for _, from := range stmt.FromClause {
		if t.transformFromClause(from) {
			changed = true
		}
	}

	// Recurse into subqueries (UNION, etc.)
	if stmt.Larg != nil {
		if t.transformSelectTargets(stmt.Larg) {
			changed = true
		}
	}
	if stmt.Rarg != nil {
		if t.transformSelectTargets(stmt.Rarg) {
			changed = true
		}
	}

	return changed
}

func (t *FuncAliasTransform) transformFromClause(node *pg_query.Node) bool {
	if node == nil {
		return false
	}

	changed := false

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil && n.RangeSubselect.Subquery != nil {
			if selectStmt := n.RangeSubselect.Subquery.GetSelectStmt(); selectStmt != nil {
				if t.transformSelectTargets(selectStmt) {
					changed = true
				}
			}
		}
	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			if t.transformFromClause(n.JoinExpr.Larg) {
				changed = true
			}
			if t.transformFromClause(n.JoinExpr.Rarg) {
				changed = true
			}
		}
	}

	return changed
}

func (t *FuncAliasTransform) getFuncAlias(fc *pg_query.FuncCall) string {
	if fc == nil || len(fc.Funcname) == 0 {
		return ""
	}

	// Get the function name (last element)
	var funcName string
	for _, name := range fc.Funcname {
		if str := name.GetString_(); str != nil {
			funcName = strings.ToLower(str.Sval)
		}
	}

	if alias, ok := funcAliasMapping[funcName]; ok {
		return alias
	}

	return ""
}
