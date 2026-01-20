package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// PostgreSQLVersionString is the version string returned by version() function.
// This mimics a PostgreSQL server to satisfy client tools like Fivetran.
const PostgreSQLVersionString = "PostgreSQL 15.0 on x86_64-pc-linux-gnu, compiled by gcc, 64-bit (Duckgres/DuckDB)"

// VersionTransform replaces version() function calls with a literal string.
// DuckDB's built-in version() returns DuckDB version, but PostgreSQL clients
// expect a PostgreSQL-formatted version string.
type VersionTransform struct{}

// NewVersionTransform creates a new VersionTransform.
func NewVersionTransform() *VersionTransform {
	return &VersionTransform{}
}

func (t *VersionTransform) Name() string {
	return "version"
}

func (t *VersionTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		t.walkAndTransform(stmt.Stmt, &changed)
	}

	return changed, nil
}

func (t *VersionTransform) walkAndTransform(node *pg_query.Node, changed *bool) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			// Check if this is a version() call
			if len(n.FuncCall.Funcname) == 1 && len(n.FuncCall.Args) == 0 {
				if first := n.FuncCall.Funcname[0]; first != nil {
					if str := first.GetString_(); str != nil && strings.EqualFold(str.Sval, "version") {
						// We can't easily replace a FuncCall with a constant in the AST,
						// so we'll change it to a different approach:
						// Replace version() with a literal by modifying the function name
						// to a macro that returns the string. Since we create the macro in
						// initPgCatalog, this should work.
						// Actually, let's just mark this as needing special handling.
						// The deparser will output version() and we rely on the macro.
						// For now, leave it unchanged - the macro handles this.
					}
				}
			}
			// Recurse into function arguments
			for _, arg := range n.FuncCall.Args {
				t.walkAndTransform(arg, changed)
			}
		}

	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			t.walkSelectStmt(n.SelectStmt, changed)
		}

	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			// Check if this ResTarget contains a version() call and replace it
			if funcCall := t.getFuncCall(n.ResTarget.Val); funcCall != nil {
				if t.isVersionCall(funcCall) {
					// Replace the FuncCall with a string constant
					n.ResTarget.Val = &pg_query.Node{
						Node: &pg_query.Node_AConst{
							AConst: &pg_query.A_Const{
								Val: &pg_query.A_Const_Sval{
									Sval: &pg_query.String{Sval: PostgreSQLVersionString},
								},
							},
						},
					}
					// Set alias to "version" if not already set
					if n.ResTarget.Name == "" {
						n.ResTarget.Name = "version"
					}
					*changed = true
					return
				}
			}
			t.walkAndTransform(n.ResTarget.Val, changed)
		}

	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			t.walkAndTransform(n.TypeCast.Arg, changed)
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
		}

	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			t.walkAndTransform(n.JoinExpr.Larg, changed)
			t.walkAndTransform(n.JoinExpr.Rarg, changed)
			t.walkAndTransform(n.JoinExpr.Quals, changed)
		}

	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
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
	}
}

func (t *VersionTransform) walkSelectStmt(stmt *pg_query.SelectStmt, changed *bool) {
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

// getFuncCall extracts a FuncCall from a node if it is one
func (t *VersionTransform) getFuncCall(node *pg_query.Node) *pg_query.FuncCall {
	if node == nil {
		return nil
	}
	if fc := node.GetFuncCall(); fc != nil {
		return fc
	}
	return nil
}

// isVersionCall checks if a FuncCall is version() or pg_catalog.version()
func (t *VersionTransform) isVersionCall(fc *pg_query.FuncCall) bool {
	if fc == nil || len(fc.Funcname) == 0 || len(fc.Args) != 0 {
		return false
	}
	// Check the last part of the function name (handles both version() and pg_catalog.version())
	last := fc.Funcname[len(fc.Funcname)-1]
	if last != nil {
		if str := last.GetString_(); str != nil {
			return strings.EqualFold(str.Sval, "version")
		}
	}
	return false
}
