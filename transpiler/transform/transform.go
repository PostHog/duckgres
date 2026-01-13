package transform

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Result holds transpilation metadata that transforms can modify.
// This is passed through all transforms to accumulate information.
type Result struct {
	// ParamCount is the number of parameters found
	ParamCount int

	// IsNoOp indicates the command should be acknowledged but not executed
	IsNoOp bool

	// NoOpTag is the command tag to return for no-op commands
	NoOpTag string

	// IsIgnoredSet indicates a SET command for an unsupported parameter
	IsIgnoredSet bool

	// Error is set when a transform detects an error that should be returned to the client
	// (e.g., unrecognized configuration parameter in SHOW command)
	Error error

	// Statements contains multiple SQL statements when a query is rewritten into
	// a sequence (e.g., writable CTE rewrite). Includes setup statements and the
	// final query. When populated, the normal deparse result should be ignored.
	Statements []string

	// CleanupStatements contains statements to execute after obtaining the cursor
	// for the final query but before streaming results. Typically DROP TEMP TABLE
	// and COMMIT statements. Execute these with best-effort (ignore errors).
	CleanupStatements []string
}

// Transform defines the interface for SQL transformations.
// Each transform modifies the AST in place and can set result metadata.
type Transform interface {
	// Name returns the transform identifier for logging/debugging
	Name() string

	// Transform modifies the AST in place.
	// Returns true if any changes were made.
	// The result parameter can be used to set metadata like IsNoOp.
	Transform(tree *pg_query.ParseResult, result *Result) (changed bool, err error)
}

// WalkFunc walks all nodes in a ParseResult and calls fn for each.
// If fn returns false, walking stops.
func WalkFunc(tree *pg_query.ParseResult, fn func(node *pg_query.Node) bool) {
	for _, stmt := range tree.Stmts {
		if stmt.Stmt != nil {
			walkNode(stmt.Stmt, fn)
		}
	}
}

// walkNode recursively walks a node and its children
func walkNode(node *pg_query.Node, fn func(*pg_query.Node) bool) bool {
	if node == nil {
		return true
	}

	if !fn(node) {
		return false
	}

	// Walk children based on node type
	// pg_query uses a union type, so we need to check each possibility
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		if n.SelectStmt != nil {
			walkSelectStmt(n.SelectStmt, fn)
		}
	case *pg_query.Node_InsertStmt:
		if n.InsertStmt != nil {
			walkInsertStmt(n.InsertStmt, fn)
		}
	case *pg_query.Node_UpdateStmt:
		if n.UpdateStmt != nil {
			walkUpdateStmt(n.UpdateStmt, fn)
		}
	case *pg_query.Node_DeleteStmt:
		if n.DeleteStmt != nil {
			walkDeleteStmt(n.DeleteStmt, fn)
		}
	case *pg_query.Node_CreateStmt:
		if n.CreateStmt != nil {
			walkCreateStmt(n.CreateStmt, fn)
		}
	case *pg_query.Node_RangeVar:
		// Leaf node for table references
	case *pg_query.Node_ColumnRef:
		if n.ColumnRef != nil {
			for _, field := range n.ColumnRef.Fields {
				walkNode(field, fn)
			}
		}
	case *pg_query.Node_FuncCall:
		if n.FuncCall != nil {
			for _, name := range n.FuncCall.Funcname {
				walkNode(name, fn)
			}
			for _, arg := range n.FuncCall.Args {
				walkNode(arg, fn)
			}
		}
	case *pg_query.Node_TypeCast:
		if n.TypeCast != nil {
			walkNode(n.TypeCast.Arg, fn)
			if n.TypeCast.TypeName != nil {
				for _, name := range n.TypeCast.TypeName.Names {
					walkNode(name, fn)
				}
			}
		}
	case *pg_query.Node_AExpr:
		if n.AExpr != nil {
			walkNode(n.AExpr.Lexpr, fn)
			walkNode(n.AExpr.Rexpr, fn)
			for _, name := range n.AExpr.Name {
				walkNode(name, fn)
			}
		}
	case *pg_query.Node_BoolExpr:
		if n.BoolExpr != nil {
			for _, arg := range n.BoolExpr.Args {
				walkNode(arg, fn)
			}
		}
	case *pg_query.Node_SubLink:
		if n.SubLink != nil {
			walkNode(n.SubLink.Testexpr, fn)
			walkNode(n.SubLink.Subselect, fn)
		}
	case *pg_query.Node_List:
		if n.List != nil {
			for _, item := range n.List.Items {
				walkNode(item, fn)
			}
		}
	case *pg_query.Node_ResTarget:
		if n.ResTarget != nil {
			walkNode(n.ResTarget.Val, fn)
		}
	case *pg_query.Node_JoinExpr:
		if n.JoinExpr != nil {
			walkNode(n.JoinExpr.Larg, fn)
			walkNode(n.JoinExpr.Rarg, fn)
			walkNode(n.JoinExpr.Quals, fn)
		}
	case *pg_query.Node_FromExpr:
		if n.FromExpr != nil {
			for _, from := range n.FromExpr.Fromlist {
				walkNode(from, fn)
			}
			walkNode(n.FromExpr.Quals, fn)
		}
	case *pg_query.Node_CaseExpr:
		if n.CaseExpr != nil {
			walkNode(n.CaseExpr.Arg, fn)
			for _, when := range n.CaseExpr.Args {
				walkNode(when, fn)
			}
			walkNode(n.CaseExpr.Defresult, fn)
		}
	case *pg_query.Node_CaseWhen:
		if n.CaseWhen != nil {
			walkNode(n.CaseWhen.Expr, fn)
			walkNode(n.CaseWhen.Result, fn)
		}
	case *pg_query.Node_CoalesceExpr:
		if n.CoalesceExpr != nil {
			for _, arg := range n.CoalesceExpr.Args {
				walkNode(arg, fn)
			}
		}
	case *pg_query.Node_NullTest:
		if n.NullTest != nil {
			walkNode(n.NullTest.Arg, fn)
		}
	case *pg_query.Node_ParamRef:
		// Leaf node for $1, $2, etc.
	case *pg_query.Node_AConst:
		// Leaf node for constants
	case *pg_query.Node_String_:
		// Leaf node
	case *pg_query.Node_Integer:
		// Leaf node
	case *pg_query.Node_CommonTableExpr:
		if n.CommonTableExpr != nil {
			walkNode(n.CommonTableExpr.Ctequery, fn)
		}
	case *pg_query.Node_WithClause:
		if n.WithClause != nil {
			for _, cte := range n.WithClause.Ctes {
				walkNode(cte, fn)
			}
		}
	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect != nil {
			walkNode(n.RangeSubselect.Subquery, fn)
		}
	case *pg_query.Node_SortBy:
		if n.SortBy != nil {
			walkNode(n.SortBy.Node, fn)
		}
	case *pg_query.Node_VariableSetStmt:
		// SET statement - handled specially
	case *pg_query.Node_VariableShowStmt:
		// SHOW statement - handled specially
	case *pg_query.Node_IndexStmt:
		// CREATE INDEX - no-op for DuckLake
	case *pg_query.Node_DropStmt:
		// DROP - may need special handling
	case *pg_query.Node_VacuumStmt:
		// VACUUM - no-op
	case *pg_query.Node_ClusterStmt:
		// CLUSTER - no-op
	case *pg_query.Node_ReindexStmt:
		// REINDEX - no-op
	case *pg_query.Node_GrantStmt:
		// GRANT/REVOKE - no-op
	case *pg_query.Node_CommentStmt:
		// COMMENT - no-op
	case *pg_query.Node_AlterTableStmt:
		if n.AlterTableStmt != nil {
			for _, cmd := range n.AlterTableStmt.Cmds {
				walkNode(cmd, fn)
			}
		}
	case *pg_query.Node_AlterTableCmd:
		if n.AlterTableCmd != nil {
			walkNode(n.AlterTableCmd.Def, fn)
		}
	case *pg_query.Node_ColumnDef:
		if n.ColumnDef != nil {
			if n.ColumnDef.TypeName != nil {
				for _, name := range n.ColumnDef.TypeName.Names {
					walkNode(name, fn)
				}
			}
			for _, constraint := range n.ColumnDef.Constraints {
				walkNode(constraint, fn)
			}
			walkNode(n.ColumnDef.RawDefault, fn)
		}
	case *pg_query.Node_Constraint:
		if n.Constraint != nil {
			walkNode(n.Constraint.RawExpr, fn)
		}
	}

	return true
}

func walkSelectStmt(stmt *pg_query.SelectStmt, fn func(*pg_query.Node) bool) {
	if stmt == nil {
		return
	}

	for _, target := range stmt.TargetList {
		walkNode(target, fn)
	}
	for _, from := range stmt.FromClause {
		walkNode(from, fn)
	}
	walkNode(stmt.WhereClause, fn)
	for _, group := range stmt.GroupClause {
		walkNode(group, fn)
	}
	walkNode(stmt.HavingClause, fn)
	for _, window := range stmt.WindowClause {
		walkNode(window, fn)
	}
	for _, sort := range stmt.SortClause {
		walkNode(sort, fn)
	}
	walkNode(stmt.LimitCount, fn)
	walkNode(stmt.LimitOffset, fn)
	if stmt.WithClause != nil {
		walkNode(&pg_query.Node{Node: &pg_query.Node_WithClause{WithClause: stmt.WithClause}}, fn)
	}
	if stmt.Larg != nil {
		walkSelectStmt(stmt.Larg, fn)
	}
	if stmt.Rarg != nil {
		walkSelectStmt(stmt.Rarg, fn)
	}
}

func walkInsertStmt(stmt *pg_query.InsertStmt, fn func(*pg_query.Node) bool) {
	if stmt == nil {
		return
	}

	if stmt.Relation != nil {
		walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: stmt.Relation}}, fn)
	}
	for _, col := range stmt.Cols {
		walkNode(col, fn)
	}
	walkNode(stmt.SelectStmt, fn)
	for _, target := range stmt.ReturningList {
		walkNode(target, fn)
	}
}

func walkUpdateStmt(stmt *pg_query.UpdateStmt, fn func(*pg_query.Node) bool) {
	if stmt == nil {
		return
	}

	if stmt.Relation != nil {
		walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: stmt.Relation}}, fn)
	}
	for _, target := range stmt.TargetList {
		walkNode(target, fn)
	}
	walkNode(stmt.WhereClause, fn)
	for _, from := range stmt.FromClause {
		walkNode(from, fn)
	}
	for _, target := range stmt.ReturningList {
		walkNode(target, fn)
	}
}

func walkDeleteStmt(stmt *pg_query.DeleteStmt, fn func(*pg_query.Node) bool) {
	if stmt == nil {
		return
	}

	if stmt.Relation != nil {
		walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: stmt.Relation}}, fn)
	}
	walkNode(stmt.WhereClause, fn)
	for _, target := range stmt.ReturningList {
		walkNode(target, fn)
	}
}

func walkCreateStmt(stmt *pg_query.CreateStmt, fn func(*pg_query.Node) bool) {
	if stmt == nil {
		return
	}

	if stmt.Relation != nil {
		walkNode(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: stmt.Relation}}, fn)
	}
	for _, elt := range stmt.TableElts {
		walkNode(elt, fn)
	}
	for _, constraint := range stmt.Constraints {
		walkNode(constraint, fn)
	}
}
