package transform

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// LockingTransform strips SELECT locking clauses (FOR UPDATE, FOR SHARE, etc.)
// that DuckDB doesn't support.
type LockingTransform struct{}

// NewLockingTransform creates a new LockingTransform.
func NewLockingTransform() *LockingTransform {
	return &LockingTransform{}
}

func (t *LockingTransform) Name() string {
	return "locking"
}

func (t *LockingTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	changed := false

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		switch n := stmt.Stmt.Node.(type) {
		case *pg_query.Node_SelectStmt:
			if n.SelectStmt != nil && len(n.SelectStmt.LockingClause) > 0 {
				// Strip the locking clause
				n.SelectStmt.LockingClause = nil
				changed = true
			}
		}
	}

	return changed, nil
}
