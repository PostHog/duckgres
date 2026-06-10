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

	// Locking clauses can appear at any depth (subquery, CTE, EXISTS sublink,
	// set-operation arm), and DuckDB rejects them all.
	WalkFunc(tree, func(node *pg_query.Node) bool {
		if sel := node.GetSelectStmt(); sel != nil {
			if stripLockingClauses(sel) {
				changed = true
			}
		}
		return true
	})

	return changed, nil
}

// stripLockingClauses clears the locking clause on sel and on its
// set-operation arms. walkSelectStmt recurses into Larg/Rarg without invoking
// the visitor on them, so the arms must be handled here.
func stripLockingClauses(sel *pg_query.SelectStmt) bool {
	if sel == nil {
		return false
	}
	changed := false
	if len(sel.LockingClause) > 0 {
		sel.LockingClause = nil
		changed = true
	}
	if stripLockingClauses(sel.Larg) {
		changed = true
	}
	if stripLockingClauses(sel.Rarg) {
		changed = true
	}
	return changed
}
