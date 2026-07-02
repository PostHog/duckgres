package transform

import pg_query "github.com/pganalyze/pg_query_go/v6"

// OnConflictTransform handles PostgreSQL ON CONFLICT (upsert) syntax.
//
// DuckDB supports ON CONFLICT for catalogs that enforce unique constraints.
// Constraint-less lake catalogs do not, so they reject ON CONFLICT instead of
// trying to emulate PostgreSQL uniqueness semantics.
type OnConflictTransform struct {
	RejectOnConflict bool
}

func NewOnConflictTransform() *OnConflictTransform {
	return &OnConflictTransform{RejectOnConflict: false}
}

func NewOnConflictTransformWithConfig(rejectOnConflict bool) *OnConflictTransform {
	return &OnConflictTransform{RejectOnConflict: rejectOnConflict}
}

func (t *OnConflictTransform) Name() string {
	return "onconflict"
}

func (t *OnConflictTransform) Transform(tree *pg_query.ParseResult, result *Result) (bool, error) {
	if !t.RejectOnConflict {
		return false, nil
	}

	WalkFunc(tree, func(node *pg_query.Node) bool {
		if insert := node.GetInsertStmt(); insert != nil && insert.OnConflictClause != nil {
			result.Error = NewFeatureNotSupported(
				"ON CONFLICT is not supported: this catalog does not enforce unique constraints")
			return false
		}
		return true
	})

	return false, nil
}
