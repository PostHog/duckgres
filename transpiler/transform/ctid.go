package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// CtidTransform replaces PostgreSQL's system column "ctid" with DuckDB's "rowid".
// PostgreSQL clients (including DuckDB's postgres extension) reference ctid for
// row identification. DuckDB uses rowid for the same purpose.
type CtidTransform struct{}

func NewCtidTransform() *CtidTransform {
	return &CtidTransform{}
}

func (t *CtidTransform) Name() string {
	return "ctid"
}

func (t *CtidTransform) Transform(tree *pg_query.ParseResult, _ *Result) (bool, error) {
	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		cr := node.GetColumnRef()
		if cr == nil {
			return true
		}

		for _, field := range cr.Fields {
			s := field.GetString_()
			if s != nil && strings.EqualFold(s.Sval, "ctid") {
				s.Sval = "rowid"
				changed = true
			}
		}

		return true
	})

	return changed, nil
}
