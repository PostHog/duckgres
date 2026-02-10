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
		// Handle ctid column references: ctid → rowid
		if cr := node.GetColumnRef(); cr != nil {
			for _, field := range cr.Fields {
				s := field.GetString_()
				if s != nil && strings.EqualFold(s.Sval, "ctid") {
					s.Sval = "rowid"
					changed = true
				}
			}
			return true
		}

		// Handle: ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid
		// DuckDB's postgres extension sends this pattern for full-table row scans.
		// DuckDB doesn't have a tid type, so replace the entire BETWEEN with TRUE.
		if aexpr := node.GetAExpr(); aexpr != nil && aexpr.Kind == pg_query.A_Expr_Kind_AEXPR_BETWEEN {
			if isCtidRef(aexpr.Lexpr) && hasTidCasts(aexpr.Rexpr) {
				node.Node = &pg_query.Node_AConst{
					AConst: &pg_query.A_Const{
						Val: &pg_query.A_Const_Boolval{
							Boolval: &pg_query.Boolean{Boolval: true},
						},
					},
				}
				changed = true
				return false
			}
		}

		return true
	})

	return changed, nil
}

// isCtidRef checks if a node is a ColumnRef referencing ctid or rowid.
func isCtidRef(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	cr := node.GetColumnRef()
	if cr == nil {
		return false
	}
	for _, field := range cr.Fields {
		s := field.GetString_()
		if s != nil && (strings.EqualFold(s.Sval, "ctid") || strings.EqualFold(s.Sval, "rowid")) {
			return true
		}
	}
	return false
}

// hasTidCasts checks if a BETWEEN bounds list contains ::tid type casts.
func hasTidCasts(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	list := node.GetList()
	if list == nil {
		return false
	}
	for _, item := range list.Items {
		tc := item.GetTypeCast()
		if tc == nil || tc.TypeName == nil {
			continue
		}
		for _, name := range tc.TypeName.Names {
			s := name.GetString_()
			if s != nil && strings.EqualFold(s.Sval, "tid") {
				return true
			}
		}
	}
	return false
}
