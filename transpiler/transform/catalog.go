package transform

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// CatalogStripTransform removes explicit catalog qualifiers from table references.
// Duckgres is a single-catalog server, so three-part names like "duckgres"."schema"."table"
// should be reduced to "schema"."table". This is needed because DuckDB's postgres extension
// sends fully-qualified names using the ATTACH alias as the catalog, but on the duckgres
// side the default catalog is the only one available.
type CatalogStripTransform struct{}

func NewCatalogStripTransform() *CatalogStripTransform {
	return &CatalogStripTransform{}
}

func (t *CatalogStripTransform) Name() string {
	return "catalogstrip"
}

func (t *CatalogStripTransform) Transform(tree *pg_query.ParseResult, _ *Result) (bool, error) {
	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		rv := node.GetRangeVar()
		if rv == nil {
			return true
		}

		// Skip "memory" - it's DuckDB's system catalog where pg_catalog views live.
		// PgCatalogTransform explicitly sets memory.main.* for these views.
		if rv.Catalogname != "" && rv.Catalogname != "memory" {
			rv.Catalogname = ""
			changed = true
		}

		return true
	})

	return changed, nil
}
