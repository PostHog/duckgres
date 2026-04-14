package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// LogicalCatalogTransform rewrites three-part references that use the
// client-visible logical database name to the executable physical catalog.
type LogicalCatalogTransform struct {
	LogicalDatabaseName string
	PhysicalCatalogName string
}

func NewLogicalCatalogTransform(logicalDatabaseName, physicalCatalogName string) *LogicalCatalogTransform {
	if physicalCatalogName == "" {
		physicalCatalogName = "ducklake"
	}
	return &LogicalCatalogTransform{
		LogicalDatabaseName: logicalDatabaseName,
		PhysicalCatalogName: physicalCatalogName,
	}
}

func (t *LogicalCatalogTransform) Name() string {
	return "logicalcatalog"
}

func (t *LogicalCatalogTransform) Transform(tree *pg_query.ParseResult, _ *Result) (bool, error) {
	if t.LogicalDatabaseName == "" {
		return false, nil
	}

	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		if t.rewriteRangeVar(node.GetRangeVar()) {
			changed = true
		}
		return true
	})

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		switch n := stmt.Stmt.Node.(type) {
		case *pg_query.Node_RenameStmt:
			if n.RenameStmt != nil && t.rewriteRangeVar(n.RenameStmt.Relation) {
				changed = true
			}
		case *pg_query.Node_DropStmt:
			if n.DropStmt != nil && t.rewriteDropObjects(n.DropStmt.Objects) {
				changed = true
			}
		}
	}

	return changed, nil
}

func (t *LogicalCatalogTransform) rewriteRangeVar(rv *pg_query.RangeVar) bool {
	if rv == nil || rv.Catalogname == "" || !strings.EqualFold(rv.Catalogname, t.LogicalDatabaseName) {
		return false
	}

	rv.Catalogname = t.PhysicalCatalogName
	if strings.EqualFold(rv.Schemaname, "public") {
		rv.Schemaname = "main"
	}
	return true
}

func (t *LogicalCatalogTransform) rewriteDropObjects(objects []*pg_query.Node) bool {
	changed := false

	for _, obj := range objects {
		list := obj.GetList()
		if list == nil || len(list.Items) < 3 {
			continue
		}

		catalog := list.Items[0].GetString_()
		if catalog == nil || !strings.EqualFold(catalog.Sval, t.LogicalDatabaseName) {
			continue
		}

		catalog.Sval = t.PhysicalCatalogName
		schema := list.Items[1].GetString_()
		if schema != nil && strings.EqualFold(schema.Sval, "public") {
			schema.Sval = "main"
		}
		changed = true
	}

	return changed
}
