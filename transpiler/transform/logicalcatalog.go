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
	// MapPublicToMain rewrites a "public" schema reference to "main" when
	// rewriting the catalog. False for backends whose physical schema is "public".
	MapPublicToMain bool
}

func NewLogicalCatalogTransform(logicalDatabaseName, physicalCatalogName string, mapPublicToMain bool) *LogicalCatalogTransform {
	if physicalCatalogName == "" {
		physicalCatalogName = "ducklake"
	}
	return &LogicalCatalogTransform{
		LogicalDatabaseName: logicalDatabaseName,
		PhysicalCatalogName: physicalCatalogName,
		MapPublicToMain:     mapPublicToMain,
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
		case *pg_query.Node_CommentStmt:
			if parts := commentRelationName(n.CommentStmt); len(parts) == 3 {
				if t.rewriteQualifiedName(parts) {
					changed = true
				}
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
	if t.MapPublicToMain && strings.EqualFold(rv.Schemaname, "public") {
		rv.Schemaname = "main"
	}
	return true
}

func (t *LogicalCatalogTransform) rewriteDropObjects(objects []*pg_query.Node) bool {
	changed := false
	for _, object := range objects {
		if list := object.GetList(); list != nil && t.rewriteQualifiedName(list.Items) {
			changed = true
		}
	}
	return changed
}

func (t *LogicalCatalogTransform) rewriteQualifiedName(parts []*pg_query.Node) bool {
	if len(parts) < 3 {
		return false
	}
	catalog := parts[0].GetString_()
	if catalog == nil || !strings.EqualFold(catalog.Sval, t.LogicalDatabaseName) {
		return false
	}
	catalog.Sval = t.PhysicalCatalogName
	if t.MapPublicToMain {
		schema := parts[1].GetString_()
		if schema != nil && strings.EqualFold(schema.Sval, "public") {
			schema.Sval = "main"
		}
	}
	return true
}

func commentRelationName(stmt *pg_query.CommentStmt) []*pg_query.Node {
	object := stmt.GetObject().GetList()
	if object == nil {
		return nil
	}
	switch stmt.GetObjtype() {
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_VIEW:
		return object.Items
	case pg_query.ObjectType_OBJECT_COLUMN:
		if len(object.Items) > 0 {
			return object.Items[:len(object.Items)-1]
		}
	}
	return nil
}
