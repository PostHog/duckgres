package transform

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// PublicSchemaTransform maps PostgreSQL's default "public" schema to DuckDB's default "main" schema.
//
// Duckgres exposes DuckDB's "main" schema as "public" via catalog compatibility views (pg_namespace,
// information_schema, etc.). Without this transform, schema-qualified user queries like
// `SELECT * FROM public.mytable` are sent to DuckDB unchanged and fail because DuckDB has no "public"
// schema by default.
type PublicSchemaTransform struct{}

func NewPublicSchemaTransform() *PublicSchemaTransform {
	return &PublicSchemaTransform{}
}

func (t *PublicSchemaTransform) Name() string {
	return "publicschema"
}

func (t *PublicSchemaTransform) Transform(tree *pg_query.ParseResult, _ *Result) (bool, error) {
	changed := false

	WalkFunc(tree, func(node *pg_query.Node) bool {
		rv := node.GetRangeVar()
		if rv == nil {
			return true
		}

		// Rewrite public → main regardless of whether a catalog is present.
		if strings.EqualFold(rv.Schemaname, "public") {
			rv.Schemaname = "main"
			changed = true
		}

		return true
	})

	return changed, nil
}
