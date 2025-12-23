package transpiler

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/transform"
)

// Transpiler converts PostgreSQL SQL to DuckDB-compatible SQL
type Transpiler struct {
	config     Config
	transforms []transform.Transform
}

// New creates a Transpiler with the given configuration.
// It registers all transforms appropriate for the config.
func New(cfg Config) *Transpiler {
	t := &Transpiler{
		config:     cfg,
		transforms: make([]transform.Transform, 0),
	}

	// Core transforms - always applied
	// Order matters: more specific transforms should come first

	// 1. pg_catalog schema and view mappings
	t.transforms = append(t.transforms, transform.NewPgCatalogTransformWithConfig(cfg.DuckLakeMode))

	// 2. information_schema mappings to compat views
	t.transforms = append(t.transforms, transform.NewInformationSchemaTransformWithConfig(cfg.DuckLakeMode))

	// 3. Type mappings (JSONB->JSON, CHAR->TEXT, etc.)
	t.transforms = append(t.transforms, transform.NewTypeMappingTransform())

	// 4. Type casts (::regtype -> ::varchar)
	t.transforms = append(t.transforms, transform.NewTypeCastTransform())

	// 5. Function mappings (array_agg->list, string_to_array->string_split, etc.)
	t.transforms = append(t.transforms, transform.NewFunctionTransform())

	// 6. Operator mappings (regex operators, etc.)
	t.transforms = append(t.transforms, transform.NewOperatorTransform())

	// 7. version() replacement
	t.transforms = append(t.transforms, transform.NewVersionTransform())

	// 8. SET/SHOW command handling
	t.transforms = append(t.transforms, transform.NewSetShowTransform())

	// 9. ON CONFLICT handling
	t.transforms = append(t.transforms, transform.NewOnConflictTransform())

	// DuckLake-specific transforms
	if cfg.DuckLakeMode {
		// DDL transforms (strip unsupported constraints, handle no-ops)
		t.transforms = append(t.transforms, transform.NewDDLTransform())
		// Temp table transforms (qualify staging table references with temp schema)
		t.transforms = append(t.transforms, transform.NewTempTableTransform())
	}

	// Placeholder transform only when needed (extended query protocol)
	if cfg.ConvertPlaceholders {
		t.transforms = append(t.transforms, transform.NewPlaceholderTransform())
	}

	return t
}

// Transpile converts a PostgreSQL SQL statement to DuckDB-compatible SQL.
// Returns the Result containing the transformed SQL and metadata.
func (t *Transpiler) Transpile(sql string) (*Result, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return &Result{SQL: sql}, nil
	}

	// Parse the SQL into an AST
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}

	// Create transform result to collect metadata from transforms
	transformResult := &transform.Result{}

	// Apply all transforms
	for _, tr := range t.transforms {
		changed, err := tr.Transform(tree, transformResult)
		if err != nil {
			return nil, err
		}

		// Check for transform-detected errors (e.g., unrecognized config param)
		if transformResult.Error != nil {
			return &Result{
				SQL:   sql,
				Error: transformResult.Error,
			}, nil
		}

		// Check for early exit conditions
		if transformResult.IsNoOp || transformResult.IsIgnoredSet {
			// For no-op commands, return the original SQL (it won't be executed)
			return &Result{
				SQL:          sql,
				ParamCount:   transformResult.ParamCount,
				IsNoOp:       transformResult.IsNoOp,
				NoOpTag:      transformResult.NoOpTag,
				IsIgnoredSet: transformResult.IsIgnoredSet,
			}, nil
		}

		_ = changed // We track changes but don't need to act on it currently
	}

	// Deparse the modified AST back to SQL
	deparsed, err := pg_query.Deparse(tree)
	if err != nil {
		return nil, err
	}

	return &Result{
		SQL:          deparsed,
		ParamCount:   transformResult.ParamCount,
		IsNoOp:       transformResult.IsNoOp,
		NoOpTag:      transformResult.NoOpTag,
		IsIgnoredSet: transformResult.IsIgnoredSet,
	}, nil
}

// TranspileMulti handles SQL strings that may contain multiple statements.
// Returns a slice of Results, one for each statement.
func (t *Transpiler) TranspileMulti(sql string) ([]*Result, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return []*Result{{SQL: sql}}, nil
	}

	// Split by semicolons (simple approach - pg_query handles this better)
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}

	results := make([]*Result, 0, len(tree.Stmts))

	for i := range tree.Stmts {
		// Create a new tree with just this statement
		singleTree := &pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{tree.Stmts[i]},
		}

		transformResult := &transform.Result{}

		// Apply all transforms
		for _, tr := range t.transforms {
			_, err := tr.Transform(singleTree, transformResult)
			if err != nil {
				return nil, err
			}

			if transformResult.IsNoOp || transformResult.IsIgnoredSet {
				break
			}
		}

		result := &Result{
			ParamCount:   transformResult.ParamCount,
			IsNoOp:       transformResult.IsNoOp,
			NoOpTag:      transformResult.NoOpTag,
			IsIgnoredSet: transformResult.IsIgnoredSet,
		}

		if !transformResult.IsNoOp && !transformResult.IsIgnoredSet {
			deparsed, err := pg_query.Deparse(singleTree)
			if err != nil {
				return nil, err
			}
			result.SQL = deparsed
		}

		results = append(results, result)
	}

	return results, nil
}
