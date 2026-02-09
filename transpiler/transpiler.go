package transpiler

import (
	"regexp"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/transform"
)

// paramRegex matches PostgreSQL-style $N parameter placeholders
var paramRegex = regexp.MustCompile(`\$(\d+)`)

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

	// 0. Writable CTE transform - MUST BE FIRST
	// This transform rewrites PostgreSQL writable CTEs into multi-statement sequences.
	// It must run before other transforms because it completely rewrites the query structure.
	t.transforms = append(t.transforms, transform.NewWritableCTETransform())

	// 1. version() replacement - MUST run before PgCatalogTransform
	// Otherwise PgCatalogTransform adds memory.main. prefix and VersionTransform won't match it
	t.transforms = append(t.transforms, transform.NewVersionTransform())

	// 2. pg_catalog schema and view mappings
	t.transforms = append(t.transforms, transform.NewPgCatalogTransformWithConfig(cfg.DuckLakeMode))

	// 3. information_schema mappings to compat views
	t.transforms = append(t.transforms, transform.NewInformationSchemaTransformWithConfig(cfg.DuckLakeMode))

	// 3.1 Map PostgreSQL "public" schema to DuckDB "main"
	t.transforms = append(t.transforms, transform.NewPublicSchemaTransform())

	// 4. Type mappings (JSONB->JSON, CHAR->TEXT, etc.)
	t.transforms = append(t.transforms, transform.NewTypeMappingTransform())

	// 5. Type casts (::regtype -> ::varchar)
	t.transforms = append(t.transforms, transform.NewTypeCastTransform())

	// 6. Function mappings (array_agg->list, string_to_array->string_split, etc.)
	t.transforms = append(t.transforms, transform.NewFunctionTransform())

	// 7. Function alias normalization (current_database() -> AS current_database)
	t.transforms = append(t.transforms, transform.NewFuncAliasTransform())

	// 8. Operator mappings (regex operators, etc.)
	t.transforms = append(t.transforms, transform.NewOperatorTransform())

	// 9. SET/SHOW command handling
	t.transforms = append(t.transforms, transform.NewSetShowTransform())

	// 10. _pg_expandarray handling (PostgreSQL array expansion function used by JDBC)
	t.transforms = append(t.transforms, transform.NewExpandArrayTransform())

	// 11. ON CONFLICT handling (strips ON CONFLICT in DuckLake mode since constraints don't exist)
	t.transforms = append(t.transforms, transform.NewOnConflictTransformWithConfig(cfg.DuckLakeMode))

	// 12. Locking clause removal (FOR UPDATE, FOR SHARE, etc.) - DuckDB doesn't support these
	t.transforms = append(t.transforms, transform.NewLockingTransform())

	// 13. ctid → rowid mapping (PostgreSQL system column to DuckDB equivalent)
	t.transforms = append(t.transforms, transform.NewCtidTransform())

	// 14. Strip catalog qualifiers (duckgres is single-catalog)
	t.transforms = append(t.transforms, transform.NewCatalogStripTransform())

	// DDL transforms only when DuckLake mode is enabled
	if cfg.DuckLakeMode {
		t.transforms = append(t.transforms, transform.NewDDLTransform())
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
		// PostgreSQL parsing failed - signal that we should try native DuckDB execution
		// Count parameters using regex since we can't use the AST
		return &Result{
			SQL:              sql,
			FallbackToNative: true,
			ParamCount:       countParametersRegex(sql),
		}, nil
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

		// Check for multi-statement rewrite (e.g., writable CTE)
		// When a transform produces multiple statements, we skip remaining transforms
		// and return the statements directly.
		if len(transformResult.Statements) > 0 {
			return &Result{
				SQL:               sql, // Keep original for logging
				Statements:        transformResult.Statements,
				CleanupStatements: transformResult.CleanupStatements,
				ParamCount:        transformResult.ParamCount,
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

// CountParameters parses SQL and counts $N placeholders without applying any transforms.
// This is used for prepared statements in native DuckDB mode where we skip transpilation
// but still need to know the parameter count for the extended query protocol.
func CountParameters(sql string) (int, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return 0, nil
	}

	tree, err := pg_query.Parse(sql)
	if err != nil {
		return 0, err
	}

	// Use the PlaceholderTransform just for counting
	pt := transform.NewPlaceholderTransform()
	result := &transform.Result{}
	_, err = pt.Transform(tree, result)
	if err != nil {
		return 0, err
	}

	return result.ParamCount, nil
}

// countParametersRegex counts $N parameter placeholders using regex.
// This is a fallback for when pg_query can't parse the SQL (e.g., DuckDB-specific syntax).
// It finds the highest $N placeholder number, which represents the parameter count.
func countParametersRegex(sql string) int {
	matches := paramRegex.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return 0
	}

	maxParam := 0
	for _, match := range matches {
		if len(match) > 1 {
			if n, err := strconv.Atoi(match[1]); err == nil && n > maxParam {
				maxParam = n
			}
		}
	}
	return maxParam
}

// ConvertAlterTableToAlterView transforms an ALTER TABLE RENAME statement
// to ALTER VIEW RENAME. This is used to retry failed ALTER TABLE commands
// when DuckDB reports that the target is a view, not a table.
// Returns the transformed SQL and true if successful, or the original SQL
// and false if the input is not an ALTER TABLE RENAME statement.
func ConvertAlterTableToAlterView(sql string) (string, bool) {
	tree, err := pg_query.Parse(sql)
	if err != nil || len(tree.Stmts) == 0 {
		return sql, false
	}

	stmt := tree.Stmts[0].Stmt
	if stmt == nil {
		return sql, false
	}

	renameStmt, ok := stmt.Node.(*pg_query.Node_RenameStmt)
	if !ok || renameStmt.RenameStmt == nil {
		return sql, false
	}

	// Only transform if it's an ALTER TABLE RENAME (renameType == OBJECT_TABLE)
	if renameStmt.RenameStmt.RenameType != pg_query.ObjectType_OBJECT_TABLE {
		return sql, false
	}

	// Change to ALTER VIEW
	renameStmt.RenameStmt.RenameType = pg_query.ObjectType_OBJECT_VIEW
	renameStmt.RenameStmt.RelationType = pg_query.ObjectType_OBJECT_VIEW

	result, err := pg_query.Deparse(tree)
	if err != nil {
		return sql, false
	}
	return result, true
}

// ConvertDropTableToDropView transforms a DROP TABLE [IF EXISTS] statement
// to DROP VIEW [IF EXISTS]. This is used to retry failed DROP TABLE commands
// when DuckDB reports that the target is a view, not a table.
// Returns the transformed SQL and true if successful, or the original SQL
// and false if the input is not a DROP TABLE statement.
func ConvertDropTableToDropView(sql string) (string, bool) {
	tree, err := pg_query.Parse(sql)
	if err != nil || len(tree.Stmts) == 0 {
		return sql, false
	}

	stmt := tree.Stmts[0].Stmt
	if stmt == nil {
		return sql, false
	}

	dropStmt, ok := stmt.Node.(*pg_query.Node_DropStmt)
	if !ok || dropStmt.DropStmt == nil {
		return sql, false
	}

	// Only transform if it's a DROP TABLE
	if dropStmt.DropStmt.RemoveType != pg_query.ObjectType_OBJECT_TABLE {
		return sql, false
	}

	// Change to DROP VIEW
	dropStmt.DropStmt.RemoveType = pg_query.ObjectType_OBJECT_VIEW

	result, err := pg_query.Deparse(tree)
	if err != nil {
		return sql, false
	}
	return result, true
}
