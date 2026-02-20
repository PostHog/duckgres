package transpiler

import (
	"log/slog"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/transform"
)

// TransformFlags is a bitmask indicating which transforms a query needs.
type TransformFlags uint32

const (
	FlagWritableCTE  TransformFlags = 1 << iota // Writable CTE rewrite
	FlagVersion                                  // version() replacement
	FlagPgCatalog                                // pg_catalog schema/view mappings
	FlagInfoSchema                               // information_schema mappings
	FlagPublicSchema                             // public -> main schema mapping
	FlagTypeMapping                              // Type mappings (JSONB->JSON, etc.)
	FlagTypeCast                                 // Type casts (::regtype -> ::varchar)
	FlagFunctions                                // Function mappings (array_agg->list, etc.)
	FlagFuncAlias                                // Function alias normalization
	FlagOperators                                // Operator mappings (regex, JSON)
	FlagSetShow                                  // SET/SHOW command handling
	FlagExpandArray                              // _pg_expandarray handling
	FlagOnConflict                               // ON CONFLICT handling
	FlagLocking                                  // FOR UPDATE/SHARE removal
	FlagCtid                                     // ctid -> rowid mapping
	FlagDDL                                      // DDL constraint stripping
	FlagPlaceholder                              // $1/$2 placeholder conversion
	flagSentinel                                 // must be last — used to derive FlagAll

	FlagAll TransformFlags = flagSentinel - 1 // All flags set
)

// Classification is the result of pre-parse query classification.
type Classification struct {
	// Direct means the query needs no transforms and can go straight to DuckDB.
	Direct bool
	// Flags indicates which transforms are needed (only meaningful when Direct is false).
	Flags TransformFlags
}

// taggedTransform pairs a transform with its bitmask flag for selective execution.
type taggedTransform struct {
	flag TransformFlags
	tr   transform.Transform
}
// Transpiler converts PostgreSQL SQL to DuckDB-compatible SQL
type Transpiler struct {
	config     Config
	transforms []taggedTransform
}

// New creates a Transpiler with the given configuration.
// It registers all transforms appropriate for the config.
func New(cfg Config) *Transpiler {
	t := &Transpiler{
		config:     cfg,
		transforms: make([]taggedTransform, 0),
	}

	// Core transforms - always registered
	// Order matters: more specific transforms should come first

	// 0. Writable CTE transform - MUST BE FIRST
	t.transforms = append(t.transforms, taggedTransform{FlagWritableCTE, transform.NewWritableCTETransform()})

	// 1. version() replacement - MUST run before PgCatalogTransform
	t.transforms = append(t.transforms, taggedTransform{FlagVersion, transform.NewVersionTransform()})

	// 2. pg_catalog schema and view mappings
	t.transforms = append(t.transforms, taggedTransform{FlagPgCatalog, transform.NewPgCatalogTransformWithConfig(cfg.DuckLakeMode)})

	// 3. information_schema mappings to compat views
	t.transforms = append(t.transforms, taggedTransform{FlagInfoSchema, transform.NewInformationSchemaTransformWithConfig(cfg.DuckLakeMode)})

	// 3.1 Map PostgreSQL "public" schema to DuckDB "main"
	t.transforms = append(t.transforms, taggedTransform{FlagPublicSchema, transform.NewPublicSchemaTransform()})

	// 4. Type mappings (JSONB->JSON, CHAR->TEXT, etc.)
	t.transforms = append(t.transforms, taggedTransform{FlagTypeMapping, transform.NewTypeMappingTransform()})

	// 5. Type casts (::regtype -> ::varchar)
	t.transforms = append(t.transforms, taggedTransform{FlagTypeCast, transform.NewTypeCastTransform()})

	// 6. Function mappings (array_agg->list, string_to_array->string_split, etc.)
	t.transforms = append(t.transforms, taggedTransform{FlagFunctions, transform.NewFunctionTransform()})

	// 7. Function alias normalization (current_database() -> AS current_database)
	t.transforms = append(t.transforms, taggedTransform{FlagFuncAlias, transform.NewFuncAliasTransform()})

	// 8. Operator mappings (regex operators, etc.)
	t.transforms = append(t.transforms, taggedTransform{FlagOperators, transform.NewOperatorTransform()})

	// 9. SET/SHOW command handling
	t.transforms = append(t.transforms, taggedTransform{FlagSetShow, transform.NewSetShowTransform()})

	// 10. _pg_expandarray handling (PostgreSQL array expansion function used by JDBC)
	t.transforms = append(t.transforms, taggedTransform{FlagExpandArray, transform.NewExpandArrayTransform()})

	// 11. ON CONFLICT handling (strips ON CONFLICT in DuckLake mode since constraints don't exist)
	t.transforms = append(t.transforms, taggedTransform{FlagOnConflict, transform.NewOnConflictTransformWithConfig(cfg.DuckLakeMode)})

	// 12. Locking clause removal (FOR UPDATE, FOR SHARE, etc.) - DuckDB doesn't support these
	t.transforms = append(t.transforms, taggedTransform{FlagLocking, transform.NewLockingTransform()})

	// 13. ctid → rowid mapping (PostgreSQL system column to DuckDB equivalent)
	t.transforms = append(t.transforms, taggedTransform{FlagCtid, transform.NewCtidTransform()})

	// DDL transforms only when DuckLake mode is enabled
	if cfg.DuckLakeMode {
		t.transforms = append(t.transforms, taggedTransform{FlagDDL, transform.NewDDLTransform()})
	}

	// Placeholder transform only when needed (extended query protocol)
	if cfg.ConvertPlaceholders {
		t.transforms = append(t.transforms, taggedTransform{FlagPlaceholder, transform.NewPlaceholderTransform()})
	}

	return t
}

// Transpile converts a PostgreSQL SQL statement to DuckDB-compatible SQL.
// It classifies the query first to skip unnecessary work:
//   - Tier 0: No PG-specific patterns detected → return original SQL directly (no parse)
//   - Tier 1: PG-specific patterns detected → parse and apply only relevant transforms
//   - Tier 2: Error-driven retry (ALTER TABLE→VIEW, DROP TABLE→VIEW) handled by caller
//
// Note: Tier 0 queries never set FallbackToNative (they bypass parsing entirely).
// DuckDB-native syntax (DESCRIBE, PIVOT, etc.) is returned as-is, which is correct
// since conn.go sends the SQL to DuckDB either way. The only difference is that
// conn.go won't call validateWithDuckDB() for Tier 0 queries, but that validation
// is a courtesy for better error messages, not a correctness requirement.
func (t *Transpiler) Transpile(sql string) (*Result, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return &Result{SQL: sql}, nil
	}

	// Tier 0: Pre-parse classification
	cls := Classify(sql, t.config)
	if cls.Direct {
		// No PG-specific patterns found — send directly to DuckDB.
		// For ConvertPlaceholders mode, count params with regex (no parse needed).
		paramCount := 0
		if t.config.ConvertPlaceholders {
			paramCount = countParametersRegex(sql)
		}
		return &Result{
			SQL:        sql,
			ParamCount: paramCount,
		}, nil
	}

	// Tier 1: Selective transform execution
	return t.transpileWithFlags(sql, cls.Flags)
}

// TranspileAll bypasses classification and runs all transforms unconditionally.
// This is useful for testing and benchmarking the full pipeline.
func (t *Transpiler) TranspileAll(sql string) (*Result, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return &Result{SQL: sql}, nil
	}
	return t.transpileWithFlags(sql, FlagAll)
}

// transpileWithFlags parses the SQL and applies only transforms whose flag is set.
func (t *Transpiler) transpileWithFlags(sql string, flags TransformFlags) (*Result, error) {
	// Parse the SQL into an AST
	tree, err := pg_query.Parse(sql)
	if err != nil {
		// PostgreSQL parsing failed - signal that we should try native DuckDB execution
		// Count parameters using regex since we can't use the AST
		slog.Debug("PostgreSQL parse failed, falling back to native DuckDB.",
			"error", err,
			"sql", sql)
		return &Result{
			SQL:              sql,
			FallbackToNative: true,
			ParamCount:       countParametersRegex(sql),
		}, nil
	}

	// Create transform result to collect metadata from transforms
	transformResult := &transform.Result{}

	// Apply selected transforms
	for _, tt := range t.transforms {
		if tt.flag&flags == 0 {
			continue // Skip transforms not needed for this query
		}

		changed, err := tt.tr.Transform(tree, transformResult)
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

	// DuckDB compatibility fixups on the AST before deparsing
	fixupAST(tree)

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

// Classify performs fast pre-parse classification of a SQL query.
// It does case-insensitive substring matching to detect PostgreSQL-specific patterns.
// Conservative: false positives (unnecessary transforms) are fine; false negatives are bugs.
func Classify(sql string, cfg Config) Classification {
	upper := strings.ToUpper(sql)

	var flags TransformFlags

	// SET/SHOW/RESET/DISCARD/BEGIN/START TRANSACTION — always process these
	// because they produce IsIgnoredSet/IsNoOp/Error side effects that conn.go depends on
	if hasAnyPrefix(upper, "SET ", "SHOW ", "RESET ", "DISCARD ", "BEGIN", "START TRANSACTION", "START ") {
		flags |= FlagSetShow
	}

	// pg_catalog references (tables, functions, types)
	if containsAny(upper,
		"PG_CATALOG", "PG_CLASS", "PG_TYPE", "PG_ATTRIBUTE", "PG_NAMESPACE",
		"PG_INDEX", "PG_CONSTRAINT", "PG_DATABASE", "PG_ROLES", "PG_STAT",
		"PG_STATIO", "PG_COLLATION", "PG_POLICY", "PG_PUBLICATION",
		"PG_INHERITS", "PG_MATVIEWS", "PG_ENUM", "PG_INDEXES",
		"PG_ATTRDEF", "PG_AM", "PG_DESCRIPTION", "PG_DEPEND",
		"PG_SHDESCRIPTION", "PG_PROC", "PG_EXTENSION",
		"PG_AVAILABLE_EXTENSIONS", "PG_SETTINGS",
		"FORMAT_TYPE", "OBJ_DESCRIPTION", "COL_DESCRIPTION",
		"PG_GET_EXPR", "PG_GET_USERBYID", "PG_TABLE_IS_VISIBLE",
		"PG_GET_INDEXDEF", "PG_GET_CONSTRAINTDEF", "PG_GET_SERIAL_SEQUENCE",
		"PG_RELATION_SIZE", "PG_TOTAL_RELATION_SIZE",
		"PG_ENCODING_TO_CHAR", "PG_IS_IN_RECOVERY", "PG_PARTITIONED_TABLE",
		"PG_RELATION_IS_PUBLISHABLE", "PG_GET_PARTKEYDEF",
		"PG_GET_STATISTICSOBJDEF_COLUMNS",
		"PG_RULES",
		"HAS_SCHEMA_PRIVILEGE(", "HAS_TABLE_PRIVILEGE(",
		"SIMILAR_TO_ESCAPE", "CURRENT_SETTING(",
		"UPTIME(", "WORKER_UPTIME(", "WORKER_VERSION(",
		"UNNEST(",
	) {
		flags |= FlagPgCatalog
	}

	// information_schema references
	if strings.Contains(upper, "INFORMATION_SCHEMA") {
		flags |= FlagInfoSchema
	}

	// public.table references (but not catalog.public.table which is 3-part)
	if strings.Contains(upper, "PUBLIC.") {
		flags |= FlagPublicSchema
	}

	// version() function
	if strings.Contains(upper, "VERSION(") {
		flags |= FlagVersion | FlagPgCatalog
	}

	// PostgreSQL type names that need mapping
	if containsAny(upper,
		"JSONB", "BYTEA", "INET", "CIDR", "MACADDR",
		"MONEY", "BPCHAR", "TSVECTOR", "TSQUERY",
		"REGPROC", "REGTYPE", "REGCLASS", "REGNAMESPACE",
		"PG_CATALOG.INT", "PG_CATALOG.VARCHAR", "PG_CATALOG.TEXT",
		"PG_CATALOG.BOOL", "PG_CATALOG.FLOAT", "PG_CATALOG.JSON",
		"PG_CATALOG.\"DEFAULT\"",
	) {
		flags |= FlagTypeMapping
	}

	// Type casts that need rewriting
	if containsAny(upper, "::REGTYPE", "::REGCLASS", "::REGNAMESPACE", "::REGPROC", "::OID") {
		flags |= FlagTypeCast
	}
	// Also catch pg_catalog. qualified casts
	if strings.Contains(upper, "::PG_CATALOG.") {
		flags |= FlagTypeCast | FlagTypeMapping
	}

	// PostgreSQL functions that need mapping
	if containsAny(upper,
		// Array functions
		"ARRAY_AGG(", "ARRAY_LENGTH(", "ARRAY_UPPER(", "ARRAY_TO_STRING(",
		"ARRAY_CAT(", "ARRAY_APPEND(", "ARRAY_PREPEND(", "ARRAY_REMOVE(", "ARRAY_POSITION(",
		"STRING_TO_ARRAY(",
		// String functions
		"BTRIM(",
		// Math functions
		"DIV(", "LOG(",
		// Aggregate functions
		"EVERY(",
		// JSON functions
		"JSON_BUILD_OBJECT(", "JSONB_BUILD_OBJECT(",
		"JSON_BUILD_ARRAY(", "JSONB_BUILD_ARRAY(",
		"JSON_AGG(", "JSONB_AGG(",
		"JSON_OBJECT_AGG(", "JSONB_OBJECT_AGG(",
		"JSON_TYPEOF(", "JSONB_TYPEOF(",
		"JSON_EXTRACT_PATH(", "JSONB_EXTRACT_PATH(",
		"JSON_EXTRACT_PATH_TEXT(", "JSONB_EXTRACT_PATH_TEXT(",
		"JSON_OBJECT(",
		// Regex functions
		"REGEXP_MATCHES(", "REGEXP_MATCH(",
		// Type/conversion functions
		"PG_TYPEOF(", "TO_CHAR(", "TO_DATE(", "TO_NUMBER(",
		// Date/time functions
		"TIMEOFDAY(", "LOCALTIME(", "LOCALTIMESTAMP(",
		// Identity functions
		"SESSION_USER", "USER(",
	) {
		flags |= FlagFunctions
	}

	// Function alias normalization
	if containsAny(upper,
		"CURRENT_DATABASE(", "CURRENT_SCHEMA(", "CURRENT_SCHEMAS(",
		"CURRENT_USER", "CURRENT_CATALOG(", "SESSION_USER",
	) {
		flags |= FlagFuncAlias
	}

	// Operators: JSON arrows and regex.
	// Note: "~" is aggressive — it matches column names and string literals too.
	// This is acceptable: false positive just runs the operator transform (cheap),
	// while a false negative would break PostgreSQL regex queries that DuckDB
	// handles via regexp_matches rewrite.
	if containsAny(upper, "->", "~") {
		flags |= FlagOperators
	}
	// Also check for SIMILAR TO which gets transformed through operators
	if strings.Contains(upper, "SIMILAR TO") {
		flags |= FlagOperators | FlagPgCatalog
	}
	// COLLATE pg_catalog."default" is handled by operators/pgcatalog
	if strings.Contains(upper, "COLLATE") {
		flags |= FlagOperators | FlagPgCatalog
	}

	// FOR UPDATE/SHARE/NO KEY UPDATE/KEY SHARE locking clauses
	if containsAny(upper, "FOR UPDATE", "FOR SHARE", "FOR NO KEY UPDATE", "FOR KEY SHARE") {
		flags |= FlagLocking
	}

	// ctid system column.
	// Substring match may false-positive on names like "DOCTID" — acceptable
	// since the ctid transform only rewrites actual ctid column references in the AST.
	if strings.Contains(upper, "CTID") {
		flags |= FlagCtid
	}

	// _pg_expandarray (JDBC pattern)
	if strings.Contains(upper, "_PG_EXPANDARRAY") {
		flags |= FlagExpandArray
	}

	// ON CONFLICT (DuckLake mode only, but always flag it so the transform can decide)
	if strings.Contains(upper, "ON CONFLICT") {
		flags |= FlagOnConflict
	}

	// DDL patterns (DuckLake mode only).
	// "UNIQUE" and "REFERENCES" may false-positive in non-DDL contexts (e.g.,
	// SELECT ... WHERE col = 'UNIQUE'), but the DDL transform only acts on
	// CREATE TABLE / ALTER TABLE AST nodes, so false positives are harmless.
	if cfg.DuckLakeMode {
		if containsAny(upper, "CREATE INDEX", "DROP INDEX", "VACUUM", "GRANT ", "REVOKE ",
			"PRIMARY KEY", "UNIQUE", "REFERENCES", "SERIAL", "BIGSERIAL",
			"DEFAULT ", "FOREIGN KEY", "ALTER TABLE", "CASCADE",
			"CHECK ", "CHECK(",
			"REINDEX", "CLUSTER", "COMMENT ON", "REFRESH ") {
			flags |= FlagDDL
		}
	}

	// Writable CTEs: WITH ... (INSERT|UPDATE|DELETE).
	// This can false-positive on read-only queries containing these keywords
	// (e.g., WHERE action = 'UPDATE'), but the writable CTE transform checks
	// the actual AST and is a no-op for non-writable CTEs.
	if strings.Contains(upper, "WITH ") {
		if containsAny(upper, "INSERT ", "UPDATE ", "DELETE ") {
			flags |= FlagWritableCTE
		}
	}

	// Parameter placeholders
	if cfg.ConvertPlaceholders && strings.Contains(sql, "$") {
		flags |= FlagPlaceholder
	}

	// pg_query's parser adds pg_catalog. prefix to many built-in functions during parsing.
	// Ensure PgCatalog transform runs whenever function-related transforms are needed,
	// to strip these prefixes before they reach DuckDB.
	if flags&(FlagFunctions|FlagFuncAlias|FlagOperators|FlagTypeCast|FlagTypeMapping) != 0 {
		flags |= FlagPgCatalog
	}

	if flags == 0 {
		return Classification{Direct: true}
	}
	return Classification{Flags: flags}
}

// containsAny returns true if s contains any of the given substrings.
func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// hasAnyPrefix returns true if s starts with any of the given prefixes.
// The check is performed after trimming leading whitespace, line comments (-- ...),
// and block comments (/* ... */).
func hasAnyPrefix(s string, prefixes ...string) bool {
	trimmed := strings.TrimLeft(s, " \t\n\r")

	// Skip leading comments (line comments and block comments)
	for {
		if strings.HasPrefix(trimmed, "--") {
			// Line comment: skip to end of line
			nl := strings.IndexByte(trimmed, '\n')
			if nl < 0 {
				return false // entire string is a comment
			}
			trimmed = strings.TrimLeft(trimmed[nl+1:], " \t\n\r")
		} else if strings.HasPrefix(trimmed, "/*") {
			// Block comment: skip to closing */
			end := strings.Index(trimmed, "*/")
			if end < 0 {
				return false // unclosed block comment
			}
			trimmed = strings.TrimLeft(trimmed[end+2:], " \t\n\r")
		} else {
			break
		}
	}

	for _, prefix := range prefixes {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
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

// countParametersRegex counts $N parameter placeholders using a stateful scan.
// This is a fallback for when pg_query can't parse the SQL (e.g., DuckDB-specific syntax).
// It ignores $N placeholders inside single-quoted strings, double-quoted identifiers,
// and comments (both -- and /* */).
// It finds the highest $N placeholder number, which represents the parameter count.
func countParametersRegex(sql string) int {
	maxParam := 0
	inString := false
	inIdent := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		// Handle block comments
		if inBlockComment {
			if c == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}

		// Handle line comments
		if inLineComment {
			if c == '\n' {
				inLineComment = false
			}
			continue
		}

		// Handle strings
		if inString {
			if c == '\'' {
				// Check for escaped quote ''
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inString = false
				}
			}
			continue
		}

		// Handle double-quoted identifiers
		if inIdent {
			if c == '"' {
				// Check for escaped quote ""
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
				} else {
					inIdent = false
				}
			}
			continue
		}

		// Check for comment start
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inLineComment = true
			i++
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			inBlockComment = true
			i++
			continue
		}

		// Check for string/ident start
		if c == '\'' {
			inString = true
			continue
		}
		if c == '"' {
			inIdent = true
			continue
		}

		// Check for parameter placeholder $N
		if c == '$' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9' {
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			numStr := sql[i+1 : j]
			if n, err := strconv.Atoi(numStr); err == nil && n > maxParam {
				maxParam = n
			}
			i = j - 1
		}
	}

	return maxParam
}

// fixupAST applies DuckDB compatibility fixups to the parsed AST before deparsing.
// These are simple, unconditional cleanups that prevent PostgreSQL-specific syntax
// from reaching DuckDB (e.g., USING btree on CREATE INDEX).
func fixupAST(tree *pg_query.ParseResult) {
	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}
		if idx, ok := stmt.Stmt.Node.(*pg_query.Node_IndexStmt); ok && idx.IndexStmt != nil {
			// DuckDB does not support USING <method> on CREATE INDEX.
			// PostgreSQL's parser sets AccessMethod to "btree" by default,
			// causing the deparser to emit "USING btree". Clear it.
			idx.IndexStmt.AccessMethod = ""
		}
	}
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
