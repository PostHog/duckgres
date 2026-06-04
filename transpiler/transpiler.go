package transpiler

import (
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/transpiler/backend"
	"github.com/posthog/duckgres/transpiler/transform"
)

// TransformFlags is a bitmask indicating which transforms a query needs.
type TransformFlags uint32

const (
	FlagWritableCTE       TransformFlags = 1 << iota // Writable CTE rewrite
	FlagVersion                                      // version() replacement
	FlagPgCatalog                                    // pg_catalog schema/view mappings
	FlagInfoSchema                                   // information_schema mappings
	FlagPublicSchema                                 // public -> main schema mapping
	FlagLogicalCatalog                               // logical database catalog -> physical catalog mapping
	FlagTypeMapping                                  // Type mappings (JSONB->JSON, etc.)
	FlagTypeCast                                     // Type casts (::regtype -> ::varchar)
	FlagFunctions                                    // Function mappings (array_agg->list, etc.)
	FlagFuncAlias                                    // Function alias normalization
	FlagBooleanPredicates                            // Boolean predicate normalization (= true -> IS TRUE)
	FlagOperators                                    // Operator mappings (regex, JSON)
	FlagSetShow                                      // SET/SHOW command handling
	FlagExpandArray                                  // _pg_expandarray handling
	FlagOnConflict                                   // ON CONFLICT handling
	FlagLocking                                      // FOR UPDATE/SHARE removal
	FlagCtid                                         // ctid -> rowid mapping
	FlagDDL                                          // DDL constraint stripping
	FlagPlaceholder                                  // $1/$2 placeholder conversion
	flagSentinel                                     // must be last — used to derive FlagAll

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
	profile    backend.Profile
	transforms []taggedTransform
}

var threePartIdentPattern = regexp.MustCompile(`(?i)(?:"[^"]+"|[a-z_][a-z0-9_$]*)\s*\.\s*(?:"[^"]+"|[a-z_][a-z0-9_$]*)\s*\.\s*(?:"[^"]+"|[a-z_][a-z0-9_$]*)`)

// New creates a Transpiler with the given configuration.
// It registers all transforms appropriate for the config.
func New(cfg Config) *Transpiler {
	profile := cfg.profile()
	catalogPolicy := profile.Catalog()
	ddlPolicy := profile.DDL()
	dmlPolicy := profile.DML()
	t := &Transpiler{
		config:     cfg,
		profile:    profile,
		transforms: make([]taggedTransform, 0),
	}

	// Core transforms - always registered
	// Order matters: more specific transforms should come first

	// 0. Writable CTE transform - MUST BE FIRST
	t.transforms = append(t.transforms, taggedTransform{FlagWritableCTE, transform.NewWritableCTETransform()})

	// 1. version() replacement - MUST run before PgCatalogTransform
	t.transforms = append(t.transforms, taggedTransform{FlagVersion, transform.NewVersionTransform()})

	// 2. pg_catalog schema and view mappings
	t.transforms = append(t.transforms, taggedTransform{FlagPgCatalog, transform.NewPgCatalogTransformWithConfig(catalogPolicy.QualifyMacros)})

	// 3. information_schema mappings to compat views
	t.transforms = append(t.transforms, taggedTransform{FlagInfoSchema, transform.NewInformationSchemaTransform()})

	// 3.1 Map PostgreSQL "public" schema to DuckDB "main" (backends whose default
	// schema is "main"; disabled for Iceberg whose schema is literally "public")
	t.transforms = append(t.transforms, taggedTransform{FlagPublicSchema, transform.NewPublicSchemaTransform(catalogPolicy.MapPublicToMain)})

	// 3.2 Map logical database catalog references to the physical backend catalog
	t.transforms = append(t.transforms, taggedTransform{FlagLogicalCatalog, transform.NewLogicalCatalogTransform(cfg.LogicalDatabaseName, catalogPolicy.PhysicalName, catalogPolicy.MapPublicToMain)})

	// 4. Type mappings (JSONB->JSON, CHAR->TEXT, etc.)
	t.transforms = append(t.transforms, taggedTransform{FlagTypeMapping, transform.NewTypeMappingTransform()})

	// 5. Type casts (::regtype -> ::varchar)
	t.transforms = append(t.transforms, taggedTransform{FlagTypeCast, transform.NewTypeCastTransform()})

	// 6. Function mappings (array_agg->list, string_to_array->string_split, etc.)
	t.transforms = append(t.transforms, taggedTransform{FlagFunctions, transform.NewFunctionTransform()})

	// 7. Function alias normalization (current_database() -> AS current_database)
	t.transforms = append(t.transforms, taggedTransform{FlagFuncAlias, transform.NewFuncAliasTransform()})

	// 8. Boolean predicate normalization for DuckLake/worker execution.
	t.transforms = append(t.transforms, taggedTransform{FlagBooleanPredicates, transform.NewBooleanPredicateTransform()})

	// 9. Operator mappings (regex operators, etc.)
	t.transforms = append(t.transforms, taggedTransform{FlagOperators, transform.NewOperatorTransform()})

	// 10. SET/SHOW command handling
	t.transforms = append(t.transforms, taggedTransform{FlagSetShow, transform.NewSetShowTransform()})

	// 11. _pg_expandarray handling (PostgreSQL array expansion function used by JDBC)
	t.transforms = append(t.transforms, taggedTransform{FlagExpandArray, transform.NewExpandArrayTransform()})

	// 12. ON CONFLICT handling (rewrites ON CONFLICT to MERGE when the backend has
	// no enforced unique constraints to infer against)
	t.transforms = append(t.transforms, taggedTransform{FlagOnConflict, transform.NewOnConflictTransformWithConfig(dmlPolicy.ConflictHandling == backend.RewriteToMerge)})

	// 13. Locking clause removal (FOR UPDATE, FOR SHARE, etc.) - DuckDB doesn't support these
	t.transforms = append(t.transforms, taggedTransform{FlagLocking, transform.NewLockingTransform()})

	// 14. ctid → rowid mapping (PostgreSQL system column to DuckDB equivalent)
	t.transforms = append(t.transforms, taggedTransform{FlagCtid, transform.NewCtidTransform()})

	// DDL transforms only when the backend requires DDL rewriting
	if ddlPolicy.NeedsTransform() {
		t.transforms = append(t.transforms, taggedTransform{FlagDDL, transform.NewDDLTransform(ddlPolicy)})
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

	// Pre-parse interceptions for SQL that isn't valid PostgreSQL syntax
	// but has well-defined duckgres semantics (e.g., SHOW CREATE TABLE).
	if rewritten, ok := interceptShowCreate(sql, t.profile.Metadata().InterceptShowCreate); ok {
		return &Result{SQL: rewritten}, nil
	}

	// CREATE OR REPLACE TABLE is DuckDB-only syntax that PostgreSQL can't
	// parse. Strip OR REPLACE so the statement runs through the full transform
	// pipeline (critically, the logical-catalog rewrite), then restore it.
	// Without this the statement would FallbackToNative and be forwarded raw,
	// reaching the worker with the un-rewritten logical catalog name.
	if inner, ok := stripCreateOrReplaceTable(sql); ok {
		res, err := t.Transpile(inner)
		if err != nil {
			return res, err
		}
		res.SQL = reinjectOrReplace(res.SQL)
		return res, nil
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
	// Protect identifiers longer than PostgreSQL's NAMEDATALEN limit from being
	// silently truncated by libpg_query during Parse/Deparse (see longident.go).
	// We parse the placeholdered SQL, then restore the originals on any SQL we
	// emit from the resulting AST. Paths that return the original `sql` need no
	// restoration since `sql` is never the placeholdered form.
	parseSQL, longIdents := protectLongIdentifiers(sql)

	// Parse the SQL into an AST
	tree, err := pg_query.Parse(parseSQL)
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
				Statements:        restoreLongIdentifiersAll(transformResult.Statements, longIdents),
				CleanupStatements: restoreLongIdentifiersAll(transformResult.CleanupStatements, longIdents),
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

	if transformResult.SQLOverride != "" {
		return &Result{
			SQL:          restoreLongIdentifiers(transformResult.SQLOverride, longIdents),
			ParamCount:   transformResult.ParamCount,
			IsNoOp:       transformResult.IsNoOp,
			NoOpTag:      transformResult.NoOpTag,
			IsIgnoredSet: transformResult.IsIgnoredSet,
		}, nil
	}

	// Deparse the modified AST back to SQL
	deparsed, err := pg_query.Deparse(tree)
	if err != nil {
		return nil, err
	}
	deparsed = restoreLongIdentifiers(deparsed, longIdents)

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
	profile := cfg.profile()
	catalogPolicy := profile.Catalog()
	ddlPolicy := profile.DDL()

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
		"PG_TABLES", "PG_VIEWS", "PG_SEQUENCES",
		"PG_ATTRDEF", "PG_AM", "PG_DESCRIPTION", "PG_DEPEND",
		"PG_SHDESCRIPTION", "PG_PROC", "PG_EXTENSION",
		"PG_AVAILABLE_EXTENSIONS", "PG_SETTINGS",
		"FORMAT_TYPE", "OBJ_DESCRIPTION", "COL_DESCRIPTION",
		"PG_GET_EXPR", "PG_GET_USERBYID", "PG_TABLE_IS_VISIBLE",
		"PG_GET_VIEWDEF", "PG_GET_INDEXDEF", "PG_GET_CONSTRAINTDEF", "PG_GET_SERIAL_SEQUENCE",
		"PG_RELATION_SIZE", "PG_TOTAL_RELATION_SIZE", "PG_TABLE_SIZE(",
		"PG_INDEXES_SIZE(", "PG_DATABASE_SIZE(", "PG_SIZE_PRETTY(",
		"PG_BACKEND_PID(", "TXID_CURRENT(", "PG_CURRENT_XACT_ID(",
		"QUOTE_IDENT(", "QUOTE_LITERAL(", "QUOTE_NULLABLE(",
		"PG_ENCODING_TO_CHAR", "PG_IS_IN_RECOVERY", "PG_PARTITIONED_TABLE",
		"PG_RELATION_IS_PUBLISHABLE", "PG_GET_PARTKEYDEF",
		"PG_GET_STATISTICSOBJDEF_COLUMNS",
		"PG_RULES", "PG_AUTH_MEMBERS", "PG_OPCLASS", "PG_CONVERSION",
		"PG_LANGUAGE", "PG_FOREIGN_SERVER", "PG_FOREIGN_DATA_WRAPPER",
		"PG_FOREIGN_TABLE", "PG_TRIGGER", "PG_LOCKS", "PG_REWRITE",
		"PG_STAT_GET_NUMSCANS(",
		"HAS_SCHEMA_PRIVILEGE(", "HAS_TABLE_PRIVILEGE(", "HAS_DATABASE_PRIVILEGE(", "HAS_ANY_COLUMN_PRIVILEGE(",
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

	if cfg.LogicalDatabaseName != "" || threePartIdentPattern.MatchString(sql) {
		logicalUpper := strings.ToUpper(cfg.LogicalDatabaseName)
		if cfg.LogicalDatabaseName == "" || containsAny(upper, logicalUpper+".", `"`+logicalUpper+`".`) || threePartIdentPattern.MatchString(sql) {
			flags |= FlagLogicalCatalog
		}
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
		"TO_TIMESTAMP(",
		// JSON functions
		"JSON_OBJECT_KEYS(", "JSONB_OBJECT_KEYS(",
		// Date/time functions
		"TIMEOFDAY(", "LOCALTIME(", "LOCALTIMESTAMP(",
		"CLOCK_TIMESTAMP(",
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

	if NeedsBooleanPredicateRewrite(sql) {
		flags |= FlagBooleanPredicates
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

	// DDL patterns (only when the backend requires DDL rewriting).
	// "UNIQUE" and "REFERENCES" may false-positive in non-DDL contexts (e.g.,
	// SELECT ... WHERE col = 'UNIQUE'), but the DDL transform only acts on
	// CREATE TABLE / ALTER TABLE AST nodes, so false positives are harmless.
	if ddlPolicy.NeedsTransform() {
		if containsAny(upper, "CREATE INDEX", "DROP INDEX", "VACUUM", "ANALYZE", "GRANT ", "REVOKE ",
			"PRIMARY KEY", "UNIQUE", "REFERENCES", "SERIAL", "BIGSERIAL",
			"DEFAULT ", "GENERATED", "FOREIGN KEY", "ALTER TABLE", "CASCADE",
			"CHECK ", "CHECK(",
			"REINDEX", "CLUSTER", "COMMENT ON", "REFRESH ") {
			flags |= FlagDDL
		}
	}

	// ClickHouse SQL macros (only when custom macros need qualification).
	// These are created in memory.main and need explicit qualification.
	if catalogPolicy.QualifyMacros {
		if containsAny(upper,
			"TOSTRING(", "TOINT32(", "TOINT64(", "TOFLOAT(",
			"TOINT32ORNULL(", "TOINT32ORZERO(",
			"INTDIV(", "MODULO(",
			"EMPTY(", "NOTEMPTY(",
			"SPLITBYCHAR(", "LENGTHUTF8(",
			"TOYEAR(", "TOMONTH(", "TODAYOFMONTH(",
			"TOYYYYMMDD(", "TOYYYYMM(",
			"PROTOCOL(", "DOMAIN(",
			"TOPLEVELDOMAIN(",
			"IPV4NUMTOSTRING(",
			"JSONEXTRACTSTRING(", "JSONHAS(",
			"GENERATEUUIDV4(",
			"IFNULL(",
		) {
			flags |= FlagPgCatalog
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

	// pg_query's parser adds pg_catalog. prefix to many built-in types and functions
	// during parsing (e.g., JSON → pg_catalog.json, int → pg_catalog.int4).
	// The PgCatalog transform must run for ANY Tier 1 query to strip these prefixes.
	if flags != 0 {
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

	// Deparser-output compatibility fixes for type casts. These must run
	// unconditionally (independent of which transforms the classifier selected)
	// because they correct the SQL the pg_query deparser produces, not the
	// user's input:
	//
	//   - The deparser canonicalizes built-in types as pg_catalog.<type>
	//     (e.g. x::json -> x::pg_catalog.json), which DuckDB rejects.
	//   - It renders a field-qualified interval cast ('2'::interval day) as
	//     '2'::"interval"(8), which DuckDB cannot convert (no unit in the value).
	//
	// The flag-gated TypeMappingTransform strips pg_catalog too, but only when
	// the classifier decided a query needs type mapping; a query that is parsed
	// for some other transform but not flagged for type mapping would otherwise
	// emit pg_catalog.<type>. Doing it here closes that gap.
	transform.WalkFunc(tree, func(node *pg_query.Node) bool {
		switch n := node.Node.(type) {
		case *pg_query.Node_TypeCast:
			if tc := n.TypeCast; tc != nil && tc.TypeName != nil {
				if rewriteIntervalTypmodCast(tc) {
					return true
				}
				stripPgCatalogFromTypeName(tc.TypeName)
			}
		case *pg_query.Node_ColumnDef:
			if cd := n.ColumnDef; cd != nil && cd.TypeName != nil {
				stripPgCatalogFromTypeName(cd.TypeName)
			}
		}
		return true
	})
}

// intervalTypmodUnit maps PostgreSQL interval typmod range masks (INTERVAL_MASK
// of a single field) to the DuckDB unit keyword. These are the bitmask values
// the parser stores for `interval <field>`: MONTH=1<<1, YEAR=1<<2, DAY=1<<3,
// HOUR=1<<10, MINUTE=1<<11, SECOND=1<<12. Combined ranges (e.g. DAY TO HOUR)
// are intentionally omitted — they are rare and have no clean DuckDB cast form.
var intervalTypmodUnit = map[int32]string{
	2:    "month",
	4:    "year",
	8:    "day",
	1024: "hour",
	2048: "minute",
	4096: "second",
}

// rewriteIntervalTypmodCast rewrites a field-qualified interval cast such as
// '2'::interval day into (2 || ' day')::interval, which DuckDB accepts. DuckDB
// cannot infer a unit from a bare value cast to INTERVAL; a unit-bearing string
// works, and concatenation handles both string and numeric operands. A bare
// interval cast (no typmod) is not re-qualified to pg_catalog.interval by the
// deparser, so the output survives deparsing. Returns true if it rewrote the cast.
func rewriteIntervalTypmodCast(tc *pg_query.TypeCast) bool {
	tn := tc.TypeName
	if tn == nil || len(tn.Names) == 0 || tc.Arg == nil {
		return false
	}
	last := tn.Names[len(tn.Names)-1].GetString_()
	if last == nil || strings.ToLower(last.Sval) != "interval" {
		return false
	}
	if len(tn.Typmods) != 1 {
		return false
	}
	ac := tn.Typmods[0].GetAConst()
	if ac == nil {
		return false
	}
	iv := ac.GetIval()
	if iv == nil {
		return false
	}
	unit, ok := intervalTypmodUnit[iv.Ival]
	if !ok {
		return false
	}

	tc.Arg = &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: &pg_query.A_Expr{
		Kind:  pg_query.A_Expr_Kind_AEXPR_OP,
		Name:  []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "||"}}}},
		Lexpr: tc.Arg,
		Rexpr: &pg_query.Node{Node: &pg_query.Node_AConst{AConst: &pg_query.A_Const{
			Val: &pg_query.A_Const_Sval{Sval: &pg_query.String{Sval: " " + unit}},
		}}},
	}}}
	tn.Names = []*pg_query.Node{{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: "interval"}}}}
	tn.Typmods = nil
	return true
}

// stripPgCatalogFromTypeName removes a leading pg_catalog qualifier from a type
// name so the deparser emits e.g. json instead of pg_catalog.json (which DuckDB
// rejects). Interval is deliberately left qualified: stripping pg_catalog from
// interval regresses the deparser to '<n>'::"interval"(<mask>). Field-qualified
// intervals are handled by rewriteIntervalTypmodCast before this runs.
func stripPgCatalogFromTypeName(tn *pg_query.TypeName) {
	if tn == nil || len(tn.Names) < 2 {
		return
	}
	first := tn.Names[0].GetString_()
	if first == nil || strings.ToLower(first.Sval) != "pg_catalog" {
		return
	}
	last := tn.Names[len(tn.Names)-1].GetString_()
	if last != nil && strings.ToLower(last.Sval) == "interval" {
		return
	}
	tn.Names = tn.Names[1:]
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
