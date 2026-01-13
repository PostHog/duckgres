package transpiler

// Config controls transpilation behavior
type Config struct {
	// DuckLakeMode enables DDL constraint stripping for DuckLake compatibility.
	// When true, PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK constraints are removed,
	// SERIAL types are converted to INTEGER, and DEFAULT now() is stripped.
	DuckLakeMode bool

	// ConvertPlaceholders converts PostgreSQL $1, $2 placeholders to ? for database/sql.
	// This is needed for the extended query protocol (prepared statements).
	ConvertPlaceholders bool
}

// DefaultConfig returns a Config with sensible defaults for simple queries.
func DefaultConfig() Config {
	return Config{
		DuckLakeMode:        false,
		ConvertPlaceholders: false,
	}
}

// Result contains the output of transpilation
type Result struct {
	// SQL is the transformed SQL string (backward compatible for single-statement queries)
	SQL string

	// Statements contains multiple SQL statements when a query is rewritten into
	// a sequence (e.g., writable CTE rewrite). Includes setup statements and the
	// final query. When populated, SQL should be ignored.
	Statements []string

	// CleanupStatements contains statements to execute after obtaining the cursor
	// for the final query but before streaming results. Typically DROP TEMP TABLE
	// and COMMIT statements. Execute these with best-effort (ignore errors).
	CleanupStatements []string

	// ParamCount is the number of parameters found (when ConvertPlaceholders is true)
	ParamCount int

	// IsNoOp indicates the command should be acknowledged but not executed.
	// This is true for commands like CREATE INDEX, VACUUM, GRANT, etc.
	IsNoOp bool

	// NoOpTag is the command tag to return for no-op commands (e.g., "CREATE INDEX")
	NoOpTag string

	// IsIgnoredSet indicates a SET command for a PostgreSQL-specific parameter
	// that should be silently acknowledged without execution.
	IsIgnoredSet bool

	// Error is set when a transform detects an error that should be returned to the client
	// (e.g., unrecognized configuration parameter in SHOW command)
	Error error
}
