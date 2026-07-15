package transpiler

import backendpkg "github.com/posthog/duckgres/transpiler/backend"

// Config controls transpilation behavior
type Config struct {
	// DuckLakeMode is a legacy convenience: when Backend is unset, DuckLakeMode==true
	// selects the DuckLake backend profile and false selects the memory profile.
	// Prefer setting Backend explicitly. When true, PRIMARY KEY, UNIQUE, FOREIGN KEY,
	// CHECK constraints are removed, SERIAL types are converted to INTEGER, and
	// DEFAULT now() is stripped.
	DuckLakeMode bool

	// Backend selects the storage backend profile. When empty, it is derived from
	// DuckLakeMode (ducklake when true, memory when false).
	Backend backendpkg.Name

	// LogicalDatabaseName is the client-visible database name for the session.
	// When set in DuckLake mode, three-part references using this catalog are
	// rewritten to PhysicalCatalogName.
	LogicalDatabaseName string

	// PhysicalCatalogName is the executable DuckDB catalog alias that backs the
	// logical database name. Defaults to "ducklake" when empty in DuckLake mode.
	PhysicalCatalogName string

	// ConvertPlaceholders converts PostgreSQL $1, $2 placeholders to ? for database/sql.
	// This is needed for the extended query protocol (prepared statements).
	ConvertPlaceholders bool
}

// DefaultConfig returns a Config with sensible defaults for simple queries.
func DefaultConfig() Config {
	return Config{
		DuckLakeMode:        false,
		LogicalDatabaseName: "",
		PhysicalCatalogName: "",
		ConvertPlaceholders: false,
	}
}

// resolveBackend returns the effective backend. An explicit Backend wins;
// otherwise it is derived from the legacy DuckLakeMode flag.
func (c Config) resolveBackend() backendpkg.Name {
	if c.Backend != "" {
		return c.Backend
	}
	if c.DuckLakeMode {
		return backendpkg.DuckLake
	}
	return backendpkg.Memory
}

// profile returns the backend profile for this config, with any explicit
// PhysicalCatalogName override applied.
func (c Config) profile() backendpkg.Profile {
	return backendpkg.WithPhysicalCatalog(backendpkg.ForName(c.resolveBackend()), c.PhysicalCatalogName)
}

func ConfigForBackend(name backendpkg.Name) Config {
	return Config{Backend: name}
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

	// QuerySourceSet, when non-nil, indicates a `SET duckgres.query_source =
	// '<value>'` on the duckgres-namespaced custom GUC. The connection layer
	// stores the pointed-to value on the session and acknowledges it as "SET"
	// (it is NOT forwarded to DuckDB, which would reject the unknown setting).
	// The value is pre-validated and canonical: "standard", "endpoints", or ""
	// (reset to default) — an invalid value never reaches here (it surfaces as
	// Error with SQLSTATE 22023 instead).
	QuerySourceSet *string

	// QuerySourceShow indicates a `SHOW duckgres.query_source`; the connection
	// layer answers it from session state (defaulting to "standard").
	QuerySourceShow bool

	// Error is set when a transform detects an error that should be returned to the client
	// (e.g., unrecognized configuration parameter in SHOW command)
	Error error

	// FallbackToNative indicates that PostgreSQL parsing failed but the query
	// should be attempted directly against DuckDB. This enables two-tier query
	// processing where DuckDB-specific syntax works automatically.
	FallbackToNative bool
}
