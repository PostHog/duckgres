package transform

// StorageBackend identifies the catalog/table-format backend a session executes
// against. It selects a BackendCapabilities preset that drives which PostgreSQL
// compatibility transforms apply.
type StorageBackend string

const (
	// BackendMemory is the standalone / in-memory DuckDB backend (no DuckLake,
	// no Iceberg). It receives no compatibility rewriting beyond the always-on
	// transforms.
	BackendMemory StorageBackend = "memory"

	// BackendDuckLake is the DuckLake (S3 + Postgres metadata) backend.
	BackendDuckLake StorageBackend = "ducklake"

	// BackendIceberg is the Iceberg-via-Lakekeeper backend.
	BackendIceberg StorageBackend = "iceberg"
)

// BackendCapabilities describes the PostgreSQL-compatibility behaviors a backend
// requires. Each field replaces a decision that was previously implied by the
// DuckLakeMode boolean. A field name mirrors the transform/call site it gates.
type BackendCapabilities struct {
	// StripsTableConstraints removes PRIMARY KEY / UNIQUE / FOREIGN KEY / CHECK /
	// EXCLUDE constraints from CREATE TABLE and rejects them in ALTER TABLE.
	StripsTableConstraints bool

	// RewritesSerial converts SERIAL/BIGSERIAL/SMALLSERIAL to plain integer types.
	RewritesSerial bool

	// StripsVolatileDefaults drops DEFAULT now()/CURRENT_TIMESTAMP/function defaults
	// and GENERATED columns, keeping only literal defaults.
	StripsVolatileDefaults bool

	// NoOpDDL acknowledges (without executing) DDL the backend cannot honor:
	// CREATE/DROP INDEX, VACUUM, REINDEX, CLUSTER, GRANT/REVOKE, COMMENT,
	// REFRESH MATERIALIZED VIEW.
	NoOpDDL bool

	// RewritesCascadeDrop converts DROP TABLE/VIEW/MATVIEW ... CASCADE to RESTRICT.
	RewritesCascadeDrop bool

	// SplitsMultiCommandAlter splits multi-command ALTER TABLE into individual
	// statements and drops unsupported alter commands.
	SplitsMultiCommandAlter bool

	// ConvertsOnConflictToMerge rewrites INSERT ... ON CONFLICT to MERGE because
	// the backend has no enforced unique constraints to infer against.
	ConvertsOnConflictToMerge bool

	// QualifiesCustomMacros prefixes custom/ClickHouse macros with memory.main
	// because they are created in the in-memory database rather than the default
	// catalog.
	QualifiesCustomMacros bool

	// InterceptsShowCreate enables the pre-parse SHOW CREATE TABLE interception.
	InterceptsShowCreate bool

	// MapPublicToMain rewrites the PostgreSQL "public" schema to DuckDB's "main"
	// schema. True for the in-memory and DuckLake backends, whose physical default
	// schema is "main". False for Iceberg, whose physical default schema is
	// literally "public" (DuckDB shadows "main" on REST catalogs).
	MapPublicToMain bool

	// PhysicalCatalogName is the executable DuckDB catalog alias backing the
	// session's logical database (e.g. "ducklake", "iceberg").
	PhysicalCatalogName string
}

// NeedsDDLTransform reports whether any DDL-rewriting behavior is enabled, i.e.
// whether the DDLTransform should be registered for this backend.
func (c BackendCapabilities) NeedsDDLTransform() bool {
	return c.StripsTableConstraints || c.RewritesSerial || c.StripsVolatileDefaults ||
		c.NoOpDDL || c.RewritesCascadeDrop || c.SplitsMultiCommandAlter
}

// CapabilitiesFor returns the capability preset for a backend.
//
// The DuckLake preset reproduces the exact behavior previously gated by
// DuckLakeMode. The Iceberg preset currently mirrors DuckLake (same DDL/DML
// constraint policy) but is selected independently so the two can diverge.
func CapabilitiesFor(b StorageBackend) BackendCapabilities {
	switch b {
	case BackendDuckLake:
		return BackendCapabilities{
			StripsTableConstraints:    true,
			RewritesSerial:            true,
			StripsVolatileDefaults:    true,
			NoOpDDL:                   true,
			RewritesCascadeDrop:       true,
			SplitsMultiCommandAlter:   true,
			ConvertsOnConflictToMerge: true,
			QualifiesCustomMacros:     true,
			InterceptsShowCreate:      true,
			MapPublicToMain:           true,
			PhysicalCatalogName:       "ducklake",
		}
	case BackendIceberg:
		return BackendCapabilities{
			StripsTableConstraints:    true,
			RewritesSerial:            true,
			StripsVolatileDefaults:    true,
			NoOpDDL:                   true,
			RewritesCascadeDrop:       true,
			SplitsMultiCommandAlter:   true,
			ConvertsOnConflictToMerge: true,
			QualifiesCustomMacros:     true,
			InterceptsShowCreate:      true,
			MapPublicToMain:           false, // Iceberg's physical schema is "public"
			PhysicalCatalogName:       "iceberg",
		}
	default: // BackendMemory and unknown
		// The in-memory backend still maps public→main (its default schema is
		// "main"); all other compatibility rewriting is off.
		return BackendCapabilities{MapPublicToMain: true}
	}
}
