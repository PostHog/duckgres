// Package iceberg holds Iceberg catalog configuration and the SQL fragments
// needed to ATTACH the DuckDB Iceberg extension catalog. Has no dependency
// on github.com/duckdb/duckdb-go: helpers return strings.
package iceberg

// Backend is the only supported Iceberg backend now that the S3 Tables path
// has been removed. Retained as a constant so the activation payload + the
// configstore column have a stable name to write/check; future backends can
// extend the type-set without breaking the wire shape.
const BackendLakekeeper = "lakekeeper"

// Config configures Iceberg catalog attachment. Lakekeeper-only: a per-tenant
// Lakekeeper instance vends an Iceberg REST catalog. Fields used:
// LakekeeperEndpoint, LakekeeperWarehouse, LakekeeperClientID,
// LakekeeperClientSecret, LakekeeperOAuth2ServerURI, Namespace.
//
// Empty Backend is treated as "lakekeeper" by ResolvedBackend so rows migrated
// from earlier schemas behave correctly.
type Config struct {
	// Enabled gates the ATTACH at worker session init. When false, no Iceberg
	// catalog is attached and the rest of the fields are ignored.
	Enabled bool

	// Backend retains its name on the wire (always "lakekeeper" or empty) so
	// downstream consumers that already carry the field don't break; the
	// dispatcher no longer branches on it.
	Backend string

	// Namespace is the default Iceberg namespace within the catalog.
	Namespace string

	// Region is the AWS region for the Lakekeeper warehouse storage profile.
	Region string

	// Lakekeeper fields.
	LakekeeperEndpoint        string // e.g. http://lakekeeper-<orgid>.lakekeeper.svc:8181/catalog
	LakekeeperWarehouse       string // warehouse NAME, e.g. "org-acme"
	LakekeeperClientID        string
	LakekeeperClientSecret    string
	LakekeeperOAuth2ServerURI string // empty means "Lakekeeper is in allowall mode" (PR1 deployment shape)
}

// ResolvedBackend returns Backend with the empty-string default applied.
// Kept for callers that still inspect the field; always returns "lakekeeper".
func (c Config) ResolvedBackend() string {
	return BackendLakekeeper
}
