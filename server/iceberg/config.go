// Package iceberg holds Iceberg catalog configuration and the SQL fragments
// needed to ATTACH the DuckDB Iceberg extension catalog. Has no dependency
// on github.com/duckdb/duckdb-go: helpers return strings.
package iceberg

// Backend constants — string-typed for wire stability across the
// control-plane → worker activation payload.
const (
	BackendS3Tables   = "s3_tables"
	BackendLakekeeper = "lakekeeper"
)

// Config configures Iceberg catalog attachment. Two backends are supported:
//
//   - "s3_tables" (legacy): per-tenant AWS S3 Tables bucket addressed by ARN,
//     with AWS SigV4 auth picked up from the worker pod's IRSA credential
//     chain. Fields used: TableBucket, Region, Namespace.
//
//   - "lakekeeper" (default for new orgs): per-tenant Lakekeeper instance
//     vends an Iceberg REST catalog. Fields used: LakekeeperEndpoint,
//     LakekeeperWarehouse, LakekeeperClientID, LakekeeperClientSecret,
//     LakekeeperOAuth2ServerURI, Namespace.
//
// Empty Backend is treated as "lakekeeper" by ResolvedBackend so rows
// migrated from earlier schemas behave correctly.
type Config struct {
	// Enabled gates the ATTACH at worker session init. When false, no Iceberg
	// catalog is attached and the rest of the fields are ignored.
	Enabled bool

	// Backend selects the catalog backend. See constants above.
	Backend string

	// Namespace is the default Iceberg namespace within the catalog. Used by
	// both backends.
	Namespace string

	// Region applies to both backends (S3 region for S3 Tables; AWS region
	// for the Lakekeeper warehouse storage profile).
	Region string

	// S3 Tables fields (Backend == "s3_tables").
	TableBucket string

	// Lakekeeper fields (Backend == "lakekeeper").
	LakekeeperEndpoint        string // e.g. http://lakekeeper-<orgid>.lakekeeper.svc:8181/catalog
	LakekeeperWarehouse       string // warehouse NAME, e.g. "org-acme"
	LakekeeperClientID        string
	LakekeeperClientSecret    string
	LakekeeperOAuth2ServerURI string // empty means "Lakekeeper is in allowall mode" (PR1 deployment shape)
}

// ResolvedBackend returns Backend with the empty-string default applied.
func (c Config) ResolvedBackend() string {
	if c.Backend == "" {
		return BackendLakekeeper
	}
	return c.Backend
}
