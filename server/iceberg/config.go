// Package iceberg holds Iceberg catalog configuration and the SQL fragments
// needed to ATTACH the DuckDB Iceberg extension catalog. Mirrors the shape of
// server/ducklake but for Iceberg-on-S3-Tables. Has no dependency on
// github.com/duckdb/duckdb-go: helpers return strings.
package iceberg

// Config configures Iceberg catalog attachment. Today the only supported
// backend is AWS S3 Tables: a per-tenant table bucket addressed by ARN, with
// AWS SigV4 auth picked up from the worker pod's IRSA credential chain.
//
// Other catalog backends (REST/Polaris/Lakekeeper, Glue, file-based metadata)
// could be added later by widening AuthType + ATTACH options.
type Config struct {
	// Enabled gates the ATTACH at worker session init. When false, no Iceberg
	// catalog is attached and the rest of the fields are ignored.
	Enabled bool

	// TableBucket is the AWS S3 Tables bucket ARN that backs this tenant's
	// catalog. Format: arn:aws:s3tables:<region>:<acct>:bucket/<name>.
	TableBucket string

	// Region is the AWS region of the table bucket. Used to build the
	// CREATE SECRET (TYPE ICEBERG, AUTHORIZATION_TYPE 'SIGV4', REGION ...)
	// statement. Defaults to us-east-1 when empty.
	Region string

	// Namespace is the default Iceberg namespace within the catalog. Today
	// the duckling composition provisions a single "main" namespace per
	// tenant; this field is informational at attach time but kept on the
	// config so the worker knows where freshly created tables should land.
	Namespace string
}
