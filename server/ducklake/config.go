// Package ducklake holds DuckLake configuration, the metadata-store version
// migration check, and the SQL fragments needed to ATTACH a DuckLake catalog.
//
// This package has no dependency on github.com/duckdb/duckdb-go: the migration
// check connects to the tenant's metadata Postgres via pgx, and the ATTACH
// statement helpers return strings. This lets the multi-tenant control plane
// run the pre-flight migration check without linking libduckdb.
package ducklake

import "time"

// Config configures DuckLake catalog attachment.
type Config struct {
	// MetadataStore is the connection string for the DuckLake metadata database.
	// Format: "postgres:host=<host> user=<user> password=<password> dbname=<db>"
	MetadataStore string

	// DisableMetadataThreadLocalCache disables postgres_scanner thread-local
	// connection caching for the hidden DuckLake metadata pool as early as
	// possible, before ATTACH creates that pool. This trades some warm-reuse
	// performance for a lower retained metadata-connection footprint.
	// Nil means use the server default (enabled).
	DisableMetadataThreadLocalCache *bool

	// ObjectStore is the S3-compatible storage path for DuckLake data files.
	// Format: "s3://bucket/path/" for S3/MinIO.
	// If not specified, uses DataPath for local storage.
	ObjectStore string

	// DataPath is the local file system path for DuckLake data files.
	// Used when ObjectStore is not set (for local/non-S3 storage).
	DataPath string

	// DeltaCatalogEnabled attaches the DuckDB Delta extension catalog at worker
	// startup/activation in addition to DuckLake. DeltaCatalogPath defaults to a
	// sibling delta/ prefix at the DuckLake object-store root when omitted.
	DeltaCatalogEnabled bool
	DeltaCatalogPath    string

	// S3 credential provider: "config" (explicit credentials) or "credential_chain"
	// (AWS SDK chain). Default: "config" if S3AccessKey is set, otherwise
	// "credential_chain".
	S3Provider string

	// S3 configuration for "config" provider (explicit credentials for MinIO or S3).
	S3Endpoint     string // e.g., "localhost:9000" for MinIO
	S3AccessKey    string // S3 access key ID
	S3SecretKey    string // S3 secret access key
	S3SessionToken string // STS session token for temporary credentials
	S3Region       string // S3 region (default: us-east-1)
	S3UseSSL       bool   // Use HTTPS for S3 connections (default: false for MinIO)
	S3URLStyle     string // "path" or "vhost" (default: "path" for MinIO compatibility)

	// S3 configuration for "credential_chain" provider (AWS SDK credential chain).
	// Chain specifies which credential sources to check, semicolon-separated.
	// Options: env, config, sts, sso, instance, process.
	// Default: checks all sources in AWS SDK order.
	S3Chain   string // e.g., "env;config" to check env vars then config files
	S3Profile string // AWS profile name to use (for "config" chain)

	// HTTPProxy routes DuckDB httpfs traffic through a forward HTTP proxy.
	// When set, DuckDB signs S3 requests for the real S3 hostname and sends them
	// through the proxy as plain HTTP (requires S3UseSSL=false). Used by the
	// local cache proxy DaemonSet for NVMe caching.
	HTTPProxy string

	// CheckpointInterval controls how often DuckLake CHECKPOINT runs.
	// CHECKPOINT performs full catalog maintenance: expire snapshots,
	// merge adjacent files, rewrite data files, and clean up orphaned files.
	// Set to 0 to disable. Default: 24h.
	CheckpointInterval time.Duration

	// DataInliningRowLimit controls the maximum number of rows to inline
	// in DuckLake metadata instead of writing to Parquet files.
	// Default: 0 (disabled). Set to a positive value to enable inlining.
	DataInliningRowLimit *int

	// Migrate is set by the control plane after running the migration check.
	// When true, AttachDuckLake uses AUTOMATIC_MIGRATION TRUE without
	// re-running the version check. This avoids redundant backups and
	// long-running checks in worker processes.
	Migrate bool `json:"migrate,omitempty" yaml:"-"`

	// SpecVersion is the target DuckLake spec version for this connection.
	// When empty, the worker uses its own built-in default.
	SpecVersion string `json:"spec_version,omitempty" yaml:"-"`

	// ViaPgBouncer is set by the control plane when the DuckLake metadata
	// connection is routed through a network-level pooler (e.g. PgBouncer)
	// rather than direct to Postgres. When true, the worker disables the
	// postgres_scanner in-process pool via `SET GLOBAL pg_pool_max_connections = 0`.
	// See duckdb/ducklake#1031: behind a network pooler, client-side pooling
	// is redundant and prevents the pooler from reclaiming idle connections.
	ViaPgBouncer bool `json:"via_pgbouncer,omitempty" yaml:"-"`
}
