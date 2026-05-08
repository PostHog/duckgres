package iceberg

import (
	"fmt"
	"strings"
)

// DefaultRegion is used when Config.Region is empty.
const DefaultRegion = "us-east-1"

// CatalogName is the schema-side name the Iceberg catalog is attached as.
// Mirrors the "delta" choice in server/ducklake — a single attach name keeps
// the call sites simple. Multi-attach (multiple Iceberg warehouses per
// worker) would require generalizing this.
const CatalogName = "iceberg"

// BuildIcebergSecretStmt builds the CREATE SECRET statement for SigV4 auth
// against AWS S3 Tables. The DuckDB iceberg extension picks up the secret by
// type (TYPE ICEBERG) when there's no SECRET name on the ATTACH.
func BuildIcebergSecretStmt(cfg Config) string {
	region := cfg.Region
	if region == "" {
		region = DefaultRegion
	}
	return fmt.Sprintf(
		"CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE ICEBERG, AUTHORIZATION_TYPE 'SIGV4', REGION '%s')",
		escapeSQLStringLiteral(region),
	)
}

// BuildIcebergAttachStmt builds the ATTACH statement for the Iceberg
// extension catalog, addressing the per-tenant S3 Tables bucket ARN.
func BuildIcebergAttachStmt(cfg Config) string {
	return fmt.Sprintf(
		"ATTACH '%s' AS %s (TYPE iceberg, ENDPOINT_TYPE 's3_tables')",
		escapeSQLStringLiteral(cfg.TableBucket),
		CatalogName,
	)
}

// escapeSQLStringLiteral doubles single quotes so the value is safe to embed
// inside a single-quoted SQL string literal. Identical helper to the one in
// server/ducklake — duplicated rather than imported to keep the iceberg
// package free of cross-package dependencies.
func escapeSQLStringLiteral(v string) string {
	return strings.ReplaceAll(v, "'", "''")
}
