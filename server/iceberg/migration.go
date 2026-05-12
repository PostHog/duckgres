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

// BuildIcebergSecretStmt builds the CREATE SECRET statement that the
// DuckDB iceberg extension uses to sign AWS S3 Tables requests when an
// `ATTACH ... (TYPE iceberg, ENDPOINT_TYPE 's3_tables')` is opened.
//
// DuckDB's TYPE ICEBERG secret is OAuth2-only — it has a single provider
// ('config') registered by OAuth2Authorization::CreateCatalogSecretFunction.
// Trying to pass AUTHORIZATION_TYPE/REGION on TYPE ICEBERG fails with
// "Unknown parameter ... with default provider 'config'".
//
// For s3_tables the iceberg extension internally signs with SigV4 and pulls
// the AWS credentials from any TYPE S3 secret in scope. credential_chain
// lets us reuse whatever the worker pod already has (IRSA web identity, EKS
// Pod Identity container creds, or env vars) without baking access keys in.
func BuildIcebergSecretStmt(cfg Config) string {
	region := cfg.Region
	if region == "" {
		region = DefaultRegion
	}
	return fmt.Sprintf(
		"CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER credential_chain, REGION '%s')",
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
