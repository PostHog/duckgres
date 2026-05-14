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
// "Unknown parameter ... with default provider 'config'". For s3_tables
// the iceberg extension internally signs with SigV4 and pulls credentials
// from any TYPE S3 secret in scope.
//
// When keyID is non-empty the secret is built with PROVIDER config and the
// supplied short-lived credentials inlined directly. This is the multitenant
// production path: the control plane assumes the per-tenant IAM role via
// STS and ships the resulting temporary credentials in the worker
// activation payload, identical to how the DuckLake S3 secret is built (see
// buildConfigSecret in server/server.go). The same role has both s3:* and
// s3tables:* permissions on the tenant's data and table buckets, so reusing
// the credentials here is correct.
//
// When keyID is empty the secret falls back to PROVIDER credential_chain.
// This preserves the legacy/standalone path where the host running the
// worker has its own AWS identity reachable via env vars, ~/.aws, or IMDS.
// On EKS deployments without IRSA/Pod Identity on the worker pod (the
// PostHog production topology) this fallback will fail at create time —
// callers in that topology must supply explicit credentials.
func BuildIcebergSecretStmt(cfg Config, keyID, secret, sessionToken string) string {
	region := cfg.Region
	if region == "" {
		region = DefaultRegion
	}
	if keyID == "" {
		return fmt.Sprintf(
			"CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER credential_chain, REGION '%s')",
			escapeSQLStringLiteral(region),
		)
	}
	stmt := fmt.Sprintf(
		"CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER config, KEY_ID '%s', SECRET '%s', REGION '%s'",
		escapeSQLStringLiteral(keyID),
		escapeSQLStringLiteral(secret),
		escapeSQLStringLiteral(region),
	)
	if sessionToken != "" {
		stmt += fmt.Sprintf(", SESSION_TOKEN '%s'", escapeSQLStringLiteral(sessionToken))
	}
	stmt += ")"
	return stmt
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
