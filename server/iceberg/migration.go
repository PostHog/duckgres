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
// The secret is always built with PROVIDER config and the supplied
// short-lived credentials inlined directly. This is the only supported
// auth model: the control plane assumes the per-tenant IAM role via STS
// and ships the resulting temporary credentials in the worker activation
// payload, identical to how the DuckLake S3 secret is built (see
// buildConfigSecret in server/server.go). The same role has both s3:* and
// s3tables:* permissions on the tenant's data and table buckets, so
// reusing the credentials here is correct.
//
// keyID and secret are required — callers must validate upstream and emit
// a clear error if the activation payload is missing them when iceberg is
// enabled. sessionToken is optional (omitted from the DDL when empty) to
// support the rare static-IAM-user case, though STS:AssumeRole always
// returns one in production.
func BuildIcebergSecretStmt(cfg Config, keyID, secret, sessionToken string) string {
	region := cfg.Region
	if region == "" {
		region = DefaultRegion
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
//
// Kept as the S3 Tables-specific builder. The dispatcher in
// server.AttachIcebergCatalog picks between this and
// BuildLakekeeperAttachStmt based on Config.ResolvedBackend.
func BuildIcebergAttachStmt(cfg Config) string {
	return fmt.Sprintf(
		"ATTACH '%s' AS %s (TYPE iceberg, ENDPOINT_TYPE 's3_tables')",
		escapeSQLStringLiteral(cfg.TableBucket),
		CatalogName,
	)
}

// LakekeeperSecretName is the DuckDB SECRET name used when ATTACHing a
// Lakekeeper REST catalog with OAuth2 client credentials. Distinct from
// iceberg_sigv4 so a worker that has both backends configured (post-migration)
// doesn't collide.
const LakekeeperSecretName = "iceberg_oauth"

// BuildLakekeeperSecretStmt builds a CREATE SECRET statement for the
// Lakekeeper REST catalog using the OAuth2 client_credentials flow. The
// DuckDB iceberg extension fetches an access token from OAUTH2_SERVER_URI
// using these CLIENT_ID/CLIENT_SECRET and caches it for subsequent REST
// calls.
//
// Returns "" when OAuth2 is not configured (allowall mode). Callers must
// then emit BuildLakekeeperAttachStmt with empty SECRET name + the
// AUTHORIZATION_TYPE 'none' option (handled inside the helper).
func BuildLakekeeperSecretStmt(cfg Config) string {
	if cfg.LakekeeperOAuth2ServerURI == "" {
		return ""
	}
	return fmt.Sprintf(
		"CREATE OR REPLACE SECRET %s (TYPE ICEBERG, CLIENT_ID '%s', CLIENT_SECRET '%s', OAUTH2_SERVER_URI '%s')",
		LakekeeperSecretName,
		escapeSQLStringLiteral(cfg.LakekeeperClientID),
		escapeSQLStringLiteral(cfg.LakekeeperClientSecret),
		escapeSQLStringLiteral(cfg.LakekeeperOAuth2ServerURI),
	)
}

// BuildLakekeeperAttachStmt builds the ATTACH statement for the Iceberg
// REST catalog served by Lakekeeper.
//
// We deliberately do NOT use ACCESS_DELEGATION_MODE 'vended_credentials':
// Lakekeeper's STS vending overflows AWS's packed-policy limit
// (PackedPolicyTooLarge), and it's unnecessary — the worker already holds S3
// creds for the warehouse bucket via the iceberg_sigv4 secret (built from the
// duckling's brokered STS creds, same per-org role/bucket; see
// attachLakekeeperCatalog). DuckDB uses that secret to read/write table data;
// Lakekeeper serves only catalog metadata.
//
// When OAUTH2_SERVER_URI is empty (allowall mode), AUTHORIZATION_TYPE 'none'
// is set instead of SECRET. When OAuth2 is configured, SECRET references
// the secret built by BuildLakekeeperSecretStmt.
func BuildLakekeeperAttachStmt(cfg Config) string {
	if cfg.LakekeeperOAuth2ServerURI == "" {
		return fmt.Sprintf(
			"ATTACH '%s' AS %s (TYPE ICEBERG, ENDPOINT '%s', AUTHORIZATION_TYPE 'none')",
			escapeSQLStringLiteral(cfg.LakekeeperWarehouse),
			CatalogName,
			escapeSQLStringLiteral(cfg.LakekeeperEndpoint),
		)
	}
	return fmt.Sprintf(
		"ATTACH '%s' AS %s (TYPE ICEBERG, ENDPOINT '%s', SECRET %s)",
		escapeSQLStringLiteral(cfg.LakekeeperWarehouse),
		CatalogName,
		escapeSQLStringLiteral(cfg.LakekeeperEndpoint),
		LakekeeperSecretName,
	)
}

// escapeSQLStringLiteral doubles single quotes so the value is safe to embed
// inside a single-quoted SQL string literal. Identical helper to the one in
// server/ducklake — duplicated rather than imported to keep the iceberg
// package free of cross-package dependencies.
func escapeSQLStringLiteral(v string) string {
	return strings.ReplaceAll(v, "'", "''")
}
