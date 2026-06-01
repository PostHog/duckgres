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

// DefaultSchema is the schema the worker guarantees exists in the Iceberg
// catalog so customers can switch to it with a bare `USE iceberg` (rewritten
// to `USE iceberg.<DefaultSchema>` in server.rewriteDirectQuery).
//
// DuckLake uses `main` for this role, but we cannot: DuckDB reserves `main` as
// a catalog's implicit local schema and shadows the REST catalog's real `main`
// namespace, so `iceberg.main` never resolves (it's absent from
// information_schema even when Lakekeeper has a `main` namespace). `public`
// (the Postgres convention) is a safe, unshadowed default.
const DefaultSchema = "public"

// BuildIcebergSecretStmt builds the CREATE SECRET statement that lets the
// DuckDB iceberg extension sign warehouse-data S3 requests with the
// duckling's brokered per-org credentials.
//
// DuckDB's TYPE ICEBERG secret is OAuth2-only (single 'config' provider
// registered by OAuth2Authorization::CreateCatalogSecretFunction), so the
// data-plane credentials have to ride on a TYPE S3 secret with PROVIDER
// config — the iceberg extension picks them up from any TYPE S3 secret in
// scope when it issues warehouse reads/writes.
//
// The control plane assumes the per-tenant IAM role via STS and ships the
// resulting short-lived credentials in the activation payload (identical to
// how the DuckLake S3 secret is built in server.buildConfigSecret); the same
// role has s3:* on the warehouse bucket, so reusing them here is correct.
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

// LakekeeperSecretName is the DuckDB SECRET name used when ATTACHing a
// Lakekeeper REST catalog with OAuth2 client credentials. Distinct from the
// data-plane iceberg_sigv4 (TYPE S3) so they coexist cleanly.
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

// LakekeeperAccessDelegationNone disables Iceberg REST credential vending on
// the ATTACH. This MUST be set explicitly: DuckDB's iceberg extension defaults
// ACCESS_DELEGATION_MODE to 'vended_credentials', so simply *omitting* the
// option does NOT turn vending off (the lesson from the first live write test).
const LakekeeperAccessDelegationNone = "ACCESS_DELEGATION_MODE 'none'"

// BuildLakekeeperAttachStmt builds the ATTACH statement for the Iceberg
// REST catalog served by Lakekeeper.
//
// We explicitly set ACCESS_DELEGATION_MODE 'none' (NOT 'vended_credentials',
// and crucially NOT omitted — see below). Two reasons vending is wrong here:
//   - Lakekeeper's STS vending overflows AWS's packed-policy limit
//     (PackedPolicyTooLarge), so vending is disabled on the warehouse anyway.
//   - With vending disabled server-side, a delegation-requesting client still
//     gets a per-table storage *config* (region/endpoint) back with NO
//     credentials. DuckDB turns that into a path-scoped S3 secret with empty
//     creds, and because its scope (the table's S3 prefix) is MORE SPECIFIC
//     than iceberg_sigv4's (s3://), it SHADOWS our working secret — every data
//     read/write then goes out anonymous and S3 returns 403. Verified live on
//     `ben` in managed-warehouse-dev: omitting the option 403'd; 'none' fixed it.
//
// With delegation off, DuckDB falls back to the ambient iceberg_sigv4 secret,
// which holds the duckling's own brokered S3 creds for the warehouse bucket
// (built in attachLakekeeperCatalog from the same per-org role/bucket). DuckDB
// uses that to read/write table data; Lakekeeper serves only catalog metadata.
//
// When OAUTH2_SERVER_URI is empty (allowall mode), AUTHORIZATION_TYPE 'none'
// is set instead of SECRET. When OAuth2 is configured, SECRET references
// the secret built by BuildLakekeeperSecretStmt.
func BuildLakekeeperAttachStmt(cfg Config) string {
	if cfg.LakekeeperOAuth2ServerURI == "" {
		return fmt.Sprintf(
			"ATTACH '%s' AS %s (TYPE ICEBERG, ENDPOINT '%s', AUTHORIZATION_TYPE 'none', %s)",
			escapeSQLStringLiteral(cfg.LakekeeperWarehouse),
			CatalogName,
			escapeSQLStringLiteral(cfg.LakekeeperEndpoint),
			LakekeeperAccessDelegationNone,
		)
	}
	return fmt.Sprintf(
		"ATTACH '%s' AS %s (TYPE ICEBERG, ENDPOINT '%s', SECRET %s, %s)",
		escapeSQLStringLiteral(cfg.LakekeeperWarehouse),
		CatalogName,
		escapeSQLStringLiteral(cfg.LakekeeperEndpoint),
		LakekeeperSecretName,
		LakekeeperAccessDelegationNone,
	)
}

// escapeSQLStringLiteral doubles single quotes so the value is safe to embed
// inside a single-quoted SQL string literal. Identical helper to the one in
// server/ducklake — duplicated rather than imported to keep the iceberg
// package free of cross-package dependencies.
func escapeSQLStringLiteral(v string) string {
	return strings.ReplaceAll(v, "'", "''")
}
