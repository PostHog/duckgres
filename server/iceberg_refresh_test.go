package server

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/posthog/duckgres/server/iceberg"
)

// TestRefreshIcebergSecretRotatesCredentials exercises the full refresh
// path against an in-memory DuckDB with the iceberg extension loaded:
// initial CREATE, then a rotated CREATE OR REPLACE, both must succeed
// without DuckDB rejecting the SQL form. This is the regression net for
// "iceberg queries 403 after STS rotation on a long-lived hot-idle
// worker" — without RefreshIcebergSecret, only DuckLake's secret would
// be rotated and the iceberg_sigv4 secret would keep its expired creds.
//
// Skips when the iceberg extension can't be installed/loaded (air-gapped
// sandbox); same posture as TestIcebergSecretAcceptedByDuckDB.
func TestRefreshIcebergSecretRotatesCredentials(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("INSTALL iceberg"); err != nil {
		t.Skipf("iceberg extension unavailable (network or sandbox): %v", err)
	}
	if _, err := db.Exec("LOAD iceberg"); err != nil {
		t.Skipf("iceberg extension load failed: %v", err)
	}

	ic := IcebergConfig{
		Enabled:     true,
		TableBucket: "arn:aws:s3tables:us-east-1:000000000000:bucket/refresh-test",
		Region:      "us-east-1",
	}

	if err := RefreshIcebergSecret(db, ic, nil, "AKIA_INITIAL", "initial-secret", "initial-token"); err != nil {
		t.Fatalf("initial refresh rejected by DuckDB: %v", err)
	}
	if err := RefreshIcebergSecret(db, ic, nil, "AKIA_ROTATED", "rotated-secret", "rotated-token"); err != nil {
		t.Fatalf("rotated refresh rejected by DuckDB: %v", err)
	}
}

// TestRefreshIcebergSecretNoOpWhenDisabled confirms that the refresh
// path returns nil without touching DuckDB when iceberg isn't enabled
// for the tenant — important because the activation layer calls this
// unconditionally on every credential-change refresh and shouldn't
// require iceberg to be enabled.
func TestRefreshIcebergSecretNoOpWhenDisabled(t *testing.T) {
	if err := RefreshIcebergSecret(nil, IcebergConfig{Enabled: false}, nil, "k", "s", "t"); err != nil {
		t.Fatalf("expected no-op when iceberg disabled, got: %v", err)
	}
}

// TestRefreshIcebergSecretRejectsEmptyCredentials guards the invariant
// that "credentials are required when iceberg is enabled" applies to
// refresh too, not just initial attach. A silent fallback here would either
// re-introduce credential_chain (the bug fixed by PR #562) or emit an
// empty-cred config secret that fails opaquely at attach time. Applies to
// BOTH backends: Lakekeeper no longer vends (PackedPolicyTooLarge), so the
// worker rotates its own iceberg_sigv4 S3 secret for Lakekeeper too.
func TestRefreshIcebergSecretRejectsEmptyCredentials(t *testing.T) {
	err := RefreshIcebergSecret(nil, IcebergConfig{
		Enabled:     true,
		Backend:     iceberg.BackendS3Tables,
		TableBucket: "arn:...",
	}, nil, "", "", "")
	if err == nil {
		t.Fatal("expected error when iceberg enabled with empty credentials, got nil")
	}
	if !strings.Contains(err.Error(), "no AWS credentials") {
		t.Fatalf("error message should name the missing-credentials cause, got: %v", err)
	}
}

// TestRefreshIcebergSecretLakekeeperRequiresCreds: Lakekeeper no longer vends
// credentials (its STS session policy overflowed AWS's packed-policy limit),
// so the worker reads/writes S3 data with its own brokered creds and must
// rotate that secret on the STS schedule — same as S3 Tables. An empty-cred
// refresh is therefore an error, not a no-op.
func TestRefreshIcebergSecretLakekeeperRequiresCreds(t *testing.T) {
	err := RefreshIcebergSecret(nil, IcebergConfig{
		Enabled:             true,
		Backend:             iceberg.BackendLakekeeper,
		LakekeeperEndpoint:  "http://lk/catalog",
		LakekeeperWarehouse: "org-x",
	}, nil, "", "", "")
	if err == nil {
		t.Fatal("expected error: Lakekeeper refresh now needs the worker's S3 creds")
	}
	if !strings.Contains(err.Error(), "no AWS credentials") {
		t.Fatalf("error message should name the missing-credentials cause, got: %v", err)
	}
}
