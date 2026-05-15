package iceberg_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/posthog/duckgres/server/iceberg"
)

// TestIcebergSecretAcceptedByDuckDB is a regression test for the bug
// fixed by PR #562 (this PR): PR #560's `TYPE S3, PROVIDER credential_chain`
// secret form parsed cleanly but failed validation at CREATE time on
// workers without an AWS identity reachable through DuckDB's built-in
// chain. The earlier `TYPE ICEBERG, AUTHORIZATION_TYPE 'SIGV4'` form
// (pre-#560) failed at bind time with "Unknown parameter ... with default
// provider 'config'".
//
// We exercise the actual SQL string against an in-memory DuckDB with the
// iceberg extension loaded, so any future change that re-introduces a
// non-parseable or validation-failing secret form is caught at unit-test
// time without needing AWS credentials or network reach to S3 Tables.
//
// The test is gated on the iceberg extension being available locally
// (INSTALL pulls from DuckDB's repository on first run). When the
// extension cannot be installed/loaded — e.g. air-gapped CI — the test
// is skipped rather than failed so the regression coverage exists where
// it can run and disappears cleanly where it can't.
func TestIcebergSecretAcceptedByDuckDB(t *testing.T) {
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

	// Explicit-credentials cases: must succeed cleanly. PROVIDER config with
	// inlined KEY_ID/SECRET does not depend on any host-side AWS state, so
	// there's no legitimate reason for these to error in any test
	// environment. A failure here is the regression we are guarding against.
	strictCases := []struct {
		name         string
		region       string
		keyID        string
		secret       string
		sessionToken string
	}{
		{"explicit creds with session token", "us-east-1", "AKIA_TEST", "shh", "session-token-fake"},
		{"explicit creds without session token", "us-west-2", "AKIA_TEST", "shh", ""},
	}
	for _, tc := range strictCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := iceberg.BuildIcebergSecretStmt(
				iceberg.Config{Region: tc.region},
				tc.keyID, tc.secret, tc.sessionToken,
			)
			if _, err := db.Exec(stmt); err != nil {
				t.Fatalf("explicit-credential secret rejected by DuckDB: %v\nstmt: %s", err, stmt)
			}
		})
	}

	// Credential-chain fallback: the secret form must parse and be
	// recognized by DuckDB, but the credential walk itself depends on the
	// host having AWS credentials reachable through DuckDB's built-in
	// chain. Locally and in sandboxed CI this typically fails with
	// "Secret Validation Failure" — same shape as the original PR #560
	// bug on EKS workers. Tolerate that specific failure mode while still
	// catching binder/parser/type errors that would indicate the SQL form
	// itself regressed.
	t.Run("credential chain fallback", func(t *testing.T) {
		stmt := iceberg.BuildIcebergSecretStmt(iceberg.Config{Region: "us-east-1"}, "", "", "")
		_, err := db.Exec(stmt)
		if err == nil {
			return
		}
		lower := strings.ToLower(err.Error())
		fatalSignals := []string{
			"unknown parameter",
			"binder error",
			"parser error",
			"unknown secret type",
			"unknown provider",
		}
		for _, sig := range fatalSignals {
			if strings.Contains(lower, sig) {
				t.Fatalf("credential_chain SQL form rejected by DuckDB: %v\nstmt: %s", err, stmt)
			}
		}
		t.Logf("credential_chain create failed at validation (expected when host has no AWS identity): %v", err)
	})
}
