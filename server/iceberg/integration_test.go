package iceberg_test

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/posthog/duckgres/server/iceberg"
)

// TestIcebergSecretAcceptedByDuckDB is a regression test for the bug fixed
// by PR #562. Earlier history on the same code:
//   - Pre-#560 form (TYPE ICEBERG, AUTHORIZATION_TYPE 'SIGV4', REGION ...)
//     failed at bind time: "Unknown parameter ... with default provider
//     'config'". TYPE ICEBERG is OAuth2-only in the iceberg extension.
//   - #560 form (TYPE S3, PROVIDER credential_chain, REGION ...) parsed
//     fine but failed validation at CREATE time on workers without an AWS
//     identity reachable through DuckDB's built-in chain — which is every
//     worker on the PostHog multitenant deploy. The credential_chain path
//     has been removed entirely; iceberg now requires explicit STS-minted
//     credentials shipped in the activation payload (same as DuckLake).
//
// This test exercises the actual SQL string against an in-memory DuckDB
// with the iceberg extension loaded, so any future change that
// re-introduces a non-parseable or validation-failing secret form is
// caught at unit-test time without needing AWS credentials or network
// reach to S3 Tables.
//
// Gated on the iceberg extension being installable locally. When the
// extension cannot be installed/loaded — e.g. air-gapped CI — the test
// skips rather than fails so the regression coverage exists where it can
// run and disappears cleanly where it can't.
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

	// PROVIDER config with inlined KEY_ID/SECRET does not depend on any
	// host-side AWS state, so there is no legitimate reason for these to
	// error in any test environment. A failure here is the regression we
	// are guarding against. SESSION_TOKEN is optional in the DDL (omitted
	// for static IAM users); both shapes must be accepted.
	cases := []struct {
		name         string
		region       string
		keyID        string
		secret       string
		sessionToken string
	}{
		{"explicit creds with session token", "us-east-1", "AKIA_TEST", "shh", "session-token-fake"},
		{"explicit creds without session token", "us-west-2", "AKIA_TEST", "shh", ""},
	}
	for _, tc := range cases {
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
}

// TestLakekeeperSQLAcceptedByDuckDB verifies that the SQL emitted by
// BuildLakekeeperSecretStmt and BuildLakekeeperAttachStmt is syntactically
// accepted by DuckDB's iceberg extension. The ATTACH will fail to connect
// to the fictitious endpoint, but DuckDB validates option names + types at
// parse/bind time, BEFORE attempting the connection — so a wrong keyword
// (e.g. ACCESS_DELEGATION_MODE vs DELEGATION_MODE) shows up here, not at
// activation time in prod.
//
// This is the equivalent of TestIcebergSecretAcceptedByDuckDB for the
// Lakekeeper backend, closing the gap noted in the PR2 deep review.
func TestLakekeeperSQLAcceptedByDuckDB(t *testing.T) {
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

	t.Run("oauth2 secret + attach with SECRET reference", func(t *testing.T) {
		cfg := iceberg.Config{
			LakekeeperEndpoint:        "http://lakekeeper.invalid/catalog",
			LakekeeperWarehouse:       "org-test",
			LakekeeperClientID:        "duckling-test",
			LakekeeperClientSecret:    "shh",
			LakekeeperOAuth2ServerURI: "http://oidc.invalid/token",
		}
		// CREATE SECRET (TYPE ICEBERG) eagerly POSTs to OAUTH2_SERVER_URI to
		// fetch a token; we point at a bogus host so the network step fails
		// fast. The point of this assertion is: a network/resolution error
		// proves DuckDB ACCEPTED the option keywords and tried to use them.
		// What we MUST reject is parse/bind-time errors that would mean
		// DuckDB never got past keyword validation.
		secretStmt := iceberg.BuildLakekeeperSecretStmt(cfg)
		if secretStmt == "" {
			t.Fatal("expected non-empty secret stmt for OAuth2 mode")
		}
		if _, err := db.Exec(secretStmt); err != nil && isParseOrBindError(err) {
			t.Fatalf("BuildLakekeeperSecretStmt option/syntax error from DuckDB:\n%s\nerr: %v", secretStmt, err)
		}
		// ATTACH likewise. May fail on the endpoint connection, but must
		// not fail at parse/bind time.
		attachStmt := iceberg.BuildLakekeeperAttachStmt(cfg)
		_, err := db.Exec(attachStmt)
		if err != nil && isParseOrBindError(err) {
			t.Fatalf("BuildLakekeeperAttachStmt option/syntax error from DuckDB:\n%s\nerr: %v", attachStmt, err)
		}
		_, _ = db.Exec("DETACH " + iceberg.CatalogName)
	})

	t.Run("allowall attach with AUTHORIZATION_TYPE 'none'", func(t *testing.T) {
		cfg := iceberg.Config{
			LakekeeperEndpoint:  "http://lakekeeper.invalid/catalog",
			LakekeeperWarehouse: "org-test",
		}
		// No SECRET in this mode.
		if got := iceberg.BuildLakekeeperSecretStmt(cfg); got != "" {
			t.Fatalf("allowall mode should produce empty secret stmt, got: %s", got)
		}
		attachStmt := iceberg.BuildLakekeeperAttachStmt(cfg)
		_, err := db.Exec(attachStmt)
		if err != nil && isParseOrBindError(err) {
			t.Fatalf("BuildLakekeeperAttachStmt (allowall) option/syntax error from DuckDB:\n%s\nerr: %v", attachStmt, err)
		}
		_, _ = db.Exec("DETACH " + iceberg.CatalogName)
	})
}

// isParseOrBindError reports whether err is a DuckDB parse/bind error —
// i.e. the statement was rejected before DuckDB attempted any I/O. Tests
// use this to distinguish "your SQL has a typo" from "your fictitious
// endpoint didn't resolve", which is the assertion that actually matters
// when validating the SQL strings produced by Build* helpers.
func isParseOrBindError(err error) bool {
	msg := err.Error()
	for _, marker := range []string{"Parser Error", "Binder Error", "Unknown parameter", "syntax error"} {
		for i := 0; i+len(marker) <= len(msg); i++ {
			if msg[i:i+len(marker)] == marker {
				return true
			}
		}
	}
	return false
}
