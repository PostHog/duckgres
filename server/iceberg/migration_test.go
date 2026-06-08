package iceberg

import "testing"

func TestBuildIcebergSecretStmtWithExplicitCreds(t *testing.T) {
	got := BuildIcebergSecretStmt(Config{Region: "us-west-2"}, "AKIA_TEST", "shh", "tok123")
	want := "CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER config, KEY_ID 'AKIA_TEST', SECRET 'shh', REGION 'us-west-2', SESSION_TOKEN 'tok123')"
	if got != want {
		t.Fatalf("BuildIcebergSecretStmt mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildIcebergSecretStmtWithoutSessionToken(t *testing.T) {
	got := BuildIcebergSecretStmt(Config{Region: "us-west-2"}, "AKIA_TEST", "shh", "")
	want := "CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER config, KEY_ID 'AKIA_TEST', SECRET 'shh', REGION 'us-west-2')"
	if got != want {
		t.Fatalf("BuildIcebergSecretStmt without session token mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildIcebergSecretStmtDefaultsRegion(t *testing.T) {
	got := BuildIcebergSecretStmt(Config{}, "AKIA_TEST", "shh", "tok")
	want := "CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER config, KEY_ID 'AKIA_TEST', SECRET 'shh', REGION 'us-east-1', SESSION_TOKEN 'tok')"
	if got != want {
		t.Fatalf("BuildIcebergSecretStmt should default to us-east-1:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildIcebergSecretStmtEscapesQuotesInCreds(t *testing.T) {
	// Defensive: STS creds shouldn't contain single quotes in practice, but
	// the helper escapes them anyway so an attacker-controlled value can't
	// break out of the SQL string literal.
	got := BuildIcebergSecretStmt(Config{Region: "us-east-1"}, "key'with'quotes", "secret'too", "tok'en")
	want := "CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER config, KEY_ID 'key''with''quotes', SECRET 'secret''too', REGION 'us-east-1', SESSION_TOKEN 'tok''en')"
	if got != want {
		t.Fatalf("BuildIcebergSecretStmt did not escape quotes in creds:\n got: %s\nwant: %s", got, want)
	}
}

// --- Lakekeeper backend ----------------------------------------------------

func TestBuildLakekeeperSecretStmt_WithOAuth2(t *testing.T) {
	got := BuildLakekeeperSecretStmt(Config{
		LakekeeperClientID:        "duckling-acme",
		LakekeeperClientSecret:    "abc123",
		LakekeeperOAuth2ServerURI: "http://oidc/token",
	})
	want := "CREATE OR REPLACE SECRET iceberg_oauth (TYPE ICEBERG, CLIENT_ID 'duckling-acme', CLIENT_SECRET 'abc123', OAUTH2_SERVER_URI 'http://oidc/token')"
	if got != want {
		t.Fatalf("BuildLakekeeperSecretStmt mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildLakekeeperSecretStmt_AllowallReturnsEmpty(t *testing.T) {
	got := BuildLakekeeperSecretStmt(Config{LakekeeperClientID: "duckling-acme"})
	if got != "" {
		t.Fatalf("expected empty secret stmt for allowall mode, got: %q", got)
	}
}

func TestBuildLakekeeperAttachStmt_Allowall(t *testing.T) {
	got := BuildLakekeeperAttachStmt(Config{
		LakekeeperEndpoint:  "http://lakekeeper-acme.lakekeeper.svc:8181/catalog",
		LakekeeperWarehouse: "org-acme",
	})
	want := "ATTACH 'org-acme' AS iceberg (TYPE ICEBERG, ENDPOINT 'http://lakekeeper-acme.lakekeeper.svc:8181/catalog', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
	if got != want {
		t.Fatalf("BuildLakekeeperAttachStmt (allowall) mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildLakekeeperAttachStmt_OAuth2(t *testing.T) {
	got := BuildLakekeeperAttachStmt(Config{
		LakekeeperEndpoint:        "http://lakekeeper-acme.lakekeeper.svc:8181/catalog",
		LakekeeperWarehouse:       "org-acme",
		LakekeeperOAuth2ServerURI: "http://oidc/token",
	})
	want := "ATTACH 'org-acme' AS iceberg (TYPE ICEBERG, ENDPOINT 'http://lakekeeper-acme.lakekeeper.svc:8181/catalog', SECRET iceberg_oauth, ACCESS_DELEGATION_MODE 'none')"
	if got != want {
		t.Fatalf("BuildLakekeeperAttachStmt (oauth2) mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildLakekeeperAttachStmt_EscapesQuotes(t *testing.T) {
	got := BuildLakekeeperAttachStmt(Config{
		LakekeeperEndpoint:  "http://h/cat?x='y'",
		LakekeeperWarehouse: "wh'name",
	})
	want := "ATTACH 'wh''name' AS iceberg (TYPE ICEBERG, ENDPOINT 'http://h/cat?x=''y''', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
	if got != want {
		t.Fatalf("BuildLakekeeperAttachStmt did not escape quotes:\n got: %s\nwant: %s", got, want)
	}
}

func TestResolvedBackend(t *testing.T) {
	// Backend is lakekeeper-only now; any input resolves to that constant.
	cases := map[string]string{
		"":                BackendLakekeeper,
		BackendLakekeeper: BackendLakekeeper,
		"future":          BackendLakekeeper,
	}
	for in, want := range cases {
		if got := (Config{Backend: in}).ResolvedBackend(); got != want {
			t.Errorf("ResolvedBackend(%q) = %q, want %q", in, got, want)
		}
	}
}
