package iceberg

import "testing"

func TestBuildIcebergAttachStmt(t *testing.T) {
	got := BuildIcebergAttachStmt(Config{
		TableBucket: "arn:aws:s3tables:us-east-1:123456789012:bucket/posthog-duckling-acme-iceberg",
	})
	want := "ATTACH 'arn:aws:s3tables:us-east-1:123456789012:bucket/posthog-duckling-acme-iceberg' AS iceberg (TYPE iceberg, ENDPOINT_TYPE 's3_tables')"
	if got != want {
		t.Fatalf("BuildIcebergAttachStmt mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildIcebergAttachStmtEscapesSingleQuotes(t *testing.T) {
	// Defensive: ARN won't contain single quotes in practice, but the
	// helper escapes them anyway so an attacker-controlled config can't
	// break out of the SQL string literal.
	got := BuildIcebergAttachStmt(Config{TableBucket: "weird'name"})
	want := "ATTACH 'weird''name' AS iceberg (TYPE iceberg, ENDPOINT_TYPE 's3_tables')"
	if got != want {
		t.Fatalf("BuildIcebergAttachStmt did not escape quote:\n got: %s\nwant: %s", got, want)
	}
}

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
