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

func TestBuildIcebergSecretStmt(t *testing.T) {
	got := BuildIcebergSecretStmt(Config{Region: "us-west-2"})
	want := "CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER credential_chain, REGION 'us-west-2')"
	if got != want {
		t.Fatalf("BuildIcebergSecretStmt mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildIcebergSecretStmtDefaultsRegion(t *testing.T) {
	got := BuildIcebergSecretStmt(Config{})
	want := "CREATE OR REPLACE SECRET iceberg_sigv4 (TYPE S3, PROVIDER credential_chain, REGION 'us-east-1')"
	if got != want {
		t.Fatalf("BuildIcebergSecretStmt should default to us-east-1:\n got: %s\nwant: %s", got, want)
	}
}
