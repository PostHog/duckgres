package usersecrets

import "testing"

func TestClassify(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  Statement
	}{
		{
			name:  "create persistent named",
			query: "CREATE PERSISTENT SECRET my_s3 (TYPE s3, KEY_ID 'k', SECRET 's')",
			want:  Statement{Kind: KindCreate, Name: "my_s3", Persistent: true},
		},
		{
			name:  "create or replace persistent",
			query: "create or replace persistent secret My_S3 (TYPE s3)",
			want:  Statement{Kind: KindCreate, Name: "my_s3", Persistent: true},
		},
		{
			name:  "create persistent if not exists",
			query: "CREATE PERSISTENT SECRET IF NOT EXISTS foo (TYPE s3)",
			want:  Statement{Kind: KindCreate, Name: "foo", Persistent: true, IfNotExists: true},
		},
		{
			name:  "create temporary",
			query: "CREATE TEMPORARY SECRET t1 (TYPE s3)",
			want:  Statement{Kind: KindCreate, Name: "t1", Temporary: true},
		},
		{
			name:  "create plain (temp by default)",
			query: "CREATE SECRET plain1 (TYPE s3)",
			want:  Statement{Kind: KindCreate, Name: "plain1"},
		},
		{
			name:  "create unnamed persistent",
			query: "CREATE PERSISTENT SECRET (TYPE s3, KEY_ID 'k')",
			want:  Statement{Kind: KindCreate, Persistent: true},
		},
		{
			name:  "leading comments and whitespace",
			query: "  -- set up creds\n/* block */ CREATE PERSISTENT SECRET c1 (TYPE gcs)",
			want:  Statement{Kind: KindCreate, Name: "c1", Persistent: true},
		},
		{
			name:  "quoted identifier",
			query: `CREATE PERSISTENT SECRET "My""Quoted" (TYPE s3)`,
			want:  Statement{Kind: KindCreate, Name: `my"quoted`, Persistent: true},
		},
		{
			name:  "drop persistent",
			query: "DROP PERSISTENT SECRET my_s3",
			want:  Statement{Kind: KindDrop, Name: "my_s3", Persistent: true},
		},
		{
			name:  "drop persistent if exists",
			query: "DROP PERSISTENT SECRET IF EXISTS my_s3;",
			want:  Statement{Kind: KindDrop, Name: "my_s3", Persistent: true},
		},
		{
			name:  "drop plain",
			query: "DROP SECRET my_s3",
			want:  Statement{Kind: KindDrop, Name: "my_s3"},
		},
		{
			name:  "drop temporary",
			query: "DROP TEMPORARY SECRET my_s3",
			want:  Statement{Kind: KindDrop, Name: "my_s3", Temporary: true},
		},
		{
			name:  "trailing semicolon ok",
			query: "CREATE PERSISTENT SECRET s (TYPE s3);",
			want:  Statement{Kind: KindCreate, Name: "s", Persistent: true},
		},
		{
			name:  "trailing semicolon then comment ok",
			query: "CREATE PERSISTENT SECRET s (TYPE s3); -- done",
			want:  Statement{Kind: KindCreate, Name: "s", Persistent: true},
		},
		{
			name:  "multi-statement flagged",
			query: "CREATE PERSISTENT SECRET s (TYPE s3); SELECT 1",
			want:  Statement{Kind: KindCreate, Name: "s", Persistent: true, MultiStatement: true},
		},
		{
			name:  "multi-statement drop flagged",
			query: "DROP PERSISTENT SECRET s; SELECT 1",
			want:  Statement{Kind: KindDrop, Name: "s", Persistent: true, MultiStatement: true},
		},
		{
			name:  "semicolon inside string literal ok",
			query: "CREATE PERSISTENT SECRET s (TYPE s3, SECRET 'a;b')",
			want:  Statement{Kind: KindCreate, Name: "s", Persistent: true},
		},
		{
			name:  "not secret ddl",
			query: "SELECT * FROM t",
			want:  Statement{},
		},
		{
			name:  "create table not matched",
			query: "CREATE TABLE secret (id INT)",
			want:  Statement{},
		},
		{
			name:  "drop secretive_thing not matched",
			query: "DROP SECRETIVE_THING x",
			want:  Statement{},
		},
		{
			name:  "create or without replace refused",
			query: "CREATE OR SECRET s (TYPE s3)",
			want:  Statement{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.query); got != tt.want {
				t.Errorf("Classify(%q) = %+v, want %+v", tt.query, got, tt.want)
			}
		})
	}
}

func TestIsReservedName(t *testing.T) {
	for _, name := range []string{"ducklake_s3", "DUCKLAKE_S3", "__default_s3", "duckgres_internal"} {
		if !IsReservedName(name) {
			t.Errorf("IsReservedName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"my_s3", "customer_gcs", "ducklake", "iceberg_sigv4", "iceberg_oauth"} {
		if IsReservedName(name) {
			t.Errorf("IsReservedName(%q) = true, want false", name)
		}
	}
}

func TestRedactForLog(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{
			query: "CREATE PERSISTENT SECRET my_s3 (TYPE s3, KEY_ID 'AKIA…', SECRET 'hunter2')",
			want:  "CREATE PERSISTENT SECRET my_s3 (…redacted)",
		},
		{
			query: "create or replace secret s (TYPE s3, SECRET 'x')",
			want:  "create or replace secret s (…redacted)",
		},
		{
			query: "-- note\nCREATE SECRET (TYPE s3, SECRET 'x')",
			want:  "-- note\nCREATE SECRET (…redacted)",
		},
		{
			// Redaction must track the tokenizer, not a flat prefix list:
			// extra whitespace between keywords must still redact.
			query: "CREATE  PERSISTENT\nSECRET s (TYPE s3, SECRET 'hunter2')",
			want:  "CREATE  PERSISTENT\nSECRET s (…redacted)",
		},
		{
			// ... and comments between keywords.
			query: "CREATE /*c*/ PERSISTENT SECRET s (TYPE s3, SECRET 'hunter2')",
			want:  "CREATE /*c*/ PERSISTENT SECRET s (…redacted)",
		},
		{
			// Multi-statement strings with a secret-DDL head drop everything
			// after the head (the trailing statements go with the options).
			query: "CREATE PERSISTENT SECRET s (TYPE s3, SECRET 'x'); SELECT 1",
			want:  "CREATE PERSISTENT SECRET s (…redacted)",
		},
		{
			query: "CREATE SECRET IF NOT EXISTS foo (TYPE s3, SECRET 'x')",
			want:  "CREATE SECRET IF NOT EXISTS foo (…redacted)",
		},
		{
			query: "DROP PERSISTENT SECRET my_s3",
			want:  "DROP PERSISTENT SECRET my_s3",
		},
		{
			query: "SELECT 'CREATE SECRET' FROM t",
			want:  "SELECT 'CREATE SECRET' FROM t",
		},
		{
			query: "CREATE SECRETIVE_TABLE (id INT)",
			want:  "CREATE SECRETIVE_TABLE (id INT)",
		},
	}
	for _, tt := range tests {
		if got := RedactForLog(tt.query); got != tt.want {
			t.Errorf("RedactForLog(%q) = %q, want %q", tt.query, got, tt.want)
		}
	}
}

func TestCipherRoundTrip(t *testing.T) {
	// 32 bytes of 'k', base64-encoded.
	key := "a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s="
	c, err := NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	stmt := "CREATE PERSISTENT SECRET my_s3 (TYPE s3, KEY_ID 'k', SECRET 's')"
	sealed, err := c.Seal("org1", "alice", "my_s3", stmt)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	got, err := c.Open("org1", "alice", "my_s3", sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got != stmt {
		t.Errorf("round trip = %q, want %q", got, stmt)
	}

	// Ciphertext is bound to its row: moving it to another row must fail.
	if _, err := c.Open("org1", "bob", "my_s3", sealed); err == nil {
		t.Error("Open with different username succeeded, want error")
	}
	if _, err := c.Open("org1", "alice", "other", sealed); err == nil {
		t.Error("Open with different secret name succeeded, want error")
	}

	// Wrong key must fail.
	other, err := NewCipher("eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHg=")
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	if _, err := other.Open("org1", "alice", "my_s3", sealed); err == nil {
		t.Error("Open with wrong key succeeded, want error")
	}
}

func TestNewCipherRejectsBadKeys(t *testing.T) {
	if _, err := NewCipher("not-base64!!!"); err == nil {
		t.Error("NewCipher with invalid base64 succeeded, want error")
	}
	if _, err := NewCipher("c2hvcnQ="); err == nil { // "short"
		t.Error("NewCipher with short key succeeded, want error")
	}
}
