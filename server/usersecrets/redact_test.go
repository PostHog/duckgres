package usersecrets

import (
	"strings"
	"testing"
)

// secretLiteral is a recognizable credential token; tests assert it never
// survives redaction.
const secretLiteral = "AKIAreallyrealkey1234"

func TestRedactForLogNonLeadingSecretDDL(t *testing.T) {
	tests := []struct {
		name  string
		query string
		// want is the exact redacted output when set; leave empty to only
		// assert the secret literal is gone.
		want string
		// passthrough asserts the query is returned unchanged.
		passthrough bool
	}{
		{
			name:  "single create persistent secret head",
			query: "CREATE PERSISTENT SECRET foo (TYPE s3, KEY_ID '" + secretLiteral + "', SECRET 'x')",
			want:  "CREATE PERSISTENT SECRET foo " + redactedPlaceholder,
		},
		{
			name:  "single create secret unnamed",
			query: "CREATE SECRET (TYPE s3, KEY_ID '" + secretLiteral + "')",
			want:  "CREATE SECRET " + redactedPlaceholder,
		},
		{
			name:        "plain select passes through",
			query:       "SELECT 1",
			passthrough: true,
		},
		{
			name:        "multi-statement non-secret passes through",
			query:       "SELECT 1; SELECT 2",
			passthrough: true,
		},
		{
			name:        "drop secret passes through",
			query:       "DROP PERSISTENT SECRET foo",
			passthrough: true,
		},
		{
			name:  "leading select then create secret",
			query: "SELECT 1; CREATE SECRET foo (TYPE s3, KEY_ID '" + secretLiteral + "')",
			want:  redactedPlaceholder,
		},
		{
			name:  "leading select then create persistent secret",
			query: "SELECT 1; CREATE PERSISTENT SECRET foo (TYPE s3, SECRET '" + secretLiteral + "')",
			want:  redactedPlaceholder,
		},
		{
			name:  "begin then create persistent secret",
			query: "BEGIN; CREATE PERSISTENT SECRET foo (TYPE s3, SECRET '" + secretLiteral + "')",
			want:  redactedPlaceholder,
		},
		{
			name:  "create secret not at end of batch",
			query: "SELECT 1; CREATE SECRET foo (TYPE s3, SECRET '" + secretLiteral + "'); SELECT 2",
			want:  redactedPlaceholder,
		},
		{
			name: "semicolon inside string literal does not split",
			// A non-secret query whose string literal contains a semicolon
			// followed by text that looks like secret DDL must NOT trigger a
			// false split that would still be safe — but more importantly it
			// must not be mis-tokenized. It's a single statement, passes through.
			query:       "SELECT 'a; CREATE SECRET evil (SECRET ''" + secretLiteral + "'')'",
			passthrough: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactForLog(tt.query)
			if tt.passthrough {
				if got != tt.query {
					t.Fatalf("expected passthrough, got %q", got)
				}
				return
			}
			if tt.want != "" && got != tt.want {
				t.Fatalf("RedactForLog(%q) = %q, want %q", tt.query, got, tt.want)
			}
			if strings.Contains(got, secretLiteral) {
				t.Fatalf("secret literal leaked in redacted output: %q", got)
			}
		})
	}
}

func TestContainsPersistentSecretDDL(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"CREATE PERSISTENT SECRET foo (TYPE s3)", true},
		{"DROP PERSISTENT SECRET foo", true},
		{"SELECT 1; CREATE PERSISTENT SECRET foo (TYPE s3)", true},
		{"BEGIN; CREATE PERSISTENT SECRET foo (TYPE s3); COMMIT", true},
		{"SELECT 1; DROP PERSISTENT SECRET foo", true},
		// Plain / temporary secrets are session-scoped, not persistent.
		{"CREATE SECRET foo (TYPE s3)", false},
		{"CREATE TEMPORARY SECRET foo (TYPE s3)", false},
		{"SELECT 1; CREATE TEMPORARY SECRET foo (TYPE s3)", false},
		{"SELECT 1; SELECT 2", false},
		{"SELECT 'CREATE PERSISTENT SECRET' FROM t", false},
	}
	for _, tt := range tests {
		if got := ContainsPersistentSecretDDL(tt.query); got != tt.want {
			t.Errorf("ContainsPersistentSecretDDL(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestSplitTopLevel(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"single", "SELECT 1", []string{"SELECT 1"}},
		{"two", "SELECT 1; SELECT 2", []string{"SELECT 1", " SELECT 2"}},
		{"trailing semicolon", "SELECT 1;", []string{"SELECT 1"}},
		{"semicolon in string", "SELECT 'a;b'", []string{"SELECT 'a;b'"}},
		{"semicolon in quoted ident", `SELECT "a;b"`, []string{`SELECT "a;b"`}},
		{"semicolon in line comment", "SELECT 1 -- a;b\n; SELECT 2", []string{"SELECT 1 -- a;b\n", " SELECT 2"}},
		{"semicolon in block comment", "SELECT 1 /* a;b */; SELECT 2", []string{"SELECT 1 /* a;b */", " SELECT 2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitTopLevel(tt.query)
			if len(got) != len(tt.want) {
				t.Fatalf("splitTopLevel(%q) = %#v, want %#v", tt.query, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("splitTopLevel(%q)[%d] = %q, want %q", tt.query, i, got[i], tt.want[i])
				}
			}
		})
	}
}
