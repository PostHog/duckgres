package provisioner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestIsSafePGIdent(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"lakekeeper_acme", true},
		{"a", true},
		{"_underscore_start", true},
		{"with-hyphen", false},
		{"1starts_with_digit", false},
		{"has space", false},
		{`"injection"`, false},
		{`; DROP TABLE`, false},
		{"", false},
		// Postgres identifier max 63 — 64 chars should reject.
		{"x" + string(make([]byte, 63)), false}, // 64 NULs after x
	}
	// Build a 63-char valid ident and verify it passes.
	maxOK := make([]byte, 63)
	for i := range maxOK {
		maxOK[i] = 'a'
	}
	cases = append(cases, struct {
		in   string
		want bool
	}{string(maxOK), true})

	for _, c := range cases {
		if got := isSafePGIdent(c.in); got != c.want {
			t.Errorf("isSafePGIdent(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"plain":       `"plain"`,
		`with"quote`:  `"with""quote"`,
		"":            `""`,
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Errorf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestEnsureDatabase_AgainstLivePG is an integration test against a real
// Postgres. Skipped unless PG_ADMIN_DSN is set. Pair with the
// docker-compose stack at tmp/lakekeeper-proto:
//
//	export PG_ADMIN_DSN='postgres://lakekeeper:lakekeeper@localhost:5434/lakekeeper?sslmode=disable'
//	go test ./controlplane/provisioner/ -run TestEnsureDatabase -v
func TestEnsureDatabase_AgainstLivePG(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set; skipping live Postgres test")
	}

	dbName := fmt.Sprintf("lakekeeper_test_%d", os.Getpid())
	t.Cleanup(func() {
		// Best-effort drop. Use a fresh connection because EnsureDatabase
		// closes its own.
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Logf("cleanup open: %v", err)
			return
		}
		defer db.Close()
		if _, err := db.Exec("DROP DATABASE IF EXISTS " + quoteIdent(dbName)); err != nil {
			t.Logf("cleanup drop: %v", err)
		}
	})

	// First call creates.
	if err := EnsureDatabase(context.Background(), dsn, dbName); err != nil {
		t.Fatalf("EnsureDatabase (first): %v", err)
	}
	// Second call is a no-op (idempotent).
	if err := EnsureDatabase(context.Background(), dsn, dbName); err != nil {
		t.Fatalf("EnsureDatabase (idempotent): %v", err)
	}
	// Verify the database actually exists.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("verify open: %v", err)
	}
	defer db.Close()
	var exists bool
	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", dbName).Scan(&exists); err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if !exists {
		t.Fatalf("database %s not found after EnsureDatabase", dbName)
	}
}

// TestEnsureRole_AgainstLivePG verifies the role create/alter/grant path
// against a real Postgres. Skipped unless PG_ADMIN_DSN is set.
func TestEnsureRole_AgainstLivePG(t *testing.T) {
	dsn := os.Getenv("PG_ADMIN_DSN")
	if dsn == "" {
		t.Skip("PG_ADMIN_DSN not set")
	}
	dbName := fmt.Sprintf("lakekeeper_role_test_%d", os.Getpid())
	roleName := dbName
	pw1 := "abcdef0123456789"
	pw2 := "fedcba9876543210"
	t.Cleanup(func() {
		db, _ := sql.Open("pgx", dsn)
		defer db.Close()
		// Order matters: drop privs before dropping the role.
		_, _ = db.Exec("REASSIGN OWNED BY " + quoteIdent(roleName) + " TO CURRENT_USER")
		_, _ = db.Exec("DROP OWNED BY " + quoteIdent(roleName))
		_, _ = db.Exec("DROP DATABASE IF EXISTS " + quoteIdent(dbName))
		_, _ = db.Exec("DROP ROLE IF EXISTS " + quoteIdent(roleName))
	})

	ctx := context.Background()
	if err := EnsureDatabase(ctx, dsn, dbName); err != nil {
		t.Fatalf("EnsureDatabase: %v", err)
	}
	if err := EnsureRole(ctx, dsn, roleName, pw1, dbName); err != nil {
		t.Fatalf("EnsureRole (create): %v", err)
	}
	// Idempotent: second call with same password is a no-op-ish ALTER.
	if err := EnsureRole(ctx, dsn, roleName, pw1, dbName); err != nil {
		t.Fatalf("EnsureRole (idempotent): %v", err)
	}
	// Verify the role can actually connect with pw1.
	connDSN := fmt.Sprintf("postgres://%s:%s@localhost:5434/%s?sslmode=disable", roleName, pw1, dbName)
	if connDB, err := sql.Open("pgx", connDSN); err != nil {
		t.Fatalf("open as role: %v", err)
	} else {
		var one int
		if err := connDB.QueryRow("SELECT 1").Scan(&one); err != nil {
			t.Errorf("connect+query as role with pw1 failed: %v", err)
		}
		connDB.Close()
	}
	// Rotate the password and verify the new one connects.
	if err := EnsureRole(ctx, dsn, roleName, pw2, dbName); err != nil {
		t.Fatalf("EnsureRole (rotate): %v", err)
	}
	connDSN2 := fmt.Sprintf("postgres://%s:%s@localhost:5434/%s?sslmode=disable", roleName, pw2, dbName)
	if connDB, err := sql.Open("pgx", connDSN2); err != nil {
		t.Fatalf("open as role with rotated pw: %v", err)
	} else {
		var one int
		if err := connDB.QueryRow("SELECT 1").Scan(&one); err != nil {
			t.Errorf("connect+query with rotated password failed: %v", err)
		}
		connDB.Close()
	}
}

func TestIsSafePGPassword(t *testing.T) {
	cases := map[string]bool{
		"abc123":                                       true,
		"hex0123456789abcdef":                          true,
		"with-allowed-chars_=.+/":                      true,
		"":                                             false,
		"has space":                                    false,
		"has'quote":                                    false,
		`has"doublequote`:                              false,
		"has\nnewline":                                 false,
		"has;semicolon":                                false,
	}
	for in, want := range cases {
		if got := isSafePGPassword(in); got != want {
			t.Errorf("isSafePGPassword(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestQuoteLiteral(t *testing.T) {
	cases := map[string]string{
		"plain":           `'plain'`,
		"with'quote":      `'with''quote'`,
		"":                `''`,
		`multiple''ticks`: `'multiple''''ticks'`,
	}
	for in, want := range cases {
		if got := quoteLiteral(in); got != want {
			t.Errorf("quoteLiteral(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEnsureRole_RejectsUnsafeInput(t *testing.T) {
	ctx := context.Background()
	// Unsafe role
	if err := EnsureRole(ctx, "postgres://stub", "bad role", "abc123", "db"); err == nil ||
		!strings.Contains(err.Error(), "unsafe role name") {
		t.Errorf("expected unsafe role error, got: %v", err)
	}
	// Unsafe db
	if err := EnsureRole(ctx, "postgres://stub", "role", "abc123", "bad db"); err == nil ||
		!strings.Contains(err.Error(), "unsafe db name") {
		t.Errorf("expected unsafe db error, got: %v", err)
	}
	// Empty password
	if err := EnsureRole(ctx, "postgres://stub", "role", "", "db"); err == nil ||
		!strings.Contains(err.Error(), "empty password") {
		t.Errorf("expected empty password error, got: %v", err)
	}
}

func TestEnsureDatabase_RejectsUnsafeIdent(t *testing.T) {
	err := EnsureDatabase(context.Background(), "postgres://stub", `evil"; DROP DATABASE foo;--`)
	if err == nil {
		t.Fatal("expected error for unsafe identifier, got nil")
	}
	if !strings.Contains(err.Error(), "unsafe identifier") {
		t.Fatalf("expected 'unsafe identifier' error, got: %v", err)
	}
}
