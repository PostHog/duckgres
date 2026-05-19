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

func TestEnsureDatabase_RejectsUnsafeIdent(t *testing.T) {
	err := EnsureDatabase(context.Background(), "postgres://stub", `evil"; DROP DATABASE foo;--`)
	if err == nil {
		t.Fatal("expected error for unsafe identifier, got nil")
	}
	if !strings.Contains(err.Error(), "unsafe identifier") {
		t.Fatalf("expected 'unsafe identifier' error, got: %v", err)
	}
}
