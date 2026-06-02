package server

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestDisablePersistentSecrets is the regression guard for the worker-side
// structural fix: with Config.DisablePersistentSecrets set (as
// duckdbservice.OpenDuckDBPair does for every worker), ConfigureMainDB must
//
//	(a) prevent CREATE PERSISTENT SECRET from succeeding, and
//	(b) not load any pre-existing on-disk persistent secret,
//
// which together make the "secret occurs in multiple storage backends"
// ambiguity impossible on workers.
func TestDisablePersistentSecrets(t *testing.T) {
	dir := t.TempDir()
	secretDir := SecretDirectory(Config{DataDir: dir})

	// Seed a persistent secret on disk using a normal (persistent-enabled)
	// connection, so we have a real file to prove (b) against.
	seed, err := sql.Open("duckdb", ":memory:?allow_unsigned_extensions=true")
	if err != nil {
		t.Fatalf("open seed duckdb: %v", err)
	}
	if _, err := seed.Exec("SET secret_directory = '" + secretDir + "'"); err != nil {
		t.Fatalf("seed set secret_directory: %v", err)
	}
	if _, err := seed.Exec("CREATE PERSISTENT SECRET seeded (TYPE s3, KEY_ID 'a', SECRET 'b')"); err != nil {
		t.Fatalf("seed create persistent secret: %v", err)
	}
	_ = seed.Close()

	// Now open a worker-style connection: DisablePersistentSecrets = true.
	db, err := sql.Open("duckdb", ":memory:?allow_unsigned_extensions=true")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	cfg := Config{DataDir: dir, DisablePersistentSecrets: true}
	if err := ConfigureMainDB(db, cfg, "worker"); err != nil {
		t.Fatalf("ConfigureMainDB: %v", err)
	}

	// (b) the seeded persistent secret must not have been loaded.
	var loaded int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'seeded'").Scan(&loaded); err != nil {
		t.Fatalf("query duckdb_secrets: %v", err)
	}
	if loaded != 0 {
		t.Errorf("seeded persistent secret was loaded (count=%d); allow_persistent_secrets=false should prevent it", loaded)
	}

	// (a) CREATE PERSISTENT SECRET must be rejected.
	if _, err := db.Exec("CREATE PERSISTENT SECRET nope (TYPE s3, KEY_ID 'a', SECRET 'b')"); err == nil {
		t.Error("CREATE PERSISTENT SECRET succeeded; expected it to be rejected when persistent secrets are disabled")
	} else if !strings.Contains(strings.ToLower(err.Error()), "persistent secrets are disabled") {
		t.Errorf("CREATE PERSISTENT SECRET rejected, but not for the expected reason: %v", err)
	}

	// A temporary (in-memory) secret — the kind workers actually use — must
	// still work, so the disable doesn't break activation.
	if _, err := db.Exec("CREATE OR REPLACE SECRET inmem (TYPE s3, KEY_ID 'a', SECRET 'b')"); err != nil {
		t.Errorf("in-memory CREATE OR REPLACE SECRET failed: %v", err)
	}
}

// TestSecretDirectory documents the path derivation the wipe and pinning rely on.
func TestSecretDirectory(t *testing.T) {
	if got := SecretDirectory(Config{}); got != "" {
		t.Errorf("SecretDirectory with empty DataDir = %q, want \"\"", got)
	}
	want := filepath.Join("/data", "secrets")
	if got := SecretDirectory(Config{DataDir: "/data"}); got != want {
		t.Errorf("SecretDirectory(/data) = %q, want %q", got, want)
	}
}
