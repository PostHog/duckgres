package server

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestPinSecretDirectory is the regression guard for the worker-side fix: with
// Config.PinSecretDirectory set (as duckdbservice.OpenDuckDBPair does for every
// worker), ConfigureMainDB redirects DuckDB's persistent-secret storage to
// <DataDir>/secrets. That means:
//
//	(a) a CREATE PERSISTENT SECRET lands under <DataDir>/secrets (deterministic,
//	    wipeable), and
//	(b) a stale secret sitting in DuckDB's $HOME default is no longer loaded, so
//	    it can't collide with the in-memory secret re-created at activation.
//
// We deliberately do NOT disable persistent secrets (that breaks DuckLake's
// ATTACH, which needs the local_file secret storage registered), so persistent
// secrets still work — they're just relocated and wiped on recycle.
func TestPinSecretDirectory(t *testing.T) {
	dir := t.TempDir()
	secretDir := SecretDirectory(Config{DataDir: dir})

	db, err := sql.Open("duckdb", ":memory:?allow_unsigned_extensions=true")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	cfg := Config{DataDir: dir, PinSecretDirectory: true}
	if err := ConfigureMainDB(db, cfg, "worker"); err != nil {
		t.Fatalf("ConfigureMainDB: %v", err)
	}

	// (a) a persistent secret must still be creatable and must land under the
	// pinned directory.
	if _, err := db.Exec("CREATE PERSISTENT SECRET pinned (TYPE s3, KEY_ID 'a', SECRET 'b')"); err != nil {
		t.Fatalf("create persistent secret: %v", err)
	}
	entries, err := os.ReadDir(secretDir)
	if err != nil {
		t.Fatalf("read pinned secret_directory %s: %v", secretDir, err)
	}
	if len(entries) == 0 {
		t.Errorf("persistent secret did not land under pinned secret_directory %s", secretDir)
	}
}

// TestPinSecretDirectoryIgnoresLegacyDefault proves point (b): a secret written
// to DuckDB's old $HOME-style default location is not loaded once the directory
// is pinned elsewhere.
func TestPinSecretDirectoryIgnoresLegacyDefault(t *testing.T) {
	dir := t.TempDir()

	// Seed a persistent secret into the *legacy* location.
	legacy := LegacySecretDirectory(Config{DataDir: dir})
	seed, err := sql.Open("duckdb", ":memory:?allow_unsigned_extensions=true")
	if err != nil {
		t.Fatalf("open seed duckdb: %v", err)
	}
	if _, err := seed.Exec("SET secret_directory = '" + legacy + "'"); err != nil {
		t.Fatalf("seed set secret_directory: %v", err)
	}
	if _, err := seed.Exec("CREATE PERSISTENT SECRET stale (TYPE s3, KEY_ID 'a', SECRET 'b')"); err != nil {
		t.Fatalf("seed create persistent secret: %v", err)
	}
	_ = seed.Close()

	// A worker-style connection pins to <DataDir>/secrets, not the legacy path.
	db, err := sql.Open("duckdb", ":memory:?allow_unsigned_extensions=true")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := ConfigureMainDB(db, Config{DataDir: dir, PinSecretDirectory: true}, "worker"); err != nil {
		t.Fatalf("ConfigureMainDB: %v", err)
	}

	var loaded int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'stale'").Scan(&loaded); err != nil {
		t.Fatalf("query duckdb_secrets: %v", err)
	}
	if loaded != 0 {
		t.Errorf("stale secret in the legacy location was loaded (count=%d); pinning should redirect away from it", loaded)
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

	if got := LegacySecretDirectory(Config{}); got != "" {
		t.Errorf("LegacySecretDirectory with empty DataDir = %q, want \"\"", got)
	}
	wantLegacy := filepath.Join("/data", ".duckdb", "stored_secrets")
	if got := LegacySecretDirectory(Config{DataDir: "/data"}); got != wantLegacy {
		t.Errorf("LegacySecretDirectory(/data) = %q, want %q", got, wantLegacy)
	}
}
