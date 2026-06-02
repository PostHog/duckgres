package duckdbservice

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/posthog/duckgres/server"
)

// TestWipePersistedSecretsOnRecycle is the unit-level guard for the
// worker-recycle behavior that Warmup relies on: a CREATE PERSISTENT SECRET
// left on disk by a prior worker incarnation must not survive into the next
// one. We do it here rather than in tests/k8s because the meaningful boundary
// is a *process restart over a surviving DataDir* (container restart within a
// pod, or a persistent volume / warm node). A tests/k8s pod-kill would be
// vacuous: worker DataDir is an EmptyDir, so pod replacement already yields a
// fresh empty /data and the test would pass even with the wipe removed.
//
// The test is explicitly non-vacuous: the middle phase asserts the persistent
// secret *does* survive a plain reopen (the original bug), and only the final
// phase — after wipePersistedSecrets — asserts it is gone (the fix).
func TestWipePersistedSecretsOnRecycle(t *testing.T) {
	cfg := server.Config{DataDir: t.TempDir()}
	secretDir := server.SecretDirectory(cfg)
	if secretDir == "" {
		t.Fatal("SecretDirectory returned empty for a non-empty DataDir")
	}

	const secretName = "test_recycle_secret"

	// open applies the same secret_directory pinning the production worker uses
	// (server.ConfigureMainDB) and returns the DB. Caller closes it.
	open := func() *sql.DB {
		t.Helper()
		db, err := sql.Open("duckdb", ":memory:?allow_unsigned_extensions=true")
		if err != nil {
			t.Fatalf("open duckdb: %v", err)
		}
		if err := server.ConfigureMainDB(db, cfg, "tester"); err != nil {
			_ = db.Close()
			t.Fatalf("ConfigureMainDB: %v", err)
		}
		return db
	}

	persistentSecretCount := func(db *sql.DB) int {
		t.Helper()
		var n int
		if err := db.QueryRow(
			"SELECT count(*) FROM duckdb_secrets() WHERE name = ? AND persistent",
			secretName,
		).Scan(&n); err != nil {
			t.Fatalf("query duckdb_secrets: %v", err)
		}
		return n
	}

	// Phase 1: create a persistent secret. It should land on disk under the
	// pinned secret_directory.
	db := open()
	if _, err := db.Exec(
		"CREATE PERSISTENT SECRET " + secretName + " (TYPE s3, KEY_ID 'a', SECRET 'b')",
	); err != nil {
		t.Fatalf("create persistent secret: %v", err)
	}
	if got := persistentSecretCount(db); got != 1 {
		t.Fatalf("after create: persistent secret count = %d, want 1", got)
	}
	_ = db.Close()

	entries, err := os.ReadDir(secretDir)
	if err != nil {
		t.Fatalf("read secret_directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("secret_directory %s is empty; persistent secret was not written to disk", secretDir)
	}

	// Also plant a file in the legacy default location (<DataDir>/.duckdb/
	// stored_secrets) — where a worker whose HOME is its DataDir accumulated
	// secrets before we pinned secret_directory. The recycle must clear this too.
	legacyDir := filepath.Join(cfg.DataDir, ".duckdb", "stored_secrets")
	if err := os.MkdirAll(legacyDir, 0o750); err != nil {
		t.Fatalf("mkdir legacy secret dir: %v", err)
	}
	legacyFile := filepath.Join(legacyDir, "old_secret.duckdb_secret")
	if err := os.WriteFile(legacyFile, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write legacy secret file: %v", err)
	}

	// Phase 2 (non-vacuous control): reopen WITHOUT recycling. The persistent
	// secret must reload from disk — this is exactly the state that, combined
	// with the in-memory secret re-created at activation, produces the
	// "secret occurs in multiple storage backends" ambiguity in production.
	db = open()
	if got := persistentSecretCount(db); got != 1 {
		t.Fatalf("after plain reopen: persistent secret count = %d, want 1 "+
			"(test is vacuous if the secret didn't survive a reopen)", got)
	}
	_ = db.Close()

	// Phase 3: recycle. This is the exact call Warmup makes on worker startup.
	wipePersistedSecrets(cfg)
	if _, err := os.Stat(secretDir); !os.IsNotExist(err) {
		t.Fatalf("secret_directory still exists after wipe (stat err = %v)", err)
	}
	if _, err := os.Stat(legacyFile); !os.IsNotExist(err) {
		t.Fatalf("legacy secret file still exists after wipe (stat err = %v)", err)
	}

	// Phase 4: reopen after recycle. The persistent secret must be gone.
	db = open()
	if got := persistentSecretCount(db); got != 0 {
		t.Fatalf("after recycle: persistent secret count = %d, want 0 "+
			"(wipePersistedSecrets did not prevent resurrection)", got)
	}
	_ = db.Close()
}
