package duckdbservice

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func openSecretTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Pin persistent secrets under the test dir (must happen before the
	// secret manager is first used).
	dir := filepath.Join(t.TempDir(), "secrets")
	if _, err := db.Exec("SET secret_directory = '" + dir + "'"); err != nil {
		t.Fatalf("set secret_directory: %v", err)
	}
	return db
}

func secretNames(t *testing.T, db *sql.DB, where string) map[string]bool {
	t.Helper()
	rows, err := db.Query("SELECT name FROM duckdb_secrets() " + where)
	if err != nil {
		t.Fatalf("duckdb_secrets: %v", err)
	}
	defer func() { _ = rows.Close() }()
	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names[name] = true
	}
	return names
}

// Wipe must drop ALL user-created secrets — persistent AND non-persistent
// (plain/TEMPORARY CREATE SECRET) — while leaving the system-managed catalog
// secrets (ducklake_s3, plus the reserved __default_*/duckgres_* prefixes)
// untouched.
func TestWipeUserSecrets(t *testing.T) {
	db := openSecretTestDB(t)
	mustExec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	// System-managed secrets (created with plain CREATE OR REPLACE SECRET, so
	// they land in in-memory/temporary storage). These must survive the wipe.
	mustExec("CREATE OR REPLACE SECRET ducklake_s3 (TYPE s3, KEY_ID 'sys', SECRET 'sys')")
	mustExec("CREATE OR REPLACE SECRET duckgres_internal (TYPE s3, KEY_ID 'sys', SECRET 'sys')")
	// User secrets: a persistent one and a temporary one. Both must be dropped.
	mustExec("CREATE PERSISTENT SECRET user_a (TYPE s3, KEY_ID 'a', SECRET 'a')")
	mustExec(`CREATE PERSISTENT SECRET "user_b" (TYPE gcs, KEY_ID 'b', SECRET 'b')`)
	mustExec("CREATE TEMPORARY SECRET user_temp (TYPE s3, KEY_ID 't', SECRET 't')")

	dropped, err := wipeUserSecrets(context.Background(), db)
	if err != nil {
		t.Fatalf("wipeUserSecrets: %v", err)
	}
	if len(dropped) != 3 {
		t.Errorf("dropped = %v, want 3 names (user_a, user_b, user_temp)", dropped)
	}
	remaining := secretNames(t, db, "")
	for _, leaked := range []string{"user_a", "user_b", "user_temp"} {
		if remaining[leaked] {
			t.Errorf("user secret %q remains after wipe; remaining: %v", leaked, remaining)
		}
	}
	for _, sys := range []string{"ducklake_s3", "duckgres_internal"} {
		if !remaining[sys] {
			t.Errorf("system secret %q was wiped; remaining: %v", sys, remaining)
		}
	}
}

// Regression for the cross-user isolation leak: a non-persistent (TEMPORARY /
// plain) secret created by one user lingers on the instance-global DuckDB and
// would be inherited by the next user of the same org. The wipe must remove it.
func TestWipeUserSecretsDropsTemporary(t *testing.T) {
	db := openSecretTestDB(t)
	// Plain CREATE SECRET is non-persistent in DuckDB — the exact passthrough
	// path the control plane allows for session-scoped secrets.
	if _, err := db.Exec("CREATE SECRET leaky (TYPE s3, KEY_ID 'private', SECRET 'private')"); err != nil {
		t.Fatalf("create temporary secret: %v", err)
	}
	if names := secretNames(t, db, ""); !names["leaky"] {
		t.Fatalf("temporary secret not present before wipe; have %v", names)
	}

	dropped, err := wipeUserSecrets(context.Background(), db)
	if err != nil {
		t.Fatalf("wipeUserSecrets: %v", err)
	}
	if len(dropped) != 1 || dropped[0] != "leaky" {
		t.Errorf("dropped = %v, want [leaky]", dropped)
	}
	if names := secretNames(t, db, ""); names["leaky"] {
		t.Errorf("temporary secret leaked across wipe (cross-user isolation bug); remaining: %v", names)
	}
}

// Replay applies statements and degrades per-statement failures to warnings
// that never include the statement text (it carries credentials).
func TestReplayUserSecrets(t *testing.T) {
	db := openSecretTestDB(t)

	warnings := replayUserSecrets(context.Background(), db, "alice", []string{
		"CREATE PERSISTENT SECRET good_one (TYPE s3, KEY_ID 'k', SECRET 'sensitive-value')",
		"CREATE PERSISTENT SECRET bad_one (TYPE not_a_type)",
	})

	if names := secretNames(t, db, "WHERE persistent"); !names["good_one"] {
		t.Errorf("good_one not replayed; have %v", names)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want exactly 1", warnings)
	}
	if !strings.Contains(warnings[0], "bad_one") {
		t.Errorf("warning %q does not name the failed secret", warnings[0])
	}
	if strings.Contains(warnings[0], "sensitive-value") {
		t.Errorf("warning leaks statement text: %q", warnings[0])
	}
}
