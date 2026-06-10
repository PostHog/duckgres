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

// Wipe must drop persistent secrets (user-created) and leave in-memory
// secrets (how the system catalog secrets are created) untouched.
func TestWipeUserPersistentSecrets(t *testing.T) {
	db := openSecretTestDB(t)
	mustExec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE OR REPLACE SECRET ducklake_s3 (TYPE s3, KEY_ID 'sys', SECRET 'sys')")
	mustExec("CREATE PERSISTENT SECRET user_a (TYPE s3, KEY_ID 'a', SECRET 'a')")
	mustExec(`CREATE PERSISTENT SECRET "user_b" (TYPE gcs, KEY_ID 'b', SECRET 'b')`)

	dropped, err := wipeUserPersistentSecrets(context.Background(), db)
	if err != nil {
		t.Fatalf("wipeUserPersistentSecrets: %v", err)
	}
	if len(dropped) != 2 {
		t.Errorf("dropped = %v, want 2 names", dropped)
	}
	if names := secretNames(t, db, "WHERE persistent"); len(names) != 0 {
		t.Errorf("persistent secrets remain after wipe: %v", names)
	}
	if names := secretNames(t, db, ""); !names["ducklake_s3"] {
		t.Errorf("system in-memory secret was wiped; remaining: %v", names)
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
