package duckdbservice

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/server"
)

func attachedDatabases(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query("SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY database_name")
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, n)
	}
	return names
}

func mustExecDB(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// Regression for the 2026-09-18 incident: a client `ATTACH ... AS db` survived
// its session on the hot-idle worker, so the next session's
// `ATTACH IF NOT EXISTS ... AS db` was a no-op and it read the PREVIOUS
// session's target. The detach must remove client catalogs and keep the
// worker-managed ones.
func TestDetachUserCatalogs(t *testing.T) {
	db := openSecretTestDB(t)
	dir := t.TempDir()
	// Worker-managed catalogs, stood in for by plain DuckDB files: only the
	// names matter to the allowlist.
	mustExecDB(t, db, "ATTACH '"+filepath.Join(dir, "lake.duckdb")+"' AS ducklake")
	mustExecDB(t, db, "ATTACH '"+filepath.Join(dir, "meta.duckdb")+"' AS __ducklake_metadata_ducklake")
	mustExecDB(t, db, "ATTACH '"+filepath.Join(dir, "delta.duckdb")+"' AS delta")
	// Client catalogs, including one needing identifier quoting.
	mustExecDB(t, db, "ATTACH '"+filepath.Join(dir, "a.duckdb")+"' AS db")
	mustExecDB(t, db, "ATTACH ':memory:' AS \"My \"\"odd\"\" Db\"")

	detached, err := detachUserCatalogs(context.Background(), db)
	if err != nil {
		t.Fatalf("detachUserCatalogs: %v", err)
	}
	sort.Strings(detached)
	if got, want := strings.Join(detached, ","), `My "odd" Db,db`; got != want {
		t.Errorf("detached = %q, want %q", got, want)
	}

	got := strings.Join(attachedDatabases(t, db), ",")
	if want := "__ducklake_metadata_ducklake,delta,ducklake,memory"; got != want {
		t.Errorf("remaining databases = %q, want %q", got, want)
	}

	// The point of the fix: the next session's IF NOT EXISTS attach must now
	// take effect instead of silently inheriting the old target.
	mustExecDB(t, db, "ATTACH IF NOT EXISTS '"+filepath.Join(dir, "b.duckdb")+"' AS db")
	var path string
	if err := db.QueryRow("SELECT path FROM duckdb_databases() WHERE database_name = 'db'").Scan(&path); err != nil {
		t.Fatalf("read db path: %v", err)
	}
	if !strings.HasSuffix(path, "b.duckdb") {
		t.Errorf("db path = %q, want the new session's target (b.duckdb)", path)
	}
}

// The primary database is a file stem, not `memory`, when the instance is
// opened on a file. It must never be a detach candidate regardless of name.
func TestDetachUserCatalogsKeepsFileBackedPrimary(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", filepath.Join(dir, "alice.duckdb"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mustExecDB(t, db, "ATTACH '"+filepath.Join(dir, "other.duckdb")+"' AS db")

	detached, err := detachUserCatalogs(context.Background(), db)
	if err != nil {
		t.Fatalf("detachUserCatalogs: %v", err)
	}
	if len(detached) != 1 || detached[0] != "db" {
		t.Errorf("detached = %v, want [db]", detached)
	}
	if got := strings.Join(attachedDatabases(t, db), ","); got != "alice" {
		t.Errorf("remaining databases = %q, want %q", got, "alice")
	}
}

func TestDetachUserCatalogsNoop(t *testing.T) {
	db := openSecretTestDB(t)
	detached, err := detachUserCatalogs(context.Background(), db)
	if err != nil {
		t.Fatalf("detachUserCatalogs: %v", err)
	}
	if len(detached) != 0 {
		t.Errorf("detached = %v, want none", detached)
	}
}

func TestIsSystemCatalog(t *testing.T) {
	for name, want := range map[string]bool{
		"ducklake":                     true,
		"DuckLake":                     true,
		"delta":                        true,
		"__ducklake_metadata_ducklake": true,
		"db":                           false,
		"ducklake2":                    false,
		"memory":                       false, // protected as the primary, not by name
	} {
		if got := isSystemCatalog(name); got != want {
			t.Errorf("isSystemCatalog(%q) = %v, want %v", name, got, want)
		}
	}
}

// End-to-end at the pool level: two consecutive sessions on one shared-warm
// worker. Session A attaches `db`; session B — possibly a different user of the
// org — must not inherit it, and B's own IF NOT EXISTS attach must win.
func TestCreateSessionDetachesPreviousSessionCatalogs(t *testing.T) {
	db := openSecretTestDB(t)
	dir := t.TempDir()
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		warmupDB:       db,
		warmupDone:     make(chan struct{}),
		cfg:            server.Config{SessionInitTimeout: time.Second},
		maxSessions:    1,
		sharedWarmMode: true,
		activation:     &activatedTenantRuntime{payload: ActivationPayload{OrgID: "analytics"}, db: db},
	}
	close(pool.warmupDone)

	a, _, err := pool.CreateSession("alice", "", 0, nil)
	if err != nil {
		t.Fatalf("CreateSession(alice): %v", err)
	}
	if _, err := a.Conn.ExecContext(context.Background(),
		"ATTACH IF NOT EXISTS '"+filepath.Join(dir, "alice_target.duckdb")+"' AS db"); err != nil {
		t.Fatalf("alice attach: %v", err)
	}
	if err := pool.DestroySession(a.ID); err != nil {
		t.Fatalf("DestroySession(alice): %v", err)
	}
	// The destroy-time detach is best-effort, but with nothing in the way it
	// should already have cleared the catalog off the hot-idle worker.
	if got := strings.Join(attachedDatabases(t, db), ","); got != "memory" {
		t.Errorf("databases after alice's session = %q, want only memory", got)
	}

	// Simulate the destroy-time detach having been skipped (it is best-effort):
	// the CreateSession detach is the one that must hold.
	mustExecDB(t, db, "ATTACH '"+filepath.Join(dir, "alice_target.duckdb")+"' AS db")

	b, _, err := pool.CreateSession("bob", "", 0, nil)
	if err != nil {
		t.Fatalf("CreateSession(bob): %v", err)
	}
	defer func() { _ = pool.DestroySession(b.ID) }()
	if got := strings.Join(attachedDatabases(t, db), ","); got != "memory" {
		t.Fatalf("bob inherited alice's catalogs: %q", got)
	}
	if _, err := b.Conn.ExecContext(context.Background(),
		"ATTACH IF NOT EXISTS '"+filepath.Join(dir, "bob_target.duckdb")+"' AS db"); err != nil {
		t.Fatalf("bob attach: %v", err)
	}
	var path string
	if err := b.Conn.QueryRowContext(context.Background(),
		"SELECT path FROM duckdb_databases() WHERE database_name = 'db'").Scan(&path); err != nil {
		t.Fatalf("read db path: %v", err)
	}
	if !strings.HasSuffix(path, "bob_target.duckdb") {
		t.Errorf("bob's db path = %q, want bob_target.duckdb", path)
	}
}
