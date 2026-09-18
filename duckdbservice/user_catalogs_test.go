package duckdbservice

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server"
)

func databaseNames(t *testing.T, db *sql.DB) map[string]bool {
	t.Helper()
	rows, err := db.Query("SELECT database_name FROM duckdb_databases()")
	if err != nil {
		t.Fatalf("duckdb_databases: %v", err)
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

// A hot-idle worker is reused across sessions of an org, and DuckDB attached
// catalogs are instance-global: a user-attached catalog (e.g. an external
// Postgres source) would otherwise linger into the next session — leaking the
// previous session's data sources (and, for postgres_scanner, its live pooled
// connections) to whoever gets the worker next. The session-create wipe must
// detach every user catalog while preserving the system-managed ones
// (ducklake, delta, memory, and the internal system/temp databases).
func TestWipeUserCatalogs(t *testing.T) {
	db := openSecretTestDB(t)
	mustExec := func(q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	// User catalogs left behind by a "previous session". One name uses
	// identifier-quoting edge cases to prove the wipe quotes safely.
	mustExec("ATTACH ':memory:' AS userdb")
	mustExec(`ATTACH ':memory:' AS "weird ""quoted"" name"`)
	// Name-simulate the reserved system-managed catalogs (the real ones are
	// attached by activation with these exact names).
	mustExec("ATTACH ':memory:' AS ducklake")
	mustExec("ATTACH ':memory:' AS delta")

	wiped, err := wipeUserCatalogs(context.Background(), db)
	if err != nil {
		t.Fatalf("wipeUserCatalogs: %v", err)
	}
	if len(wiped) != 2 {
		t.Fatalf("expected 2 wiped catalogs, got %v", wiped)
	}
	names := databaseNames(t, db)
	for _, gone := range []string{"userdb", `weird "quoted" name`} {
		if names[gone] {
			t.Errorf("catalog %q survived the wipe", gone)
		}
	}
	for _, kept := range []string{"ducklake", "delta", "memory", "system", "temp"} {
		if !names[kept] {
			t.Errorf("reserved/internal catalog %q was wiped", kept)
		}
	}
}

// CreateSession on a shared-warm (k8s) worker must detach catalogs attached by
// the previous session of the same worker: DuckDB ATTACH is instance-global
// and the worker is reused, so without the wipe the next session inherits the
// previous session's external sources (and their authenticated pools).
func TestCreateSessionWipesPreviousSessionCatalogs(t *testing.T) {
	pool := &SessionPool{
		sessions:       make(map[string]*Session),
		stopRefresh:    make(map[string]func()),
		duckLakeSem:    make(chan struct{}, 1),
		cfg:            server.Config{Users: map[string]string{"postgres": "postgres"}},
		startTime:      time.Now(),
		warmupDone:     make(chan struct{}),
		sharedWarmMode: true,
		maxSessions:    1, // k8s workers run with DUCKGRES_DUCKDB_MAX_SESSIONS=1
	}
	close(pool.warmupDone)
	pool.createDBPair = func(server.Config, chan struct{}, string, time.Time, string) (*DuckDBPair, error) {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return nil, err
		}
		return PairFromMain(db), nil
	}
	pool.activateDBConnection = func(*sql.DB, server.Config, chan struct{}, string) error { return nil }
	if err := pool.activateTenant(ActivationPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			OwnerEpoch:   1,
			CPInstanceID: "cp-test",
			WorkerID:     1,
		},
		OrgID: "test-org",
	}); err != nil {
		t.Fatalf("activateTenant: %v", err)
	}

	first, _, err := pool.CreateSession("alice", "", 0, nil)
	if err != nil {
		t.Fatalf("first CreateSession: %v", err)
	}
	// Alice's session attaches an external source catalog.
	if _, err := pool.warmupDB.Exec("ATTACH ':memory:' AS userdb"); err != nil {
		t.Fatalf("attach user catalog: %v", err)
	}
	if err := pool.DestroySession(first.ID); err != nil {
		t.Fatalf("DestroySession: %v", err)
	}

	if _, _, err := pool.CreateSession("bob", "", 0, nil); err != nil {
		t.Fatalf("second CreateSession: %v", err)
	}
	if databaseNames(t, pool.warmupDB)["userdb"] {
		t.Fatal("user-attached catalog from the previous session survived CreateSession")
	}
	if !databaseNames(t, pool.warmupDB)["memory"] {
		t.Fatal("reserved memory catalog was wiped")
	}
}
