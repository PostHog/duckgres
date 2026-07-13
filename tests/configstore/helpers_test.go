//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
	integrationtest "github.com/posthog/duckgres/tests/integration"
)

var ensureIntegrationPostgresOnce sync.Once

func newIsolatedConfigStore(t *testing.T) *cpconfigstore.ConfigStore {
	t.Helper()

	_, connStr := newIsolatedConfigStoreSchema(t)

	store, err := cpconfigStoreNew(connStr)
	if err != nil {
		t.Fatalf("new config store: %v", err)
	}

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("store sql db: %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	return store
}

func newIsolatedConfigStoreSchema(t *testing.T) (*sql.DB, string) {
	t.Helper()

	ensureIntegrationPostgres(t)

	schema := fmt.Sprintf("managed_warehouse_%d", time.Now().UnixNano())
	adminDB, err := sql.Open("postgres", "host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable")
	if err != nil {
		t.Fatalf("open postgres admin db: %v", err)
	}
	t.Cleanup(func() {
		_ = adminDB.Close()
	})

	if _, err := adminDB.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`)
	})

	connStr := "host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable search_path=" + schema
	return adminDB, connStr
}

// testDefaultTeamID returns a pointer to a placeholder PostHog team id for
// seeded org rows — default_team_id is NOT NULL (migration 000020), so every
// test fixture that inserts an org must carry one.
func testDefaultTeamID() *int64 {
	teamID := int64(1)
	return &teamID
}

func cpconfigStoreNew(connStr string) (*cpconfigstore.ConfigStore, error) {
	return cpconfigstore.NewConfigStore(connStr, time.Hour)
}

func ensureIntegrationPostgres(t *testing.T) {
	t.Helper()

	var err error
	ensureIntegrationPostgresOnce.Do(func() {
		if integrationtest.IsPostgresRunning(35432) {
			return
		}
		err = integrationtest.StartPostgresContainer()
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
}

func applyConfigStoreSeed(t *testing.T, store *cpconfigstore.ConfigStore, path string) error {
	t.Helper()

	seedSQL, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read seed sql: %w", err)
	}

	sqlDB, err := store.DB().DB()
	if err != nil {
		return fmt.Errorf("store sql db: %w", err)
	}
	if _, err := sqlDB.Exec(string(seedSQL)); err != nil {
		return fmt.Errorf("exec seed sql: %w", err)
	}
	return nil
}

func findProjectRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		next := filepath.Dir(dir)
		if next == dir {
			t.Fatal("could not find project root")
		}
		dir = next
	}
}
