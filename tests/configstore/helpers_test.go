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

	store, err := cpconfigstore.NewConfigStore("host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable search_path="+schema, time.Hour)
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

func newConfigStoreOnSameSchema(t *testing.T, store *cpconfigstore.ConfigStore) *cpconfigstore.ConfigStore {
	t.Helper()

	var schema string
	if err := store.DB().Raw("SELECT current_schema()").Scan(&schema).Error; err != nil {
		t.Fatalf("current schema: %v", err)
	}
	other, err := cpconfigstore.NewConfigStore("host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable search_path="+schema, time.Hour)
	if err != nil {
		t.Fatalf("new config store on schema %s: %v", schema, err)
	}
	sqlDB, err := other.DB().DB()
	if err != nil {
		t.Fatalf("other store sql db: %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})
	return other
}

func assertWarmCapacityMissBucketCount(t *testing.T, store *cpconfigstore.ConfigStore, scope string, reason cpconfigstore.WorkerClaimMissReason, bucketStart time.Time, want int64) {
	t.Helper()

	var got int64
	if err := store.DB().Table(store.RuntimeSchema()+".warm_capacity_miss_buckets").
		Where("scope = ? AND reason = ? AND bucket_start = ?", scope, string(reason), bucketStart).
		Select("COALESCE(SUM(count), 0)").
		Scan(&got).Error; err != nil {
		t.Fatalf("lookup warm capacity miss bucket: %v", err)
	}
	if got != want {
		t.Fatalf("expected bucket count %d for scope=%q reason=%q bucket=%s, got %d", want, scope, reason, bucketStart.Format(time.RFC3339), got)
	}
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
