//go:build kubernetes

package configstore

import (
	"fmt"
	"net"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Postgres-backed test helpers for configstore tests under the `kubernetes`
// build tag. Mirror of the admin package's helper (api_postgres_test.go),
// kept deliberately small: no docker-compose management here — CI/the local
// runner owns the container; a test that needs Postgres and finds it down
// fails with a clear message instead of silently being skipped.
const testConfigStoreConnString = "host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable"

func newPostgresConfigStore(t *testing.T) *ConfigStore {
	t.Helper()

	conn, err := net.DialTimeout("tcp", "127.0.0.1:35432", time.Second)
	if err != nil {
		t.Skipf("config-store Postgres not running on 127.0.0.1:35432 (start tests/integration/docker-compose.yml postgres): %v", err)
	}
	_ = conn.Close()

	store, err := NewConfigStore(testConfigStoreConnString, time.Hour)
	if err != nil {
		t.Fatalf("new config store: %v", err)
	}

	resetConfigStoreTestTables(t, store.DB())

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	return store
}

func resetConfigStoreTestTables(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, model := range []any{&ServiceGrant{}, &ManagedWarehouse{}, &OrgUser{}, &OrgTeam{}, &Org{}} {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(model).Error; err != nil {
			panic(fmt.Sprintf("delete %T: %v", model, err))
		}
	}
}
