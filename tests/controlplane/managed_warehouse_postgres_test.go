//go:build kubernetes && (linux || darwin)

package controlplane_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/posthog/duckgres/controlplane/configstore"
	integrationtest "github.com/posthog/duckgres/tests/integration"
)

var ensureIntegrationPostgresOnce sync.Once

func TestManagedWarehouseConfigStorePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if !store.DB().Migrator().HasTable(&configstore.ManagedWarehouse{}) {
		t.Fatal("expected managed warehouse table to be auto-migrated")
	}

	passwordHash, err := configstore.HashPassword("secret")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}

	if err := store.DB().Create(&configstore.Org{Name: "analytics"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := store.DB().Create(&configstore.OrgUser{
		Username: "alice",
		Password: passwordHash,
		OrgID:    "analytics",
	}).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouse{
		OrgID: "analytics",
		WarehouseDatabase: configstore.ManagedWarehouseDatabase{
			Region:       "us-east-1",
			Endpoint:     "analytics.cluster.example",
			Port:         5432,
			DatabaseName: "analytics_wh",
			Username:     "warehouse_user",
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "dedicated_rds",
			Engine:       "postgres",
			Region:       "us-east-1",
			Endpoint:     "analytics-metadata.cluster.example",
			Port:         5432,
			DatabaseName: "ducklake_metadata",
			Username:     "metadata_user",
		},
		MetadataStoreCredentials: configstore.SecretRef{
			Namespace: "duckgres",
			Name:      "analytics-metadata",
			Key:       "dsn",
		},
		State:              configstore.ManagedWarehouseStateReady,
		MetadataStoreState: configstore.ManagedWarehouseStateReady,
	}).Error; err != nil {
		t.Fatalf("create warehouse: %v", err)
	}

	if err := store.Reload(); err != nil {
		t.Fatalf("reload store: %v", err)
	}

	orgCfg := store.Snapshot().Orgs["analytics"]
	if orgCfg == nil {
		t.Fatal("expected analytics org in snapshot")
	}
	if orgCfg.Warehouse == nil {
		t.Fatal("expected warehouse to be preloaded into snapshot")
	}
	if orgCfg.Warehouse.WarehouseDatabase.DatabaseName != "analytics_wh" {
		t.Fatalf("expected analytics_wh, got %q", orgCfg.Warehouse.WarehouseDatabase.DatabaseName)
	}
	if orgCfg.Warehouse.MetadataStore.Kind != "dedicated_rds" {
		t.Fatalf("expected metadata store kind dedicated_rds, got %q", orgCfg.Warehouse.MetadataStore.Kind)
	}
	if orgCfg.Warehouse.MetadataStore.DatabaseName != "ducklake_metadata" {
		t.Fatalf("expected ducklake_metadata, got %q", orgCfg.Warehouse.MetadataStore.DatabaseName)
	}
	if orgCfg.Users["alice"] != passwordHash {
		t.Fatal("expected user credentials to remain loaded in snapshot")
	}

	if err := store.DB().Create(&configstore.Org{Name: "cleanup"}).Error; err != nil {
		t.Fatalf("create cleanup org: %v", err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouse{
		OrgID: "cleanup",
		State: configstore.ManagedWarehouseStateReady,
	}).Error; err != nil {
		t.Fatalf("create cleanup warehouse: %v", err)
	}

	if err := store.DB().Delete(&configstore.Org{Name: "cleanup"}).Error; err != nil {
		t.Fatalf("delete org: %v", err)
	}

	var count int64
	if err := store.DB().Model(&configstore.ManagedWarehouse{}).Where("org_id = ?", "cleanup").Count(&count).Error; err != nil {
		t.Fatalf("count warehouses: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected warehouse to be deleted via cascade, count=%d", count)
	}
}

func TestLocalConfigStoreSeedSQL(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := applyConfigStoreSeed(t, store, filepath.Join(findProjectRoot(), "k8s", "local-config-store.seed.sql")); err != nil {
		t.Fatalf("apply local seed: %v", err)
	}

	if err := store.Reload(); err != nil {
		t.Fatalf("reload store: %v", err)
	}

	snap := store.Snapshot()
	orgCfg := snap.Orgs["local"]
	if orgCfg == nil {
		t.Fatal("expected local org from seed")
	}
	if orgCfg.Warehouse == nil {
		t.Fatal("expected local warehouse from seed")
	}
	if orgCfg.Warehouse.WarehouseDatabase.DatabaseName != "duckgres_local" {
		t.Fatalf("expected duckgres_local warehouse db, got %q", orgCfg.Warehouse.WarehouseDatabase.DatabaseName)
	}
	if orgCfg.Warehouse.MetadataStore.DatabaseName != "ducklake_metadata_local" {
		t.Fatalf("expected ducklake_metadata_local metadata db, got %q", orgCfg.Warehouse.MetadataStore.DatabaseName)
	}
	if orgCfg.Warehouse.WarehouseDatabaseCredentials.Name != "duckgres-local-warehouse-db" {
		t.Fatalf("expected duckgres-local-warehouse-db secret ref, got %q", orgCfg.Warehouse.WarehouseDatabaseCredentials.Name)
	}
	if orgCfg.Warehouse.State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected ready warehouse state, got %q", orgCfg.Warehouse.State)
	}
	if orgCfg.Warehouse.MetadataStoreState != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected ready metadata store state, got %q", orgCfg.Warehouse.MetadataStoreState)
	}
	if _, ok := orgCfg.Users["postgres"]; !ok {
		t.Fatal("expected seeded postgres user to belong to local org")
	}
}

func newIsolatedConfigStore(t *testing.T) *configstore.ConfigStore {
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

	store, err := configstore.NewConfigStore("host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable search_path="+schema, time.Hour)
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
