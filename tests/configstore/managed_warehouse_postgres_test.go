//go:build linux || darwin

package configstore_test

import (
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestManagedWarehouseConfigStorePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if !store.DB().Migrator().HasTable(&configstore.ManagedWarehouse{}) {
		t.Fatal("expected managed warehouse table to be auto-migrated")
	}

	passwordHash, err := configstore.HashPassword("secret")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}

	if err := store.DB().Create(&configstore.Org{Name: "analytics", DatabaseName: "analytics"}).Error; err != nil {
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

	if err := store.DB().Create(&configstore.Org{Name: "cleanup", DatabaseName: "cleanup"}).Error; err != nil {
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
