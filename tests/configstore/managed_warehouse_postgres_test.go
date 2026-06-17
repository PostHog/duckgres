//go:build linux || darwin

package configstore_test

import (
	"errors"
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
		return
	}
	if orgCfg.Warehouse == nil {
		t.Fatal("expected warehouse to be preloaded into snapshot")
		return
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

func TestFinalizeWarehouseDeletionRemovesOrgConfigRows(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := store.DB().Create(&configstore.Org{Name: "analytics", DatabaseName: "analytics"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := store.DB().Create(&configstore.OrgUser{
		OrgID:    "analytics",
		Username: "root",
		Password: "hash",
	}).Error; err != nil {
		t.Fatalf("create org user: %v", err)
	}
	if err := store.DB().Create(&configstore.OrgUserSecret{
		OrgID:      "analytics",
		Username:   "root",
		SecretName: "persisted_secret",
		Ciphertext: []byte("sealed"),
	}).Error; err != nil {
		t.Fatalf("create org user secret: %v", err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateDeleting,
	}).Error; err != nil {
		t.Fatalf("create warehouse: %v", err)
	}

	if err := store.FinalizeWarehouseDeletion("analytics"); err != nil {
		t.Fatalf("finalize deletion: %v", err)
	}

	assertRowCount(t, store, &configstore.Org{}, "name = ?", "analytics", 0)
	assertRowCount(t, store, &configstore.ManagedWarehouse{}, "org_id = ?", "analytics", 0)
	assertRowCount(t, store, &configstore.OrgUser{}, "org_id = ?", "analytics", 0)
	assertRowCount(t, store, &configstore.OrgUserSecret{}, "org_id = ?", "analytics", 0)
}

func TestFinalizeWarehouseDeletionRequiresDeletingState(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := store.DB().Create(&configstore.Org{Name: "analytics", DatabaseName: "analytics"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStateReady,
	}).Error; err != nil {
		t.Fatalf("create warehouse: %v", err)
	}

	err := store.FinalizeWarehouseDeletion("analytics")
	if !errors.Is(err, configstore.ErrWarehouseStateMismatch) {
		t.Fatalf("expected ErrWarehouseStateMismatch, got %v", err)
	}

	assertRowCount(t, store, &configstore.Org{}, "name = ?", "analytics", 1)
	assertRowCount(t, store, &configstore.ManagedWarehouse{}, "org_id = ?", "analytics", 1)
}

func assertRowCount(t *testing.T, store *configstore.ConfigStore, model any, query string, arg any, want int64) {
	t.Helper()

	var count int64
	if err := store.DB().Model(model).Where(query, arg).Count(&count).Error; err != nil {
		t.Fatalf("count %T: %v", model, err)
	}
	if count != want {
		t.Fatalf("count %T = %d, want %d", model, count, want)
	}
}
