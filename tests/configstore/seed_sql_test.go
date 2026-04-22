//go:build linux || darwin

package configstore_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestLocalConfigStoreSeedSQL(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := applyConfigStoreSeed(t, store, filepath.Join(findProjectRoot(t), "k8s", "local-config-store.seed.sql")); err != nil {
		t.Fatalf("apply local seed: %v", err)
	}

	if err := store.Reload(); err != nil {
		t.Fatalf("reload store: %v", err)
	}

	snap := store.Snapshot()
	orgCfg := snap.Orgs["local"]
	if orgCfg == nil {
		t.Fatal("expected local org from seed")
		return
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
	if orgCfg.Warehouse.WarehouseDatabaseCredentials.Name != "local-warehouse-db" {
		t.Fatalf("expected local-warehouse-db secret ref, got %q", orgCfg.Warehouse.WarehouseDatabaseCredentials.Name)
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

func TestKindConfigStoreSeedSQL(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := applyConfigStoreSeed(t, store, filepath.Join(findProjectRoot(t), "k8s", "kind", "config-store.seed.sql")); err != nil {
		t.Fatalf("apply kind seed: %v", err)
	}

	if err := store.Reload(); err != nil {
		t.Fatalf("reload store: %v", err)
	}

	snap := store.Snapshot()
	orgCfg := snap.Orgs["local"]
	if orgCfg == nil {
		t.Fatal("expected local org from kind seed")
		return
	}
	if orgCfg.Warehouse == nil {
		t.Fatal("expected local warehouse from kind seed")
	}
	if got := orgCfg.Warehouse.MetadataStore.Endpoint; got != "duckgres-local-ducklake-metadata" {
		t.Fatalf("expected kind metadata endpoint, got %q", got)
	}
	if got := orgCfg.Warehouse.MetadataStore.Port; got != 5432 {
		t.Fatalf("expected kind metadata port 5432, got %d", got)
	}
	if got := orgCfg.Warehouse.S3.Endpoint; got != "duckgres-local-minio:9000" {
		t.Fatalf("expected kind s3 endpoint, got %q", got)
	}
	if got := orgCfg.Warehouse.S3.Bucket; got != "duckgres-local" {
		t.Fatalf("expected duckgres-local bucket, got %q", got)
	}
	if got := orgCfg.Warehouse.MetadataStoreCredentials.Name; got != "local-metadata" {
		t.Fatalf("expected kind metadata secret ref, got %q", got)
	}
	if got := orgCfg.Warehouse.S3Credentials.Name; got != "local-s3" {
		t.Fatalf("expected kind s3 secret ref, got %q", got)
	}
}

func TestTenantIsolationConfigStoreSeedSQL(t *testing.T) {
	seedPath := filepath.Join(findProjectRoot(t), "tests", "k8s", "testdata", "tenant-isolation.seed.sql")
	seedSQL, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("read tenant isolation seed: %v", err)
	}
	if !strings.Contains(string(seedSQL), "ON CONFLICT (org_id, username) DO UPDATE") {
		t.Fatalf("expected composite org user upsert target in %s", seedPath)
	}
	if strings.Contains(string(seedSQL), "ON CONFLICT (username) DO UPDATE") {
		t.Fatalf("expected tenant isolation seed to avoid username-only org user upserts in %s", seedPath)
	}

	store := newIsolatedConfigStore(t)

	if err := applyConfigStoreSeed(t, store, seedPath); err != nil {
		t.Fatalf("apply tenant isolation seed: %v", err)
	}

	if err := store.Reload(); err != nil {
		t.Fatalf("reload store: %v", err)
	}

	snap := store.Snapshot()
	for _, tc := range []struct {
		orgID         string
		databaseName  string
		metadataDB    string
		warehouseUser string
	}{
		{orgID: "analytics", databaseName: "analytics", metadataDB: "ducklake_metadata_analytics", warehouseUser: "analytics"},
		{orgID: "billing", databaseName: "billing", metadataDB: "ducklake_metadata_billing", warehouseUser: "billing"},
	} {
		orgCfg := snap.Orgs[tc.orgID]
		if orgCfg == nil {
			t.Fatalf("expected %s org from tenant isolation seed", tc.orgID)
			return
		}
		if orgCfg.DatabaseName != tc.databaseName {
			t.Fatalf("expected %s database name %q, got %q", tc.orgID, tc.databaseName, orgCfg.DatabaseName)
		}
		if orgCfg.Warehouse == nil {
			t.Fatalf("expected %s warehouse from tenant isolation seed", tc.orgID)
		}
		if orgCfg.Warehouse.MetadataStore.DatabaseName != tc.metadataDB {
			t.Fatalf("expected %s metadata db %q, got %q", tc.orgID, tc.metadataDB, orgCfg.Warehouse.MetadataStore.DatabaseName)
		}
		if _, ok := orgCfg.Users[tc.warehouseUser]; !ok {
			t.Fatalf("expected seeded user %q for org %s", tc.warehouseUser, tc.orgID)
		}
	}
}
