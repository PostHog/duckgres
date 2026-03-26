//go:build linux || darwin

package configstore_test

import (
	"path/filepath"
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
	if got := orgCfg.Warehouse.MetadataStoreCredentials.Name; got != "duckgres-local-metadata" {
		t.Fatalf("expected kind metadata secret ref, got %q", got)
	}
	if got := orgCfg.Warehouse.S3Credentials.Name; got != "duckgres-local-s3" {
		t.Fatalf("expected kind s3 secret ref, got %q", got)
	}
}
