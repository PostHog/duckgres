//go:build kubernetes && (linux || darwin)

package controlplane_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestKindConfigStoreSeedSQL(t *testing.T) {
	store := newIsolatedConfigStore(t)

	if err := applyConfigStoreSeed(t, store, filepath.Join(findProjectRoot(), "k8s", "kind", "config-store.seed.sql")); err != nil {
		t.Fatalf("apply kind seed: %v", err)
	}

	if err := store.Reload(); err != nil {
		t.Fatalf("reload store: %v", err)
	}

	snap := store.Snapshot()
	teamCfg := snap.Teams["local"]
	if teamCfg == nil {
		t.Fatal("expected local team from kind seed")
	}
	if teamCfg.Warehouse == nil {
		t.Fatal("expected local warehouse from kind seed")
	}
	if got := teamCfg.Warehouse.MetadataStore.Endpoint; got != "duckgres-local-ducklake-metadata" {
		t.Fatalf("expected kind metadata endpoint, got %q", got)
	}
	if got := teamCfg.Warehouse.MetadataStore.Port; got != 5432 {
		t.Fatalf("expected kind metadata port 5432, got %d", got)
	}
	if got := teamCfg.Warehouse.S3.Endpoint; got != "duckgres-local-minio:9000" {
		t.Fatalf("expected kind s3 endpoint, got %q", got)
	}
	if got := teamCfg.Warehouse.S3.Bucket; got != "duckgres-local" {
		t.Fatalf("expected duckgres-local bucket, got %q", got)
	}
	if got := teamCfg.Warehouse.MetadataStoreCredentials.Name; got != "duckgres-local-metadata" {
		t.Fatalf("expected kind metadata secret ref, got %q", got)
	}
	if got := teamCfg.Warehouse.S3Credentials.Name; got != "duckgres-local-s3" {
		t.Fatalf("expected kind s3 secret ref, got %q", got)
	}
}

func applyConfigStoreSeed(t *testing.T, store *configstore.ConfigStore, path string) error {
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
