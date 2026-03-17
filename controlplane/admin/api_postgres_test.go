//go:build kubernetes

package admin

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	integrationtest "github.com/posthog/duckgres/tests/integration"
	"gorm.io/gorm"
)

const testConfigStoreConnString = "host=127.0.0.1 port=35432 user=postgres password=postgres dbname=testdb sslmode=disable"

func newPostgresConfigStore(t *testing.T) *configstore.ConfigStore {
	t.Helper()

	if !integrationtest.IsPostgresRunning(35432) {
		if err := startIntegrationPostgres(t); err != nil {
			t.Fatalf("start postgres container: %v", err)
		}
	}

	store, err := configstore.NewConfigStore(testConfigStoreConnString, time.Hour)
	if err != nil {
		t.Fatalf("new config store: %v", err)
	}

	resetConfigStoreTables(t, store.DB())

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	return store
}

func startIntegrationPostgres(t *testing.T) error {
	t.Helper()

	cmd := exec.Command("docker-compose", "-f", filepath.Join(findProjectRoot(t), "tests", "integration", "docker-compose.yml"), "up", "-d", "postgres")
	if err := cmd.Run(); err != nil {
		return err
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if integrationtest.IsPostgresRunning(35432) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for postgres on 127.0.0.1:35432")
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

func resetConfigStoreTables(t *testing.T, db *gorm.DB) {
	t.Helper()

	for _, model := range []any{
		&configstore.ManagedWarehouse{},
		&configstore.TeamUser{},
		&configstore.Team{},
	} {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(model).Error; err != nil {
			t.Fatalf("delete %T: %v", model, err)
		}
	}
}

func TestUpsertManagedWarehousePreservesCreatedAt(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	if err := store.DB().Create(&configstore.Team{Name: "analytics"}).Error; err != nil {
		t.Fatalf("create team: %v", err)
	}

	createdAt := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	original := &configstore.ManagedWarehouse{
		TeamName:  "analytics",
		State:     configstore.ManagedWarehouseStatePending,
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
		Aurora: configstore.ManagedWarehouseAurora{
			DatabaseName: "analytics_wh",
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "shared_aurora",
			DatabaseName: "analytics_metadata",
		},
	}
	if err := store.DB().Create(original).Error; err != nil {
		t.Fatalf("create warehouse: %v", err)
	}

	replacementCreatedAt := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	stored, ok, err := apiStore.UpsertManagedWarehouse("analytics", &configstore.ManagedWarehouse{
		CreatedAt:     replacementCreatedAt,
		UpdatedAt:     replacementCreatedAt,
		State:         configstore.ManagedWarehouseStateReady,
		StatusMessage: "ready",
		Aurora: configstore.ManagedWarehouseAurora{
			DatabaseName: "analytics_ready",
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "dedicated_rds",
			Engine:       "postgres",
			DatabaseName: "ducklake_metadata",
		},
	})
	if err != nil {
		t.Fatalf("upsert warehouse: %v", err)
	}
	if !ok {
		t.Fatal("expected warehouse upsert to succeed")
	}

	if !stored.CreatedAt.Equal(createdAt) {
		t.Fatalf("expected created_at %s, got %s", createdAt.Format(time.RFC3339Nano), stored.CreatedAt.Format(time.RFC3339Nano))
	}
	if stored.Aurora.DatabaseName != "analytics_ready" {
		t.Fatalf("expected updated aurora db name, got %q", stored.Aurora.DatabaseName)
	}
	if stored.MetadataStore.DatabaseName != "ducklake_metadata" {
		t.Fatalf("expected updated metadata db name, got %q", stored.MetadataStore.DatabaseName)
	}
}
