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
		&configstore.OrgUser{},
		&configstore.Org{},
	} {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(model).Error; err != nil {
			t.Fatalf("delete %T: %v", model, err)
		}
	}
}

func TestUpsertManagedWarehousePreservesCreatedAt(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	if err := store.DB().Create(&configstore.Org{Name: "analytics"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}

	createdAt := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	original := &configstore.ManagedWarehouse{
		OrgID:        "analytics",
		DucklingName: "analytics",
		State:        configstore.ManagedWarehouseStatePending,
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt,
		WarehouseDatabase: configstore.ManagedWarehouseDatabase{
			Endpoint: "analytics-wh.cluster.example",
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "shared",
			DatabaseName: "analytics_metadata",
		},
	}
	if err := store.DB().Create(original).Error; err != nil {
		t.Fatalf("create warehouse: %v", err)
	}

	replacementCreatedAt := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	stored, ok, err := apiStore.UpsertManagedWarehouse("analytics", &configstore.ManagedWarehouse{
		CreatedAt: replacementCreatedAt,
		UpdatedAt: replacementCreatedAt,
		// Changed vs the seeded row: the ON CONFLICT assignment-column list
		// must carry duckling_name or this change is silently dropped.
		DucklingName:  "analytics-renamed",
		State:         configstore.ManagedWarehouseStateReady,
		StatusMessage: "ready",
		WarehouseDatabase: configstore.ManagedWarehouseDatabase{
			Endpoint: "analytics-ready.cluster.example",
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "dedicated_rds",
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
	if stored.WarehouseDatabase.Endpoint != "analytics-ready.cluster.example" {
		t.Fatalf("expected updated warehouse db endpoint, got %q", stored.WarehouseDatabase.Endpoint)
	}
	if stored.MetadataStore.DatabaseName != "ducklake_metadata" {
		t.Fatalf("expected updated metadata db name, got %q", stored.MetadataStore.DatabaseName)
	}
	if stored.DucklingName != "analytics-renamed" {
		t.Fatalf("expected updated duckling_name analytics-renamed, got %q", stored.DucklingName)
	}
}

func TestDeleteOrgCascadesDeletedWarehousePostgres(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	if err := store.DB().Create(&configstore.Org{Name: "analytics", DatabaseName: "analytics_db"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	wh := &configstore.ManagedWarehouse{OrgID: "analytics", State: configstore.ManagedWarehouseStateReady}
	if err := store.DB().Create(wh).Error; err != nil {
		t.Fatalf("create warehouse: %v", err)
	}

	// A non-terminal warehouse row must still block org deletion — the infra is
	// live and deleting the org would orphan the Duckling CR.
	if _, err := apiStore.DeleteOrg("analytics"); err != errWarehouseStillExists {
		t.Fatalf("DeleteOrg with a ready warehouse: got %v, want errWarehouseStillExists", err)
	}

	// Simulate a completed deprovision: the provisioner leaves the row behind in
	// the terminal "deleted" state. The org must now delete, cascading the row
	// away so the database_name is released (no longer squatted).
	if err := store.DB().Model(&configstore.ManagedWarehouse{}).Where("org_id = ?", "analytics").
		Update("state", configstore.ManagedWarehouseStateDeleted).Error; err != nil {
		t.Fatalf("mark warehouse deleted: %v", err)
	}
	deleted, err := apiStore.DeleteOrg("analytics")
	if err != nil {
		t.Fatalf("DeleteOrg after deprovision: %v", err)
	}
	if !deleted {
		t.Fatal("expected org row to be deleted")
	}

	var orgs, warehouses int64
	store.DB().Model(&configstore.Org{}).Where("name = ?", "analytics").Count(&orgs)
	store.DB().Model(&configstore.ManagedWarehouse{}).Where("org_id = ?", "analytics").Count(&warehouses)
	if orgs != 0 {
		t.Fatalf("expected org row gone, found %d", orgs)
	}
	if warehouses != 0 {
		t.Fatalf("expected deleted warehouse row cascaded away, found %d", warehouses)
	}

	// The database_name is now free for reuse (no unique-index squat).
	if err := store.DB().Create(&configstore.Org{Name: "other", DatabaseName: "analytics_db"}).Error; err != nil {
		t.Fatalf("expected database_name to be reusable after org deletion: %v", err)
	}
}

func TestMutateManagedWarehouseSerializesConcurrentWriters(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	if err := store.DB().Create(&configstore.Org{Name: "analytics"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := store.DB().Create(&configstore.ManagedWarehouse{
		OrgID: "analytics",
		State: configstore.ManagedWarehouseStatePending,
	}).Error; err != nil {
		t.Fatalf("seed warehouse: %v", err)
	}

	// Fan out N concurrent Mutate calls, each incrementing a counter encoded
	// in StatusMessage. With plain Get+Upsert these would race and drop some
	// increments. With SELECT ... FOR UPDATE inside a transaction, every
	// writer observes the prior one's commit and the counter lands at N.
	const writers = 8
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func() {
			_, _, err := apiStore.MutateManagedWarehouse("analytics", func(w *configstore.ManagedWarehouse) error {
				var n int
				_, _ = fmt.Sscanf(w.StatusMessage, "n=%d", &n)
				w.StatusMessage = fmt.Sprintf("n=%d", n+1)
				return nil
			})
			errs <- err
		}()
	}
	for i := 0; i < writers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("mutate %d: %v", i, err)
		}
	}

	final, err := apiStore.GetManagedWarehouse("analytics")
	if err != nil {
		t.Fatalf("get warehouse: %v", err)
	}
	want := fmt.Sprintf("n=%d", writers)
	if final.StatusMessage != want {
		t.Fatalf("status_message = %q, want %q (lost updates under concurrency)", final.StatusMessage, want)
	}
}
