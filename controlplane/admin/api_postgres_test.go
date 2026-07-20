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
	// Billing usage buffers have no gorm model (raw goose-migrated tables) but
	// leak across tests on the shared schema all the same.
	for _, table := range []string{"duckgres_org_compute_usage", "duckgres_org_storage_usage"} {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatalf("delete %s: %v", table, err)
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
	if err := configstore.SetOrgBillingTeamTx(store.DB(), "analytics", 1); err != nil {
		t.Fatalf("seed billing team: %v", err)
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

	var orgs, warehouses, teams int64
	store.DB().Model(&configstore.Org{}).Where("name = ?", "analytics").Count(&orgs)
	store.DB().Model(&configstore.ManagedWarehouse{}).Where("org_id = ?", "analytics").Count(&warehouses)
	store.DB().Model(&configstore.OrgTeam{}).Where("org_id = ?", "analytics").Count(&teams)
	if orgs != 0 {
		t.Fatalf("expected org row gone, found %d", orgs)
	}
	if warehouses != 0 {
		t.Fatalf("expected deleted warehouse row cascaded away, found %d", warehouses)
	}
	if teams != 0 {
		t.Fatalf("expected org team rows cascaded away (FK ON DELETE CASCADE), found %d", teams)
	}

	// The database_name is now free for reuse (no unique-index squat).
	if err := store.DB().Create(&configstore.Org{Name: "other", DatabaseName: "analytics_db"}).Error; err != nil {
		t.Fatalf("expected database_name to be reusable after org deletion: %v", err)
	}
}

// TestUpdateOrgReattributesUsagePostgres drives the real gormAPIStore against
// Postgres: an UpdateOrg carrying reattributeUsageTeam must move the org's
// buffered billing buckets (both tables) to the new team in the same
// transaction as the org-row update — folding a colliding target-key row
// additively — while an UpdateOrg without it leaves the buckets alone.
func TestUpdateOrgReattributesUsagePostgres(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	oldTeam := int64(1)
	if err := store.DB().Create(&configstore.Org{Name: "acme", DatabaseName: "acmedb"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := configstore.SetOrgBillingTeamTx(store.DB(), "acme", oldTeam); err != nil {
		t.Fatalf("seed billing team: %v", err)
	}

	bucket := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	seed := func(teamID, cpuSeconds, byteSeconds int64) {
		t.Helper()
		if err := store.FlushComputeUsage([]configstore.ComputeUsageDelta{{
			OrgID: "acme", TeamID: teamID, QuerySource: "standard",
			Millicores: 2000, MiB: 4096, BucketStart: bucket,
			CPUSeconds: cpuSeconds, MemorySeconds: cpuSeconds * 2,
		}}); err != nil {
			t.Fatalf("seed compute usage: %v", err)
		}
		if err := store.UpsertStorageSample("acme", teamID, bucket, byteSeconds); err != nil {
			t.Fatalf("seed storage usage: %v", err)
		}
	}
	seed(1, 10, 1000)
	// Pre-existing rows under the target team at the same bucket — the
	// collision the additive fold must absorb instead of violating the PK.
	seed(2, 5, 500)

	// An update WITHOUT reattribution must not touch the buckets.
	merged := configstore.Org{MaxWorkers: 3, DefaultTeamID: &oldTeam}
	if _, ok, err := apiStore.UpdateOrg("acme", merged, nil); err != nil || !ok {
		t.Fatalf("UpdateOrg without reattribution: ok=%v err=%v", ok, err)
	}
	var teams []int64
	if err := store.DB().Raw(`SELECT DISTINCT team_id FROM duckgres_org_compute_usage WHERE org_id = 'acme' ORDER BY team_id`).Scan(&teams).Error; err != nil {
		t.Fatalf("read teams: %v", err)
	}
	if len(teams) != 2 {
		t.Fatalf("buckets mutated by non-team update: teams=%v", teams)
	}

	// The team change moves everything onto team 2 and folds the collision.
	newTeam := int64(2)
	merged.DefaultTeamID = &newTeam
	updated, ok, err := apiStore.UpdateOrg("acme", merged, &newTeam)
	if err != nil || !ok {
		t.Fatalf("UpdateOrg with reattribution: ok=%v err=%v", ok, err)
	}
	if updated.DefaultTeamID == nil || *updated.DefaultTeamID != 2 {
		t.Fatalf("org default_team_id = %v, want 2", updated.DefaultTeamID)
	}
	// The billing mark moved to team 2; the old team row stays, demoted.
	var billingTeams []int64
	if err := store.DB().Raw(`SELECT team_id FROM duckgres_org_teams WHERE org_id = 'acme' AND is_billing_team IS TRUE`).Scan(&billingTeams).Error; err != nil {
		t.Fatalf("read billing teams: %v", err)
	}
	if len(billingTeams) != 1 || billingTeams[0] != 2 {
		t.Fatalf("billing team rows = %v, want [2]", billingTeams)
	}
	var compute []struct {
		TeamID        int64
		CPUSeconds    int64
		MemorySeconds int64
	}
	if err := store.DB().Raw(`SELECT team_id, cpu_seconds, memory_seconds FROM duckgres_org_compute_usage WHERE org_id = 'acme'`).Scan(&compute).Error; err != nil {
		t.Fatalf("read compute rows: %v", err)
	}
	if len(compute) != 1 || compute[0].TeamID != 2 || compute[0].CPUSeconds != 15 || compute[0].MemorySeconds != 30 {
		t.Fatalf("compute rows not folded onto team 2: %+v", compute)
	}
	var storage []struct {
		TeamID      int64
		ByteSeconds int64
	}
	if err := store.DB().Raw(`SELECT team_id, byte_seconds FROM duckgres_org_storage_usage WHERE org_id = 'acme'`).Scan(&storage).Error; err != nil {
		t.Fatalf("read storage rows: %v", err)
	}
	if len(storage) != 1 || storage[0].TeamID != 2 || storage[0].ByteSeconds != 1500 {
		t.Fatalf("storage rows not folded onto team 2: %+v", storage)
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
