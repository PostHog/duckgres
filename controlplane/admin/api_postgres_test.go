//go:build kubernetes

package admin

import (
	"errors"
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

// TestAdminOrgTeamStorePostgres exercises the real gormAPIStore team methods:
// billing repoint carries the buffered usage buckets, and create enforces
// per-org schema uniqueness. (Delete rules live in configstore.DeleteOrgTeamTx
// and are covered by tests/configstore/org_teams_postgres_test.go — the admin
// UI deletes through the provisioning-registered route.)
func TestAdminOrgTeamStorePostgres(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	if err := store.DB().Create(&configstore.Org{Name: "teamsorg", DatabaseName: "teamsorgdb"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, is_billing_team, created_at, updated_at)
		VALUES ('teamsorg', 1, 'team_1', TRUE, TRUE, ?, ?), ('teamsorg', 2, 'team_2', TRUE, NULL, ?, ?)`,
		base, base, base.Add(time.Hour), base.Add(time.Hour)).Error; err != nil {
		t.Fatalf("seed teams: %v", err)
	}
	bucket := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	if err := store.FlushComputeUsage([]configstore.ComputeUsageDelta{{
		OrgID: "teamsorg", TeamID: 1, QuerySource: "standard",
		Millicores: 2000, MiB: 4096, BucketStart: bucket,
		CPUSeconds: 10, MemorySeconds: 20,
	}}); err != nil {
		t.Fatalf("seed usage: %v", err)
	}

	// Repoint billing to team 2: the buffered bucket must follow atomically.
	prev, updated, err := apiStore.UpdateOrgTeam("teamsorg", 2, orgTeamUpdate{MakeBilling: true})
	if err != nil {
		t.Fatalf("repoint billing: %v", err)
	}
	if prev.IsBillingTeam != nil {
		t.Fatalf("prev must be the pre-update row (not billing), got %+v", prev)
	}
	if updated.IsBillingTeam == nil || !*updated.IsBillingTeam {
		t.Fatalf("team 2 must be billing, got %+v", updated)
	}
	var usageTeams []int64
	if err := store.DB().Raw(`SELECT team_id FROM duckgres_org_compute_usage WHERE org_id = 'teamsorg'`).Scan(&usageTeams).Error; err != nil {
		t.Fatalf("read usage teams: %v", err)
	}
	if len(usageTeams) != 1 || usageTeams[0] != 2 {
		t.Fatalf("usage teams after repoint = %v, want [2]", usageTeams)
	}

	// Create with a schema another team already holds: refused.
	err = apiStore.CreateOrgTeam("teamsorg", &configstore.OrgTeam{TeamID: 3, SchemaName: "team_1", Enabled: true})
	if !errors.Is(err, configstore.ErrOrgTeamSchemaConflict) {
		t.Fatalf("duplicate schema create err = %v, want ErrOrgTeamSchemaConflict", err)
	}
	// A duplicate (org, team) is the other conflict shape.
	err = apiStore.CreateOrgTeam("teamsorg", &configstore.OrgTeam{TeamID: 1, SchemaName: "fresh", Enabled: true})
	if !errors.Is(err, errOrgTeamExists) {
		t.Fatalf("duplicate team create err = %v, want errOrgTeamExists", err)
	}
	// A fresh schema works and appears in the cross-org list.
	if err := apiStore.CreateOrgTeam("teamsorg", &configstore.OrgTeam{TeamID: 3, SchemaName: "team_3", Enabled: true}); err != nil {
		t.Fatalf("create team 3: %v", err)
	}
	teams, err := apiStore.ListAllOrgTeams()
	if err != nil {
		t.Fatalf("list all teams: %v", err)
	}
	var got []int64
	for _, tm := range teams {
		if tm.OrgID == "teamsorg" {
			got = append(got, tm.TeamID)
		}
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("teamsorg teams = %v, want [1 2 3]", got)
	}
}

// TestAdminUpdateOrgTeamBreakGlassPostgres exercises the operator break-glass
// fields of gormAPIStore.UpdateOrgTeam against real Postgres: schema_name
// change (happy path, per-org 409, cross-org reuse) and the tri-state legacy
// table-name set / clear / preserve-on-omit semantics, including that the
// pre-update row comes back for audit.
func TestAdminUpdateOrgTeamBreakGlassPostgres(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	for _, org := range []string{"repairorg", "otherorg"} {
		if err := store.DB().Create(&configstore.Org{Name: org, DatabaseName: org + "db"}).Error; err != nil {
			t.Fatalf("create org %s: %v", org, err)
		}
	}
	if err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, is_billing_team, created_at, updated_at)
		VALUES ('repairorg', 1, 'team_1', TRUE, TRUE, now(), now()),
		       ('repairorg', 2, 'team_2', TRUE, NULL, now(), now()),
		       ('otherorg', 9, 'shared', TRUE, TRUE, now(), now())`).Error; err != nil {
		t.Fatalf("seed teams: %v", err)
	}

	str := func(s string) *string { return &s }

	// Schema change happy path; prev carries the replaced value.
	prev, updated, err := apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{SchemaName: str("repaired_wh")})
	if err != nil {
		t.Fatalf("schema change: %v", err)
	}
	if prev.SchemaName != "team_1" || updated.SchemaName != "repaired_wh" {
		t.Fatalf("schema change prev/updated = %q/%q, want team_1/repaired_wh", prev.SchemaName, updated.SchemaName)
	}

	// Conflict with another team in the same org.
	if _, _, err := apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{SchemaName: str("team_2")}); !errors.Is(err, configstore.ErrOrgTeamSchemaConflict) {
		t.Fatalf("same-org conflict err = %v, want ErrOrgTeamSchemaConflict", err)
	}

	// The same schema in a different org is allowed (uniqueness is per org).
	if _, updated, err = apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{SchemaName: str("shared")}); err != nil || updated.SchemaName != "shared" {
		t.Fatalf("cross-org schema reuse: updated = %+v, err = %v", updated, err)
	}

	// Legacy names: set all three...
	_, updated, err = apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{
		EventsTableNameSet: true, EventsTableName: str("legacy_events"),
		PersonsTableNameSet: true, PersonsTableName: str("legacy_persons"),
		SchemaDataImportsNameSet: true, SchemaDataImportsName: str("legacy_imports"),
	})
	if err != nil {
		t.Fatalf("set legacy names: %v", err)
	}
	if updated.EventsTableName == nil || *updated.EventsTableName != "legacy_events" ||
		updated.PersonsTableName == nil || *updated.PersonsTableName != "legacy_persons" ||
		updated.SchemaDataImportsName == nil || *updated.SchemaDataImportsName != "legacy_imports" {
		t.Fatalf("legacy names not stored: %+v", updated)
	}

	// ...then clear one; the omitted others must be preserved.
	_, updated, err = apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{EventsTableNameSet: true})
	if err != nil {
		t.Fatalf("clear events_table_name: %v", err)
	}
	if updated.EventsTableName != nil {
		t.Fatalf("events_table_name = %v, want NULL after clear", *updated.EventsTableName)
	}
	if updated.PersonsTableName == nil || *updated.PersonsTableName != "legacy_persons" ||
		updated.SchemaDataImportsName == nil || *updated.SchemaDataImportsName != "legacy_imports" {
		t.Fatalf("omitted legacy names must be preserved, got %+v", updated)
	}
	// The schema change earlier must have survived unrelated updates.
	if updated.SchemaName != "shared" {
		t.Fatalf("schema_name = %q, want shared preserved on omit", updated.SchemaName)
	}

	// earliest_event_date: set (real DATE round-trip), preserve on omit, clear.
	date, err := configstore.ParseEventDate("2023-04-17")
	if err != nil {
		t.Fatalf("parse date: %v", err)
	}
	prev, updated, err = apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{
		EarliestEventDateSet: true, EarliestEventDate: &date,
	})
	if err != nil {
		t.Fatalf("set earliest_event_date: %v", err)
	}
	if prev.EarliestEventDate != nil {
		t.Fatalf("prev earliest_event_date = %v, want NULL", prev.EarliestEventDate)
	}
	if updated.EarliestEventDate == nil || updated.EarliestEventDate.String() != "2023-04-17" {
		t.Fatalf("earliest_event_date = %v, want 2023-04-17", updated.EarliestEventDate)
	}
	_, updated, err = apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{Enabled: boolPtrAdmin(false)})
	if err != nil {
		t.Fatalf("unrelated update: %v", err)
	}
	if updated.EarliestEventDate == nil || updated.EarliestEventDate.String() != "2023-04-17" {
		t.Fatalf("omitted earliest_event_date must be preserved, got %v", updated.EarliestEventDate)
	}
	prev, updated, err = apiStore.UpdateOrgTeam("repairorg", 1, orgTeamUpdate{EarliestEventDateSet: true})
	if err != nil {
		t.Fatalf("clear earliest_event_date: %v", err)
	}
	if prev.EarliestEventDate == nil || updated.EarliestEventDate != nil {
		t.Fatalf("clear: prev/updated = %v/%v, want 2023-04-17/NULL", prev.EarliestEventDate, updated.EarliestEventDate)
	}
}

func boolPtrAdmin(b bool) *bool { return &b }
