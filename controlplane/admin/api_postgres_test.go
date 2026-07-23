//go:build kubernetes

package admin

import (
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	integrationtest "github.com/posthog/duckgres/tests/integration"
	"gorm.io/gorm"
)

func TestAdminAdmissionConfigMutationsSerializePostgres(t *testing.T) {
	tests := []struct {
		name     string
		orgID    string
		seed     func(t *testing.T, store *configstore.ConfigStore)
		mutation func(store *gormAPIStore) error
	}{
		{
			name:  "update org",
			orgID: "admission-update-org",
			seed: func(t *testing.T, store *configstore.ConfigStore) {
				t.Helper()
				if err := store.DB().Create(&configstore.Org{Name: "admission-update-org", DatabaseName: "admission_update_org"}).Error; err != nil {
					t.Fatalf("create org: %v", err)
				}
			},
			mutation: func(store *gormAPIStore) error {
				_, found, err := store.UpdateOrg("admission-update-org", configstore.Org{MaxWorkers: 3, MaxVCPUs: 4}, nil)
				if err == nil && !found {
					return errors.New("updated org was not found")
				}
				return err
			},
		},
		{
			name:  "delete org",
			orgID: "admission-delete-org",
			seed: func(t *testing.T, store *configstore.ConfigStore) {
				t.Helper()
				if err := store.DB().Create(&configstore.Org{Name: "admission-delete-org", DatabaseName: "admission_delete_org"}).Error; err != nil {
					t.Fatalf("create org: %v", err)
				}
			},
			mutation: func(store *gormAPIStore) error {
				found, err := store.DeleteOrg("admission-delete-org")
				if err == nil && !found {
					return errors.New("deleted org was not found")
				}
				return err
			},
		},
		{
			name:  "update user",
			orgID: "admission-update-user",
			seed: func(t *testing.T, store *configstore.ConfigStore) {
				t.Helper()
				seedAdmissionMutationUser(t, store, "admission-update-user", "alice")
			},
			mutation: func(store *gormAPIStore) error {
				maxVCPUs := 2
				_, found, err := store.UpdateUser("admission-update-user", "alice", "", nil, &maxVCPUs)
				if err == nil && !found {
					return errors.New("updated user was not found")
				}
				return err
			},
		},
		{
			name:  "delete user",
			orgID: "admission-delete-user",
			seed: func(t *testing.T, store *configstore.ConfigStore) {
				t.Helper()
				seedAdmissionMutationUser(t, store, "admission-delete-user", "alice")
			},
			mutation: func(store *gormAPIStore) error {
				found, err := store.DeleteUser("admission-delete-user", "alice")
				if err == nil && !found {
					return errors.New("deleted user was not found")
				}
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newPostgresConfigStore(t)
			apiStore := newGormAPIStore(store).(*gormAPIStore)
			tt.seed(t, store)
			assertAdminMutationWaitsForAdmissionLock(t, store, tt.orgID, func() error {
				return tt.mutation(apiStore)
			})
		})
	}
}

func seedAdmissionMutationUser(t *testing.T, store *configstore.ConfigStore, orgID, username string) {
	t.Helper()
	if err := store.DB().Create(&configstore.Org{Name: orgID, DatabaseName: orgID}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := store.DB().Create(&configstore.OrgUser{OrgID: orgID, Username: username, Password: "hash"}).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
}

func assertAdminMutationWaitsForAdmissionLock(
	t *testing.T,
	store *configstore.ConfigStore,
	orgID string,
	mutation func() error,
) {
	t.Helper()

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	holder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin admission lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()

	var holderPID int
	if err := holder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("read admission lock holder pid: %v", err)
	}
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", adminAdmissionLockKey(orgID)); err != nil {
		t.Fatalf("take admission lock: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- mutation() }()

	waitForBlockedAdminMutation(t, sqlDB, holderPID, result)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release admission lock: %v", err)
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("admin mutation after admission lock release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for admin mutation after admission lock release")
	}
}

func waitForBlockedAdminMutation(t *testing.T, db *sql.DB, holderPID int, result <-chan error) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-result:
			t.Fatalf("admin mutation completed before admission lock release: %v", err)
		default:
		}

		var waiters int
		if err := db.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND datname = current_database()
			  AND $1 = ANY(pg_blocking_pids(pid))
		`, holderPID).Scan(&waiters); err != nil {
			t.Fatalf("poll admin mutation admission-lock waiter: %v", err)
		}
		if waiters > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timed out waiting for admin mutation to block on the admission lock")
}

func adminAdmissionLockKey(orgID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("duckgres:org-connections:" + orgID))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

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
	if err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('analytics', 1, 'team_1', TRUE, now(), now())`).Error; err != nil {
		t.Fatalf("seed team: %v", err)
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
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('teamsorg', 1, 'team_1', TRUE, ?, ?), ('teamsorg', 2, 'team_2', TRUE, ?, ?)`,
		base, base, base.Add(time.Hour), base.Add(time.Hour)).Error; err != nil {
		t.Fatalf("seed teams: %v", err)
	}

	// Create with a schema another team already holds: refused.
	err := apiStore.CreateOrgTeam("teamsorg", &configstore.OrgTeam{TeamID: 3, SchemaName: "team_1", Enabled: true})
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
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('repairorg', 1, 'team_1', TRUE, now(), now()),
		       ('repairorg', 2, 'team_2', TRUE, now(), now()),
		       ('otherorg', 9, 'shared', TRUE, now(), now())`).Error; err != nil {
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

// TestAdminCreateOrgTeamDisabledPostgres pins the gorm default-tag pitfall on
// the ADMIN create surface (POST /teams {"enabled":false}): Enabled carries
// `default:true`, gorm omits zero-valued default-tagged fields from the
// INSERT, and without the explicit follow-up column write the DB default
// silently stores TRUE. Same bug class as configstore.UpsertOrgTeamTx's
// create path (TestCreateOrgTeamDisabledPostgres).
func TestAdminCreateOrgTeamDisabledPostgres(t *testing.T) {
	store := newPostgresConfigStore(t)
	apiStore := newGormAPIStore(store).(*gormAPIStore)

	if err := store.DB().Create(&configstore.Org{Name: "heldorg", DatabaseName: "heldorgdb"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	team := &configstore.OrgTeam{TeamID: 5, SchemaName: "team_5", Enabled: false}
	if err := apiStore.CreateOrgTeam("heldorg", team); err != nil {
		t.Fatalf("create disabled team: %v", err)
	}
	var got configstore.OrgTeam
	if err := store.DB().First(&got, "org_id = ? AND team_id = ?", "heldorg", 5).Error; err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got.Enabled {
		t.Fatal("admin-created team with enabled=false was stored as enabled=true (gorm default-tag pitfall)")
	}
	// The struct handed back to the handler (the 201 body) must not carry
	// gorm's RETURNING write-back either.
	if team.Enabled {
		t.Fatal("returned team struct carries enabled=true (RETURNING write-back not undone)")
	}
}
