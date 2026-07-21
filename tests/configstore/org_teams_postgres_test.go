//go:build linux || darwin

package configstore_test

import (
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
	"gorm.io/gorm"
)

func seedOrg(t *testing.T, store *configstore.ConfigStore, name string) {
	t.Helper()
	if err := store.DB().Create(&configstore.Org{Name: name, DatabaseName: name + "db"}).Error; err != nil {
		t.Fatalf("create org %s: %v", name, err)
	}
}

func readTeam(t *testing.T, store *configstore.ConfigStore, orgID string, teamID int64) configstore.OrgTeam {
	t.Helper()
	var team configstore.OrgTeam
	if err := store.DB().First(&team, "org_id = ? AND team_id = ?", orgID, teamID).Error; err != nil {
		t.Fatalf("read team (org=%s team=%d): %v", orgID, teamID, err)
	}
	return team
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

// TestUpsertOrgTeamGrandfatherPostgres pins the grandfather contract of the
// provisioning upsert: an existing row's schema_name and legacy table names
// ARE overwritable (the PostHog backfill replaces the migration's "team_<id>"
// placeholder through this path), while omitted presence-aware fields
// preserve the stored values.
func TestUpsertOrgTeamGrandfatherPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	pstore := provisioning.NewGormStore(store)

	created, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{
		TeamID:     7,
		SchemaName: "team_7",
	})
	if err != nil {
		t.Fatalf("create upsert: %v", err)
	}
	if !created.Enabled || created.EventsTableName != nil {
		t.Fatalf("created row = %+v, want enabled default true, legacy names NULL", created)
	}
	// backfill_enabled is NOT NULL DEFAULT TRUE (migration 000027): a create
	// that omits the field gets TRUE.
	if created.BackfillEnabled == nil || !*created.BackfillEnabled {
		t.Fatalf("created row backfill_enabled = %v, want default TRUE", created.BackfillEnabled)
	}

	// Grandfather: replace the placeholder schema and set explicit names;
	// disable backfill; enabled omitted must be preserved.
	updated, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{
		TeamID:                7,
		SchemaName:            "legacy_wh",
		BackfillEnabled:       boolPtr(false),
		EventsTableName:       strPtr("legacy_events"),
		PersonsTableName:      strPtr("legacy_persons"),
		SchemaDataImportsName: strPtr("legacy_imports"),
	})
	if err != nil {
		t.Fatalf("grandfather upsert: %v", err)
	}
	if updated.SchemaName != "legacy_wh" {
		t.Fatalf("schema_name = %q, want overwritten legacy_wh", updated.SchemaName)
	}
	if updated.EventsTableName == nil || *updated.EventsTableName != "legacy_events" ||
		updated.PersonsTableName == nil || *updated.PersonsTableName != "legacy_persons" ||
		updated.SchemaDataImportsName == nil || *updated.SchemaDataImportsName != "legacy_imports" {
		t.Fatalf("legacy names not stored: %+v", updated)
	}
	if updated.BackfillEnabled == nil || *updated.BackfillEnabled {
		t.Fatalf("backfill_enabled = %v, want false", updated.BackfillEnabled)
	}
	if !updated.Enabled {
		t.Fatal("omitted enabled must preserve the stored value")
	}

	// Explicit "" clears a legacy override back to NULL (derive again).
	cleared, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{
		TeamID:          7,
		SchemaName:      "legacy_wh",
		EventsTableName: strPtr(""),
	})
	if err != nil {
		t.Fatalf("clearing upsert: %v", err)
	}
	if cleared.EventsTableName != nil {
		t.Fatalf("events_table_name = %v, want NULL after explicit empty", *cleared.EventsTableName)
	}
	if cleared.PersonsTableName == nil || *cleared.PersonsTableName != "legacy_persons" {
		t.Fatalf("omitted persons_table_name must be preserved, got %+v", cleared)
	}
	if cleared.EarliestEventDate != nil {
		t.Fatalf("earliest_event_date = %v, want NULL before ever being set", cleared.EarliestEventDate)
	}

	// earliest_event_date round-trip through the real DATE column: set...
	date, err := configstore.ParseEventDate("2023-04-17")
	if err != nil {
		t.Fatalf("parse date: %v", err)
	}
	withDate, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{
		TeamID:               7,
		SchemaName:           "legacy_wh",
		EarliestEventDateSet: true,
		EarliestEventDate:    &date,
	})
	if err != nil {
		t.Fatalf("set earliest_event_date: %v", err)
	}
	if withDate.EarliestEventDate == nil || withDate.EarliestEventDate.String() != "2023-04-17" {
		t.Fatalf("earliest_event_date = %v, want 2023-04-17", withDate.EarliestEventDate)
	}
	if got := readTeam(t, store, "acme", 7); got.EarliestEventDate == nil || got.EarliestEventDate.String() != "2023-04-17" {
		t.Fatalf("re-read earliest_event_date = %v, want 2023-04-17", got.EarliestEventDate)
	}

	// ...preserve when the Set flag is off...
	preserved, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{TeamID: 7, SchemaName: "legacy_wh"})
	if err != nil {
		t.Fatalf("preserving upsert: %v", err)
	}
	if preserved.EarliestEventDate == nil || preserved.EarliestEventDate.String() != "2023-04-17" {
		t.Fatalf("unset earliest_event_date must be preserved, got %v", preserved.EarliestEventDate)
	}

	// ...and clear back to NULL with Set + nil.
	dateCleared, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{
		TeamID:               7,
		SchemaName:           "legacy_wh",
		EarliestEventDateSet: true,
	})
	if err != nil {
		t.Fatalf("clearing earliest_event_date: %v", err)
	}
	if dateCleared.EarliestEventDate != nil {
		t.Fatalf("earliest_event_date = %v, want NULL after explicit clear", dateCleared.EarliestEventDate)
	}
}

func TestUpsertOrgTeamSchemaConflictPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	seedOrg(t, store, "other")
	pstore := provisioning.NewGormStore(store)

	if _, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{TeamID: 1, SchemaName: "shared"}); err != nil {
		t.Fatalf("seed team: %v", err)
	}

	// Same org, different team, same schema → conflict.
	if _, err := pstore.UpsertOrgTeam("acme", configstore.OrgTeamUpsert{TeamID: 2, SchemaName: "shared"}); !errors.Is(err, configstore.ErrOrgTeamSchemaConflict) {
		t.Fatalf("duplicate schema in org: err = %v, want ErrOrgTeamSchemaConflict", err)
	}

	// The same schema in a DIFFERENT org is fine (uniqueness is per org).
	if _, err := pstore.UpsertOrgTeam("other", configstore.OrgTeamUpsert{TeamID: 9, SchemaName: "shared"}); err != nil {
		t.Fatalf("same schema across orgs must be allowed: %v", err)
	}

	// Unknown org → not found.
	if _, err := pstore.UpsertOrgTeam("ghost", configstore.OrgTeamUpsert{TeamID: 1, SchemaName: "x"}); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("unknown org: err = %v, want gorm.ErrRecordNotFound", err)
	}

	// The unique index backs the pre-check: a direct insert that bypasses the
	// helper still fails at the database.
	err := store.DB().Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, created_at, updated_at)
		VALUES ('acme', 3, 'shared', TRUE, now(), now())`).Error
	if err == nil {
		t.Fatal("direct duplicate-schema insert must violate the unique index")
	}
}

// TestDeleteOrgTeamPostgres covers the transactional delete rules: last-team
// refusal, billing handover to the OLDEST remaining team, and the usage
// bucket re-attribution riding in the same transaction.
func TestDeleteOrgTeamPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	pstore := provisioning.NewGormStore(store)

	// Billing team 1 (oldest), then 3, then 2 — created_at decides succession,
	// NOT team id, so the successor must be team 3.
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for _, seed := range []struct {
		team      int64
		schema    string
		createdAt time.Time
		billing   bool
	}{
		{1, "team_1", base, true},
		{3, "team_3", base.Add(time.Hour), false},
		{2, "team_2", base.Add(2 * time.Hour), false},
	} {
		var billing interface{}
		if seed.billing {
			billing = true
		}
		if err := store.DB().Exec(`
			INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, is_billing_team, created_at, updated_at)
			VALUES ('acme', ?, ?, TRUE, ?, ?, ?)`,
			seed.team, seed.schema, billing, seed.createdAt, seed.createdAt).Error; err != nil {
			t.Fatalf("seed team %d: %v", seed.team, err)
		}
	}
	bucket := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	seedUsage(t, store, "acme", 1, bucket, 10, 20, 1000)

	// Deleting a non-billing team: no handover, billing untouched.
	res, err := pstore.DeleteOrgTeam("acme", 2)
	if err != nil {
		t.Fatalf("delete non-billing team: %v", err)
	}
	if res.WasBilling || res.NewBillingTeamID != 0 {
		t.Fatalf("non-billing delete result = %+v, want no handover", res)
	}

	// Deleting the billing team: the OLDEST remaining team (3) takes over and
	// the buffered usage moves to it atomically.
	res, err = pstore.DeleteOrgTeam("acme", 1)
	if err != nil {
		t.Fatalf("delete billing team: %v", err)
	}
	if !res.WasBilling || res.NewBillingTeamID != 3 {
		t.Fatalf("billing delete result = %+v, want handover to team 3", res)
	}
	successor := readTeam(t, store, "acme", 3)
	if successor.IsBillingTeam == nil || !*successor.IsBillingTeam {
		t.Fatalf("team 3 must carry the billing mark, got %+v", successor)
	}
	compute := computeRowsForOrg(t, store, "acme")
	if len(compute) != 1 || compute[0].TeamID != 3 || compute[0].CPUSeconds != 10 {
		t.Fatalf("compute usage not re-attributed to team 3: %+v", compute)
	}
	storage := storageRowsForOrg(t, store, "acme")
	if len(storage) != 1 || storage[0].TeamID != 3 || storage[0].ByteSeconds != 1000 {
		t.Fatalf("storage usage not re-attributed to team 3: %+v", storage)
	}

	// The last team cannot be deleted.
	if _, err := pstore.DeleteOrgTeam("acme", 3); !errors.Is(err, configstore.ErrLastOrgTeam) {
		t.Fatalf("last-team delete: err = %v, want ErrLastOrgTeam", err)
	}
	if got := readTeam(t, store, "acme", 3); got.TeamID != 3 {
		t.Fatal("last team must survive the refused delete")
	}

	// Unknown team → not found.
	if _, err := pstore.DeleteOrgTeam("acme", 99); !errors.Is(err, configstore.ErrOrgTeamNotFound) {
		t.Fatalf("unknown team delete: err = %v, want ErrOrgTeamNotFound", err)
	}
}

// TestProvisionWithTeamIDAndSchemaNamePostgres: the org-create path with the
// new team_id+schema_name pair creates the first team row as the billing team
// with the EXPLICIT schema instead of the conventional "team_<id>". (The
// legacy default_team_id path — conventional schema — is pinned by
// TestProvisionReattributesUsageOnTeamChangePostgres.)
func TestProvisionWithTeamIDAndSchemaNamePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	pstore := provisioning.NewGormStore(store)

	if err := pstore.Provision(provisioning.ProvisionRequest{
		OrgID:         "schemaorg",
		DatabaseName:  "schemaorgdb",
		DefaultTeamID: 42,
		SchemaName:    "custom_wh",
		Warehouse:     &configstore.ManagedWarehouse{DucklingName: "schemaorg"},
		RootUserHash:  "hash",
	}); err != nil {
		t.Fatalf("provision with schema: %v", err)
	}

	team := readTeam(t, store, "schemaorg", 42)
	if team.SchemaName != "custom_wh" {
		t.Fatalf("schema_name = %q, want explicit custom_wh", team.SchemaName)
	}
	if team.IsBillingTeam == nil || !*team.IsBillingTeam {
		t.Fatalf("first team must be the billing team, got %+v", team)
	}
	if !team.Enabled {
		t.Fatal("first team must be enabled")
	}
}
