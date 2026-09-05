//go:build linux || darwin

package configstore_test

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// seedTrinoOrg creates the org + its `root` user, the two rows every
// Trino projection reads through.
func seedTrinoOrg(t *testing.T, store *configstore.ConfigStore, name string) {
	t.Helper()
	seedOrg(t, store, name)
	if err := store.CreateOrgUser(name, "root", "$2a$10$hash-"+name); err != nil {
		t.Fatalf("create root user for %s: %v", name, err)
	}
}

func trinoRow(t *testing.T, store *configstore.ConfigStore, orgID string) *configstore.ManagedWarehouseTrino {
	t.Helper()
	row, err := store.GetManagedWarehouseTrino(orgID)
	if err != nil {
		t.Fatalf("GetManagedWarehouseTrino(%s): %v", orgID, err)
	}
	return row
}

func TestTrinoTablesAreMigratedPostgres(t *testing.T) {
	// The tables are goose-migrated (000038), not AutoMigrated — the
	// config schema has no AutoMigrate pass. If the migration stops being
	// applied, every Trino store call fails at runtime instead of here.
	store := newIsolatedConfigStore(t)
	for _, table := range []string{"duckgres_managed_warehouse_trino", "duckgres_trino_cluster_bootstrap"} {
		if !store.DB().Migrator().HasTable(table) {
			t.Errorf("expected %s to exist after migrations", table)
		}
	}
}

func TestEnableTrinoIsIdempotentAndPreservesReconcileState(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")

	if err := store.EnableTrino("acme", configstore.TrinoSettings{Tier: "free"}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	row := trinoRow(t, store, "acme")
	if row == nil || !row.Enabled || row.Tier != "free" {
		t.Fatalf("unexpected row after enable: %+v", row)
	}
	if row.State != configstore.ManagedWarehouseStatePending {
		t.Errorf("state = %q, want pending on a fresh enable", row.State)
	}
	if row.TrinoCellID != "" {
		t.Errorf("EnableTrino must not assign a cell; got %q", row.TrinoCellID)
	}

	// The reconcile loop advances the row...
	if err := store.AssignTrinoCell("acme", "cell-001"); err != nil {
		t.Fatalf("AssignTrinoCell: %v", err)
	}
	now := time.Now().UTC()
	if err := store.UpdateTrinoState("acme", configstore.TrinoStateUpdate{
		State:   configstore.ManagedWarehouseStateReady,
		ReadyAt: &now,
	}); err != nil {
		t.Fatalf("UpdateTrinoState: %v", err)
	}

	// ...and a re-enable (tier change) must NOT clobber it. This is the
	// whole reason the upsert names only enabled/tier/updated_at.
	if err := store.EnableTrino("acme", configstore.TrinoSettings{Tier: "growth"}); err != nil {
		t.Fatalf("EnableTrino (re-enable): %v", err)
	}
	row = trinoRow(t, store, "acme")
	if row.Tier != "growth" {
		t.Errorf("tier = %q, want growth", row.Tier)
	}
	if row.State != configstore.ManagedWarehouseStateReady {
		t.Errorf("state = %q, want the reconcile loop's ready to survive a re-enable", row.State)
	}
	if row.ReadyAt == nil {
		t.Error("ready_at must survive a re-enable")
	}
	if row.TrinoCellID != "cell-001" {
		t.Errorf("trino_cell_id = %q, want the claim to survive a re-enable", row.TrinoCellID)
	}
}

func TestAssignTrinoCellNeverStealsAnOwnedOrg(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", configstore.TrinoSettings{}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}

	if err := store.AssignTrinoCell("acme", "cell-001"); err != nil {
		t.Fatalf("AssignTrinoCell (first): %v", err)
	}
	// A second cell tries to claim the same org. The conditional WHERE is
	// the only thing stopping two coordinators from both projecting this
	// tenant's credentials and catalog.
	if err := store.AssignTrinoCell("acme", "cell-002"); err != nil {
		t.Fatalf("AssignTrinoCell (second) must be a silent no-op, got %v", err)
	}
	if got := trinoRow(t, store, "acme").TrinoCellID; got != "cell-001" {
		t.Fatalf("trino_cell_id = %q, want cell-001 (the second claim must not steal it)", got)
	}
}

func TestUpdateTrinoStateIgnoresDisabledOrgs(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", configstore.TrinoSettings{}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	if err := store.DisableTrino("acme"); err != nil {
		t.Fatalf("DisableTrino: %v", err)
	}

	// A reconcile tick that started before the disable must not write a
	// stale "ready" onto a row that is being torn down.
	if err := store.UpdateTrinoState("acme", configstore.TrinoStateUpdate{
		State:         configstore.ManagedWarehouseStateReady,
		StatusMessage: "stale",
	}); err != nil {
		t.Fatalf("UpdateTrinoState: %v", err)
	}
	row := trinoRow(t, store, "acme")
	if row.State != configstore.ManagedWarehouseStatePending || row.StatusMessage != "" {
		t.Fatalf("a disabled row must be untouched by the reconcile writer, got %+v", row)
	}
}

func TestUpdateTrinoStateTruncatesLongStatusMessage(t *testing.T) {
	// status_message is a 1024-char column; a joined multi-error can be
	// far longer, and a "value too long" would silently lose the state
	// write that tells an operator what broke.
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", configstore.TrinoSettings{}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	long := ""
	for len(long) < 4000 {
		long += "catalog: create catalog org_acme: trino: 503 service unavailable; "
	}
	if err := store.UpdateTrinoState("acme", configstore.TrinoStateUpdate{
		State:         configstore.ManagedWarehouseStateFailed,
		StatusMessage: long,
	}); err != nil {
		t.Fatalf("UpdateTrinoState with a long message: %v", err)
	}
	row := trinoRow(t, store, "acme")
	if len(row.StatusMessage) != 1024 {
		t.Fatalf("status_message length = %d, want it clipped to 1024", len(row.StatusMessage))
	}
	if row.StatusMessage[len(row.StatusMessage)-3:] != "..." {
		t.Error("a clipped status_message must end with the ... marker")
	}
}

func TestDisableTrinoPreservesTheCellAndClearsFailure(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", configstore.TrinoSettings{Tier: "scale"}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	if err := store.AssignTrinoCell("acme", "cell-001"); err != nil {
		t.Fatalf("AssignTrinoCell: %v", err)
	}
	failedAt := time.Now().UTC()
	if err := store.UpdateTrinoState("acme", configstore.TrinoStateUpdate{
		State:         configstore.ManagedWarehouseStateFailed,
		StatusMessage: "catalog: boom",
		FailedAt:      &failedAt,
	}); err != nil {
		t.Fatalf("UpdateTrinoState: %v", err)
	}

	if err := store.DisableTrino("acme"); err != nil {
		t.Fatalf("DisableTrino: %v", err)
	}
	row := trinoRow(t, store, "acme")
	if row == nil {
		t.Fatal("the row must be KEPT so the owning cell observes the transition and tears down")
	}
	if row.Enabled {
		t.Error("expected enabled=false")
	}
	if row.State != configstore.ManagedWarehouseStatePending {
		t.Errorf("state = %q, want pending", row.State)
	}
	if row.StatusMessage != "" || row.FailedAt != nil {
		t.Errorf("the previous lifecycle's failure must be cleared, got msg=%q failed_at=%v", row.StatusMessage, row.FailedAt)
	}
	// The cell that owns the org is the one that still has to drop its
	// catalog and Secret key, so the stamp must survive.
	if row.TrinoCellID != "cell-001" {
		t.Errorf("trino_cell_id = %q, want cell-001 to survive a disable", row.TrinoCellID)
	}
}

func TestDisableTrinoOnUnknownOrgIsNoOp(t *testing.T) {
	store := newIsolatedConfigStore(t)
	if err := store.DisableTrino("never-enabled"); err != nil {
		t.Fatalf("DisableTrino on an unknown org must be a no-op, got %v", err)
	}
}

func TestListTrinoEnabledOrgsJoinsRootUserAndCarriesCell(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	seedTrinoOrg(t, store, "beta")
	// An org WITHOUT a root user: the INNER JOIN drops it, because
	// projecting a half-built password file is worse than skipping.
	seedOrg(t, store, "rootless")

	for _, org := range []string{"acme", "beta", "rootless"} {
		if err := store.EnableTrino(org, configstore.TrinoSettings{Tier: "free"}); err != nil {
			t.Fatalf("EnableTrino(%s): %v", org, err)
		}
	}
	if err := store.AssignTrinoCell("acme", "cell-001"); err != nil {
		t.Fatalf("AssignTrinoCell: %v", err)
	}
	// beta stays unassigned — the listing must still return it so a cell
	// can claim it. Filtering by cell in SQL would make a freshly enabled
	// org invisible to every cell forever.

	got, err := store.ListTrinoEnabledOrgs()
	if err != nil {
		t.Fatalf("ListTrinoEnabledOrgs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 orgs (rootless dropped by the root-user join), got %d: %+v", len(got), got)
	}
	// Ordered by org_id so projections are byte-stable across ticks.
	if got[0].OrgID != "acme" || got[1].OrgID != "beta" {
		t.Fatalf("expected [acme beta] in order, got %+v", got)
	}
	if got[0].CellID != "cell-001" {
		t.Errorf("acme CellID = %q, want cell-001", got[0].CellID)
	}
	if got[1].CellID != "" {
		t.Errorf("beta CellID = %q, want empty (unassigned)", got[1].CellID)
	}
	if got[0].RootPasswordHash != "$2a$10$hash-acme" {
		t.Errorf("acme RootPasswordHash = %q, want the root user's bcrypt", got[0].RootPasswordHash)
	}
	if got[0].DatabaseName != "acmedb" {
		t.Errorf("acme DatabaseName = %q, want acmedb", got[0].DatabaseName)
	}
	if got[0].Tier != "free" {
		t.Errorf("acme Tier = %q, want free", got[0].Tier)
	}

	// A disabled org leaves the listing entirely.
	if err := store.DisableTrino("beta"); err != nil {
		t.Fatalf("DisableTrino: %v", err)
	}
	got, err = store.ListTrinoEnabledOrgs()
	if err != nil {
		t.Fatalf("ListTrinoEnabledOrgs (after disable): %v", err)
	}
	if len(got) != 1 || got[0].OrgID != "acme" {
		t.Fatalf("expected only acme after disabling beta, got %+v", got)
	}
}

// Deleting the Org row must take its Trino opt-in with it — otherwise a
// re-created org would inherit a stranded row.
// database_name is the org's Trino principal — the username it authenticates
// as and the stem of its catalog name — so an org without one has no
// derivable Trino identity and must be dropped by the listing, exactly as a
// root-less org is. Returning it would project a catalog literally named
// `org_`, which every such org would collide on.
func TestListTrinoEnabledOrgsRequiresADatabaseName(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")

	// An org with a root user but no database_name.
	if err := store.DB().Create(&configstore.Org{Name: "nameless", DatabaseName: ""}).Error; err != nil {
		t.Fatalf("create nameless org: %v", err)
	}
	if err := store.CreateOrgUser("nameless", "root", "$2a$10$hash-nameless"); err != nil {
		t.Fatalf("create root user for nameless: %v", err)
	}

	for _, org := range []string{"acme", "nameless"} {
		if err := store.EnableTrino(org, configstore.TrinoSettings{Tier: "free"}); err != nil {
			t.Fatalf("EnableTrino(%s): %v", org, err)
		}
	}

	got, err := store.ListTrinoEnabledOrgs()
	if err != nil {
		t.Fatalf("ListTrinoEnabledOrgs: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected only acme (nameless dropped for having no database_name), got %+v", got)
	}
	if got[0].OrgID != "acme" {
		t.Fatalf("expected acme, got %+v", got[0])
	}
	// And the principal is carried through, since every Trino-facing name
	// is derived from it.
	if got[0].DatabaseName != "acmedb" {
		t.Errorf("DatabaseName = %q, want acmedb", got[0].DatabaseName)
	}
	if got[0].TrinoPrincipal() != "acmedb" {
		t.Errorf("TrinoPrincipal() = %q, want acmedb", got[0].TrinoPrincipal())
	}
}

func TestTrinoRowCascadesOnOrgDeletePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", configstore.TrinoSettings{}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	if err := store.DB().Exec(`DELETE FROM duckgres_org_users WHERE org_id = 'acme'`).Error; err != nil {
		t.Fatalf("delete org users: %v", err)
	}
	if err := store.DB().Exec(`DELETE FROM duckgres_orgs WHERE name = 'acme'`).Error; err != nil {
		t.Fatalf("delete org: %v", err)
	}
	if row := trinoRow(t, store, "acme"); row != nil {
		t.Fatalf("expected the trino row to CASCADE away with the org, got %+v", row)
	}
}

// The FK is the other half of the API's 404 preflight: without it,
// enabling Trino on a nonexistent org would write an orphan row.
func TestEnableTrinoOnUnknownOrgViolatesForeignKeyPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	if err := store.EnableTrino("nosuchorg", configstore.TrinoSettings{}); err == nil {
		t.Fatal("expected a foreign-key violation enabling Trino for an org that does not exist")
	}
}

// Every one of an org's own logins must reach the Trino projection, not just
// `root` — that is the whole point of per-user Trino access. The listing also
// has to fail closed on a disabled user, which must never reach a password
// file.
func TestListTrinoEnabledOrgsProjectsEveryLogin(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", configstore.TrinoSettings{Tier: "free"}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	for _, u := range []struct{ name, hash string }{
		{"analyst", "$2a$10$analyst"},
		{"dashboards", "$2a$10$dashboards"},
		{"leaver", "$2a$10$leaver"},
	} {
		if err := store.CreateOrgUser("acme", u.name, u.hash); err != nil {
			t.Fatalf("CreateOrgUser(%s): %v", u.name, err)
		}
	}
	if err := store.SetOrgUserDisabled("acme", "leaver", true); err != nil {
		t.Fatalf("SetOrgUserDisabled: %v", err)
	}
	if err := store.ReloadSnapshot(); err != nil {
		t.Fatalf("ReloadSnapshot: %v", err)
	}

	got, err := store.ListTrinoEnabledOrgs()
	if err != nil {
		t.Fatalf("ListTrinoEnabledOrgs: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 org, got %d", len(got))
	}
	byName := map[string]configstore.TrinoOrgUser{}
	for _, u := range got[0].Users {
		byName[u.Username] = u
	}
	// root appears here too, alongside the bare org principal, so the same
	// credential works under either username.
	for _, want := range []struct{ name, hash string }{
		{"root", "$2a$10$hash-acme"},
		{"analyst", "$2a$10$analyst"},
		{"dashboards", "$2a$10$dashboards"},
	} {
		u, ok := byName[want.name]
		if !ok {
			t.Errorf("login %q missing from the projection", want.name)
			continue
		}
		// The hash is copied through unchanged: it is the SAME bcrypt the
		// pgwire handshake verifies, so one password works on both engines.
		if u.PasswordHash != want.hash {
			t.Errorf("login %q hash = %q, want %q", want.name, u.PasswordHash, want.hash)
		}
		if u.Scope != nil {
			t.Errorf("login %q must be unscoped", want.name)
		}
	}
	if _, ok := byName["leaver"]; ok {
		t.Error("a disabled login must never reach the password file")
	}
	if len(byName) != 3 {
		t.Errorf("projected logins = %v, want exactly root/analyst/dashboards", byName)
	}
}

// A project-scoped login must arrive carrying the SAME scope the pgwire
// session path enforces, so Trino and DuckDB cannot disagree about which
// schemas the login may read.
func TestListTrinoEnabledOrgsCarriesProjectScopes(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", configstore.TrinoSettings{Tier: "free"}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	if _, err := configstore.UpsertOrgTeamTx(store.DB(), "acme", configstore.OrgTeamUpsert{
		TeamID:     7,
		SchemaName: "posthog_7",
	}); err != nil {
		t.Fatalf("UpsertOrgTeamTx: %v", err)
	}
	if err := store.CreateOrgUser("acme", "posthog_team_7", "$2a$10$team7"); err != nil {
		t.Fatalf("CreateOrgUser: %v", err)
	}
	// No configstore mutator binds a login to a team (the admin API owns that
	// surface), so bind it directly — the point under test is the listing,
	// not the admin handler.
	if err := store.DB().Exec(
		`UPDATE duckgres_org_users SET access_mode = 'project_reader', team_id = 7
		  WHERE org_id = 'acme' AND username = 'posthog_team_7'`).Error; err != nil {
		t.Fatalf("bind project login: %v", err)
	}
	if err := store.ReloadSnapshot(); err != nil {
		t.Fatalf("ReloadSnapshot: %v", err)
	}

	got, err := store.ListTrinoEnabledOrgs()
	if err != nil {
		t.Fatalf("ListTrinoEnabledOrgs: %v", err)
	}
	var scoped *configstore.TrinoOrgUser
	for i, u := range got[0].Users {
		if u.Username == "posthog_team_7" {
			scoped = &got[0].Users[i]
		}
	}
	if scoped == nil {
		t.Fatal("the project login is missing from the projection")
	}
	if scoped.Scope == nil {
		t.Fatal("the project login must carry a scope, or it would read the whole catalog")
	}
	if scoped.TeamID == nil || *scoped.TeamID != 7 {
		t.Fatalf("TeamID = %v, want 7 — the scope group is keyed on it", scoped.TeamID)
	}
	// Exactly what OrgUserQueryAccess reports for the same user, which is
	// what pgwire enforces.
	want, ok := store.OrgUserQueryAccess("acme", "posthog_team_7")
	if !ok {
		t.Fatal("OrgUserQueryAccess must report the login as scoped")
	}
	if !reflect.DeepEqual(*scoped.Scope, want) {
		t.Errorf("scope = %+v, want %+v (the same policy pgwire enforces)", *scoped.Scope, want)
	}
	if !slices.Contains(scoped.Scope.AllowedSchemas, "posthog_7") {
		t.Errorf("AllowedSchemas = %v, must contain the team's schema", scoped.Scope.AllowedSchemas)
	}
}
