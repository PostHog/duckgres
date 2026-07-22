package configstore

import (
	"reflect"
	"testing"
)

func TestOrgUserQueryAccessDerivesProjectNamespaces(t *testing.T) {
	events := "events_prod"
	persons := "persons_prod"
	imports := "posthog_data_imports_prod"
	teamID := int64(42)
	key := OrgUserKey{OrgID: "acme", Username: "posthog_team_42"}
	cs := &ConfigStore{snapshot: &Snapshot{
		Orgs: map[string]*OrgConfig{
			"acme": {
				Teams: []OrgTeamConfig{{
					TeamID:                teamID,
					SchemaName:            "team_42",
					Enabled:               true,
					EventsTableName:       &events,
					PersonsTableName:      &persons,
					SchemaDataImportsName: &imports,
				}},
			},
		},
		OrgUserAccess: map[OrgUserKey]OrgUserAccessConfig{
			key: {Mode: OrgUserAccessModeProjectReader, TeamID: &teamID},
		},
	}}

	got, ok := cs.OrgUserQueryAccess("acme", "posthog_team_42")
	if !ok {
		t.Fatal("expected a project reader policy")
	}
	wantSchemas := []string{"posthog_data_imports_prod", "shadow_42_models", "team_42"}
	wantRelations := []string{"posthog.events_prod", "posthog.persons_prod"}
	if !reflect.DeepEqual(got.AllowedSchemas, wantSchemas) {
		t.Fatalf("AllowedSchemas = %v, want %v", got.AllowedSchemas, wantSchemas)
	}
	if !reflect.DeepEqual(got.AllowedRelations, wantRelations) {
		t.Fatalf("AllowedRelations = %v, want %v", got.AllowedRelations, wantRelations)
	}
	if !got.ReadOnly {
		t.Fatal("project reader policy must be read-only")
	}
}

// A backfilled legacy team can carry overrides that EQUAL the derived default
// names (posthog org team 2: events_table_name="events" → posthog.events). A
// non-NULL override always means "this team's table lives in the shared
// legacy posthog schema", so the grant must not depend on the override's
// spelling.
func TestOrgUserQueryAccessGrantsDefaultNamedLegacyTables(t *testing.T) {
	events := "events"
	persons := "persons"
	teamID := int64(2)
	key := OrgUserKey{OrgID: "acme", Username: "posthog_team_2"}
	cs := &ConfigStore{snapshot: &Snapshot{
		Orgs: map[string]*OrgConfig{
			"acme": {
				Teams: []OrgTeamConfig{{
					TeamID:           teamID,
					SchemaName:       "team_2",
					Enabled:          true,
					EventsTableName:  &events,
					PersonsTableName: &persons,
				}},
			},
		},
		OrgUserAccess: map[OrgUserKey]OrgUserAccessConfig{
			key: {Mode: OrgUserAccessModeProjectReader, TeamID: &teamID},
		},
	}}

	got, ok := cs.OrgUserQueryAccess("acme", "posthog_team_2")
	if !ok {
		t.Fatal("expected a project reader policy")
	}
	wantRelations := []string{"posthog.events", "posthog.persons"}
	if !reflect.DeepEqual(got.AllowedRelations, wantRelations) {
		t.Fatalf("AllowedRelations = %v, want %v", got.AllowedRelations, wantRelations)
	}
}

// NULL overrides mean "derive from schema_name" — the team is on the
// per-team-schema model and gets no legacy posthog-schema relations.
func TestOrgUserQueryAccessGrantsNoLegacyTablesWithoutOverrides(t *testing.T) {
	teamID := int64(7)
	key := OrgUserKey{OrgID: "acme", Username: "posthog_team_7"}
	cs := &ConfigStore{snapshot: &Snapshot{
		Orgs: map[string]*OrgConfig{
			"acme": {
				Teams: []OrgTeamConfig{{
					TeamID:     teamID,
					SchemaName: "team_7",
					Enabled:    true,
				}},
			},
		},
		OrgUserAccess: map[OrgUserKey]OrgUserAccessConfig{
			key: {Mode: OrgUserAccessModeProjectReader, TeamID: &teamID},
		},
	}}

	got, ok := cs.OrgUserQueryAccess("acme", "posthog_team_7")
	if !ok {
		t.Fatal("expected a project reader policy")
	}
	if len(got.AllowedRelations) != 0 {
		t.Fatalf("AllowedRelations = %v, want none", got.AllowedRelations)
	}
}

func TestOrgUserQueryAccessFailsClosedForMissingOrDisabledTeam(t *testing.T) {
	teamID := int64(42)
	key := OrgUserKey{OrgID: "acme", Username: "posthog_team_42"}
	cs := &ConfigStore{snapshot: &Snapshot{
		Orgs: map[string]*OrgConfig{"acme": {Teams: []OrgTeamConfig{{TeamID: teamID, SchemaName: "team_42"}}}},
		OrgUserAccess: map[OrgUserKey]OrgUserAccessConfig{
			key: {Mode: OrgUserAccessModeProjectReader, TeamID: &teamID},
		},
	}}

	got, ok := cs.OrgUserQueryAccess("acme", "posthog_team_42")
	if !ok || !got.ReadOnly || len(got.AllowedSchemas) != 0 || len(got.AllowedRelations) != 0 {
		t.Fatalf("disabled team policy must deny all project relations: %#v, ok=%v", got, ok)
	}
}

func TestOrgUserSessionQueryAccessDistinguishesUnrestrictedFromRevoked(t *testing.T) {
	unrestricted := OrgUserKey{OrgID: "acme", Username: "root"}
	disabled := OrgUserKey{OrgID: "acme", Username: "disabled"}
	cs := &ConfigStore{snapshot: &Snapshot{
		OrgUserPassword: map[OrgUserKey]string{unrestricted: "hash", disabled: "hash"},
		OrgUserRevision: map[OrgUserKey]string{unrestricted: "revision-1", disabled: "revision-1"},
		OrgUserDisabled: map[OrgUserKey]bool{disabled: true},
		OrgUserAccess:   map[OrgUserKey]OrgUserAccessConfig{},
	}}

	policy, revision, ok := cs.OrgUserSessionQueryAccess("acme", "root")
	if !ok || policy != nil || revision == "" {
		t.Fatalf("unrestricted user = (%#v, %q, %v), want (nil, non-empty, true)", policy, revision, ok)
	}
	cs.snapshot.OrgUserRevision[unrestricted] = "revision-2"
	_, rotatedRevision, ok := cs.OrgUserSessionQueryAccess("acme", "root")
	if !ok || rotatedRevision == "" || rotatedRevision == revision {
		t.Fatalf("password rotation did not change credential revision: before=%q after=%q", revision, rotatedRevision)
	}
	for _, username := range []string{"disabled", "missing"} {
		if policy, revision, ok := cs.OrgUserSessionQueryAccess("acme", username); ok || policy != nil || revision != "" {
			t.Fatalf("revoked user %q = (%#v, %q, %v), want (nil, empty, false)", username, policy, revision, ok)
		}
	}
}
