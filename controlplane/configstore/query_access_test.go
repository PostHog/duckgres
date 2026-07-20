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
