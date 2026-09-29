//go:build kubernetes

package controlplane

import (
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoUnconfiguredOwnerCountDoesNotChangeAssignments(t *testing.T) {
	cells := []trinoCell{{ID: "registered:pool-a"}, {ID: "registered:pool-b"}}
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "tenant-a", CellID: "registered:pool-a"},
		{OrgID: "tenant-b", CellID: "registered:pool-b"},
		{OrgID: "tenant-c", CellID: "removed-owner"},
		{OrgID: "tenant-d"},
	}
	if got := unconfiguredTrinoOwnerCount(cells, orgs); got != 2 {
		t.Fatalf("unknown owner count = %d, want 2", got)
	}
	if orgs[2].CellID != "removed-owner" || orgs[3].CellID != "" {
		t.Fatal("ownership audit mutated persisted assignments")
	}
}

func TestTrinoSharedPoolNeedsNoLegacyConfiguration(t *testing.T) {
	t.Setenv(envTrinoCellsFile, sharedPoolRegistry(t, blueprintFile(t)))
	t.Setenv("DUCKGRES_TRINO_REGISTRY_ONLY", "")
	t.Setenv("DUCKGRES_TRINO_COORDINATOR_URL", "")
	cells, _, err := resolveTrinoCells()
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 1 || cells[0].ID != "registered:cell-001" {
		t.Fatalf("unexpected pool identity: %+v", cells)
	}
}

func TestTrinoRejectsRemovedStaticTopology(t *testing.T) {
	if _, err := parseTrinoCellRegistry([]byte(testTrinoRegistryJSON)); err == nil {
		t.Fatal("removed fixed topology was accepted")
	}
}
