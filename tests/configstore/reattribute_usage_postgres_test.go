//go:build linux || darwin

package configstore_test

import (
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

type reattrComputeRow struct {
	TeamID        int64
	QuerySource   string
	BucketStart   time.Time
	CPUSeconds    int64
	MemorySeconds int64
}

type reattrStorageRow struct {
	TeamID      int64
	BucketStart time.Time
	ByteSeconds int64
}

func computeRowsForOrg(t *testing.T, store *configstore.ConfigStore, orgID string) []reattrComputeRow {
	t.Helper()
	var rows []reattrComputeRow
	if err := store.DB().Raw(`
SELECT team_id, query_source, bucket_start, cpu_seconds, memory_seconds
FROM duckgres_org_compute_usage WHERE org_id = ?
ORDER BY team_id, query_source, bucket_start`, orgID).Scan(&rows).Error; err != nil {
		t.Fatalf("read compute rows for %s: %v", orgID, err)
	}
	return rows
}

func storageRowsForOrg(t *testing.T, store *configstore.ConfigStore, orgID string) []reattrStorageRow {
	t.Helper()
	var rows []reattrStorageRow
	if err := store.DB().Raw(`
SELECT team_id, bucket_start, byte_seconds
FROM duckgres_org_storage_usage WHERE org_id = ?
ORDER BY team_id, bucket_start`, orgID).Scan(&rows).Error; err != nil {
		t.Fatalf("read storage rows for %s: %v", orgID, err)
	}
	return rows
}

func seedUsage(t *testing.T, store *configstore.ConfigStore, orgID string, teamID int64, bucket time.Time, cpuSeconds, memSeconds, byteSeconds int64) {
	t.Helper()
	if err := store.FlushComputeUsage([]configstore.ComputeUsageDelta{{
		OrgID:         orgID,
		TeamID:        teamID,
		QuerySource:   "standard",
		Millicores:    2000,
		MiB:           4096,
		BucketStart:   bucket,
		CPUSeconds:    cpuSeconds,
		MemorySeconds: memSeconds,
	}}); err != nil {
		t.Fatalf("seed compute usage: %v", err)
	}
	if err := store.UpsertStorageSample(orgID, teamID, bucket, byteSeconds); err != nil {
		t.Fatalf("seed storage usage: %v", err)
	}
}

// TestReattributeUsageTeamPostgres covers the billing-bucket re-attribution
// that runs when an org's default_team_id changes: every buffered (unacked)
// bucket in BOTH usage tables moves to the new team, colliding rows (a target
// key that already exists — e.g. a stale-snapshot straggler after an earlier
// flip) fold additively instead of violating the PK, and other orgs' buckets
// are untouched.
func TestReattributeUsageTeamPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	b1 := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	b2 := b1.Add(time.Minute)

	// acme has buckets under the old team 1 at b1+b2, plus a pre-existing
	// team-2 bucket at b2 (the collision case: same key once team 1 → 2).
	seedUsage(t, store, "acme", 1, b1, 10, 20, 1000)
	seedUsage(t, store, "acme", 1, b2, 30, 60, 2000)
	seedUsage(t, store, "acme", 2, b2, 5, 7, 500)
	// A second org under team 1 must not be touched.
	seedUsage(t, store, "other", 1, b1, 100, 200, 999)

	moved, err := store.ReattributeUsageTeam("acme", 2)
	if err != nil {
		t.Fatalf("ReattributeUsageTeam: %v", err)
	}
	// 2 compute rows + 2 storage rows moved off team 1.
	if moved != 4 {
		t.Fatalf("moved = %d, want 4", moved)
	}

	compute := computeRowsForOrg(t, store, "acme")
	if len(compute) != 2 {
		t.Fatalf("acme compute rows = %+v, want 2 rows all under team 2", compute)
	}
	for i, want := range []reattrComputeRow{
		{TeamID: 2, QuerySource: "standard", BucketStart: b1, CPUSeconds: 10, MemorySeconds: 20},
		// b2 folded: moved team-1 row summed into the pre-existing team-2 row.
		{TeamID: 2, QuerySource: "standard", BucketStart: b2, CPUSeconds: 35, MemorySeconds: 67},
	} {
		got := compute[i]
		if got.TeamID != want.TeamID || got.QuerySource != want.QuerySource ||
			!got.BucketStart.Equal(want.BucketStart) ||
			got.CPUSeconds != want.CPUSeconds || got.MemorySeconds != want.MemorySeconds {
			t.Fatalf("acme compute row %d = %+v, want %+v", i, got, want)
		}
	}

	storage := storageRowsForOrg(t, store, "acme")
	if len(storage) != 2 {
		t.Fatalf("acme storage rows = %+v, want 2 rows all under team 2", storage)
	}
	for i, want := range []reattrStorageRow{
		{TeamID: 2, BucketStart: b1, ByteSeconds: 1000},
		{TeamID: 2, BucketStart: b2, ByteSeconds: 2500}, // folded 2000 + 500
	} {
		got := storage[i]
		if got.TeamID != want.TeamID || !got.BucketStart.Equal(want.BucketStart) || got.ByteSeconds != want.ByteSeconds {
			t.Fatalf("acme storage row %d = %+v, want %+v", i, got, want)
		}
	}

	// Other orgs keep their team and values.
	otherCompute := computeRowsForOrg(t, store, "other")
	if len(otherCompute) != 1 || otherCompute[0].TeamID != 1 || otherCompute[0].CPUSeconds != 100 {
		t.Fatalf("other org compute rows mutated: %+v", otherCompute)
	}
	otherStorage := storageRowsForOrg(t, store, "other")
	if len(otherStorage) != 1 || otherStorage[0].TeamID != 1 || otherStorage[0].ByteSeconds != 999 {
		t.Fatalf("other org storage rows mutated: %+v", otherStorage)
	}

	// Re-running with the same team is a no-op (everything already there).
	moved, err = store.ReattributeUsageTeam("acme", 2)
	if err != nil {
		t.Fatalf("ReattributeUsageTeam (repeat): %v", err)
	}
	if moved != 0 {
		t.Fatalf("repeat moved = %d, want 0", moved)
	}
}

// TestProvisionReattributesUsageOnTeamChangePostgres: re-provisioning an
// EXISTING org with a different default_team_id must repoint the org's
// billing team row (duckgres_org_teams) AND re-attribute its buffered usage
// buckets to the new team, atomically.
func TestProvisionReattributesUsageOnTeamChangePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	oldTeam := int64(1)
	if err := store.DB().Create(&configstore.Org{Name: "reprov", DatabaseName: "reprovdb"}).Error; err != nil {
		t.Fatalf("create org: %v", err)
	}
	if err := configstore.SetOrgBillingTeamTx(store.DB(), "reprov", oldTeam); err != nil {
		t.Fatalf("seed billing team: %v", err)
	}
	bucket := time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	seedUsage(t, store, "reprov", oldTeam, bucket, 10, 20, 1000)

	pstore := provisioning.NewGormStore(store)
	if err := pstore.Provision(provisioning.ProvisionRequest{
		OrgID:         "reprov",
		DatabaseName:  "reprovdb",
		DefaultTeamID: 2,
		Warehouse:     &configstore.ManagedWarehouse{DucklingName: "reprov"},
		RootUserHash:  "hash",
	}); err != nil {
		t.Fatalf("re-provision with new team: %v", err)
	}

	org, err := pstore.GetOrg("reprov")
	if err != nil {
		t.Fatalf("read org: %v", err)
	}
	if org.DefaultTeamID == nil || *org.DefaultTeamID != 2 {
		t.Fatalf("org billing team = %v, want 2", org.DefaultTeamID)
	}
	// The old team row survives the flip, demoted; team 2 carries the
	// (single) billing mark and the conventional schema name.
	var teamRows []configstore.OrgTeam
	if err := store.DB().Where("org_id = ?", "reprov").Order("team_id").Find(&teamRows).Error; err != nil {
		t.Fatalf("read org team rows: %v", err)
	}
	if len(teamRows) != 2 || teamRows[0].TeamID != 1 || teamRows[1].TeamID != 2 {
		t.Fatalf("org team rows = %+v, want teams [1 2]", teamRows)
	}
	if teamRows[0].IsBillingTeam != nil && *teamRows[0].IsBillingTeam {
		t.Fatal("old team 1 must be demoted from billing")
	}
	if teamRows[1].IsBillingTeam == nil || !*teamRows[1].IsBillingTeam {
		t.Fatal("team 2 must carry the billing mark")
	}
	if teamRows[1].SchemaName != "team_2" {
		t.Fatalf("team 2 schema_name = %q, want team_2", teamRows[1].SchemaName)
	}

	compute := computeRowsForOrg(t, store, "reprov")
	if len(compute) != 1 || compute[0].TeamID != 2 || compute[0].CPUSeconds != 10 {
		t.Fatalf("compute rows not re-attributed to team 2: %+v", compute)
	}
	storage := storageRowsForOrg(t, store, "reprov")
	if len(storage) != 1 || storage[0].TeamID != 2 || storage[0].ByteSeconds != 1000 {
		t.Fatalf("storage rows not re-attributed to team 2: %+v", storage)
	}
}
