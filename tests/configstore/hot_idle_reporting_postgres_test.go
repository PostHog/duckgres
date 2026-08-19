//go:build linux || darwin

package configstore_test

import (
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Hot-idle reporting + the cap sweep's listing, against the real migrated
// schema: per-org hot-idle counts with resolved worker shapes for the admin
// dashboard, and the oldest-first ordering the cap reaper relies on to retire
// the longest-parked excess first.
func TestHotIdleReportingPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedOrg(t, store, "acme")
	seedOrg(t, store, "globex")
	if err := store.DB().Exec(`UPDATE duckgres_orgs SET default_worker_cpu = '2', default_worker_memory = '8Gi' WHERE name = 'acme'`).Error; err != nil {
		t.Fatalf("set acme default profile: %v", err)
	}

	now := time.Now().UTC()
	ago := func(d time.Duration) *time.Time { t := now.Add(-d); return &t }
	records := []configstore.WorkerRecord{
		// acme: three hot-idle workers — one with an explicit profile, two
		// falling back to the org default — plus a Hot one (not idle, must
		// not count).
		{WorkerID: 2101, PodName: "w-acme-1", Image: "img", State: configstore.WorkerStateHotIdle, OrgID: "acme", OwnerCPInstanceID: "cp-a", HotIdleSince: ago(3 * time.Hour), ProfileCPU: "4", ProfileMemory: "16Gi", LastHeartbeatAt: now},
		{WorkerID: 2102, PodName: "w-acme-2", Image: "img", State: configstore.WorkerStateHotIdle, OrgID: "acme", OwnerCPInstanceID: "cp-a", HotIdleSince: ago(1 * time.Hour), LastHeartbeatAt: now},
		{WorkerID: 2103, PodName: "w-acme-3", Image: "img", State: configstore.WorkerStateHotIdle, OrgID: "acme", OwnerCPInstanceID: "cp-a", HotIdleSince: ago(2 * time.Hour), LastHeartbeatAt: now},
		{WorkerID: 2104, PodName: "w-acme-4", Image: "img", State: configstore.WorkerStateHot, OrgID: "acme", OwnerCPInstanceID: "cp-a", LastHeartbeatAt: now},
		// globex: one hot-idle worker.
		{WorkerID: 2105, PodName: "w-globex-1", Image: "img", State: configstore.WorkerStateHotIdle, OrgID: "globex", OwnerCPInstanceID: "cp-a", HotIdleSince: ago(30 * time.Minute), ProfileCPU: "1", ProfileMemory: "4Gi", LastHeartbeatAt: now},
	}
	for _, r := range records {
		r := r
		if err := store.UpsertWorkerRecord(&r); err != nil {
			t.Fatalf("upsert worker %d: %v", r.WorkerID, err)
		}
	}

	// ---- per-org reporting ----
	stats, err := store.ListHotIdleByOrg()
	if err != nil {
		t.Fatalf("ListHotIdleByOrg: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("want 2 orgs with hot-idle workers, got %d: %+v", len(stats), stats)
	}
	byOrg := map[string]configstore.HotIdleOrgStat{}
	for _, s := range stats {
		byOrg[s.OrgID] = s
	}
	acme, ok := byOrg["acme"]
	if !ok || acme.Count != 3 {
		t.Fatalf("acme stat wrong (Hot worker must not count): %+v", acme)
	}
	// CPU: 4 (explicit) + 2 + 2 (org default) = 8 cores; mem: 16 + 8 + 8 = 32 Gi.
	if acme.CPUCores != 8 {
		t.Fatalf("acme cpu = %v, want 8 (explicit profile + org-default fallback)", acme.CPUCores)
	}
	if acme.MemoryBytes != 32*1024*1024*1024 {
		t.Fatalf("acme mem = %v, want 32Gi", acme.MemoryBytes)
	}
	if acme.OldestHotIdleSince == nil || acme.OldestHotIdleSince.After(now.Add(-2*time.Hour)) {
		t.Fatalf("acme oldest since = %v, want ~3h ago", acme.OldestHotIdleSince)
	}
	globex, ok := byOrg["globex"]
	if !ok || globex.Count != 1 || globex.CPUCores != 1 {
		t.Fatalf("globex stat wrong: %+v", globex)
	}

	// ---- cap sweep listing: one org, oldest first ----
	snaps, err := store.ListOrgHotIdleSnapshots("acme")
	if err != nil {
		t.Fatalf("ListOrgHotIdleSnapshots: %v", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("want 3 acme hot-idle snapshots, got %d", len(snaps))
	}
	// Oldest (3h ago = worker 2101) first — the cap reaper retires the prefix.
	if snaps[0].WorkerID() != 2101 || snaps[1].WorkerID() != 2103 || snaps[2].WorkerID() != 2102 {
		t.Fatalf("not ordered oldest-first: %d %d %d", snaps[0].WorkerID(), snaps[1].WorkerID(), snaps[2].WorkerID())
	}
	// An org with no hot-idle workers returns empty, not error.
	snaps, err = store.ListOrgHotIdleSnapshots("no-such-org")
	if err != nil || len(snaps) != 0 {
		t.Fatalf("unknown org: snaps=%d err=%v", len(snaps), err)
	}
}
