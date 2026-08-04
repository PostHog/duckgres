//go:build linux || darwin

package configstore_test

import (
	"testing"
	"time"
)

func TestWorkerSpawnLogHeadroomStatsPostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)

	// Empty log: zero burst, zero size (the controller falls back).
	stats, err := store.HeadroomSpawnStats(time.Hour, 5*time.Minute, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("HeadroomSpawnStats on empty log: %v", err)
	}
	if stats.PeakBurst != 0 || stats.MaxCPUMillis != 0 || stats.MaxMemBytes != 0 {
		t.Fatalf("empty log stats = %+v, want zeros", stats)
	}

	if err := store.RecordWorkerSpawn("org-a", 15000, 120<<30); err != nil {
		t.Fatalf("RecordWorkerSpawn: %v", err)
	}

	// Seed controlled timestamps directly (RecordWorkerSpawn stamps now()):
	// a 3-spawn burst 10 minutes ago, a lone spawn 40 minutes ago, a large
	// old spawn outside the burst window but inside the sizing window, and
	// one ancient row beyond both windows.
	seed := func(orgID string, cpuMillis, memBytes int64, age time.Duration) {
		t.Helper()
		if err := store.DB().Exec(
			`INSERT INTO duckgres_worker_spawn_log (org_id, cpu_millis, mem_bytes, spawned_at) VALUES (?, ?, ?, now() - make_interval(secs => ?))`,
			orgID, cpuMillis, memBytes, age.Seconds(),
		).Error; err != nil {
			t.Fatalf("seed spawn row: %v", err)
		}
	}
	// Identical timestamps so the burst provably shares one epoch bucket
	// (staggered ones could straddle a 5-minute boundary and flake).
	seed("org-a", 15000, 120<<30, 10*time.Minute)
	seed("org-a", 15000, 120<<30, 10*time.Minute)
	seed("org-b", 8000, 16<<30, 10*time.Minute)
	seed("org-b", 8000, 16<<30, 40*time.Minute)
	seed("org-c", 32000, 240<<30, 3*time.Hour) // sizing window only
	seed("org-d", 64000, 500<<30, 8*24*time.Hour)

	stats, err = store.HeadroomSpawnStats(time.Hour, 5*time.Minute, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("HeadroomSpawnStats: %v", err)
	}
	// Burst: the three seeded spawns 10min ago share one 5-minute bucket (the
	// now() row and the 40min row are in other buckets).
	if stats.PeakBurst != 3 {
		t.Fatalf("PeakBurst = %d, want 3", stats.PeakBurst)
	}
	// Size: componentwise max over the 7d window includes the 3h-old 32c/240Gi
	// spawn but not the 8d-old 64c/500Gi one.
	if stats.MaxCPUMillis != 32000 {
		t.Fatalf("MaxCPUMillis = %d, want 32000", stats.MaxCPUMillis)
	}
	if want := int64(240 << 30); stats.MaxMemBytes != want {
		t.Fatalf("MaxMemBytes = %d, want %d", stats.MaxMemBytes, want)
	}

	// Prune drops only rows older than the cutoff.
	dropped, err := store.PruneWorkerSpawnLog(time.Now().Add(-7 * 24 * time.Hour))
	if err != nil {
		t.Fatalf("PruneWorkerSpawnLog: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("pruned %d rows, want 1 (the 8d-old row)", dropped)
	}
	var remaining int64
	if err := store.DB().Raw(`SELECT COUNT(*) FROM duckgres_worker_spawn_log`).Scan(&remaining).Error; err != nil {
		t.Fatalf("count remaining spawn rows: %v", err)
	}
	if remaining != 6 {
		t.Fatalf("remaining spawn rows = %d, want 6", remaining)
	}
}
