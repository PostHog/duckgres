package configstore

import (
	"fmt"
	"time"
)

// HeadroomSpawnStats is what the dynamic headroom controller needs from the
// spawn log: how bursty spawning has been recently (peak count in any one
// bucket of the burst window) and the largest worker shape spawned within the
// sizing window (componentwise — the CPU max and memory max may come from
// different spawns).
type HeadroomSpawnStats struct {
	PeakBurst    int   // max spawns in any single bucket of the burst window
	MaxCPUMillis int64 // largest spawned CPU request in the sizing window; 0 = no data
	MaxMemBytes  int64 // largest spawned memory request in the sizing window; 0 = no data
}

// RecordWorkerSpawn appends one row to the worker spawn log. Called
// best-effort at pod create — the caller logs and drops on error; a log
// failure must never fail the spawn.
func (cs *ConfigStore) RecordWorkerSpawn(orgID string, cpuMillis, memBytes int64) error {
	err := cs.db.Exec(
		`INSERT INTO duckgres_worker_spawn_log (org_id, cpu_millis, mem_bytes) VALUES (?, ?, ?)`,
		orgID, cpuMillis, memBytes,
	).Error
	if err != nil {
		return fmt.Errorf("record worker spawn: %w", err)
	}
	return nil
}

// HeadroomSpawnStats aggregates the spawn log for the headroom controller.
// PeakBurst groups spawns of the last burstWindow into fixed bucket-sized
// bins; MaxCPUMillis/MaxMemBytes are the componentwise max over the (longer)
// sizeWindow, so slot sizing stays honest across quiet hours while the count
// reacts to the last hour only.
func (cs *ConfigStore) HeadroomSpawnStats(burstWindow, bucket, sizeWindow time.Duration) (HeadroomSpawnStats, error) {
	var stats HeadroomSpawnStats
	if burstWindow <= 0 || bucket <= 0 || sizeWindow <= 0 {
		return stats, fmt.Errorf("headroom spawn stats: non-positive window")
	}
	now := time.Now().UTC()

	const burstQ = `
SELECT COALESCE(MAX(c), 0) FROM (
    SELECT COUNT(*) AS c
    FROM duckgres_worker_spawn_log
    WHERE spawned_at > ?
    GROUP BY FLOOR(EXTRACT(EPOCH FROM spawned_at) / ?)
) buckets`
	row := cs.db.Raw(burstQ, now.Add(-burstWindow), int64(bucket.Seconds())).Row()
	if err := row.Scan(&stats.PeakBurst); err != nil {
		return HeadroomSpawnStats{}, fmt.Errorf("headroom spawn stats: peak burst: %w", err)
	}

	const sizeQ = `
SELECT COALESCE(MAX(cpu_millis), 0), COALESCE(MAX(mem_bytes), 0)
FROM duckgres_worker_spawn_log
WHERE spawned_at > ?`
	row = cs.db.Raw(sizeQ, now.Add(-sizeWindow)).Row()
	if err := row.Scan(&stats.MaxCPUMillis, &stats.MaxMemBytes); err != nil {
		return HeadroomSpawnStats{}, fmt.Errorf("headroom spawn stats: max size: %w", err)
	}
	return stats, nil
}

// PruneWorkerSpawnLog deletes spawn-log rows older than the cutoff (the
// sizing window). Called from the leader-only headroom reconcile; returns the
// number of rows dropped.
func (cs *ConfigStore) PruneWorkerSpawnLog(olderThan time.Time) (int64, error) {
	res := cs.db.Exec(`DELETE FROM duckgres_worker_spawn_log WHERE spawned_at < ?`, olderThan.UTC())
	if res.Error != nil {
		return 0, fmt.Errorf("prune worker spawn log: %w", res.Error)
	}
	return res.RowsAffected, nil
}
