package configstore

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// ComputeUsageDelta is one org's accumulated compute-usage for a single
// time-bucket, ready to be flushed into the durable buffer. Counts are whole
// vCPU-seconds / GiB-seconds (the in-process counter rounds millicore-/MiB-
// seconds down to whole units at flush time).
type ComputeUsageDelta struct {
	OrgID         string
	BucketStart   time.Time
	CPUSeconds    int64
	MemorySeconds int64
}

// ComputeUsageBucket is one durable buffer row read back for draining.
type ComputeUsageBucket struct {
	OrgID         string
	BucketStart   time.Time
	CPUSeconds    int64
	MemorySeconds int64
}

// FlushComputeUsage applies a batch of per-(org, bucket) deltas to the durable
// buffer via UPSERT-increment, so contributions from every CP pod sum into the
// same row. Best-effort by contract — the caller logs and drops on error; a
// flush failure must never reach the request/teardown path.
func (cs *ConfigStore) FlushComputeUsage(deltas []ComputeUsageDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	return cs.db.Transaction(func(tx *gorm.DB) error {
		const stmt = `
INSERT INTO duckgres_org_compute_usage (org_id, bucket_start, cpu_seconds, memory_seconds)
VALUES (?, ?, ?, ?)
ON CONFLICT (org_id, bucket_start)
DO UPDATE SET cpu_seconds    = duckgres_org_compute_usage.cpu_seconds    + EXCLUDED.cpu_seconds,
              memory_seconds = duckgres_org_compute_usage.memory_seconds + EXCLUDED.memory_seconds`
		for _, d := range deltas {
			if d.CPUSeconds == 0 && d.MemorySeconds == 0 {
				continue
			}
			if err := tx.Exec(stmt, d.OrgID, d.BucketStart.UTC(), d.CPUSeconds, d.MemorySeconds).Error; err != nil {
				return fmt.Errorf("upsert compute usage (org=%s bucket=%s): %w", d.OrgID, d.BucketStart, err)
			}
		}
		return nil
	})
}

// ListDrainableComputeBuckets returns every closed, not-yet-drained buffer row,
// oldest first. A bucket is drainable when it is closed (bucket_start <=
// closedBefore, i.e. now - width - grace) and strictly newer than the org's
// high-water mark (so a late re-INSERT of an already-shipped bucket is skipped).
// Ordering by bucket_start lets the drainer advance the per-org high-water mark
// monotonically.
func (cs *ConfigStore) ListDrainableComputeBuckets(closedBefore time.Time) ([]ComputeUsageBucket, error) {
	const q = `
SELECT u.org_id, u.bucket_start, u.cpu_seconds, u.memory_seconds
FROM duckgres_org_compute_usage u
LEFT JOIN duckgres_org_compute_drain_state s ON s.org_id = u.org_id
WHERE u.bucket_start <= ?
  AND (s.last_drained_bucket IS NULL OR u.bucket_start > s.last_drained_bucket)
ORDER BY u.org_id, u.bucket_start`

	rows, err := cs.db.Raw(q, closedBefore.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("list drainable compute buckets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ComputeUsageBucket
	for rows.Next() {
		var b ComputeUsageBucket
		if err := rows.Scan(&b.OrgID, &b.BucketStart, &b.CPUSeconds, &b.MemorySeconds); err != nil {
			return nil, fmt.Errorf("scan drainable compute bucket: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// MarkComputeBucketDrained commits the ship-then-delete step for one bucket
// after ingestion confirms success: it advances the org's high-water mark to
// this bucket and deletes the buffer row, atomically. Called ONLY after a 2xx
// from ingestion.
func (cs *ConfigStore) MarkComputeBucketDrained(orgID string, bucketStart time.Time) error {
	bucket := bucketStart.UTC()
	return cs.db.Transaction(func(tx *gorm.DB) error {
		const upsertHW = `
INSERT INTO duckgres_org_compute_drain_state (org_id, last_drained_bucket)
VALUES (?, ?)
ON CONFLICT (org_id)
DO UPDATE SET last_drained_bucket = GREATEST(duckgres_org_compute_drain_state.last_drained_bucket, EXCLUDED.last_drained_bucket)`
		if err := tx.Exec(upsertHW, orgID, bucket).Error; err != nil {
			return fmt.Errorf("advance compute drain high-water (org=%s): %w", orgID, err)
		}
		const del = `DELETE FROM duckgres_org_compute_usage WHERE org_id = ? AND bucket_start = ?`
		if err := tx.Exec(del, orgID, bucket).Error; err != nil {
			return fmt.Errorf("delete drained compute bucket (org=%s): %w", orgID, err)
		}
		return nil
	})
}

// SweepDrainedComputeBuckets hard-deletes any lingering buffer rows at or below
// each org's high-water mark — the safety net for a late re-INSERT of an
// already-shipped bucket (a CP pod that stalled past the grace window). Returns
// the number of rows removed.
func (cs *ConfigStore) SweepDrainedComputeBuckets() (int64, error) {
	const del = `
DELETE FROM duckgres_org_compute_usage u
USING duckgres_org_compute_drain_state s
WHERE s.org_id = u.org_id
  AND u.bucket_start <= s.last_drained_bucket`
	res := cs.db.Exec(del)
	if res.Error != nil {
		return 0, fmt.Errorf("sweep drained compute buckets: %w", res.Error)
	}
	return res.RowsAffected, nil
}
