package configstore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// ComputeUsageDelta is one accumulator's compute-usage for a single billing
// key + time-bucket, ready to be flushed into the durable buffer. Counts are
// whole vCPU-seconds / GiB-seconds (the in-process counter rounds millicore-/
// MiB-seconds down to whole units at flush time, carrying the remainder). The
// worker size travels in milli-units (millicores, MiB) and is stored as exact
// NUMERIC decimals (vCPU / GiB), so fractional sizes group exactly with no
// float-equality issues.
type ComputeUsageDelta struct {
	OrgID         string
	TeamID        string
	QuerySource   string
	Millicores    int64
	MiB           int64
	BucketStart   time.Time
	CPUSeconds    int64
	MemorySeconds int64
}

// ComputeUsageRow is one aggregated row served by the billing pull API: the
// sum of every closed bucket for one key on one UTC day. CPU and MemGiB are
// the exact NUMERIC decimal texts (e.g. "8", "1.5", "0.5") carried as
// json.Number so they serialize as unquoted JSON numbers with full precision.
type ComputeUsageRow struct {
	Date          string      `json:"date"`
	OrgID         string      `json:"org_id"`
	TeamID        string      `json:"team_id"`
	QuerySource   string      `json:"query_source"`
	CPU           json.Number `json:"cpu"`
	MemGiB        json.Number `json:"mem_gib"`
	CPUSeconds    int64       `json:"cpu_seconds"`
	MemorySeconds int64       `json:"memory_seconds"`
}

// milliUnitDecimal renders value/denom as an exact decimal string with no
// trailing zeros, using integer math only. denom must divide a power of 10
// (1000 and 1024 both do), so the result is always finite: 1500/1000 → "1.5",
// 512/1024 → "0.5", 8000/1000 → "8". Used to feed worker sizes into the
// NUMERIC key columns in a canonical text form.
func milliUnitDecimal(value, denom int64) string {
	if value <= 0 || denom <= 0 {
		return "0"
	}
	whole := value / denom
	rem := value % denom
	if rem == 0 {
		return fmt.Sprintf("%d", whole)
	}
	// 10^10 is divisible by both 1000 and 1024·5^10 — scale the remainder to a
	// fixed 10 decimal digits, then trim trailing zeros.
	const scale = int64(10_000_000_000) // 10^10
	frac := rem * (scale / denom)
	digits := strings.TrimRight(fmt.Sprintf("%010d", frac), "0")
	return fmt.Sprintf("%d.%s", whole, digits)
}

// FlushComputeUsage applies a batch of per-key deltas to the durable buffer
// via UPSERT-increment, so contributions from every CP pod sum into the same
// row. Best-effort by contract — the caller logs and drops on error; a flush
// failure must never reach the request/teardown path.
func (cs *ConfigStore) FlushComputeUsage(deltas []ComputeUsageDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	return cs.db.Transaction(func(tx *gorm.DB) error {
		const stmt = `
INSERT INTO duckgres_org_compute_usage (org_id, team_id, query_source, cpu, mem_gib, bucket_start, cpu_seconds, memory_seconds)
VALUES (?, ?, ?, ?::numeric, ?::numeric, ?, ?, ?)
ON CONFLICT (org_id, team_id, query_source, cpu, mem_gib, bucket_start)
DO UPDATE SET cpu_seconds    = duckgres_org_compute_usage.cpu_seconds    + EXCLUDED.cpu_seconds,
              memory_seconds = duckgres_org_compute_usage.memory_seconds + EXCLUDED.memory_seconds`
		for _, d := range deltas {
			if d.CPUSeconds == 0 && d.MemorySeconds == 0 {
				continue
			}
			cpu := milliUnitDecimal(d.Millicores, 1000)
			mem := milliUnitDecimal(d.MiB, 1024)
			if err := tx.Exec(stmt, d.OrgID, d.TeamID, d.QuerySource, cpu, mem, d.BucketStart.UTC(), d.CPUSeconds, d.MemorySeconds).Error; err != nil {
				return fmt.Errorf("upsert compute usage (org=%s bucket=%s): %w", d.OrgID, d.BucketStart, err)
			}
		}
		return nil
	})
}

// AggregateComputeUsage sums every buffered bucket in the half-open window
// (low, high] into one row per key per UTC day, for the billing pull API.
// Callers pass low = the ack cursor and high = the newest closed bucket, so a
// minute that is still accumulating is never served. The NUMERIC size columns
// are canonical decimal texts on insert, so ::text round-trips them exactly.
func (cs *ConfigStore) AggregateComputeUsage(low, high time.Time) ([]ComputeUsageRow, error) {
	const q = `
SELECT to_char((bucket_start AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS date,
       org_id, team_id, query_source, cpu::text, mem_gib::text,
       SUM(cpu_seconds), SUM(memory_seconds)
FROM duckgres_org_compute_usage
WHERE bucket_start > ? AND bucket_start <= ?
GROUP BY 1, org_id, team_id, query_source, cpu, mem_gib
ORDER BY 1, org_id, team_id, query_source, cpu, mem_gib`

	rows, err := cs.db.Raw(q, low.UTC(), high.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate compute usage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ComputeUsageRow
	for rows.Next() {
		var r ComputeUsageRow
		var cpu, mem string
		if err := rows.Scan(&r.Date, &r.OrgID, &r.TeamID, &r.QuerySource, &cpu, &mem, &r.CPUSeconds, &r.MemorySeconds); err != nil {
			return nil, fmt.Errorf("scan compute usage row: %w", err)
		}
		r.CPU, r.MemGiB = json.Number(cpu), json.Number(mem)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ComputeBillingCursor returns the ack cursor (the watermark billing last
// acked). ok=false when no ack has ever happened — the pull window then
// starts at the epoch (i.e. "everything buffered").
func (cs *ConfigStore) ComputeBillingCursor() (cursor time.Time, ok bool, err error) {
	row := cs.db.Raw(`SELECT last_acked FROM duckgres_compute_billing_cursor WHERE id = 1`).Row()
	var t time.Time
	if scanErr := row.Scan(&t); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("read compute billing cursor: %w", scanErr)
	}
	return t.UTC(), true, nil
}

// AckComputeUsage commits billing's watermark: it advances the cursor to
// watermarkHigh (monotonically — an older value never moves it backwards) and
// deletes every buffered bucket at or below it, atomically. Idempotent: a
// retried ack at or below the current cursor deletes nothing and leaves the
// cursor unchanged. Returns the number of buffer rows deleted.
func (cs *ConfigStore) AckComputeUsage(watermarkHigh time.Time) (int64, error) {
	high := watermarkHigh.UTC()
	var deleted int64
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		const upsert = `
INSERT INTO duckgres_compute_billing_cursor (id, last_acked)
VALUES (1, ?)
ON CONFLICT (id)
DO UPDATE SET last_acked = GREATEST(duckgres_compute_billing_cursor.last_acked, EXCLUDED.last_acked)`
		if err := tx.Exec(upsert, high).Error; err != nil {
			return fmt.Errorf("advance compute billing cursor: %w", err)
		}
		res := tx.Exec(`DELETE FROM duckgres_org_compute_usage WHERE bucket_start <= ?`, high)
		if res.Error != nil {
			return fmt.Errorf("delete acked compute buckets: %w", res.Error)
		}
		deleted = res.RowsAffected
		return nil
	})
	return deleted, err
}

// GCComputeUsage hard-deletes buffered buckets older than the cutoff
// regardless of ack — the safety net that bounds table growth if billing
// stops pulling entirely. Returns the number of rows dropped; a nonzero
// count means billing is not keeping up (the caller alerts on it).
func (cs *ConfigStore) GCComputeUsage(olderThan time.Time) (int64, error) {
	res := cs.db.Exec(`DELETE FROM duckgres_org_compute_usage WHERE bucket_start < ?`, olderThan.UTC())
	if res.Error != nil {
		return 0, fmt.Errorf("gc compute usage: %w", res.Error)
	}
	return res.RowsAffected, nil
}
