package configstore

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// StorageUsageRow is one aggregated storage row served by the billing pull
// API: the sum of every closed bucket for one (org, team) on one UTC day,
// exposed as exact-decimal GiB-seconds (byte-seconds / 2^30 — a finite
// decimal). See docs/design/billing-pull-api.md "Storage metric".
type StorageUsageRow struct {
	Date       string      `json:"date"`
	OrgID      string      `json:"org_id"`
	TeamID     int64       `json:"team_id"`
	GiBSeconds json.Number `json:"gib_seconds"`
}

// UpsertStorageSample credits one sampler observation — tracked_bytes ×
// interval_seconds byte-seconds — into the (org, team, bucket) row. Additive
// UPSERT so retried/overlapping writers sum rather than clobber. Best-effort
// by contract: the sampler logs and skips on error, never fails anything.
func (cs *ConfigStore) UpsertStorageSample(orgID string, teamID int64, bucketStart time.Time, byteSeconds int64) error {
	if byteSeconds <= 0 {
		return nil
	}
	const stmt = `
INSERT INTO duckgres_org_storage_usage (org_id, team_id, bucket_start, byte_seconds)
VALUES (?, ?, ?, ?)
ON CONFLICT (org_id, team_id, bucket_start)
DO UPDATE SET byte_seconds = duckgres_org_storage_usage.byte_seconds + EXCLUDED.byte_seconds`
	if err := cs.db.Exec(stmt, orgID, teamID, bucketStart.UTC(), byteSeconds).Error; err != nil {
		return fmt.Errorf("upsert storage sample (org=%s bucket=%s): %w", orgID, bucketStart, err)
	}
	return nil
}

// AggregateStorageUsage sums every buffered storage bucket in the half-open
// window (low, high] into one row per (org, team) per UTC day — the storage
// half of the billing pull response, over the same watermark window as
// compute. byte-seconds are summed in Postgres (NUMERIC, exact) and converted
// to GiB-seconds in Go with integer math so no precision is lost.
func (cs *ConfigStore) AggregateStorageUsage(low, high time.Time) ([]StorageUsageRow, error) {
	const q = `
SELECT to_char((bucket_start AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS date,
       org_id, team_id, SUM(byte_seconds)::text
FROM duckgres_org_storage_usage
WHERE bucket_start > ? AND bucket_start <= ?
GROUP BY 1, org_id, team_id
ORDER BY 1, org_id, team_id`

	rows, err := cs.db.Raw(q, low.UTC(), high.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate storage usage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []StorageUsageRow
	for rows.Next() {
		var r StorageUsageRow
		var byteSeconds string
		if err := rows.Scan(&r.Date, &r.OrgID, &r.TeamID, &byteSeconds); err != nil {
			return nil, fmt.Errorf("scan storage usage row: %w", err)
		}
		gib, err := byteSecondsToGiBSeconds(byteSeconds)
		if err != nil {
			return nil, fmt.Errorf("convert storage usage row (org=%s date=%s): %w", r.OrgID, r.Date, err)
		}
		r.GiBSeconds = gib
		out = append(out, r)
	}
	return out, rows.Err()
}

// byteSecondsToGiBSeconds divides an integer byte-seconds decimal string by
// 2^30, exactly. 1/2^30 = 5^30/10^30, so the result always terminates within
// 30 fractional digits — computed with big.Int (values exceed float64 AND
// int64 range for large warehouses), then trailing zeros trimmed.
func byteSecondsToGiBSeconds(byteSeconds string) (json.Number, error) {
	v, ok := new(big.Int).SetString(strings.TrimSpace(byteSeconds), 10)
	if !ok {
		return "", fmt.Errorf("non-integer byte_seconds %q", byteSeconds)
	}
	neg := v.Sign() < 0
	if neg {
		v.Neg(v) // defensive; the sampler never writes negatives
	}
	five30 := new(big.Int).Exp(big.NewInt(5), big.NewInt(30), nil)
	v.Mul(v, five30) // v / 2^30 == v·5^30 / 10^30
	digits := v.String()
	if len(digits) <= 30 {
		digits = strings.Repeat("0", 30-len(digits)+1) + digits
	}
	intPart := digits[:len(digits)-30]
	frac := strings.TrimRight(digits[len(digits)-30:], "0")
	out := intPart
	if frac != "" {
		out += "." + frac
	}
	if neg {
		out = "-" + out
	}
	return json.Number(out), nil
}

// GCStorageUsage hard-deletes buffered storage buckets older than the cutoff
// regardless of ack — same safety net as GCComputeUsage. Returns the number
// of rows dropped.
func (cs *ConfigStore) GCStorageUsage(olderThan time.Time) (int64, error) {
	res := cs.db.Exec(`DELETE FROM duckgres_org_storage_usage WHERE bucket_start < ?`, olderThan.UTC())
	if res.Error != nil {
		return 0, fmt.Errorf("gc storage usage: %w", res.Error)
	}
	return res.RowsAffected, nil
}
