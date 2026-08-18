package configstore

import (
	"encoding/json"
	"fmt"
	"time"
)

// MonthlyComputeUsageRow is the compute family of the admin console's monthly
// usage view: every buffered bucket on or after the window start summed into
// one row per UTC month per (org, team). Unlike the billing pull API's daily
// rows this collapses the query_source and worker-size key parts — the page
// answers "how much did each team burn this month", not billing's exact-key
// metering. SchemaName joins the team's schema for display (nil when the team
// row is gone or the defensive team_id=0 bucket is in play).
type MonthlyComputeUsageRow struct {
	Month         string  `json:"month"` // "YYYY-MM", UTC
	OrgID         string  `json:"org_id"`
	TeamID        int64   `json:"team_id"`
	SchemaName    *string `json:"schema_name"`
	CPUSeconds    int64   `json:"cpu_seconds"`
	MemorySeconds int64   `json:"memory_seconds"`
}

// MonthlyStorageUsageRow is the storage family of the monthly usage view: the
// month's byte-seconds per (org, team) as exact-decimal GiB-seconds (see
// byteSecondsToGiBSeconds).
type MonthlyStorageUsageRow struct {
	Month      string      `json:"month"`
	OrgID      string      `json:"org_id"`
	TeamID     int64       `json:"team_id"`
	SchemaName *string     `json:"schema_name"`
	GiBSeconds json.Number `json:"gib_seconds"`
}

// AggregateComputeUsageMonthly sums every buffered compute bucket at or after
// from into one row per UTC month per (org, team). The window is inclusive at
// the low end (unlike the billing pull API's (low, high]) because the caller
// passes a month boundary, not an ack cursor. Reads the SAME buffer the pull
// API serves — acked buckets are already deleted (AckComputeUsage) and buckets
// older than 30 days are GC'd, so this is a window over retained usage, not
// all-time history.
func (cs *ConfigStore) AggregateComputeUsageMonthly(from time.Time) ([]MonthlyComputeUsageRow, error) {
	const q = `
SELECT to_char(date_trunc('month', bucket_start AT TIME ZONE 'UTC'), 'YYYY-MM') AS month,
       u.org_id, u.team_id, t.schema_name,
       SUM(u.cpu_seconds), SUM(u.memory_seconds)
FROM duckgres_org_compute_usage u
LEFT JOIN duckgres_org_teams t ON t.org_id = u.org_id AND t.team_id = u.team_id
WHERE u.bucket_start >= ?
GROUP BY 1, u.org_id, u.team_id, t.schema_name
ORDER BY 1 DESC, u.org_id, u.team_id`

	rows, err := cs.db.Raw(q, from.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate monthly compute usage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []MonthlyComputeUsageRow
	for rows.Next() {
		var r MonthlyComputeUsageRow
		if err := rows.Scan(&r.Month, &r.OrgID, &r.TeamID, &r.SchemaName, &r.CPUSeconds, &r.MemorySeconds); err != nil {
			return nil, fmt.Errorf("scan monthly compute usage row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AggregateStorageUsageMonthly is the storage half of
// AggregateComputeUsageMonthly — same window, same grouping, byte-seconds
// converted to exact GiB-seconds.
func (cs *ConfigStore) AggregateStorageUsageMonthly(from time.Time) ([]MonthlyStorageUsageRow, error) {
	const q = `
SELECT to_char(date_trunc('month', bucket_start AT TIME ZONE 'UTC'), 'YYYY-MM') AS month,
       u.org_id, u.team_id, t.schema_name, SUM(u.byte_seconds)::text
FROM duckgres_org_storage_usage u
LEFT JOIN duckgres_org_teams t ON t.org_id = u.org_id AND t.team_id = u.team_id
WHERE u.bucket_start >= ?
GROUP BY 1, u.org_id, u.team_id, t.schema_name
ORDER BY 1 DESC, u.org_id, u.team_id`

	rows, err := cs.db.Raw(q, from.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate monthly storage usage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []MonthlyStorageUsageRow
	for rows.Next() {
		var r MonthlyStorageUsageRow
		var byteSeconds string
		if err := rows.Scan(&r.Month, &r.OrgID, &r.TeamID, &r.SchemaName, &byteSeconds); err != nil {
			return nil, fmt.Errorf("scan monthly storage usage row: %w", err)
		}
		gib, err := byteSecondsToGiBSeconds(byteSeconds)
		if err != nil {
			return nil, fmt.Errorf("convert monthly storage usage row (org=%s month=%s): %w", r.OrgID, r.Month, err)
		}
		r.GiBSeconds = gib
		out = append(out, r)
	}
	return out, rows.Err()
}
