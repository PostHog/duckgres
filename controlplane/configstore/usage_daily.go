package configstore

import (
	"encoding/json"
	"fmt"
	"time"
)

// DailyComputeUsageRow is the compute family of the org detail page's usage
// charts: one org's buffered buckets summed per UTC day per team. Same data
// source and retention caveats as AggregateComputeUsageMonthly — the buffer,
// not all-time history.
type DailyComputeUsageRow struct {
	Date          string  `json:"date"` // "YYYY-MM-DD", UTC
	TeamID        int64   `json:"team_id"`
	SchemaName    *string `json:"schema_name"`
	CPUSeconds    int64   `json:"cpu_seconds"`
	MemorySeconds int64   `json:"memory_seconds"`
}

// DailyStorageUsageRow is the storage family of the daily view: byte-seconds
// per UTC day per team as exact-decimal GiB-seconds.
type DailyStorageUsageRow struct {
	Date       string      `json:"date"`
	TeamID     int64       `json:"team_id"`
	SchemaName *string     `json:"schema_name"`
	GiBSeconds json.Number `json:"gib_seconds"`
}

// AggregateComputeUsageDaily sums one org's buffered compute buckets at or
// after from into one row per UTC day per team. The org filter is the query's
// WHERE clause — the caller's :id path segment flows straight here, so one
// org's usage can never leak into another org's page.
func (cs *ConfigStore) AggregateComputeUsageDaily(orgID string, from time.Time) ([]DailyComputeUsageRow, error) {
	const q = `
SELECT to_char((bucket_start AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS date,
       u.team_id, t.schema_name,
       SUM(u.cpu_seconds), SUM(u.memory_seconds)
FROM duckgres_org_compute_usage u
LEFT JOIN duckgres_org_teams t ON t.org_id = u.org_id AND t.team_id = u.team_id
WHERE u.org_id = ? AND u.bucket_start >= ?
GROUP BY 1, u.team_id, t.schema_name
ORDER BY 1, u.team_id`

	rows, err := cs.db.Raw(q, orgID, from.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate daily compute usage (org=%s): %w", orgID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []DailyComputeUsageRow
	for rows.Next() {
		var r DailyComputeUsageRow
		if err := rows.Scan(&r.Date, &r.TeamID, &r.SchemaName, &r.CPUSeconds, &r.MemorySeconds); err != nil {
			return nil, fmt.Errorf("scan daily compute usage row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AggregateStorageUsageDaily is the storage half of
// AggregateComputeUsageDaily — same org scope and window, byte-seconds
// converted to exact GiB-seconds.
func (cs *ConfigStore) AggregateStorageUsageDaily(orgID string, from time.Time) ([]DailyStorageUsageRow, error) {
	const q = `
SELECT to_char((bucket_start AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS date,
       u.team_id, t.schema_name, SUM(u.byte_seconds)::text
FROM duckgres_org_storage_usage u
LEFT JOIN duckgres_org_teams t ON t.org_id = u.org_id AND t.team_id = u.team_id
WHERE u.org_id = ? AND u.bucket_start >= ?
GROUP BY 1, u.team_id, t.schema_name
ORDER BY 1, u.team_id`

	rows, err := cs.db.Raw(q, orgID, from.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate daily storage usage (org=%s): %w", orgID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []DailyStorageUsageRow
	for rows.Next() {
		var r DailyStorageUsageRow
		var byteSeconds string
		if err := rows.Scan(&r.Date, &r.TeamID, &r.SchemaName, &byteSeconds); err != nil {
			return nil, fmt.Errorf("scan daily storage usage row: %w", err)
		}
		gib, err := byteSecondsToGiBSeconds(byteSeconds)
		if err != nil {
			return nil, fmt.Errorf("convert daily storage usage row (org=%s date=%s): %w", orgID, r.Date, err)
		}
		r.GiBSeconds = gib
		out = append(out, r)
	}
	return out, rows.Err()
}
