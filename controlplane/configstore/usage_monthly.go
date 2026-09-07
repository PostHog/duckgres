package configstore

import (
	"encoding/json"
	"fmt"
	"time"
)

// MonthlyScanUsageRow reports retained query scan bytes, including failed and cancelled queries.
type MonthlyScanUsageRow struct {
	Month        string      `json:"month"` // "YYYY-MM", UTC
	OrgID        string      `json:"org_id"`
	TeamID       int64       `json:"team_id"`
	SchemaName   *string     `json:"schema_name"`
	BytesScanned json.Number `json:"bytes_scanned"`
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

// AggregateScanUsageMonthly sums retained query usage by completion time in UTC.
func (cs *ConfigStore) AggregateScanUsageMonthly(from time.Time) ([]MonthlyScanUsageRow, error) {
	const q = `
SELECT to_char(date_trunc('month', completed_at AT TIME ZONE 'UTC'), 'YYYY-MM') AS month,
       u.org_id, u.team_id, t.schema_name,
       SUM(u.physical_input_bytes)::text
FROM duckgres_trino_query_usage u
LEFT JOIN duckgres_org_teams t ON t.org_id = u.org_id AND t.team_id = u.team_id
WHERE u.org_id IS NOT NULL AND u.completed_at >= ?
GROUP BY 1, u.org_id, u.team_id, t.schema_name
ORDER BY 1 DESC, u.org_id, u.team_id`

	rows, err := cs.db.Raw(q, from.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate monthly scan usage: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []MonthlyScanUsageRow
	for rows.Next() {
		var r MonthlyScanUsageRow
		if err := rows.Scan(&r.Month, &r.OrgID, &r.TeamID, &r.SchemaName, &r.BytesScanned); err != nil {
			return nil, fmt.Errorf("decode monthly scan usage row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AggregateStorageUsageMonthly sums retained storage samples in UTC,
// converting byte-seconds to exact GiB-seconds.
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
