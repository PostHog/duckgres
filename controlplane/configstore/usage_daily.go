package configstore

import (
	"encoding/json"
	"fmt"
	"time"
)

// DailyScanUsageRow reports retained query scan bytes, including failed and cancelled queries.
type DailyScanUsageRow struct {
	Date         string      `json:"date"` // "YYYY-MM-DD", UTC
	TeamID       int64       `json:"team_id"`
	SchemaName   *string     `json:"schema_name"`
	BytesScanned json.Number `json:"bytes_scanned"`
}

// DailyStorageUsageRow is the storage family of the daily view: byte-seconds
// per UTC day per team as exact-decimal GiB-seconds.
type DailyStorageUsageRow struct {
	Date       string      `json:"date"`
	TeamID     int64       `json:"team_id"`
	SchemaName *string     `json:"schema_name"`
	GiBSeconds json.Number `json:"gib_seconds"`
}

// AggregateScanUsageDaily sums retained query usage by completion time in UTC.
func (cs *ConfigStore) AggregateScanUsageDaily(orgID string, from time.Time) ([]DailyScanUsageRow, error) {
	const q = `
SELECT to_char((completed_at AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') AS date,
       u.team_id, t.schema_name,
       SUM(u.physical_input_bytes)::text
FROM duckgres_trino_query_usage u
LEFT JOIN duckgres_org_teams t ON t.org_id = u.org_id AND t.team_id = u.team_id
WHERE u.org_id = ? AND u.completed_at >= ?
GROUP BY 1, u.team_id, t.schema_name
ORDER BY 1, u.team_id`

	rows, err := cs.db.Raw(q, orgID, from.UTC()).Rows()
	if err != nil {
		return nil, fmt.Errorf("aggregate daily scan usage (org=%s): %w", orgID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []DailyScanUsageRow
	for rows.Next() {
		var r DailyScanUsageRow
		if err := rows.Scan(&r.Date, &r.TeamID, &r.SchemaName, &r.BytesScanned); err != nil {
			return nil, fmt.Errorf("decode daily scan usage row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AggregateStorageUsageDaily sums retained storage samples in UTC,
// converting byte-seconds to exact GiB-seconds.
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
