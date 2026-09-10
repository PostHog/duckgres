package configstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

var ErrBillingBatchNotFound = errors.New("billing batch not found")

// ScanUsageRow sums native physical input bytes for one org/team/completion day.
// PostgreSQL SUM(bigint) is NUMERIC; preserve exact totals beyond int64 in JSON.
type ScanUsageRow struct {
	Date         string      `json:"date"`
	OrgID        string      `json:"org_id"`
	TeamID       int64       `json:"team_id"`
	BytesScanned json.Number `json:"bytes_scanned"`
	QueryCount   int64       `json:"query_count"`
}

// BillingBatch is frozen on creation and retained after acknowledgement.
// BillingMonth is the UTC month of creation, allowing late events without
// reopening invoices. Row dates retain the original usage date for reporting.
type BillingBatch struct {
	ID           string            `json:"batch_id"`
	CreatedAt    time.Time         `json:"created_at"`
	BillingMonth string            `json:"billing_month"`
	Scans        []ScanUsageRow    `json:"scans"`
	Storage      []StorageUsageRow `json:"storage"`
}

const MaxBillingBatchEvents = 10000

func lockBillingConsumer(tx *gorm.DB) (sql.NullString, error) {
	var id sql.NullString
	err := tx.Raw(`SELECT outstanding_batch_id FROM duckgres_billing_consumer WHERE id = 1 FOR UPDATE`).Row().Scan(&id)
	return id, err
}

// NextBillingBatch returns the unacknowledged batch, or atomically claims at
// most limit query events plus limit storage buckets. Row membership, rather
// than a sequence/time cursor, ensures delayed commits cannot be skipped.
// A nil batch means no attributable, unexported usage is currently available.
func (cs *ConfigStore) NextBillingBatch(ctx context.Context, limit int) (*BillingBatch, error) {
	if limit <= 0 || limit > MaxBillingBatchEvents {
		return nil, fmt.Errorf("billing batch limit must be between 1 and %d", MaxBillingBatchEvents)
	}
	var batch *BillingBatch
	err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		outstanding, err := lockBillingConsumer(tx)
		if err != nil {
			return err
		}
		if outstanding.Valid {
			var payload []byte
			if err := tx.Raw(`SELECT payload FROM duckgres_billing_batches WHERE batch_id = ?`, outstanding.String).Row().Scan(&payload); err != nil {
				return err
			}
			return json.Unmarshal(payload, &batch)
		}
		now := time.Now().UTC()
		batch = &BillingBatch{ID: uuid.NewString(), CreatedAt: now, BillingMonth: now.Format("2006-01"), Scans: []ScanUsageRow{}, Storage: []StorageUsageRow{}}
		if err := tx.Exec(`INSERT INTO duckgres_billing_batches (batch_id, created_at, payload) VALUES (?, ?, '{}'::jsonb)`, batch.ID, now).Error; err != nil {
			return err
		}
		claimed := tx.Exec(`WITH pending AS (
SELECT q.id, COALESCE(q.org_id, p.org_id) AS org_id,
       CASE WHEN q.org_id IS NULL THEN p.team_id ELSE q.team_id END AS team_id
FROM duckgres_trino_query_usage q LEFT JOIN duckgres_trino_usage_principals p ON p.principal = q.principal
WHERE q.batch_id IS NULL AND (q.org_id IS NOT NULL OR p.org_id IS NOT NULL)
ORDER BY q.id LIMIT ? FOR UPDATE OF q
)
UPDATE duckgres_trino_query_usage q SET batch_id = ?, org_id = pending.org_id, team_id = pending.team_id
FROM pending WHERE q.id = pending.id`, limit, batch.ID)
		if claimed.Error != nil {
			return claimed.Error
		}
		if err := claimStorageUsage(tx, batch.ID, limit); err != nil {
			return err
		}
		if err := aggregateBatchScans(tx, batch); err != nil {
			return err
		}
		if err := aggregateBatchStorage(tx, batch); err != nil {
			return err
		}
		if len(batch.Scans) == 0 && len(batch.Storage) == 0 {
			// No delivered history exists for this provisional row.
			if err := tx.Exec(`DELETE FROM duckgres_billing_batches WHERE batch_id = ?`, batch.ID).Error; err != nil {
				return err
			}
			batch = nil
			return nil
		}
		payload, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE duckgres_billing_batches SET payload = ?::jsonb WHERE batch_id = ?`, string(payload), batch.ID).Error; err != nil {
			return err
		}
		return tx.Exec(`UPDATE duckgres_billing_consumer SET outstanding_batch_id = ? WHERE id = 1`, batch.ID).Error
	})
	if err != nil {
		return nil, fmt.Errorf("next billing batch: %w", err)
	}
	return batch, nil
}

func claimStorageUsage(tx *gorm.DB, batchID string, limit int) error {
	return tx.Exec(`WITH pending AS MATERIALIZED (
SELECT org_id, team_id, bucket_start, byte_seconds,
       byte_seconds - exported_byte_seconds AS delta
FROM duckgres_org_storage_usage WHERE byte_seconds > exported_byte_seconds
ORDER BY bucket_start, org_id, team_id LIMIT ? FOR UPDATE
), claimed AS (
UPDATE duckgres_org_storage_usage s SET exported_byte_seconds = pending.byte_seconds
FROM pending WHERE s.org_id = pending.org_id AND s.team_id = pending.team_id AND s.bucket_start = pending.bucket_start
RETURNING s.org_id, s.team_id, s.bucket_start, pending.delta
)
INSERT INTO duckgres_billing_batch_storage (batch_id, org_id, team_id, bucket_start, byte_seconds)
SELECT ?, org_id, team_id, bucket_start, delta FROM claimed`, limit, batchID).Error
}

func aggregateBatchScans(tx *gorm.DB, batch *BillingBatch) error {
	rows, err := tx.Raw(`SELECT to_char((completed_at AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD'), org_id, team_id,
SUM(physical_input_bytes)::text, COUNT(*) FROM duckgres_trino_query_usage
WHERE batch_id = ? GROUP BY 1, org_id, team_id ORDER BY 1, org_id, team_id`, batch.ID).Rows()
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var row ScanUsageRow
		var bytes string
		if err := rows.Scan(&row.Date, &row.OrgID, &row.TeamID, &bytes, &row.QueryCount); err != nil {
			return err
		}
		row.BytesScanned = json.Number(bytes)
		batch.Scans = append(batch.Scans, row)
	}
	return rows.Err()
}

func aggregateBatchStorage(tx *gorm.DB, batch *BillingBatch) error {
	rows, err := tx.Raw(`SELECT to_char((bucket_start AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD'), org_id, team_id,
SUM(byte_seconds)::text FROM duckgres_billing_batch_storage
WHERE batch_id = ? GROUP BY 1, org_id, team_id ORDER BY 1, org_id, team_id`, batch.ID).Rows()
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var row StorageUsageRow
		var bytes string
		if err := rows.Scan(&row.Date, &row.OrgID, &row.TeamID, &bytes); err != nil {
			return err
		}
		row.GiBSeconds, err = byteSecondsToGiBSeconds(bytes)
		if err != nil {
			return err
		}
		batch.Storage = append(batch.Storage, row)
	}
	return rows.Err()
}

// AckBillingBatch acknowledges an exact batch ID without deleting any usage,
// membership, or batch history. Repeated acknowledgements are harmless, including
// an old ack arriving while a newer batch is outstanding.
func (cs *ConfigStore) AckBillingBatch(ctx context.Context, batchID string) error {
	return cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := lockBillingConsumer(tx); err != nil {
			return err
		}
		result := tx.Exec(`UPDATE duckgres_billing_batches SET acknowledged_at = COALESCE(acknowledged_at, now()) WHERE batch_id = ?`, batchID)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrBillingBatchNotFound
		}
		return tx.Exec(`UPDATE duckgres_billing_consumer SET outstanding_batch_id = NULL WHERE id = 1 AND outstanding_batch_id = ?`, batchID).Error
	})
}

// GetBillingBatch retrieves the original immutable payload, including after ack,
// so consumers can audit or replay with the same idempotency key.
func (cs *ConfigStore) GetBillingBatch(ctx context.Context, batchID string) (*BillingBatch, error) {
	var payload []byte
	err := cs.db.WithContext(ctx).Raw(`SELECT payload FROM duckgres_billing_batches WHERE batch_id = ?`, batchID).Row().Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBillingBatchNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get billing batch: %w", err)
	}
	var batch BillingBatch
	if err := json.Unmarshal(payload, &batch); err != nil {
		return nil, fmt.Errorf("decode billing batch: %w", err)
	}
	return &batch, nil
}
