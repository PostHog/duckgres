package configstore

import (
	"fmt"

	"gorm.io/gorm"
)

// ReattributeUsageTeam moves every buffered (unacked) billing bucket for the
// org — both metric families — onto newTeamID, so the next billing pull
// reports them under the org's new default team instead of the stale one.
// Runs in its own transaction; returns the number of buffer rows moved across
// both tables. See ReattributeUsageTeamTx for the semantics.
func (cs *ConfigStore) ReattributeUsageTeam(orgID string, newTeamID int64) (int64, error) {
	var moved int64
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		var err error
		moved, err = ReattributeUsageTeamTx(tx, orgID, newTeamID)
		return err
	})
	return moved, err
}

// ReattributeUsageTeamTx re-attributes the org's buffered usage buckets
// (duckgres_org_compute_usage + duckgres_org_storage_usage) to newTeamID
// inside the caller's transaction, so the move commits atomically with the
// billing-team change (SetOrgBillingTeamTx).
//
// team_id is part of both tables' primary keys, so this is a fold-then-delete
// per table, not a blind UPDATE: an additive upsert copies every row keyed
// under a different team onto the newTeamID key (summing the value columns
// when the target row already exists — e.g. a team changed A→B→A, or a
// straggler bucket from the old era sharing a bucket_start), then the source
// rows are deleted. The SELECT groups per target key so two distinct stale
// teams landing on the same key fold cleanly instead of tripping ON
// CONFLICT's affect-row-twice error.
//
// In-flight race (bounded, tolerated): connections and storage samples keep
// stamping the OLD team id until the config snapshot poll (~30s) picks up the
// new one, so a flush racing this transaction can land a small residual
// old-team bucket just after the move (or, in the sliver between the fold and
// the delete, lose one ≤15s flush increment — within the flusher's
// best-effort log-and-drop contract). Billing tolerates the residual (it
// shows up as one tiny old-team row on a later pull), and re-running the
// update folds it in. Do NOT add locking around the metering hot path for
// this.
//
// Returns the number of source rows moved across both tables.
func ReattributeUsageTeamTx(tx *gorm.DB, orgID string, newTeamID int64) (int64, error) {
	var moved int64
	for _, t := range []struct {
		insert string
		table  string
	}{
		{
			insert: `
INSERT INTO duckgres_org_compute_usage (org_id, team_id, query_source, cpu, mem_gib, bucket_start, cpu_seconds, memory_seconds)
SELECT org_id, ?, query_source, cpu, mem_gib, bucket_start, SUM(cpu_seconds), SUM(memory_seconds)
FROM duckgres_org_compute_usage
WHERE org_id = ? AND team_id <> ?
GROUP BY org_id, query_source, cpu, mem_gib, bucket_start
ON CONFLICT (org_id, team_id, query_source, cpu, mem_gib, bucket_start)
DO UPDATE SET cpu_seconds    = duckgres_org_compute_usage.cpu_seconds    + EXCLUDED.cpu_seconds,
              memory_seconds = duckgres_org_compute_usage.memory_seconds + EXCLUDED.memory_seconds`,
			table: "duckgres_org_compute_usage",
		},
		{
			insert: `
INSERT INTO duckgres_org_storage_usage (org_id, team_id, bucket_start, byte_seconds)
SELECT org_id, ?, bucket_start, SUM(byte_seconds)
FROM duckgres_org_storage_usage
WHERE org_id = ? AND team_id <> ?
GROUP BY org_id, bucket_start
ON CONFLICT (org_id, team_id, bucket_start)
DO UPDATE SET byte_seconds = duckgres_org_storage_usage.byte_seconds + EXCLUDED.byte_seconds`,
			table: "duckgres_org_storage_usage",
		},
	} {
		if err := tx.Exec(t.insert, newTeamID, orgID, newTeamID).Error; err != nil {
			return moved, fmt.Errorf("reattribute %s (org=%s team=%d): %w", t.table, orgID, newTeamID, err)
		}
		res := tx.Exec(`DELETE FROM `+t.table+` WHERE org_id = ? AND team_id <> ?`, orgID, newTeamID)
		if res.Error != nil {
			return moved, fmt.Errorf("delete reattributed %s rows (org=%s team=%d): %w", t.table, orgID, newTeamID, res.Error)
		}
		moved += res.RowsAffected
	}
	return moved, nil
}
