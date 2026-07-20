package configstore

import (
	"fmt"

	"gorm.io/gorm"
)

// OrgBillingTeamIDTx returns the org's current billing team id (the
// duckgres_org_teams row with is_billing_team = TRUE) inside the caller's
// transaction, or 0 when the org has none.
func OrgBillingTeamIDTx(tx *gorm.DB, orgID string) (int64, error) {
	var ids []int64
	if err := tx.Model(&OrgTeam{}).
		Where("org_id = ? AND is_billing_team IS TRUE", orgID).
		Pluck("team_id", &ids).Error; err != nil {
		return 0, fmt.Errorf("read billing team (org=%s): %w", orgID, err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return ids[0], nil
}

// SetOrgBillingTeamTx points the org's billing team at teamID inside the
// caller's transaction. Any other row currently marked billing is demoted
// first — the partial unique index (at most one billing row per org, migration
// 000024) is checked immediately, not deferred, so the order matters. When the
// org doesn't have teamID yet the row is inserted with the conventional
// "team_<id>" schema and enabled = TRUE; an existing row keeps its
// schema/enabled/backfill state and only gains the billing mark.
//
// Callers that change an EXISTING org's billing team must re-attribute its
// buffered usage buckets in the same transaction (ReattributeUsageTeamTx) so
// the billing pull never strands usage under the stale team.
func SetOrgBillingTeamTx(tx *gorm.DB, orgID string, teamID int64) error {
	if err := tx.Exec(`
		UPDATE duckgres_org_teams
		SET is_billing_team = NULL, updated_at = now()
		WHERE org_id = ? AND team_id <> ? AND is_billing_team IS TRUE`,
		orgID, teamID).Error; err != nil {
		return fmt.Errorf("demote billing team (org=%s): %w", orgID, err)
	}
	if err := tx.Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, is_billing_team, created_at, updated_at)
		VALUES (?, ?, ?, TRUE, TRUE, now(), now())
		ON CONFLICT (org_id, team_id)
		DO UPDATE SET is_billing_team = TRUE, updated_at = now()`,
		orgID, teamID, fmt.Sprintf("team_%d", teamID)).Error; err != nil {
		return fmt.Errorf("set billing team (org=%s team=%d): %w", orgID, teamID, err)
	}
	return nil
}
