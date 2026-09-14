package configstore

import (
	"context"
	"errors"
)

// TrinoRolloutCanaryEligible checks current ownership without loading credentials.
func (cs *ConfigStore) TrinoRolloutCanaryEligible(ctx context.Context, orgID, cellID, principal string) (bool, error) {
	if orgID == "" || cellID == "" || principal == "" {
		return false, nil
	}
	var count int64
	err := cs.db.WithContext(ctx).Table("duckgres_managed_warehouse_trino AS t").
		Joins("JOIN duckgres_orgs AS o ON o.name = t.org_id").
		Joins("JOIN duckgres_managed_warehouses AS w ON w.org_id = t.org_id").
		Joins("JOIN duckgres_org_users AS u ON u.org_id = t.org_id AND u.username = 'root'").
		Where("t.org_id = ? AND t.trino_cell_id = ? AND o.database_name = ?", orgID, cellID, principal).
		Where("t.enabled = ? AND t.state = ? AND w.state = ?", true, ManagedWarehouseStateReady, ManagedWarehouseStateReady).
		Where("u.disabled = ? AND u.password <> ''", false).Count(&count).Error
	if err != nil {
		return false, errors.New("canary eligibility query failed")
	}
	return count == 1, nil
}
