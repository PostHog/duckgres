package configstore

import "context"

// ListAdmittedTrinoOrgs includes previous admissions after transient state failures.
// Missing credential rows cannot remove a warehouse from the rollout certificate.
func (cs *ConfigStore) ListAdmittedTrinoOrgs(ctx context.Context, cell string) ([]TrinoEnabledOrg, error) {
	var orgs []TrinoEnabledOrg
	err := cs.db.WithContext(ctx).Table("duckgres_managed_warehouse_trino AS t").
		Select("t.backend, t.hoglake_initialized, t.org_id, COALESCE(o.database_name, '') AS database_name, t.trino_cell_id AS cell_id, t.state").
		Joins("LEFT JOIN duckgres_orgs AS o ON o.name = t.org_id").
		Where("t.enabled = ? AND t.trino_cell_id = ? AND (t.ready_at IS NOT NULL OR t.state = ?)", true, cell, ManagedWarehouseStateReady).
		Order("t.org_id").Scan(&orgs).Error
	return orgs, err
}
