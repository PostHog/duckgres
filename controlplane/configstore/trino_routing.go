package configstore

import (
	"context"
	"errors"
)

const MaxTrinoRoutingPrincipals = 100000

// TrinoRoutingPrincipal contains only the principal and durable cell ownership.
type TrinoRoutingPrincipal struct {
	Principal string
	CellID    string
}

// ListTrinoRoutingPrincipals reads eligible assignments in one database snapshot.
// Passwords are eligibility predicates only; the query never selects credentials.
func (cs *ConfigStore) ListTrinoRoutingPrincipals(ctx context.Context) ([]TrinoRoutingPrincipal, error) {
	var rows []TrinoRoutingPrincipal
	err := cs.db.WithContext(ctx).Table("duckgres_managed_warehouse_trino AS t").
		Select("o.database_name AS principal, t.trino_cell_id AS cell_id").
		Joins("INNER JOIN duckgres_orgs AS o ON o.name = t.org_id").
		Joins("INNER JOIN duckgres_managed_warehouses AS w ON w.org_id = t.org_id").
		Joins("INNER JOIN duckgres_org_users AS u ON u.org_id = t.org_id AND u.username = 'root'").
		Where("t.enabled = ? AND t.state = ? AND w.state = ?", true, ManagedWarehouseStateReady, ManagedWarehouseStateReady).
		Where("u.disabled = ? AND u.password <> ''", false).
		Where("o.database_name <> '' AND t.trino_cell_id <> ''").
		Order("o.database_name ASC").Limit(MaxTrinoRoutingPrincipals + 1).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	if len(rows) > MaxTrinoRoutingPrincipals {
		return nil, errors.New("trino routing snapshot exceeds principal limit")
	}
	return rows, nil
}
