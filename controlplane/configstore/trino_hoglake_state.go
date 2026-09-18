package configstore

import (
	"context"
	"errors"
)

// GetTrinoHoglakeInitialized reads the durable metadata boundary, including for
// disabled clients. Missing rows and a different backend fail closed.
func (cs *ConfigStore) GetTrinoHoglakeInitialized(ctx context.Context, orgID string) (bool, error) {
	var row ManagedWarehouseTrino
	if err := cs.db.WithContext(ctx).Select("backend", "hoglake_initialized").First(&row, "org_id = ?", orgID).Error; err != nil {
		return false, err
	}
	if row.Backend != TrinoBackendHoglake {
		return false, errors.New("tenant is not configured for Hoglake")
	}
	return row.HoglakeInitialized, nil
}

// MarkTrinoHoglakeInitialized is monotonic and idempotent. Do not require enabled:
// a disable racing successful bootstrap must retain the resource's identity too.
func (cs *ConfigStore) MarkTrinoHoglakeInitialized(ctx context.Context, orgID string) error {
	result := cs.db.WithContext(ctx).Model(&ManagedWarehouseTrino{}).Where("org_id = ? AND backend = ? AND backend_selected = ?", orgID, TrinoBackendHoglake, true).Update("hoglake_initialized", true)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("hoglake tenant initialization row is unavailable")
	}
	return nil
}
