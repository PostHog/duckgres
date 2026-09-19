package configstore

import (
	"errors"

	"gorm.io/gorm"
)

// ErrHoglakeLifecycleProtected prevents ownership loss until an explicit
// Hoglake retirement workflow can fence the retained catalog and storage.
var ErrHoglakeLifecycleProtected = errors.New("hoglake ownership is retained; warehouse replacement, deprovisioning and organization deletion require an explicit Hoglake retirement workflow")

// CheckHoglakeLifecycleTx must run under LockOrgConnectionAdmissionTx in the
// same transaction as the destructive mutation. Disabled tenants remain owners.
func CheckHoglakeLifecycleTx(tx *gorm.DB, orgID string) error {
	var count int64
	if err := tx.Model(&ManagedWarehouseTrino{}).Where("org_id = ? AND backend = ?", orgID, TrinoBackendHoglake).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return ErrHoglakeLifecycleProtected
	}
	return nil
}

// CheckWarehouseDeletionAllowed also fences previously queued deletions. New
// Hoglake ownership cannot be introduced while a warehouse is deleting/deleted.
func (cs *ConfigStore) CheckWarehouseDeletionAllowed(orgID string) error {
	return cs.db.Transaction(func(tx *gorm.DB) error {
		if err := LockOrgConnectionAdmissionTx(tx, orgID); err != nil {
			return err
		}
		return CheckHoglakeLifecycleTx(tx, orgID)
	})
}

func checkHoglakeWarehouseActiveTx(tx *gorm.DB, orgID string) error {
	var count int64
	if err := tx.Model(&ManagedWarehouse{}).Where("org_id = ? AND state IN ?", orgID, []ManagedWarehouseProvisioningState{ManagedWarehouseStateDeleting, ManagedWarehouseStateDeleted}).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return ErrHoglakeLifecycleProtected
	}
	return nil
}
