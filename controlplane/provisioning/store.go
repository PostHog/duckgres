package provisioning

import (
	"errors"
	"fmt"

	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

// gormStore implements Store using a ConfigStore's GORM DB.
type gormStore struct {
	cs *configstore.ConfigStore
}

// NewGormStore creates a Store backed by the given ConfigStore.
func NewGormStore(cs *configstore.ConfigStore) Store {
	return &gormStore{cs: cs}
}

func (s *gormStore) GetOrg(orgID string) (*configstore.Org, error) {
	var org configstore.Org
	if err := s.cs.DB().First(&org, "name = ?", orgID).Error; err != nil {
		return nil, err
	}
	return &org, nil
}

func (s *gormStore) CreateOrgUser(orgID, username, passwordHash string) error {
	return s.cs.CreateOrgUser(orgID, username, passwordHash)
}

func (s *gormStore) UpdateOrgUserPassword(orgID, username, passwordHash string) error {
	return s.cs.UpdateOrgUserPassword(orgID, username, passwordHash)
}

func (s *gormStore) GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error) {
	var warehouse configstore.ManagedWarehouse
	if err := s.cs.DB().First(&warehouse, "org_id = ?", orgID).Error; err != nil {
		return nil, err
	}
	return &warehouse, nil
}

func (s *gormStore) CreatePendingWarehouse(orgID, databaseName string, warehouse *configstore.ManagedWarehouse) error {
	return s.cs.DB().Transaction(func(tx *gorm.DB) error {
		// Auto-create org if it doesn't exist (PostHog calls provision, duckgres creates everything)
		org := configstore.Org{Name: orgID, DatabaseName: databaseName}
		if err := tx.Where("name = ?", orgID).FirstOrCreate(&org).Error; err != nil {
			return err
		}
		// Update database name if org already existed with a different one
		if org.DatabaseName != databaseName {
			if err := tx.Model(&org).Update("database_name", databaseName).Error; err != nil {
				return err
			}
		}

		// Check for existing warehouse in non-terminal state
		var existing configstore.ManagedWarehouse
		err := tx.First(&existing, "org_id = ?", orgID).Error
		if err == nil {
			if existing.State != configstore.ManagedWarehouseStateFailed &&
				existing.State != configstore.ManagedWarehouseStateDeleted {
				return errors.New("warehouse already exists in non-terminal state")
			}
			if err := tx.Delete(&existing).Error; err != nil {
				return err
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		warehouse.OrgID = orgID
		warehouse.State = configstore.ManagedWarehouseStatePending
		warehouse.WarehouseDatabaseState = configstore.ManagedWarehouseStatePending
		warehouse.MetadataStoreState = configstore.ManagedWarehouseStatePending
		warehouse.S3State = configstore.ManagedWarehouseStatePending
		warehouse.IdentityState = configstore.ManagedWarehouseStatePending
		warehouse.SecretsState = configstore.ManagedWarehouseStatePending
		return tx.Create(warehouse).Error
	})
}

func (s *gormStore) IsDatabaseNameAvailable(name string) (bool, error) {
	var count int64
	if err := s.cs.DB().Model(&configstore.Org{}).Where("database_name = ?", name).Count(&count).Error; err != nil {
		return false, err
	}
	return count == 0, nil
}

// SetWarehouseDeleting atomically transitions a warehouse from expectedState to deleting.
// Returns gorm.ErrRecordNotFound if no warehouse exists, or an error if the CAS fails.
func (s *gormStore) SetWarehouseDeleting(orgID string, expectedState configstore.ManagedWarehouseProvisioningState) error {
	result := s.cs.DB().Model(&configstore.ManagedWarehouse{}).
		Where("org_id = ? AND state = ?", orgID, expectedState).
		Update("state", configstore.ManagedWarehouseStateDeleting)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		// Distinguish "not found" from "wrong state"
		var count int64
		s.cs.DB().Model(&configstore.ManagedWarehouse{}).Where("org_id = ?", orgID).Count(&count)
		if count == 0 {
			return gorm.ErrRecordNotFound
		}
		return fmt.Errorf("warehouse %q not in expected state %q", orgID, expectedState)
	}
	return nil
}
