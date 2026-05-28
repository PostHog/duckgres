package provisioning

import (
	"errors"
	"fmt"

	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrWarehouseNonTerminal is returned by Provision / CreatePendingWarehouse
// when the caller tries to (re-)provision an org whose warehouse row is
// in a non-terminal state (Pending / Provisioning / Ready / Deleting).
// HTTP handlers map this to 409. Callers that need to "recreate" a
// warehouse should call /deprovision first to move it into Deleting →
// Deleted, then retry /provision.
var ErrWarehouseNonTerminal = errors.New("warehouse already exists in non-terminal state")

// ProvisionRequest is the all-or-nothing input the Provision endpoint
// dispatches into a single configstore transaction. Warehouse + root
// user are always written; Trino is opt-in (Trino == nil skips it).
//
// The shape mirrors the public HTTP request: the handler builds it
// from the validated request body and passes it down so the Store can
// commit (or roll back) every write atomically. Partial failure can no
// longer leave the warehouse in Pending state without a root user, or
// vice versa.
type ProvisionRequest struct {
	OrgID        string
	DatabaseName string
	Warehouse    *configstore.ManagedWarehouse
	// RootUserHash is the bcrypt hash of the freshly-generated root
	// password. Plaintext stays in the handler (returned to the
	// caller); only the hash is persisted, same as before.
	RootUserHash string
	// Trino, when non-nil, additionally writes a ManagedWarehouseTrino
	// row inside the same transaction. nil leaves Trino opt-out alone.
	Trino *configstore.TrinoSettings
}

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
		return createPendingWarehouseTx(tx, orgID, databaseName, warehouse)
	})
}

// createPendingWarehouseTx is the inner half of CreatePendingWarehouse —
// the same logic, but taking an existing *gorm.DB (typically a
// transaction handle) so it can be composed into a larger transaction
// (see Provision). Self-contained: the caller wraps it in Transaction
// (or extends an existing one) as appropriate.
//
// Returns a sentinel-comparable error string ("warehouse already
// exists in non-terminal state") so HTTP handlers can map to 409
// without an extra error type.
func createPendingWarehouseTx(tx *gorm.DB, orgID, databaseName string, warehouse *configstore.ManagedWarehouse) error {
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
			return ErrWarehouseNonTerminal
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
	// Track iceberg as a provisioning component only when the tenant opted
	// in (e.g. cnpg-shard, which is always iceberg-backed). Leaving it
	// empty for non-iceberg warehouses keeps them out of the iceberg
	// readiness gate.
	if warehouse.Iceberg.Enabled {
		warehouse.IcebergState = configstore.ManagedWarehouseStatePending
	}
	return tx.Create(warehouse).Error
}

// Provision is the all-or-nothing entrypoint for POST /provision: one
// configstore transaction wrapping warehouse + root-user + optional
// Trino-opt-in writes. Partial failure rolls back every write so the
// caller's retry sees the same starting state, not a half-provisioned
// org with a non-terminal warehouse blocking re-creation.
//
// Each individual write is idempotent (OnConflict on the user upsert,
// FirstOrCreate on the Org row, OnConflict on the Trino row), so a
// retry of a successfully-committed transaction returns the same
// success — but the plaintext password is regenerated on every call
// and is only readable from the HTTP response. A retry after a dropped
// response will not recover the original plaintext; the caller's
// recovery path is /reset-password once the warehouse reaches Ready.
//
// The "warehouse already exists in non-terminal state" error is
// passed through verbatim from createPendingWarehouseTx so the HTTP
// handler can map it to 409.
func (s *gormStore) Provision(req ProvisionRequest) error {
	if req.OrgID == "" {
		return errors.New("Provision: OrgID is required")
	}
	if req.DatabaseName == "" {
		return errors.New("Provision: DatabaseName is required")
	}
	if req.Warehouse == nil {
		return errors.New("Provision: Warehouse is required")
	}
	if req.RootUserHash == "" {
		return errors.New("Provision: RootUserHash is required")
	}
	return s.cs.DB().Transaction(func(tx *gorm.DB) error {
		// 1. Warehouse + Org (extracted helper).
		if err := createPendingWarehouseTx(tx, req.OrgID, req.DatabaseName, req.Warehouse); err != nil {
			return err
		}

		// 2. Root user. Same OnConflict semantics as
		// ConfigStore.CreateOrgUser so a retry overwrites the prior
		// hash — important because the handler regenerates a fresh
		// plaintext on every call.
		user := configstore.OrgUser{
			OrgID:    req.OrgID,
			Username: "root",
			Password: req.RootUserHash,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "org_id"}, {Name: "username"}},
			DoUpdates: clause.AssignmentColumns([]string{"password", "updated_at"}),
		}).Create(&user).Error; err != nil {
			return fmt.Errorf("create root user: %w", err)
		}

		// 3. Optional Trino opt-in. State seeds to Pending so the
		// reconcile loop sees a fresh row to act on; the OnConflict
		// columns deliberately exclude State / StatusMessage /
		// ReadyAt / FailedAt so a re-provision doesn't clobber the
		// reconcile loop's prior outcome.
		if req.Trino != nil {
			trinoRow := configstore.ManagedWarehouseTrino{
				OrgID:   req.OrgID,
				Enabled: true,
				Tier:    req.Trino.Tier,
				State:   configstore.ManagedWarehouseStatePending,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "org_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"enabled", "tier", "updated_at"}),
			}).Create(&trinoRow).Error; err != nil {
				return fmt.Errorf("enable trino: %w", err)
			}
		}
		return nil
	})
}

func (s *gormStore) IsDatabaseNameAvailable(name string) (bool, error) {
	var count int64
	if err := s.cs.DB().Model(&configstore.Org{}).Where("database_name = ?", name).Count(&count).Error; err != nil {
		return false, err
	}
	return count == 0, nil
}

// EnableTrino persists the per-org Trino opt-in. Idempotent — the
// configstore implementation upserts on (org_id) so re-enabling updates
// the tier without flipping Enabled false-then-true.
func (s *gormStore) EnableTrino(orgID string, settings configstore.TrinoSettings) error {
	return s.cs.EnableTrino(orgID, settings)
}

// DisableTrino marks the org's Trino row as disabled. The row is kept
// (with Enabled=false) so the provisioner observes the transition and
// can clean up downstream state. No-op when no row exists.
func (s *gormStore) DisableTrino(orgID string) error {
	return s.cs.DisableTrino(orgID)
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
