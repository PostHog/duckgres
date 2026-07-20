package provisioning

import (
	"errors"
	"fmt"
	"log/slog"

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

// ErrDefaultTeamIDRequired is returned by Provision when the request would
// create a NEW org without a default_team_id. Every org must carry its billing
// PostHog team from birth — the request's team id becomes the org's first
// duckgres_org_teams row, marked is_billing_team (pull-based compute billing
// keys usage buckets by it); all pre-existing orgs have been backfilled.
// Re-provisioning an EXISTING org without the field stays valid — the stored
// billing team is kept, never wiped. HTTP handlers map this to 400.
var ErrDefaultTeamIDRequired = errors.New("default_team_id is required when creating a new org")

// ProvisionRequest is the all-or-nothing input the Provision endpoint
// dispatches into a single configstore transaction. Warehouse + root
// user are always written.
//
// The shape mirrors the public HTTP request: the handler builds it
// from the validated request body and passes it down so the Store can
// commit (or roll back) every write atomically. Partial failure can no
// longer leave the warehouse in Pending state without a root user, or
// vice versa.
type ProvisionRequest struct {
	OrgID        string
	DatabaseName string
	// DefaultTeamID is the org's billing PostHog team id (an integer,
	// matching PostHog's Team.id), stored as the org's duckgres_org_teams
	// billing row. REQUIRED when the org does not exist yet (Provision
	// returns ErrDefaultTeamIDRequired otherwise); optional (0) on
	// re-provision of an existing org, where it keeps the stored billing
	// team (never a wipe). Prerequisite for pull-based compute billing.
	DefaultTeamID int64
	Warehouse     *configstore.ManagedWarehouse
	// RootUserHash is the bcrypt hash of the freshly-generated root
	// password. Plaintext stays in the handler (returned to the
	// caller); only the hash is persisted, same as before.
	RootUserHash string
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
	if err := s.cs.DB().Preload("Teams").First(&org, "name = ?", orgID).Error; err != nil {
		return nil, err
	}
	org.DefaultTeamID = org.BillingTeamID()
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
		// No default_team_id on this standalone path — an existing org keeps
		// its billing team as-is; creating a NEW org through here now fails
		// with ErrDefaultTeamIDRequired (same invariant as Provision).
		return createPendingWarehouseTx(tx, orgID, databaseName, 0, warehouse)
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
func createPendingWarehouseTx(tx *gorm.DB, orgID, databaseName string, defaultTeamID int64, warehouse *configstore.ManagedWarehouse) error {
	// Auto-create org if it doesn't exist (PostHog calls provision, duckgres
	// creates everything). A NEW org MUST carry default_team_id — it becomes
	// the org's first duckgres_org_teams row, marked as the billing team
	// (pull-based compute billing keys usage buckets by it; all pre-existing
	// orgs are backfilled) — creating one without it is rejected.
	// Re-provisioning an existing org without it keeps the stored billing
	// team (never a wipe).
	var org configstore.Org
	orgCreated := false
	err := tx.Where("name = ?", orgID).First(&org).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		if defaultTeamID == 0 {
			return ErrDefaultTeamIDRequired
		}
		org = configstore.Org{Name: orgID, DatabaseName: databaseName}
		if err := tx.Create(&org).Error; err != nil {
			return err
		}
		orgCreated = true
	case err != nil:
		return err
	}
	// Update database name if org already existed with a different one
	if org.DatabaseName != databaseName {
		if err := tx.Model(&org).Update("database_name", databaseName).Error; err != nil {
			return err
		}
	}
	// If a default_team_id was supplied, persist it as the org's billing team
	// row. Only ever set, never cleared here, so an omitted value is a no-op
	// rather than a wipe.
	if defaultTeamID != 0 {
		oldTeamID := int64(0)
		if !orgCreated {
			var err error
			oldTeamID, err = configstore.OrgBillingTeamIDTx(tx, orgID)
			if err != nil {
				return err
			}
		}
		if err := configstore.SetOrgBillingTeamTx(tx, orgID, defaultTeamID); err != nil {
			return err
		}
		// The org's billing team changed on a re-provision: re-attribute its
		// buffered (unacked) billing buckets to the new team in this same
		// transaction, so the next billing pull reports them under the new
		// team instead of stranding them under the stale one. A
		// freshly-created org has no buckets, so it skips this. In-flight
		// metering under the old team (config-snapshot poll lag, ~30s) can
		// still land a small residual old-team bucket — tolerated, see
		// configstore.ReattributeUsageTeamTx.
		if !orgCreated && oldTeamID != defaultTeamID {
			moved, err := configstore.ReattributeUsageTeamTx(tx, orgID, defaultTeamID)
			if err != nil {
				return err
			}
			slog.Info("Re-attributed org usage buckets to new billing team.",
				"org", orgID, "old_team", oldTeamID, "new_team", defaultTeamID, "rows", moved)
		}
	}

	// Check for existing warehouse in non-terminal state
	var existing configstore.ManagedWarehouse
	err = tx.First(&existing, "org_id = ?", orgID).Error
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
	warehouse.MetadataStoreState = configstore.ManagedWarehouseStatePending
	warehouse.S3State = configstore.ManagedWarehouseStatePending
	warehouse.IdentityState = configstore.ManagedWarehouseStatePending
	warehouse.SecretsState = configstore.ManagedWarehouseStatePending
	return tx.Create(warehouse).Error
}

// Provision is the all-or-nothing entrypoint for POST /provision: one
// configstore transaction wrapping warehouse + root-user writes.
// Partial failure rolls back every write so the caller's retry sees the
// same starting state, not a half-provisioned org with a non-terminal
// warehouse blocking re-creation.
//
// Each individual write is idempotent (OnConflict on the user upsert,
// FirstOrCreate on the Org row), so a
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
		if err := createPendingWarehouseTx(tx, req.OrgID, req.DatabaseName, req.DefaultTeamID, req.Warehouse); err != nil {
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

// SetWarehouseDeleting atomically transitions a warehouse from expectedState to deleting.
// Returns gorm.ErrRecordNotFound if no warehouse exists, or an error if the CAS fails.
func (s *gormStore) SetWarehouseDeleting(orgID string, expectedState configstore.ManagedWarehouseProvisioningState) error {
	// Also stamp a status_message so a client polling warehouse/status sees a
	// live "Deprovisioning..." message during teardown — mirroring how the
	// provisioning phases stamp status_message. Without this the message stays
	// stale (e.g. "Infrastructure ready") until the provisioner flips it to
	// "Resources deleted" at the very end.
	result := s.cs.DB().Model(&configstore.ManagedWarehouse{}).
		Where("org_id = ? AND state = ?", orgID, expectedState).
		Updates(map[string]interface{}{
			"state":          configstore.ManagedWarehouseStateDeleting,
			"status_message": "Deprovisioning...",
		})
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
