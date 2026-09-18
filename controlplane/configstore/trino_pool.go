package configstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Fenced access to the shared Trino pool state.
//
// Two rules hold everywhere in this file:
//
//   - Leader election coordinates EXECUTION; the database fences EFFECTS. Every
//     write carries a TrinoPoolLease and is refused if the pool's stored epoch
//     has moved on, so a superseded leader that has not noticed yet cannot
//     write on top of its successor.
//   - A conflict is never resolved by re-reading a fresher epoch. Losing the
//     CAS means losing authority; the caller stops, it does not retry harder.

// UpsertTrinoPoolSpec applies the desired configuration resolved from the
// registry and blueprint. It touches only desired fields; the authority epoch,
// the freeze flag and every runtime column belong to the operator.
//
// It is FENCED. Publishing desired state changes what the
// operator will do next, so it is a lifecycle mutation like any other: a
// delayed old leader, or a replica still holding stale configuration, must not
// be able to overwrite a newer desired spec.
//
// The pool row must already exist for a fenced write to be possible, so the
// first publication seeds it - see SeedTrinoPool.
func (cs *ConfigStore) UpsertTrinoPoolSpec(ctx context.Context, lease TrinoPoolLease, spec TrinoPoolSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	if spec.PoolID != lease.PoolID {
		return fmt.Errorf("%w: spec is for pool %q, lease covers %q", ErrTrinoPoolConflict, spec.PoolID, lease.PoolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Model(&TrinoPool{}).Where("pool_id = ?", spec.PoolID).Updates(map[string]any{
			"public_id":                spec.PublicID,
			"api_mode":                 spec.APIMode,
			"desired_release_id":       spec.DesiredReleaseID,
			"desired_blueprint_digest": spec.DesiredBlueprintDigest,
			"desired_instances":        spec.DesiredInstances,
			"min_serving":              spec.MinServing,
			"max_surge":                spec.MaxSurge,
			"max_repair":               spec.MaxRepair,
			"updated_at":               time.Now().UTC(),
		}).Error
	})
}

// SeedTrinoPool creates the pool row if it does not exist yet. It is the one
// unfenced write, because a fence needs a row to lock: it only ever INSERTs,
// never updates, so it cannot overwrite anything another leader published.
func (cs *ConfigStore) SeedTrinoPool(ctx context.Context, spec TrinoPoolSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	pool := TrinoPool{
		PoolID:                 spec.PoolID,
		PublicID:               spec.PublicID,
		APIMode:                spec.APIMode,
		DesiredReleaseID:       spec.DesiredReleaseID,
		DesiredBlueprintDigest: spec.DesiredBlueprintDigest,
		DesiredInstances:       spec.DesiredInstances,
		MinServing:             spec.MinServing,
		MaxSurge:               spec.MaxSurge,
		MaxRepair:              spec.MaxRepair,
	}
	return cs.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&pool).Error
}

func (s TrinoPoolSpec) validate() error {
	if s.PoolID == "" || s.PublicID == "" {
		return errors.New("trino pool spec requires a pool identity")
	}
	if s.APIMode != TrinoPoolAPIModeLegacy && s.APIMode != TrinoPoolAPIModeShared {
		return fmt.Errorf("unsupported trino pool api mode %q", s.APIMode)
	}
	// A desired count of zero is missing configuration, never an instruction to
	// empty the pool. Callers that cannot resolve a count must freeze instead.
	if s.DesiredInstances < 1 {
		return errors.New("trino pool spec requires a positive desired instance count")
	}
	if s.MinServing < 1 || s.MinServing > s.DesiredInstances {
		return errors.New("trino pool spec requires a minimum serving count between one and the desired count")
	}
	if s.MaxSurge < 0 || s.MaxRepair < 0 {
		return errors.New("trino pool spec requires non-negative budgets")
	}
	return nil
}

// GetTrinoPool returns the pool row, or nil when the pool is unknown.
func (cs *ConfigStore) GetTrinoPool(ctx context.Context, poolID string) (*TrinoPool, error) {
	var pool TrinoPool
	err := cs.db.WithContext(ctx).Where("pool_id = ?", poolID).First(&pool).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pool, nil
}

// FreezeTrinoPool holds the pool at its last-good state. This is what missing
// or invalid desired configuration does: no creates, no drains, no deletes, and
// explicitly NOT a desired count of zero.
func (cs *ConfigStore) FreezeTrinoPool(ctx context.Context, lease TrinoPoolLease, poolID, reason string) error {
	if reason == "" {
		return errors.New("freezing a trino pool requires a reason")
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Model(&TrinoPool{}).Where("pool_id = ?", poolID).
			Updates(map[string]any{"frozen": true, "frozen_reason": reason, "updated_at": time.Now().UTC()}).Error
	})
}

// ThawTrinoPool clears the freeze once desired configuration is readable again.
func (cs *ConfigStore) ThawTrinoPool(ctx context.Context, lease TrinoPoolLease, poolID string) error {
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Model(&TrinoPool{}).Where("pool_id = ? AND frozen", poolID).
			Updates(map[string]any{"frozen": false, "frozen_reason": "", "updated_at": time.Now().UTC()}).Error
	})
}

// AcquireTrinoPoolAuthority bumps the pool's authority epoch and records the new
// owner. The bump is what invalidates a previous leader's in-flight writes, so
// it happens under the pool row lock: a takeover either lands before an old
// write's CAS or makes that CAS fail. A lease expiry on its own decides nothing.
func (cs *ConfigStore) AcquireTrinoPoolAuthority(ctx context.Context, poolID, owner string) (TrinoPoolLease, error) {
	if owner == "" {
		return TrinoPoolLease{}, errors.New("acquiring trino pool authority requires an owner identity")
	}
	var lease TrinoPoolLease
	err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pool, err := lockTrinoPool(ctx, tx, poolID)
		if err != nil {
			return err
		}
		epoch := pool.AuthorityEpoch + 1
		if err := tx.Model(&TrinoPool{}).Where("pool_id = ?", poolID).Updates(map[string]any{
			"authority_epoch": epoch, "authority_owner": owner, "updated_at": time.Now().UTC(),
		}).Error; err != nil {
			return err
		}
		lease = TrinoPoolLease{PoolID: poolID, Owner: owner, Epoch: epoch}
		return nil
	})
	return lease, err
}

// lockTrinoPool takes the pool row lock. Every fenced mutation starts here, so
// all of them serialize against each other and against a takeover.
func lockTrinoPool(ctx context.Context, tx *gorm.DB, poolID string) (*TrinoPool, error) {
	var pool TrinoPool
	err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("pool_id = ?", poolID).First(&pool).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w: pool %q is not configured", ErrTrinoPoolConflict, poolID)
	}
	if err != nil {
		return nil, err
	}
	return &pool, nil
}

// checkLease verifies the caller still holds authority. It is called inside the
// pool row lock, so the answer cannot change under the caller's feet.
func checkLease(pool *TrinoPool, lease TrinoPoolLease) error {
	if pool.PoolID != lease.PoolID || pool.AuthorityEpoch != lease.Epoch || pool.AuthorityOwner != lease.Owner {
		return fmt.Errorf("%w: authority is %q at epoch %d, caller holds %q at epoch %d",
			ErrTrinoPoolConflict, pool.AuthorityOwner, pool.AuthorityEpoch, lease.Owner, lease.Epoch)
	}
	return nil
}

// withPoolAuthority runs fn under the pool row lock with the lease verified.
func (cs *ConfigStore) withPoolAuthority(ctx context.Context, lease TrinoPoolLease, fn func(*gorm.DB, *TrinoPool) error) error {
	return cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pool, err := lockTrinoPool(ctx, tx, lease.PoolID)
		if err != nil {
			return err
		}
		if err := checkLease(pool, lease); err != nil {
			return err
		}
		return fn(tx, pool)
	})
}
