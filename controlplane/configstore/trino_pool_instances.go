package configstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/posthog/duckgres/controlplane/trinopool"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ListTrinoPoolInstances returns every instance of a pool, including terminal
// tombstones. The planner needs the tombstones to tell "three instances have
// been retired here" apart from "three instances are running".
func (cs *ConfigStore) ListTrinoPoolInstances(ctx context.Context, poolID string) ([]TrinoPoolInstance, error) {
	var instances []TrinoPoolInstance
	err := cs.db.WithContext(ctx).Where("pool_id = ?", poolID).Order("created_at, instance_id").Find(&instances).Error
	return instances, err
}

// GetTrinoPoolInstance returns one instance, or nil when it is unknown.
func (cs *ConfigStore) GetTrinoPoolInstance(ctx context.Context, instanceID string) (*TrinoPoolInstance, error) {
	var instance TrinoPoolInstance
	err := cs.db.WithContext(ctx).Where("instance_id = ?", instanceID).First(&instance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &instance, nil
}

// CreateTrinoPoolInstance persists an instance identity BEFORE anything is
// created in Kubernetes. That ordering is what makes a lost create response
// recoverable: the name is deterministic and already recorded, so the next tick
// reads the object back instead of creating a second one.
//
// The identity is never reused, including after retirement — the primary key
// covers terminal rows, and the partial unique index refuses to hand a live
// endpoint to a second instance.
func (cs *ConfigStore) CreateTrinoPoolInstance(ctx context.Context, lease TrinoPoolLease, spec TrinoPoolInstanceSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	if spec.PoolID != lease.PoolID {
		return fmt.Errorf("%w: instance belongs to pool %q, lease covers %q", ErrTrinoPoolConflict, spec.PoolID, lease.PoolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		snapshot := spec.BlueprintSnapshot
		if snapshot == "" {
			snapshot = "{}"
		}
		instance := TrinoPoolInstance{
			InstanceID:        spec.InstanceID,
			PoolID:            spec.PoolID,
			ReleaseID:         spec.ReleaseID,
			SpecDigest:        spec.SpecDigest,
			BlueprintSnapshot: snapshot,
			Phase:             string(spec.Phase),
			PhaseChangedAt:    time.Now().UTC(),
			OwnerEpoch:        lease.Epoch,
			Repair:            spec.Repair,
			RepairFor:         spec.RepairFor,
			EndpointURL:       spec.EndpointURL,
			ValidationReceipt: "{}",
			RetirementReceipt: "{}",
		}
		return tx.Create(&instance).Error
	})
}

func (s TrinoPoolInstanceSpec) validate() error {
	if s.InstanceID == "" || len(s.InstanceID) > 63 {
		return errors.New("trino pool instance requires an identity")
	}
	if s.ReleaseID == "" || s.SpecDigest == "" {
		return errors.New("trino pool instance requires a release and spec digest")
	}
	if !s.Phase.Valid() {
		return fmt.Errorf("unknown trino pool instance phase %q", s.Phase)
	}
	return nil
}

// AdvanceTrinoPoolInstance moves an instance between phases. The transition is
// validated against the lifecycle first (so an illegal move never reaches the
// database at all), then applied as a CAS on the CURRENT phase and the pool's
// authority epoch.
//
// The expected-phase CAS is what makes concurrent operators safe: whoever reads
// PENDING and writes CREATING first wins, and the loser is told it lost rather
// than overwriting a decision it never saw.
func (cs *ConfigStore) AdvanceTrinoPoolInstance(
	ctx context.Context,
	lease TrinoPoolLease,
	instanceID string,
	from, to trinopool.Phase,
	updates map[string]any,
) error {
	if err := trinopool.ValidateTransition(from, to); err != nil {
		return err
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		assignments := map[string]any{
			"phase":            string(to),
			"phase_changed_at": time.Now().UTC(),
			"owner_epoch":      lease.Epoch,
			"updated_at":       time.Now().UTC(),
		}
		for key, value := range updates {
			if _, reserved := assignments[key]; reserved {
				return fmt.Errorf("update key %q is owned by the phase transition", key)
			}
			assignments[key] = value
		}
		result := tx.Model(&TrinoPoolInstance{}).
			Where("instance_id = ? AND pool_id = ? AND phase = ?", instanceID, lease.PoolID, string(from)).
			Updates(assignments)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: instance %q is no longer in phase %s", ErrTrinoPoolConflict, instanceID, from)
		}
		return nil
	})
}

// RecordTrinoPoolInstanceFields checkpoints observed facts that are not phase
// changes: Kubernetes UIDs after a create, the coordinator's process identity
// after a probe, a Gateway incarnation after a read-back. Still fenced, because
// a superseded leader's observations are no more trustworthy than its writes.
func (cs *ConfigStore) RecordTrinoPoolInstanceFields(ctx context.Context, lease TrinoPoolLease, instanceID string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	if _, forbidden := updates["phase"]; forbidden {
		return errors.New("phase changes must go through AdvanceTrinoPoolInstance")
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		assignments := map[string]any{"updated_at": time.Now().UTC()}
		for key, value := range updates {
			assignments[key] = value
		}
		result := tx.Model(&TrinoPoolInstance{}).
			Where("instance_id = ? AND pool_id = ?", instanceID, lease.PoolID).
			Updates(assignments)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: instance %q is unknown", ErrTrinoPoolConflict, instanceID)
		}
		return nil
	})
}

// AdvanceTrinoPoolProjection moves the accepted authorization-projection
// watermark forward. It is monotonic and fenced: a stale leader cannot move it,
// and nobody can move it backwards. Serving replicas compare their own snapshot
// against this value before emitting an OPA bundle, so a regressing body is
// never produced in the first place.
func (cs *ConfigStore) AdvanceTrinoPoolProjection(ctx context.Context, lease TrinoPoolLease, revision int64, digest string) error {
	if revision < 1 {
		return errors.New("projection revision must be positive")
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		projection := TrinoPoolProjection{
			PoolID:           lease.PoolID,
			AuthorityEpoch:   lease.Epoch,
			AcceptedRevision: revision,
			AcceptedDigest:   digest,
			UpdatedAt:        time.Now().UTC(),
		}
		result := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "pool_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"authority_epoch":   lease.Epoch,
				"accepted_revision": revision,
				"accepted_digest":   digest,
				"updated_at":        time.Now().UTC(),
			}),
			// Monotonic: an older revision is not an update, it is a no-op that
			// the caller is told about.
			Where: clause.Where{Exprs: []clause.Expression{
				gorm.Expr("duckgres_trino_pool_projection.accepted_revision < ?", revision),
			}},
		}).Create(&projection)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: projection revision %d does not advance the accepted watermark", ErrTrinoPoolConflict, revision)
		}
		return nil
	})
}

// GetTrinoPoolProjection returns the accepted watermark. A zero value means no
// projection has been accepted yet.
func (cs *ConfigStore) GetTrinoPoolProjection(ctx context.Context, poolID string) (TrinoPoolProjection, error) {
	var projection TrinoPoolProjection
	err := cs.db.WithContext(ctx).Where("pool_id = ?", poolID).First(&projection).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return TrinoPoolProjection{PoolID: poolID}, nil
	}
	return projection, err
}
