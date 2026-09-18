package configstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Durable operations and their steps.
//
// The whole point of these rows is that an external effect is preceded by a
// recorded intent. When a response is lost, the operator does not guess and it
// does not mint a new identity: it reads the same operation back and continues.

// BeginTrinoPoolOperation records an intent, or returns the existing one when
// the identical intent was already recorded. A reused id with a different
// intent hash is a conflict — applying it would perform an effect nobody
// recorded, and silently overwriting it would erase the original.
func (cs *ConfigStore) BeginTrinoPoolOperation(ctx context.Context, lease TrinoPoolLease, spec TrinoPoolOperationSpec) (TrinoPoolOperation, error) {
	if spec.OperationID == "" || spec.IntentHash == "" {
		return TrinoPoolOperation{}, errors.New("trino pool operation requires an id and intent hash")
	}
	if spec.PoolID != lease.PoolID {
		return TrinoPoolOperation{}, fmt.Errorf("%w: operation belongs to pool %q", ErrTrinoPoolConflict, spec.PoolID)
	}
	switch spec.Kind {
	case TrinoPoolOperationReplace, TrinoPoolOperationScaleUp, TrinoPoolOperationRepair,
		TrinoPoolOperationRetire, TrinoPoolOperationPublish:
	default:
		return TrinoPoolOperation{}, fmt.Errorf("unsupported trino pool operation kind %q", spec.Kind)
	}

	var operation TrinoPoolOperation
	err := cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		existing := TrinoPoolOperation{}
		err := tx.Where("operation_id = ?", spec.OperationID).First(&existing).Error
		switch {
		case err == nil:
			if existing.IntentHash != spec.IntentHash {
				return fmt.Errorf("%w: operation %q", ErrTrinoPoolIntentChanged, spec.OperationID)
			}
			existing.Replayed = true
			operation = existing
			return nil
		case !errors.Is(err, gorm.ErrRecordNotFound):
			return err
		}

		created := TrinoPoolOperation{
			OperationID: spec.OperationID,
			PoolID:      spec.PoolID,
			InstanceID:  spec.InstanceID,
			Kind:        spec.Kind,
			IntentHash:  spec.IntentHash,
			OwnerEpoch:  lease.Epoch,
			Phase:       "pending",
			Receipts:    "{}",
		}
		if err := tx.Create(&created).Error; err != nil {
			return err
		}
		operation = created
		return nil
	})
	return operation, err
}

// GetTrinoPoolOperation reads an operation back. This is the answer to a lost
// response: the recorded outcome, not a fresh attempt.
func (cs *ConfigStore) GetTrinoPoolOperation(ctx context.Context, operationID string) (*TrinoPoolOperation, error) {
	var operation TrinoPoolOperation
	err := cs.db.WithContext(ctx).Where("operation_id = ?", operationID).First(&operation).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &operation, nil
}

// ListOpenTrinoPoolOperations returns the operations a new leader must resume.
func (cs *ConfigStore) ListOpenTrinoPoolOperations(ctx context.Context, poolID string) ([]TrinoPoolOperation, error) {
	var operations []TrinoPoolOperation
	err := cs.db.WithContext(ctx).
		Where("pool_id = ? AND terminal_at IS NULL", poolID).
		Order("created_at, operation_id").Find(&operations).Error
	return operations, err
}

// RecordTrinoPoolOperationStep records one step's outcome, idempotently. A
// replay with the identical payload returns the recorded result — that is how a
// lost Gateway response becomes a lookup rather than a second mutation. A
// different payload under the same step id is a conflict.
func (cs *ConfigStore) RecordTrinoPoolOperationStep(ctx context.Context, operationID, stepID, payloadHash, outcome, result string) (TrinoPoolOperationStep, error) {
	if operationID == "" || stepID == "" || payloadHash == "" {
		return TrinoPoolOperationStep{}, errors.New("trino pool step requires an operation, step id and payload hash")
	}
	if result == "" {
		result = "{}"
	}
	var step TrinoPoolOperationStep
	err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		existing := TrinoPoolOperationStep{}
		err := tx.Where("operation_id = ? AND step_id = ?", operationID, stepID).First(&existing).Error
		switch {
		case err == nil:
			if existing.PayloadHash != payloadHash {
				return fmt.Errorf("%w: operation %q step %q", ErrTrinoPoolIntentChanged, operationID, stepID)
			}
			existing.Replayed = true
			step = existing
			return nil
		case !errors.Is(err, gorm.ErrRecordNotFound):
			return err
		}
		created := TrinoPoolOperationStep{
			OperationID: operationID,
			StepID:      stepID,
			PayloadHash: payloadHash,
			Outcome:     outcome,
			Result:      result,
			RecordedAt:  time.Now().UTC(),
		}
		if err := tx.Create(&created).Error; err != nil {
			return err
		}
		step = created
		return nil
	})
	return step, err
}

// UpdateTrinoPoolOperation checkpoints progress, an error, or the next attempt
// time. Attempts and next_attempt_at are persisted rather than kept in the
// leader's memory so a restart does not reset a backoff to zero.
func (cs *ConfigStore) UpdateTrinoPoolOperation(ctx context.Context, lease TrinoPoolLease, operationID string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		assignments := map[string]any{"updated_at": time.Now().UTC()}
		for key, value := range updates {
			assignments[key] = value
		}
		result := tx.Model(&TrinoPoolOperation{}).
			Where("operation_id = ? AND pool_id = ? AND terminal_at IS NULL", operationID, lease.PoolID).
			Updates(assignments)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: operation %q is unknown or already terminal", ErrTrinoPoolConflict, operationID)
		}
		return nil
	})
}

// FinishTrinoPoolOperation marks an operation terminal. A timeout is NOT a
// terminal success: callers pass the outcome they actually observed.
func (cs *ConfigStore) FinishTrinoPoolOperation(ctx context.Context, lease TrinoPoolLease, operationID, phase, lastError string) error {
	return cs.UpdateTrinoPoolOperation(ctx, lease, operationID, map[string]any{
		"phase":       phase,
		"last_error":  lastError,
		"terminal_at": time.Now().UTC(),
	})
}

// SetTrinoPoolPublicationDesired records that a warehouse SHOULD be published at
// a revision. Desired is not admitted: the tenant cannot query until the
// Gateway's publication barrier commits and that outcome is checkpointed here.
func (cs *ConfigStore) SetTrinoPoolPublicationDesired(ctx context.Context, lease TrinoPoolLease, poolID, orgID string, revision int64) error {
	if orgID == "" {
		return errors.New("publication requires an org")
	}
	if poolID != lease.PoolID {
		return fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Exec(`
			INSERT INTO duckgres_trino_pool_publications (pool_id, org_id, desired_revision, state, gateway_receipt)
			VALUES (?, ?, ?, ?, '{}')
			ON CONFLICT (pool_id, org_id) DO UPDATE SET
				desired_revision = GREATEST(duckgres_trino_pool_publications.desired_revision, EXCLUDED.desired_revision),
				updated_at = now()`,
			poolID, orgID, revision, TrinoPublicationPending).Error
	})
}

// RecordTrinoPoolPublicationAdmitted checkpoints a COMMITTED Gateway barrier.
// The Gateway receipt is authoritative from the moment it commits, so this is a
// checkpoint of something already true — never a retraction point. Recovery
// after an interrupted checkpoint re-reads the Gateway and completes; it does
// not close a gate the Gateway has opened.
func (cs *ConfigStore) RecordTrinoPoolPublicationAdmitted(ctx context.Context, lease TrinoPoolLease, poolID, orgID string, revision int64, publicationID, receipt string) error {
	if poolID != lease.PoolID {
		return fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	if receipt == "" {
		receipt = "{}"
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		result := tx.Model(&TrinoPoolPublication{}).
			Where("pool_id = ? AND org_id = ?", poolID, orgID).
			Updates(map[string]any{
				"admitted_revision":  revision,
				"published_revision": revision,
				"publication_id":     publicationID,
				"state":              TrinoPublicationAdmitted,
				"gateway_receipt":    receipt,
				"last_error":         "",
				"updated_at":         time.Now().UTC(),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: publication for org %q is unknown", ErrTrinoPoolConflict, orgID)
		}
		return nil
	})
}

// GetTrinoPoolPublication returns one warehouse's publication state, or nil.
func (cs *ConfigStore) GetTrinoPoolPublication(ctx context.Context, poolID, orgID string) (*TrinoPoolPublication, error) {
	var publication TrinoPoolPublication
	err := cs.db.WithContext(ctx).Where("pool_id = ? AND org_id = ?", poolID, orgID).First(&publication).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &publication, nil
}
