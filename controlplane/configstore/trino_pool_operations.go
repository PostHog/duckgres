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
func (cs *ConfigStore) RecordTrinoPoolOperationStep(ctx context.Context, lease TrinoPoolLease, operationID, stepID, payloadHash, outcome, result string) (TrinoPoolOperationStep, error) {
	if operationID == "" || stepID == "" || payloadHash == "" {
		return TrinoPoolOperationStep{}, errors.New("trino pool step requires an operation, step id and payload hash")
	}
	if result == "" {
		result = "{}"
	}
	var step TrinoPoolOperationStep
	// Fenced like every other lifecycle write: a superseded leader recording
	// step outcomes into a live operation would make the read-back path report
	// its abandoned attempt as the operation's result.
	err := cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		existing := TrinoPoolOperationStep{}
		err := tx.Where("operation_id = ? AND step_id = ?", operationID, stepID).First(&existing).Error
		switch {
		case err == nil:
			if existing.PayloadHash != payloadHash {
				return fmt.Errorf("%w: operation %q step %q", ErrTrinoPoolIntentChanged, operationID, stepID)
			}
			// A step is recorded UNKNOWN before the effect and re-recorded with
			// the outcome after it. Returning the stored row unchanged made that
			// second call a no-op, so a step could never leave UNKNOWN and the
			// cross-leader read-back that keys on OK was unreachable: every
			// retry re-called the external system and relied on ITS replay
			// guard instead of this journal.
			//
			// An outcome only ever advances out of UNKNOWN. A terminal outcome
			// is never overwritten - re-deciding a recorded OK or FAILED is
			// exactly the rewriting of history the journal exists to prevent.
			if existing.Outcome == TrinoPoolStepOutcomeUnknown && outcome != "" && outcome != TrinoPoolStepOutcomeUnknown {
				if err := tx.Model(&TrinoPoolOperationStep{}).
					Where("operation_id = ? AND step_id = ?", operationID, stepID).
					Updates(map[string]any{
						"outcome":     outcome,
						"result":      result,
						"recorded_at": time.Now().UTC(),
					}).Error; err != nil {
					return err
				}
				existing.Outcome = outcome
				existing.Result = result
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

// RecordTrinoPoolTenantPrincipals checkpoints the binding a tenant's principals
// were last published under.
//
// It is durable because "already published" cannot live in a leader's memory: a
// restart or a leadership move would either republish every tenant blindly or,
// worse, treat an unpublished tenant as done.
func (cs *ConfigStore) RecordTrinoPoolTenantPrincipals(ctx context.Context, lease TrinoPoolLease, poolID, orgID, principalRevision string) error {
	if orgID == "" || principalRevision == "" {
		return errors.New("a principal publication requires an org and a revision")
	}
	if poolID != lease.PoolID {
		return fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Exec(`
			INSERT INTO duckgres_trino_pool_publications
				(pool_id, org_id, principal_revision, state, gateway_receipt)
			VALUES (?, ?, ?, ?, '{}')
			ON CONFLICT (pool_id, org_id) DO UPDATE SET
				principal_revision = EXCLUDED.principal_revision,
				-- A revoked tenant that is published again is live again; any
				-- other state stays where it was, because publishing a binding
				-- is not an admission.
				state = CASE WHEN duckgres_trino_pool_publications.state = ?
					THEN ? ELSE duckgres_trino_pool_publications.state END,
				last_error = '',
				updated_at = now()`,
			poolID, orgID, principalRevision, TrinoPublicationPublished,
			TrinoPublicationRevoked, TrinoPublicationPublished).Error
	})
}

// RecordTrinoPoolPublicationOpen checkpoints an OPEN barrier.
//
// The identity is recorded BEFORE the Gateway call that creates it, so a lost
// response is resolved by reading that publication back rather than by opening
// a second barrier for the same tenant - which the Gateway refuses anyway, and
// which would leave the first one open forever.
func (cs *ConfigStore) RecordTrinoPoolPublicationOpen(ctx context.Context, lease TrinoPoolLease, poolID, orgID, publicationID, targetRevision string) error {
	if orgID == "" || publicationID == "" || targetRevision == "" {
		return errors.New("an open publication requires an org, a publication id and a target revision")
	}
	if poolID != lease.PoolID {
		return fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Exec(`
			INSERT INTO duckgres_trino_pool_publications
				(pool_id, org_id, publication_id, target_revision, state, gateway_receipt)
			VALUES (?, ?, ?, ?, ?, '{}')
			ON CONFLICT (pool_id, org_id) DO UPDATE SET
				publication_id = EXCLUDED.publication_id,
				target_revision = EXCLUDED.target_revision,
				state = EXCLUDED.state,
				updated_at = now()`,
			poolID, orgID, publicationID, targetRevision, TrinoPublicationAdmitting).Error
	})
}

// BeginTrinoPoolPublicationAttempt bumps a tenant's occurrence counter and
// returns the new value.
//
// Every durable step identity for that tenant carries it, so an attempt that
// follows an abandoned barrier - or a second revocation after the tenant was
// re-enabled - is a NEW operation. Reusing the identity would replay the first
// attempt's recorded outcome and leave the current intent unapplied, which for
// a revocation means a tenant nobody revoked stays admitted.
func (cs *ConfigStore) BeginTrinoPoolPublicationAttempt(ctx context.Context, lease TrinoPoolLease, poolID, orgID string) (int64, error) {
	if orgID == "" {
		return 0, errors.New("a publication attempt requires an org")
	}
	if poolID != lease.PoolID {
		return 0, fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	var attempt int64
	err := cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Raw(`
			INSERT INTO duckgres_trino_pool_publications (pool_id, org_id, attempt, state, gateway_receipt)
			VALUES (?, ?, 1, ?, '{}')
			ON CONFLICT (pool_id, org_id) DO UPDATE SET
				attempt = duckgres_trino_pool_publications.attempt + 1,
				updated_at = now()
			RETURNING attempt`,
			poolID, orgID, TrinoPublicationPending).Scan(&attempt).Error
	})
	return attempt, err
}

// RecordTrinoPoolPublicationFailure records a tenant's failed attempt and the
// wait it earned, so one unserviceable warehouse cannot busy-loop or starve the
// tenants the driver would otherwise reach after it.
func (cs *ConfigStore) RecordTrinoPoolPublicationFailure(ctx context.Context, lease TrinoPoolLease, poolID, orgID string, nextAttemptAt time.Time, lastError string) error {
	if poolID != lease.PoolID {
		return fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Exec(`
			INSERT INTO duckgres_trino_pool_publications
				(pool_id, org_id, attempts, next_attempt_at, last_error, state, gateway_receipt)
			VALUES (?, ?, 1, ?, ?, ?, '{}')
			ON CONFLICT (pool_id, org_id) DO UPDATE SET
				attempts = duckgres_trino_pool_publications.attempts + 1,
				next_attempt_at = EXCLUDED.next_attempt_at,
				last_error = EXCLUDED.last_error,
				updated_at = now()`,
			poolID, orgID, nextAttemptAt.UTC(), lastError, TrinoPublicationPending).Error
	})
}

// ClearTrinoPoolPublicationFailure clears a tenant's backoff after a step that
// worked, so a tenant that recovers is not held behind a wait it no longer
// deserves.
func (cs *ConfigStore) ClearTrinoPoolPublicationFailure(ctx context.Context, lease TrinoPoolLease, poolID, orgID string) error {
	if poolID != lease.PoolID {
		return fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Model(&TrinoPoolPublication{}).
			Where("pool_id = ? AND org_id = ? AND (attempts > 0 OR next_attempt_at IS NOT NULL)", poolID, orgID).
			Updates(map[string]any{
				"attempts":        0,
				"next_attempt_at": nil,
				"last_error":      "",
				"updated_at":      time.Now().UTC(),
			}).Error
	})
}

// RecordTrinoPoolPublicationCommitted checkpoints a COMMITTED barrier.
//
// The Gateway's record is authoritative from the moment it commits, so this is
// a checkpoint of something already true and never a retraction point: recovery
// after an interrupted checkpoint re-reads the Gateway and completes.
func (cs *ConfigStore) RecordTrinoPoolPublicationCommitted(ctx context.Context, lease TrinoPoolLease, poolID, orgID, targetRevision, receipt string) error {
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
				"admitted_target_revision": targetRevision,
				"state":                    TrinoPublicationAdmitted,
				"gateway_receipt":          receipt,
				"last_error":               "",
				"updated_at":               time.Now().UTC(),
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

// RecordTrinoPoolTenantRevoked checkpoints a withdrawn admission. The row is
// kept: deleting it would read as "never published", and the next tick would
// republish the binding of a tenant that is meant to be gone.
func (cs *ConfigStore) RecordTrinoPoolTenantRevoked(ctx context.Context, lease TrinoPoolLease, poolID, orgID, reason string) error {
	if poolID != lease.PoolID {
		return fmt.Errorf("%w: publication belongs to pool %q", ErrTrinoPoolConflict, poolID)
	}
	return cs.withPoolAuthority(ctx, lease, func(tx *gorm.DB, _ *TrinoPool) error {
		return tx.Model(&TrinoPoolPublication{}).
			Where("pool_id = ? AND org_id = ?", poolID, orgID).
			Updates(map[string]any{
				"state":                    TrinoPublicationRevoked,
				"admitted_target_revision": "",
				"publication_id":           "",
				"target_revision":          "",
				"last_error":               reason,
				"updated_at":               time.Now().UTC(),
			}).Error
	})
}

// ListTrinoPoolPublications returns every tenant this pool has published,
// including revoked ones - which is what lets a tenant that disappeared from
// the projection be revoked exactly once rather than every tick.
func (cs *ConfigStore) ListTrinoPoolPublications(ctx context.Context, poolID string) ([]TrinoPoolPublication, error) {
	var publications []TrinoPoolPublication
	err := cs.db.WithContext(ctx).Where("pool_id = ?", poolID).Order("org_id").Find(&publications).Error
	return publications, err
}
