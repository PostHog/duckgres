package configstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/posthog/duckgres/controlplane/trinopool"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const TrinoPoolRecoveryBlockedMessage = "Recovery is blocked; inspect the control-plane logs before retrying."

var (
	ErrTrinoPoolRecoveryInvalid  = errors.New("invalid trino pool recovery request")
	ErrTrinoPoolRecoveryConflict = errors.New("trino pool recovery conflict")
	trinoPoolRecoveryOperationID = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
)

// TrinoPoolRecovery is immutable authorization to discard one exact instance's
// retained work. The pool leader performs the lifecycle effects separately.
type TrinoPoolRecovery struct {
	OperationID              string    `gorm:"primaryKey;column:operation_id"`
	PoolID                   string    `gorm:"column:pool_id"`
	InstanceID               string    `gorm:"column:instance_id"`
	ExpectedGeneration       int64     `gorm:"column:expected_generation"`
	Incarnation              string    `gorm:"column:incarnation"`
	PodUID                   string    `gorm:"column:pod_uid"`
	BootID                   string    `gorm:"column:boot_id"`
	NodeID                   string    `gorm:"column:node_id"`
	CoordinatorID            string    `gorm:"column:coordinator_id"`
	RequestedBy              string    `gorm:"column:requested_by"`
	Reason                   string    `gorm:"column:reason"`
	DestructiveAuthorization bool      `gorm:"column:destructive_authorization"`
	CreatedAt                time.Time `gorm:"column:created_at"`
}

func (TrinoPoolRecovery) TableName() string { return "duckgres_trino_pool_recoveries" }

func (r TrinoPoolRecovery) validate() error {
	if !r.DestructiveAuthorization || r.ExpectedGeneration <= 0 || r.ExpectedGeneration > math.MaxInt64-4 {
		return fmt.Errorf("%w: explicit destructive authorization and a generation with room for retirement are required", ErrTrinoPoolRecoveryInvalid)
	}
	for field, value := range map[string]string{
		"pool": r.PoolID, "instance": r.InstanceID, "incarnation": r.Incarnation,
		"pod": r.PodUID, "boot": r.BootID, "node": r.NodeID, "coordinator": r.CoordinatorID,
	} {
		if strings.TrimSpace(value) == "" || len(value) > 256 {
			return fmt.Errorf("%w: %s identity is required and must be at most 256 bytes", ErrTrinoPoolRecoveryInvalid, field)
		}
	}
	if !trinoPoolRecoveryOperationID.MatchString(r.OperationID) ||
		strings.TrimSpace(r.RequestedBy) == "" || len(r.RequestedBy) > 320 ||
		strings.TrimSpace(r.Reason) == "" || len(r.Reason) > 256 {
		return fmt.Errorf("%w: operation, authenticated actor and bounded reason are required", ErrTrinoPoolRecoveryInvalid)
	}
	return nil
}

func (r TrinoPoolRecovery) sameIntent(other TrinoPoolRecovery) bool {
	r.CreatedAt = time.Time{}
	other.CreatedAt = time.Time{}
	return r == other
}

func (r TrinoPoolRecovery) matches(instance TrinoPoolInstance) bool {
	return instance.PoolID == r.PoolID && instance.InstanceID == r.InstanceID &&
		instance.Phase == string(trinopool.PhaseDraining) &&
		instance.GatewayGeneration == r.ExpectedGeneration && instance.GatewayIncarnation == r.Incarnation &&
		instance.CoordinatorPodUID == r.PodUID && instance.CoordinatorBootID == r.BootID &&
		instance.CoordinatorNodeID == r.NodeID && instance.CoordinatorID == r.CoordinatorID
}

// RequestTrinoPoolRecovery records authorization without borrowing or acquiring
// operator authority. Pool-first locking serializes the snapshot with lifecycle
// writes; only the active operator may act on the resulting request.
func (cs *ConfigStore) RequestTrinoPoolRecovery(ctx context.Context, poolID, instanceID string, request TrinoPoolRecovery) (*TrinoPoolRecovery, error) {
	if (request.PoolID != "" && request.PoolID != poolID) || (request.InstanceID != "" && request.InstanceID != instanceID) {
		return nil, fmt.Errorf("%w: request scope does not match the target", ErrTrinoPoolRecoveryInvalid)
	}
	request.PoolID, request.InstanceID = poolID, instanceID
	if err := request.validate(); err != nil {
		return nil, err
	}
	var result TrinoPoolRecovery
	err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pool, err := lockTrinoPool(ctx, tx, poolID)
		if err != nil {
			if errors.Is(err, ErrTrinoPoolConflict) {
				return fmt.Errorf("%w: target pool is unavailable", ErrTrinoPoolRecoveryConflict)
			}
			return err
		}
		var existing TrinoPoolRecovery
		err = tx.Where("operation_id = ? OR instance_id = ?", request.OperationID, instanceID).First(&existing).Error
		if err == nil {
			if !existing.sameIntent(request) {
				return fmt.Errorf("%w: an immutable request already uses this operation or instance", ErrTrinoPoolRecoveryConflict)
			}
			result = existing
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var instance TrinoPoolInstance
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("pool_id = ? AND instance_id = ?", poolID, instanceID).First(&instance).Error
		if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && !request.matches(instance)) {
			return fmt.Errorf("%w: the draining instance no longer matches the approved snapshot", ErrTrinoPoolRecoveryConflict)
		}
		if err != nil {
			return err
		}
		var serving int64
		if err := tx.Model(&TrinoPoolInstance{}).Where("pool_id = ? AND phase = ?", poolID, trinopool.PhaseServing).Count(&serving).Error; err != nil {
			return err
		}
		if serving < int64(pool.MinServing) {
			return fmt.Errorf("%w: pool is below its minimum serving count", ErrTrinoPoolRecoveryConflict)
		}
		request.CreatedAt = time.Now().UTC()
		if err := tx.Clauses(clause.Returning{}).Create(&request).Error; err != nil {
			return err
		}
		result = request
		return nil
	})
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return nil, fmt.Errorf("%w: operation or instance already has a recovery request", ErrTrinoPoolRecoveryConflict)
		}
		return nil, err
	}
	return &result, nil
}

func (cs *ConfigStore) GetTrinoPoolRecovery(ctx context.Context, poolID, instanceID string) (*TrinoPoolRecovery, error) {
	var request TrinoPoolRecovery
	err := cs.db.WithContext(ctx).Where("pool_id = ? AND instance_id = ?", poolID, instanceID).First(&request).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &request, nil
}

func (cs *ConfigStore) ListTrinoPoolRecoveries(ctx context.Context, poolID string) ([]TrinoPoolRecovery, error) {
	var requests []TrinoPoolRecovery
	// Retain completed authorizations for audit without polling their history
	// every reconcile tick. The instance predicate matches the live index.
	err := cs.db.WithContext(ctx).Model(&TrinoPoolRecovery{}).
		Select("duckgres_trino_pool_recoveries.*").
		Joins("JOIN duckgres_trino_pool_instances AS instance ON instance.instance_id = duckgres_trino_pool_recoveries.instance_id AND instance.pool_id = duckgres_trino_pool_recoveries.pool_id").
		Where("instance.pool_id = ? AND instance.phase NOT IN ('RETIRED', 'FAILURE_RETIRED')", poolID).
		Order("duckgres_trino_pool_recoveries.created_at, duckgres_trino_pool_recoveries.operation_id").Find(&requests).Error
	return requests, err
}
