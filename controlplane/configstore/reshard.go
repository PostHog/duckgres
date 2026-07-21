package configstore

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Reshard operations move an org's DuckLake metadata catalog between metadata
// stores (cnpg shard -> cnpg shard, external -> cnpg, cnpg -> external). The
// operation row is the durable state machine driven by the provisioner-side
// reshard runner; the log table is the verbose operator-facing trail the admin
// console polls incrementally. See docs/design/resharding.md.

// ReshardState is the operation lifecycle state.
type ReshardState string

const (
	ReshardStatePending   ReshardState = "pending"
	ReshardStateRunning   ReshardState = "running"
	ReshardStateSucceeded ReshardState = "succeeded"
	ReshardStateFailed    ReshardState = "failed"
	ReshardStateCancelled ReshardState = "cancelled"
)

// Terminal reports whether the state is final.
func (s ReshardState) Terminal() bool {
	return s == ReshardStateSucceeded || s == ReshardStateFailed || s == ReshardStateCancelled
}

// ReshardOperation is one metadata-store migration. The external target
// password is deliberately NOT a column — it is ephemeral (held in runner
// memory only); the row stores the AWS Secrets Manager secret NAME only.
type ReshardOperation struct {
	ID           int64  `gorm:"primaryKey" json:"id"`
	OrgID        string `gorm:"size:255;not null" json:"org_id"`
	DucklingName string `gorm:"size:255;not null" json:"duckling_name"`

	// Source metadata store: kind + shard (cnpg) or endpoint (external).
	SourceKind     string `gorm:"size:32;not null" json:"source_kind"`
	FromShard      string `gorm:"size:255;not null;default:''" json:"from_shard"`
	SourceEndpoint string `gorm:"size:512;not null;default:''" json:"source_endpoint"`
	SourceUser     string `gorm:"size:255;not null;default:''" json:"source_user"`
	// SourcePasswordSecret is the AWS SM secret NAME of an external SOURCE
	// (recorded at create from the warehouse row) so an ext→cnpg rollback can
	// re-render the original external block.
	SourcePasswordSecret string `gorm:"size:512;not null;default:''" json:"source_password_secret"`
	SourceDatabase       string `gorm:"size:255;not null;default:''" json:"source_database"`

	// Pre-reshard compaction spec state (key-absent vs explicit value), so a
	// takeover runner restores it exactly.
	CompactionWasPresent bool `gorm:"not null;default:false" json:"compaction_was_present"`
	CompactionWasEnabled bool `gorm:"not null;default:false" json:"compaction_was_enabled"`

	// Target metadata store: kind + shard (cnpg) or endpoint + SM secret name
	// (external).
	TargetKind           string `gorm:"size:32;not null" json:"target_kind"`
	ToShard              string `gorm:"size:255;not null;default:''" json:"to_shard"`
	TargetEndpoint       string `gorm:"size:512;not null;default:''" json:"target_endpoint"`
	TargetPasswordSecret string `gorm:"size:512;not null;default:''" json:"target_password_secret"`
	TargetUser           string `gorm:"size:255;not null;default:''" json:"target_user"`
	TargetDatabase       string `gorm:"size:255;not null;default:''" json:"target_database"`

	State           ReshardState `gorm:"size:32;not null;default:'pending'" json:"state"`
	Step            string       `gorm:"size:64;not null;default:''" json:"step"`
	Error           string       `gorm:"not null;default:''" json:"error"`
	CancelRequested bool         `gorm:"not null;default:false" json:"cancel_requested"`

	DrainTimeoutSeconds int64 `gorm:"not null;default:1800" json:"drain_timeout_seconds"`
	// CutoverTimeoutSeconds bounds how long the flip waits for the
	// composition to converge + the target to answer before rolling back.
	// 0 = the runner default (15m, DUCKGRES_RESHARD_FLIP_TIMEOUT override).
	CutoverTimeoutSeconds int64 `gorm:"not null;default:0" json:"cutover_timeout_seconds"`

	// Runner fencing: claiming CP instance + monotonically bumped epoch. Every
	// runner-side write is CAS-fenced on (runner_cp, runner_epoch) so a zombie
	// ex-runner's writes fail after a takeover.
	RunnerCP        string     `gorm:"size:255;not null;default:''" json:"runner_cp"`
	RunnerEpoch     int64      `gorm:"not null;default:0" json:"runner_epoch"`
	RespawnAttempts int64      `gorm:"not null;default:0" json:"respawn_attempts"`
	RunnerImage     string     `gorm:"size:1024;not null;default:''" json:"runner_image"`
	HeartbeatAt     *time.Time `json:"heartbeat_at"`

	// Maintenance-mode window: warehouse state resharding from block to
	// unblock. UnblockedAt-BlockedAt is the org's client-visible downtime.
	BlockedAt   *time.Time `json:"blocked_at"`
	UnblockedAt *time.Time `json:"unblocked_at"`

	// Copy report counters (mirrored into the end-of-op report log entry).
	TablesCopied int64 `gorm:"not null;default:0" json:"tables_copied"`
	RowsCopied   int64 `gorm:"not null;default:0" json:"rows_copied"`
	BytesCopied  int64 `gorm:"not null;default:0" json:"bytes_copied"`

	// BackupS3URI is the s3:// URI of the pre-flip pg_dump of the SOURCE
	// catalog, taken (after drain + pause-compaction, before the flip) into the
	// org's own data bucket under _reshard_catalog_backups/. Empty until the
	// backup step records it; stays empty when a best-effort backup was skipped
	// on a non-destructive direction. The safety net for a catalog lost at the
	// flip — recovery is a `pg_restore` of this artifact.
	BackupS3URI string `gorm:"size:1024;not null;default:''" json:"backup_s3_uri"`

	// PasswordURL is where the reshard-runner pod pulls the EPHEMERAL external
	// target password from: the internal-secret-authed admin endpoint on the
	// control-plane replica that took the start request and holds the password
	// in memory (http://<cp-pod-ip>:8080/api/v1/reshards/<id>/password). Only
	// the URL is persisted — the password itself is never at rest anywhere.
	// Empty for cnpg targets. Recorded so the leader reconciler can respawn a
	// crashed runner pod with the same handoff wiring (the pull still 404s if
	// the stashing replica is gone, which fails the op with a clear re-run
	// message).
	PasswordURL string `gorm:"size:1024;not null;default:''" json:"password_url"`

	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

func (ReshardOperation) TableName() string { return "duckgres_reshard_operations" }

// ReshardLogEntry is one verbose log line of an operation.
type ReshardLogEntry struct {
	ID          int64     `gorm:"primaryKey" json:"id"`
	OperationID int64     `gorm:"not null;index" json:"operation_id"`
	TS          time.Time `gorm:"column:ts;not null" json:"ts"`
	Level       string    `gorm:"size:16;not null;default:'info'" json:"level"`
	Message     string    `gorm:"not null" json:"message"`
}

func (ReshardLogEntry) TableName() string { return "duckgres_reshard_operation_log" }

// ErrReshardConflict is returned by CreateReshardOperation when the org
// already has an active (pending/running) operation.
var ErrReshardConflict = errors.New("org already has an active reshard operation")

// isUniqueViolationErr reports a Postgres 23505 unique-constraint violation
// (same SQLState-through-errors.As pattern as provisioning.isUniqueViolation).
func isUniqueViolationErr(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	return errors.As(err, &s) && s.SQLState() == "23505"
}

// ErrReshardFenced is returned by runner-fenced writes when the (runner,
// epoch) pair no longer owns the operation — the caller lost a takeover and
// must stop driving the op.
var ErrReshardFenced = errors.New("reshard operation fenced: runner no longer owns it")

// CreateReshardOperation inserts a pending operation. The partial unique
// index on (org_id) WHERE state IN (pending, running) makes a second active
// op fail; that maps to ErrReshardConflict.
func (cs *ConfigStore) CreateReshardOperation(op *ReshardOperation) error {
	op.State = ReshardStatePending
	op.CreatedAt = time.Now().UTC()
	if err := cs.db.Create(op).Error; err != nil {
		if isUniqueViolationErr(err) {
			return ErrReshardConflict
		}
		return fmt.Errorf("create reshard operation: %w", err)
	}
	return nil
}

// SetReshardOperationPasswordURL records the password pull URL on a still-
// PENDING operation (the start handler sets it right after the insert, before
// the runner pod spawns; the URL embeds the op id so it cannot be set on the
// struct before Create assigns one). CAS on state=pending — once a runner
// claimed the op the wiring is frozen.
func (cs *ConfigStore) SetReshardOperationPasswordURL(id int64, url string) error {
	res := cs.db.Model(&ReshardOperation{}).
		Where("id = ? AND state = ?", id, ReshardStatePending).
		Update("password_url", url)
	if res.Error != nil {
		return fmt.Errorf("set reshard password url: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("set reshard password url: operation %d is not pending", id)
	}
	return nil
}

func (cs *ConfigStore) SetReshardOperationRunnerImage(id int64, image string) error {
	res := cs.db.Model(&ReshardOperation{}).
		Where("id = ? AND state = ?", id, ReshardStatePending).
		Update("runner_image", image)
	if res.Error != nil {
		return fmt.Errorf("set reshard runner image: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("set reshard runner image: operation %d is not pending", id)
	}
	return nil
}

// ListActiveReshardOperations returns every pending/running operation, oldest
// first — the leader reconciler's working set (respawn dead runner pods, fail
// ops whose pod can't be kept alive).
func (cs *ConfigStore) ListActiveReshardOperations() ([]ReshardOperation, error) {
	var ops []ReshardOperation
	err := cs.db.Where("state IN ?", []ReshardState{ReshardStatePending, ReshardStateRunning}).
		Order("id").Find(&ops).Error
	if err != nil {
		return nil, fmt.Errorf("list active reshard operations: %w", err)
	}
	return ops, nil
}

// IncrementReshardRespawnAttempts durably records one reconciler intervention
// and returns the new count. The count survives leader and process changes.
func (cs *ConfigStore) IncrementReshardRespawnAttempts(id int64) (int64, error) {
	var attempts int64
	err := cs.db.Raw(`
		UPDATE duckgres_reshard_operations
		SET respawn_attempts = respawn_attempts + 1
		WHERE id = ? AND state IN (?, ?)
		RETURNING respawn_attempts`, id, ReshardStatePending, ReshardStateRunning).
		Scan(&attempts).Error
	if err != nil {
		return 0, fmt.Errorf("increment reshard respawn attempts: %w", err)
	}
	if attempts == 0 {
		return 0, fmt.Errorf("increment reshard respawn attempts: operation %d is not active", id)
	}
	return attempts, nil
}

// GetReshardOperation loads one operation by id.
func (cs *ConfigStore) GetReshardOperation(id int64) (*ReshardOperation, error) {
	var op ReshardOperation
	if err := cs.db.First(&op, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &op, nil
}

// ListReshardOperationsForOrg returns the org's operations, newest first.
func (cs *ConfigStore) ListReshardOperationsForOrg(orgID string, limit int) ([]ReshardOperation, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var ops []ReshardOperation
	if err := cs.db.Where("org_id = ?", orgID).Order("id DESC").Limit(limit).Find(&ops).Error; err != nil {
		return nil, fmt.Errorf("list reshard operations: %w", err)
	}
	return ops, nil
}

// ListReshardOperations returns operations across ALL orgs, newest first —
// the admin console's global reshards page.
func (cs *ConfigStore) ListReshardOperations(limit int) ([]ReshardOperation, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var ops []ReshardOperation
	if err := cs.db.Order("id DESC").Limit(limit).Find(&ops).Error; err != nil {
		return nil, fmt.Errorf("list reshard operations: %w", err)
	}
	return ops, nil
}

// ClaimReshardOperation atomically claims a claimable operation for a runner:
// state pending, or state running with a heartbeat older than staleAfter (a
// takeover of a dead runner). The claim bumps runner_epoch — the fence that
// invalidates every write of the previous runner. Returns the claimed op or
// nil when nothing was claimable.
func (cs *ConfigStore) ClaimReshardOperation(id int64, runnerCP string, staleAfter time.Duration) (*ReshardOperation, error) {
	now := time.Now().UTC()
	staleBefore := now.Add(-staleAfter)
	res := cs.db.Model(&ReshardOperation{}).
		Where("id = ? AND (state = ? OR (state = ? AND (heartbeat_at IS NULL OR heartbeat_at < ?)))",
			id, ReshardStatePending, ReshardStateRunning, staleBefore).
		Updates(map[string]interface{}{
			"state":        ReshardStateRunning,
			"runner_cp":    runnerCP,
			"runner_epoch": gorm.Expr("runner_epoch + 1"),
			"heartbeat_at": now,
			"started_at":   gorm.Expr("COALESCE(started_at, ?)", now),
		})
	if res.Error != nil {
		return nil, fmt.Errorf("claim reshard operation: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return nil, nil
	}
	return cs.GetReshardOperation(id)
}

// reshardFencedUpdate applies updates to a running op iff (runner, epoch)
// still owns it. RowsAffected==0 → ErrReshardFenced.
func (cs *ConfigStore) reshardFencedUpdate(id int64, runnerCP string, epoch int64, updates map[string]interface{}) error {
	res := cs.db.Model(&ReshardOperation{}).
		Where("id = ? AND state = ? AND runner_cp = ? AND runner_epoch = ?", id, ReshardStateRunning, runnerCP, epoch).
		Updates(updates)
	if res.Error != nil {
		return fmt.Errorf("reshard fenced update: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrReshardFenced
	}
	return nil
}

// UpdateReshardStep moves the op to a new step (and refreshes the heartbeat).
func (cs *ConfigStore) UpdateReshardStep(id int64, runnerCP string, epoch int64, step string) error {
	return cs.reshardFencedUpdate(id, runnerCP, epoch, map[string]interface{}{
		"step":         step,
		"heartbeat_at": time.Now().UTC(),
	})
}

// UpdateReshardFields applies arbitrary fenced column updates (blocked_at,
// counters, …) and refreshes the heartbeat.
func (cs *ConfigStore) UpdateReshardFields(id int64, runnerCP string, epoch int64, updates map[string]interface{}) error {
	updates["heartbeat_at"] = time.Now().UTC()
	return cs.reshardFencedUpdate(id, runnerCP, epoch, updates)
}

// TouchReshardHeartbeat refreshes the heartbeat; ErrReshardFenced tells the
// runner it lost a takeover and must abandon the op.
func (cs *ConfigStore) TouchReshardHeartbeat(id int64, runnerCP string, epoch int64) error {
	return cs.reshardFencedUpdate(id, runnerCP, epoch, map[string]interface{}{
		"heartbeat_at": time.Now().UTC(),
	})
}

// FinishReshardOperation moves a running op to a terminal state (fenced).
func (cs *ConfigStore) FinishReshardOperation(id int64, runnerCP string, epoch int64, state ReshardState, errMsg string) error {
	if !state.Terminal() {
		return fmt.Errorf("finish reshard operation: %q is not terminal", state)
	}
	return cs.reshardFencedUpdate(id, runnerCP, epoch, map[string]interface{}{
		"state":       state,
		"error":       errMsg,
		"finished_at": time.Now().UTC(),
	})
}

// FinalizeReshardOperation atomically admits traffic and marks the operation
// succeeded. Neither state may become visible without the other: a crash after
// only one write would otherwise leave an unsafe takeover or a stranded org.
func (cs *ConfigStore) FinalizeReshardOperation(id int64, runnerCP string, epoch int64, orgID string, warehouseUpdates map[string]interface{}, unblockedAt time.Time) error {
	return cs.db.Transaction(func(tx *gorm.DB) error {
		warehouse := tx.Model(&ManagedWarehouse{}).
			Where("org_id = ? AND state = ?", orgID, ManagedWarehouseStateResharding).
			Updates(warehouseUpdates)
		if warehouse.Error != nil {
			return fmt.Errorf("finalize reshard warehouse: %w", warehouse.Error)
		}
		if warehouse.RowsAffected == 0 {
			return fmt.Errorf("warehouse %q expected state %q: %w", orgID, ManagedWarehouseStateResharding, ErrWarehouseStateMismatch)
		}

		finished := tx.Model(&ReshardOperation{}).
			Where("id = ? AND state = ? AND runner_cp = ? AND runner_epoch = ?", id, ReshardStateRunning, runnerCP, epoch).
			Updates(map[string]interface{}{
				"state":        ReshardStateSucceeded,
				"error":        "",
				"unblocked_at": unblockedAt,
				"finished_at":  unblockedAt,
				"heartbeat_at": unblockedAt,
			})
		if finished.Error != nil {
			return fmt.Errorf("finalize reshard operation: %w", finished.Error)
		}
		if finished.RowsAffected == 0 {
			return ErrReshardFenced
		}
		return nil
	})
}

// FinishPendingReshardOperation terminates an UNCLAIMED (pending) op — the
// cancel path for an op no runner picked up yet. Not fenced (there is no
// runner); CAS on state=pending.
func (cs *ConfigStore) FinishPendingReshardOperation(id int64, state ReshardState, errMsg string) (bool, error) {
	if !state.Terminal() {
		return false, fmt.Errorf("finish pending reshard operation: %q is not terminal", state)
	}
	res := cs.db.Model(&ReshardOperation{}).
		Where("id = ? AND state = ?", id, ReshardStatePending).
		Updates(map[string]interface{}{
			"state":       state,
			"error":       errMsg,
			"finished_at": time.Now().UTC(),
		})
	if res.Error != nil {
		return false, fmt.Errorf("finish pending reshard operation: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// RequestReshardCancel flags a pending/running op for cancellation. The
// runner honors it between steps and inside wait loops. Returns false when
// the op is already terminal.
func (cs *ConfigStore) RequestReshardCancel(id int64) (bool, error) {
	res := cs.db.Model(&ReshardOperation{}).
		Where("id = ? AND state IN ?", id, []ReshardState{ReshardStatePending, ReshardStateRunning}).
		Update("cancel_requested", true)
	if res.Error != nil {
		return false, fmt.Errorf("request reshard cancel: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// AppendReshardLog appends one operator-visible log line. Best-effort at the
// call sites — a log failure never fails the operation step itself.
func (cs *ConfigStore) AppendReshardLog(opID int64, level, message string) error {
	entry := ReshardLogEntry{
		OperationID: opID,
		TS:          time.Now().UTC(),
		Level:       level,
		Message:     message,
	}
	if err := cs.db.Create(&entry).Error; err != nil {
		return fmt.Errorf("append reshard log: %w", err)
	}
	return nil
}

// ListReshardLog returns log entries with id > afterID, oldest first — the
// incremental poll contract of the admin console (pass the last seen id).
func (cs *ConfigStore) ListReshardLog(opID, afterID int64, limit int) ([]ReshardLogEntry, error) {
	if limit <= 0 || limit > 2000 {
		limit = 500
	}
	var entries []ReshardLogEntry
	err := cs.db.Where("operation_id = ? AND id > ?", opID, afterID).
		Order("id").Limit(limit).Find(&entries).Error
	if err != nil {
		return nil, fmt.Errorf("list reshard log: %w", err)
	}
	return entries, nil
}

// ListWorkerRecordsForOrg returns the org's non-terminal worker records —
// the reshard drain's "workers gone" check (a live worker runs a
// DuckLakeCheckpointer that writes the catalog independent of sessions) and
// the source of hot-idle records the runner retires after the drain grace.
func (cs *ConfigStore) ListWorkerRecordsForOrg(orgID string) ([]WorkerRecord, error) {
	var records []WorkerRecord
	err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("org_id = ? AND state NOT IN ?", orgID, []WorkerState{WorkerStateRetired, WorkerStateLost}).
		Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("list worker records for org: %w", err)
	}
	return records, nil
}

// SetWarehouseResharding CAS-flips the warehouse ready→resharding INSIDE a
// transaction that holds the org's connection advisory lock — the same lock
// the lease-grant path takes. After this commits, no new connection lease can
// ever be granted for the org: any in-flight grant either committed before us
// (and shows up as an active lease the drain waits out) or runs after us and
// sees state=resharding.
func (cs *ConfigStore) SetWarehouseResharding(orgID string) error {
	return cs.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org-connections:"+orgID)).Error; err != nil {
			return err
		}
		res := tx.Model(&ManagedWarehouse{}).
			Where("org_id = ? AND state = ?", orgID, ManagedWarehouseStateReady).
			Updates(map[string]interface{}{
				"state":          ManagedWarehouseStateResharding,
				"status_message": "metadata-store reshard in progress",
			})
		if res.Error != nil {
			return fmt.Errorf("set warehouse resharding: %w", res.Error)
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("warehouse %q expected state %q: %w", orgID, ManagedWarehouseStateReady, ErrWarehouseStateMismatch)
		}
		return nil
	})
}

// OrgConnectionDrainStatus is the cluster-wide drain picture for one org.
type OrgConnectionDrainStatus struct {
	ActiveLeases int64
	QueuedConns  int64
}

// Drained reports whether nothing is connected or waiting to connect.
func (d OrgConnectionDrainStatus) Drained() bool {
	return d.ActiveLeases == 0 && d.QueuedConns == 0
}

// OrgConnectionDrainState reads, in one statement, the org's active leases and
// pending queue rows. The org admission lock keeps the pending-to-lease
// transition stable while both categories are classified. Both counts must be
// zero for a sound drain barrier; the queue mirror for an active lease is not
// counted a second time.
func (cs *ConfigStore) OrgConnectionDrainState(orgID string) (OrgConnectionDrainStatus, error) {
	tables := cs.orgConnectionRuntimeTables()
	var status OrgConnectionDrainStatus
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		// The drain loop may be the only remaining actor after a request owner
		// dies. Run the same serialized cleanup as admission so expired owners
		// cannot wedge resharding indefinitely.
		if err := lockOrgConnectionAdmission(tx, orgID); err != nil {
			return err
		}
		now, err := cs.orgConnectionDatabaseNow(tx)
		if err != nil {
			return err
		}
		if err := cs.cleanupOrgConnectionRowsLocked(tx, orgID, now); err != nil {
			return err
		}
		return tx.Raw(
			"SELECT "+
				"(SELECT COUNT(*) FROM "+tables.lease+" WHERE org_id = ?) AS active_leases, "+
				"(SELECT COUNT(*) FROM "+tables.queue+" WHERE org_id = ? AND granted_at IS NULL) AS queued_conns",
			orgID,
			orgID,
		).Scan(&status).Error
	})
	if err != nil {
		return OrgConnectionDrainStatus{}, fmt.Errorf("org connection drain state: %w", err)
	}
	return status, nil
}

// ExternalMetadataStoreInfo is one distinct external Postgres metadata store
// currently referenced by a live managed warehouse — offered by the reshard
// form as a known cnpg→external target (endpoint + SM secret NAME only; the
// password is never stored anywhere in duckgres).
type ExternalMetadataStoreInfo struct {
	Endpoint          string `json:"endpoint"`
	PasswordAWSSecret string `json:"password_aws_secret"`
	User              string `json:"user"`
	Database          string `json:"database"`
}

// ListExternalMetadataStores returns the distinct external metadata stores of
// non-deleted warehouses, ordered by endpoint.
func (cs *ConfigStore) ListExternalMetadataStores() ([]ExternalMetadataStoreInfo, error) {
	var stores []ExternalMetadataStoreInfo
	err := cs.db.Model(&ManagedWarehouse{}).
		Select("DISTINCT metadata_store_endpoint AS endpoint, metadata_store_password_aws_secret AS password_aws_secret, metadata_store_username AS user, metadata_store_database_name AS database").
		Where("metadata_store_kind = ? AND metadata_store_endpoint <> '' AND state NOT IN ?",
			MetadataStoreKindExternal,
			[]ManagedWarehouseProvisioningState{ManagedWarehouseStateDeleting, ManagedWarehouseStateDeleted}).
		Order("endpoint").
		Scan(&stores).Error
	if err != nil {
		return nil, fmt.Errorf("list external metadata stores: %w", err)
	}
	return stores, nil
}
