package configstore

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// OrgUserKey is the composite key for org-scoped user lookups.
type OrgUserKey struct {
	OrgID    string
	Username string
}

var ErrWorkerOwnerEpochMismatch = errors.New("worker owner epoch mismatch")

// Snapshot holds a point-in-time copy of all config data for fast lookups.
type Snapshot struct {
	Orgs            map[string]*OrgConfig
	DatabaseOrg     map[string]string     // database name -> org ID
	OrgUserPassword map[OrgUserKey]string // (orgID, username) -> bcrypt hash
	Global          GlobalConfig
	DuckLake        DuckLakeConfig
	RateLimit       RateLimitConfig
	QueryLog        QueryLogConfig
}

// ConfigStore manages configuration stored in a PostgreSQL database.
type ConfigStore struct {
	db            *gorm.DB
	runtimeSchema string
	mu            sync.RWMutex
	snapshot      *Snapshot
	pollInterval  time.Duration
	onChange      []func(old, new *Snapshot)
}

// NewConfigStore connects to the PostgreSQL config store, runs migrations,
// ensures singleton rows exist, and loads the initial snapshot.
func NewConfigStore(connStr string, pollInterval time.Duration) (*ConfigStore, error) {
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	db, err := gorm.Open(postgres.Open(connStr), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("connect to config store: %w", err)
	}

	// Migrate OrgUser PK from (username) to (org_id, username) if needed.
	// GORM AutoMigrate cannot alter primary keys, so we do it manually.
	if err := migrateOrgUserPK(db); err != nil {
		return nil, fmt.Errorf("migrate org user PK: %w", err)
	}

	// Auto-migrate all models
	if err := db.AutoMigrate(
		&Org{},
		&ManagedWarehouse{},
		&OrgUser{},
		&GlobalConfig{},
		&DuckLakeConfig{},
		&RateLimitConfig{},
		&QueryLogConfig{},
	); err != nil {
		return nil, fmt.Errorf("auto-migrate config store: %w", err)
	}

	runtimeSchema, err := resolveRuntimeSchema(db)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime schema: %w", err)
	}
	if err := ensureRuntimeSchema(db, runtimeSchema); err != nil {
		return nil, fmt.Errorf("ensure runtime schema: %w", err)
	}
	if err := autoMigrateRuntimeTables(db, runtimeSchema); err != nil {
		return nil, fmt.Errorf("auto-migrate runtime schema: %w", err)
	}

	// Ensure singleton rows exist with defaults
	db.FirstOrCreate(&GlobalConfig{}, GlobalConfig{ID: 1})
	db.FirstOrCreate(&DuckLakeConfig{}, DuckLakeConfig{ID: 1})
	db.FirstOrCreate(&RateLimitConfig{}, RateLimitConfig{ID: 1})
	db.FirstOrCreate(&QueryLogConfig{}, QueryLogConfig{ID: 1})

	cs := &ConfigStore{
		db:            db,
		runtimeSchema: runtimeSchema,
		pollInterval:  pollInterval,
	}

	// Load initial snapshot
	snap, err := cs.load()
	if err != nil {
		return nil, fmt.Errorf("load initial config: %w", err)
	}
	cs.snapshot = snap

	slog.Info("Config store connected.", "orgs", len(snap.Orgs), "users", len(snap.OrgUserPassword))
	return cs, nil
}

// Start begins the polling goroutine that periodically reloads config.
func (cs *ConfigStore) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(cs.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				newSnap, err := cs.load()
				if err != nil {
					slog.Warn("Config store poll failed.", "error", err)
					continue
				}

				cs.mu.Lock()
				oldSnap := cs.snapshot
				cs.snapshot = newSnap
				callbacks := make([]func(old, new *Snapshot), len(cs.onChange))
				copy(callbacks, cs.onChange)
				cs.mu.Unlock()

				// Fire callbacks outside the lock
				for _, fn := range callbacks {
					fn(oldSnap, newSnap)
				}
			}
		}
	}()
}

// load fetches all config from the database and builds a Snapshot.
func (cs *ConfigStore) load() (*Snapshot, error) {
	var orgs []Org
	if err := cs.db.Preload("Users").Preload("Warehouse").Find(&orgs).Error; err != nil {
		return nil, fmt.Errorf("load orgs: %w", err)
	}

	var global GlobalConfig
	cs.db.First(&global, 1)

	var duckLake DuckLakeConfig
	cs.db.First(&duckLake, 1)

	var rateLimit RateLimitConfig
	cs.db.First(&rateLimit, 1)

	var queryLog QueryLogConfig
	cs.db.First(&queryLog, 1)

	snap := &Snapshot{
		Orgs:            make(map[string]*OrgConfig),
		DatabaseOrg:     make(map[string]string),
		OrgUserPassword: make(map[OrgUserKey]string),
		Global:          global,
		DuckLake:        duckLake,
		RateLimit:       rateLimit,
		QueryLog:        queryLog,
	}

	for _, o := range orgs {
		oc := &OrgConfig{
			Name:         o.Name,
			DatabaseName: o.DatabaseName,
			MaxWorkers:   o.MaxWorkers,
			MemoryBudget: o.MemoryBudget,
			IdleTimeoutS: o.IdleTimeoutS,
			Users:        make(map[string]string),
			Warehouse:    copyManagedWarehouseConfig(o.Warehouse),
		}
		if o.DatabaseName != "" {
			snap.DatabaseOrg[o.DatabaseName] = o.Name
		}
		for _, u := range o.Users {
			oc.Users[u.Username] = u.Password
			snap.OrgUserPassword[OrgUserKey{OrgID: o.Name, Username: u.Username}] = u.Password
		}
		snap.Orgs[o.Name] = oc
	}

	return snap, nil
}

// Snapshot returns the current config snapshot.
func (cs *ConfigStore) Snapshot() *Snapshot {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.snapshot
}

// ResolveDatabase maps a database name to an org ID. Returns "" if not found.
func (cs *ConfigStore) ResolveDatabase(database string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return ""
	}
	return cs.snapshot.DatabaseOrg[database]
}

// ValidateOrgUser checks username/password scoped to a specific org.
func (cs *ConfigStore) ValidateOrgUser(orgID, username, password string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return false
	}
	storedHash, ok := cs.snapshot.OrgUserPassword[OrgUserKey{OrgID: orgID, Username: username}]
	if !ok {
		// Spend time on a dummy bcrypt compare to avoid timing leaks on username enumeration.
		_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$000000000000000000000000000000000000000000000000000000"), []byte(password))
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)) == nil
}

// FindAndValidateUser scans all orgs to find and authenticate a user by username/password.
// This is used for Flight SQL which doesn't have SNI-based org routing.
func (cs *ConfigStore) FindAndValidateUser(username, password string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return "", false
	}
	for key, storedHash := range cs.snapshot.OrgUserPassword {
		if key.Username == username {
			if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)) == nil {
				return key.OrgID, true
			}
			return "", false
		}
	}
	_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$000000000000000000000000000000000000000000000000000000"), []byte(password))
	return "", false
}

// HashPassword hashes a plaintext password using bcrypt.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

// OnChange registers a callback that fires when the config snapshot changes.
func (cs *ConfigStore) OnChange(fn func(old, new *Snapshot)) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.onChange = append(cs.onChange, fn)
}

// ListWarehousesByStates returns all warehouses with a state matching one of the given values.
// This is a direct DB query, not snapshot-based, for use by the provisioning controller.
func (cs *ConfigStore) ListWarehousesByStates(states []ManagedWarehouseProvisioningState) ([]ManagedWarehouse, error) {
	var warehouses []ManagedWarehouse
	if err := cs.db.Where("state IN ?", states).Find(&warehouses).Error; err != nil {
		return nil, fmt.Errorf("list warehouses by states: %w", err)
	}
	return warehouses, nil
}

// UpdateWarehouseState performs a compare-and-swap update on a warehouse row.
// Only updates if the current state matches expectedState, preventing races.
func (cs *ConfigStore) UpdateWarehouseState(orgID string, expectedState ManagedWarehouseProvisioningState, updates map[string]interface{}) error {
	result := cs.db.Model(&ManagedWarehouse{}).
		Where("org_id = ? AND state = ?", orgID, expectedState).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update warehouse state: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("warehouse %q not in expected state %q", orgID, expectedState)
	}
	return nil
}

func migrateOrgUserPK(db *gorm.DB) error {
	// Check if the PK already has 2 columns (idempotent)
	var count int64
	db.Raw(`
		SELECT COUNT(*) FROM information_schema.key_column_usage
		WHERE table_name = 'duckgres_org_users'
		AND constraint_name = 'duckgres_org_users_pkey'
	`).Scan(&count)
	if count >= 2 {
		return nil // Already migrated
	}
	if count == 0 {
		return nil // Table doesn't exist yet, AutoMigrate will create it
	}
	// Migrate: drop old single-column PK, add composite PK
	return db.Exec(`
		ALTER TABLE duckgres_org_users DROP CONSTRAINT duckgres_org_users_pkey;
		ALTER TABLE duckgres_org_users ADD PRIMARY KEY (org_id, username);
	`).Error
}

func resolveRuntimeSchema(db *gorm.DB) (string, error) {
	var currentSchema string
	if err := db.Raw("SELECT current_schema()").Scan(&currentSchema).Error; err != nil {
		return "", err
	}
	if currentSchema == "" || currentSchema == "public" {
		return "cp_runtime", nil
	}
	return currentSchema + "_runtime", nil
}

func ensureRuntimeSchema(db *gorm.DB, runtimeSchema string) error {
	return db.Exec(`CREATE SCHEMA IF NOT EXISTS "` + quoteIdentifier(runtimeSchema) + `"`).Error
}

func autoMigrateRuntimeTables(db *gorm.DB, runtimeSchema string) error {
	for _, spec := range []struct {
		table string
		model any
	}{
		{table: runtimeSchema + ".cp_instances", model: &ControlPlaneInstance{}},
		{table: runtimeSchema + ".worker_records", model: &WorkerRecord{}},
		{table: runtimeSchema + ".flight_session_records", model: &FlightSessionRecord{}},
	} {
		if err := db.Table(spec.table).AutoMigrate(spec.model); err != nil {
			return err
		}
	}
	return nil
}

func quoteIdentifier(v string) string {
	return strings.ReplaceAll(v, `"`, `""`)
}

// DB exposes the GORM database for direct CRUD operations (used by admin API).
func (cs *ConfigStore) DB() *gorm.DB {
	return cs.db
}

// RuntimeSchema returns the dedicated runtime coordination schema name.
func (cs *ConfigStore) RuntimeSchema() string {
	return cs.runtimeSchema
}

func (cs *ConfigStore) runtimeTable(base string) string {
	return cs.runtimeSchema + "." + base
}

// UpsertControlPlaneInstance inserts or updates a runtime control-plane instance row.
func (cs *ConfigStore) UpsertControlPlaneInstance(instance *ControlPlaneInstance) error {
	if err := cs.db.Table(cs.runtimeTable(instance.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"pod_name", "pod_uid", "boot_id", "state", "started_at", "last_heartbeat_at", "draining_at", "expired_at", "updated_at"}),
	}).Create(instance).Error; err != nil {
		return fmt.Errorf("upsert control plane instance: %w", err)
	}
	return nil
}

// GetControlPlaneInstance returns a runtime control-plane instance row by id.
func (cs *ConfigStore) GetControlPlaneInstance(id string) (*ControlPlaneInstance, error) {
	var instance ControlPlaneInstance
	if err := cs.db.Table(cs.runtimeTable(instance.TableName())).First(&instance, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("get control plane instance: %w", err)
	}
	return &instance, nil
}

// ExpireControlPlaneInstances marks stale control-plane instance rows as expired.
func (cs *ConfigStore) ExpireControlPlaneInstances(cutoff time.Time) (int64, error) {
	now := time.Now()
	result := cs.db.Table(cs.runtimeTable((&ControlPlaneInstance{}).TableName())).
		Where("state <> ? AND last_heartbeat_at < ?", ControlPlaneInstanceStateExpired, cutoff).
		Updates(map[string]any{
			"state":      ControlPlaneInstanceStateExpired,
			"expired_at": now,
			"updated_at": now,
		})
	if result.Error != nil {
		return 0, fmt.Errorf("expire control plane instances: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// ExpireDrainingControlPlaneInstances marks draining control-plane rows expired
// once their draining_at timestamp exceeds the configured handover timeout.
func (cs *ConfigStore) ExpireDrainingControlPlaneInstances(before time.Time) (int64, error) {
	now := time.Now()
	result := cs.db.Table(cs.runtimeTable((&ControlPlaneInstance{}).TableName())).
		Where("state = ? AND draining_at IS NOT NULL AND draining_at <= ?", ControlPlaneInstanceStateDraining, before).
		Updates(map[string]any{
			"state":      ControlPlaneInstanceStateExpired,
			"expired_at": now,
			"updated_at": now,
		})
	if result.Error != nil {
		return 0, fmt.Errorf("expire draining control plane instances: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// UpsertWorkerRecord inserts or updates a runtime worker row.
func (cs *ConfigStore) UpsertWorkerRecord(record *WorkerRecord) error {
	if err := cs.db.Table(cs.runtimeTable(record.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "worker_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"pod_name", "pod_uid", "state", "org_id", "owner_cp_instance_id", "owner_epoch", "activation_started_at", "last_heartbeat_at", "retire_reason", "updated_at"}),
	}).Create(record).Error; err != nil {
		return fmt.Errorf("upsert worker record: %w", err)
	}
	return nil
}

// GetWorkerRecord returns a runtime worker row by worker id.
func (cs *ConfigStore) GetWorkerRecord(workerID int) (*WorkerRecord, error) {
	var record WorkerRecord
	if err := cs.db.Table(cs.runtimeTable(record.TableName())).First(&record, "worker_id = ?", workerID).Error; err != nil {
		return nil, fmt.Errorf("get worker record: %w", err)
	}
	return &record, nil
}

// ClaimIdleWorker atomically claims one idle worker row for a control-plane instance.
// The selected row is locked with SKIP LOCKED and transitioned to reserved while
// incrementing owner_epoch. When maxOrgWorkers is set, org claims are serialized
// under the same advisory lock used for spawn-slot allocation.
func (cs *ConfigStore) ClaimIdleWorker(ownerCPInstanceID, orgID string, maxOrgWorkers int) (*WorkerRecord, error) {
	var claimed *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if orgID != "" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org:"+orgID)).Error; err != nil {
				return err
			}
		}
		if maxOrgWorkers > 0 && orgID != "" {
			count, err := cs.countActiveWorkers(tx, "org_id = ?", orgID)
			if err != nil {
				return err
			}
			if count >= int64(maxOrgWorkers) {
				return nil
			}
		}

		var current WorkerRecord
		err := tx.Table(cs.runtimeTable(current.TableName())).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state = ?", WorkerStateIdle).
			Order("worker_id ASC").
			Take(&current).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}

		now := time.Now()
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", current.WorkerID).
			Updates(map[string]any{
				"state":                WorkerStateReserved,
				"org_id":               orgID,
				"owner_cp_instance_id": ownerCPInstanceID,
				"owner_epoch":          gorm.Expr("owner_epoch + 1"),
				"updated_at":           now,
			}).Error; err != nil {
			return err
		}

		if err := tx.Table(cs.runtimeTable(current.TableName())).
			First(&current, "worker_id = ?", current.WorkerID).Error; err != nil {
			return err
		}
		claimed = &current
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("claim idle worker: %w", err)
	}
	return claimed, nil
}

// ClaimHotIdleWorker atomically claims one hot-idle worker row that was
// previously activated for the given org. The selected row is locked with
// SKIP LOCKED and transitioned to reserved while incrementing owner_epoch.
func (cs *ConfigStore) ClaimHotIdleWorker(ownerCPInstanceID, orgID string) (*WorkerRecord, error) {
	var claimed *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		var current WorkerRecord
		err := tx.Table(cs.runtimeTable(current.TableName())).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state = ? AND org_id = ?", WorkerStateHotIdle, orgID).
			Order("worker_id ASC").
			Take(&current).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}

		now := time.Now()
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", current.WorkerID).
			Updates(map[string]any{
				"state":                WorkerStateReserved,
				"owner_cp_instance_id": ownerCPInstanceID,
				"owner_epoch":          gorm.Expr("owner_epoch + 1"),
				"updated_at":           now,
			}).Error; err != nil {
			return err
		}

		if err := tx.Table(cs.runtimeTable(current.TableName())).
			First(&current, "worker_id = ?", current.WorkerID).Error; err != nil {
			return err
		}
		claimed = &current
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("claim hot-idle worker: %w", err)
	}
	return claimed, nil
}

// ListExpiredHotIdleWorkers returns hot-idle workers whose updated_at timestamp
// is at or before the given cutoff time.
func (cs *ConfigStore) ListExpiredHotIdleWorkers(before time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("state = ? AND updated_at <= ?", WorkerStateHotIdle, before).
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list expired hot-idle workers: %w", err)
	}
	return workers, nil
}

// TakeOverWorker transfers durable worker ownership to a new control-plane
// instance when the caller still has the expected prior owner_epoch.
func (cs *ConfigStore) TakeOverWorker(workerID int, ownerCPInstanceID, orgID string, expectedOwnerEpoch int64) (*WorkerRecord, error) {
	var claimed *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		var current WorkerRecord
		err := tx.Table(cs.runtimeTable(current.TableName())).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("worker_id = ?", workerID).
			Take(&current).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}
		if current.OwnerEpoch != expectedOwnerEpoch {
			return ErrWorkerOwnerEpochMismatch
		}
		now := time.Now()
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", current.WorkerID).
			Updates(map[string]any{
				"state":                WorkerStateReserved,
				"org_id":               orgID,
				"owner_cp_instance_id": ownerCPInstanceID,
				"owner_epoch":          gorm.Expr("owner_epoch + 1"),
				"updated_at":           now,
			}).Error; err != nil {
			return err
		}
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			First(&current, "worker_id = ?", current.WorkerID).Error; err != nil {
			return err
		}
		claimed = &current
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("take over worker: %w", err)
	}
	return claimed, nil
}

// CreateSpawningWorkerSlot creates a durable spawning worker row under advisory-lock
// protected org/global capacity checks. A nil result means capacity blocked the spawn.
func (cs *ConfigStore) CreateSpawningWorkerSlot(ownerCPInstanceID, orgID string, ownerEpoch int64, podNamePrefix string, maxOrgWorkers, maxGlobalWorkers int) (*WorkerRecord, error) {
	if strings.TrimSpace(podNamePrefix) == "" {
		return nil, fmt.Errorf("pod name prefix is required")
	}

	var created *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if orgID != "" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org:"+orgID)).Error; err != nil {
				return err
			}
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:global-worker-capacity")).Error; err != nil {
			return err
		}

		if maxOrgWorkers > 0 && orgID != "" {
			count, err := cs.countActiveWorkers(tx, "org_id = ?", orgID)
			if err != nil {
				return err
			}
			if count >= int64(maxOrgWorkers) {
				return nil
			}
		}

		if maxGlobalWorkers > 0 {
			count, err := cs.countActiveWorkers(tx)
			if err != nil {
				return err
			}
			if count >= int64(maxGlobalWorkers) {
				return nil
			}
		}

		var workerID int64
		if err := tx.Raw("SELECT COALESCE(MAX(worker_id), 0) + 1 FROM " + cs.runtimeTable((&WorkerRecord{}).TableName())).Scan(&workerID).Error; err != nil {
			return err
		}
		now := time.Now()
		record := &WorkerRecord{
			WorkerID:          int(workerID),
			PodName:           fmt.Sprintf("%s-%d", podNamePrefix, workerID),
			State:             WorkerStateSpawning,
			OrgID:             orgID,
			OwnerCPInstanceID: ownerCPInstanceID,
			OwnerEpoch:        ownerEpoch,
			LastHeartbeatAt:   now,
		}
		if err := tx.Table(cs.runtimeTable(record.TableName())).Create(record).Error; err != nil {
			return err
		}
		created = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create spawning worker slot: %w", err)
	}
	return created, nil
}

// CreateNeutralWarmWorkerSlot creates a durable spawning worker row for the shared
// neutral warm pool under advisory-lock protected cluster-wide warm-target and
// global capacity checks. A nil result means capacity already satisfies the target
// or the global worker cap blocked the spawn.
func (cs *ConfigStore) CreateNeutralWarmWorkerSlot(ownerCPInstanceID, podNamePrefix string, targetWarmWorkers, maxGlobalWorkers int) (*WorkerRecord, error) {
	if strings.TrimSpace(podNamePrefix) == "" {
		return nil, fmt.Errorf("pod name prefix is required")
	}

	var created *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:shared-warm-target")).Error; err != nil {
			return err
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:global-worker-capacity")).Error; err != nil {
			return err
		}

		if targetWarmWorkers > 0 {
			count, err := cs.countNeutralWarmWorkers(tx)
			if err != nil {
				return err
			}
			if count >= int64(targetWarmWorkers) {
				return nil
			}
		}

		if maxGlobalWorkers > 0 {
			count, err := cs.countActiveWorkers(tx)
			if err != nil {
				return err
			}
			if count >= int64(maxGlobalWorkers) {
				return nil
			}
		}

		var workerID int64
		if err := tx.Raw("SELECT COALESCE(MAX(worker_id), 0) + 1 FROM " + cs.runtimeTable((&WorkerRecord{}).TableName())).Scan(&workerID).Error; err != nil {
			return err
		}
		now := time.Now()
		record := &WorkerRecord{
			WorkerID:          int(workerID),
			PodName:           fmt.Sprintf("%s-%d", podNamePrefix, workerID),
			State:             WorkerStateSpawning,
			OrgID:             "",
			OwnerCPInstanceID: ownerCPInstanceID,
			OwnerEpoch:        0,
			LastHeartbeatAt:   now,
		}
		if err := tx.Table(cs.runtimeTable(record.TableName())).Create(record).Error; err != nil {
			return err
		}
		created = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create neutral warm worker slot: %w", err)
	}
	return created, nil
}

// ListOrphanedWorkers returns workers whose owning control-plane instance has
// already been marked expired long enough ago to pass the orphan grace cutoff.
// Retired/lost rows are included so a replacement janitor can finish deleting
// worker pods when the original control plane died after persisting retirement
// but before the Kubernetes delete completed.
func (cs *ConfigStore) ListOrphanedWorkers(before time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	cleanupStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
		WorkerStateDraining,
		WorkerStateRetired,
		WorkerStateLost,
	}
	workerTable := cs.runtimeTable((&WorkerRecord{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	err := cs.db.Table(workerTable+" AS w").
		Select("w.*").
		Joins("JOIN "+cpTable+" AS cp ON cp.id = w.owner_cp_instance_id").
		Where("w.state IN ?", cleanupStates).
		Where("cp.state = ?", ControlPlaneInstanceStateExpired).
		Where("cp.expired_at IS NOT NULL AND cp.expired_at <= ?", before).
		Order("w.worker_id ASC").
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list orphaned workers: %w", err)
	}
	return workers, nil
}

// ListStuckWorkers returns workers stuck in spawning, reserved, or activating
// beyond their respective cutoffs.
func (cs *ConfigStore) ListStuckWorkers(spawningBefore, activatingBefore time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	workerTable := cs.runtimeTable((&WorkerRecord{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	err := cs.db.Table(workerTable+" AS w").
		Select("w.*").
		Joins("LEFT JOIN "+cpTable+" AS cp ON cp.id = w.owner_cp_instance_id").
		Where("(w.state = ? AND w.updated_at <= ?) OR (w.state IN ? AND w.updated_at <= ?)",
			WorkerStateSpawning,
			spawningBefore,
			[]WorkerState{WorkerStateReserved, WorkerStateActivating},
			activatingBefore,
		).
		Where("cp.id IS NULL OR cp.state <> ?", ControlPlaneInstanceStateExpired).
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list stuck workers: %w", err)
	}
	return workers, nil
}

// ExpireFlightSessionRecords marks reconnectable Flight sessions expired when
// their reconnect deadline has passed.
func (cs *ConfigStore) ExpireFlightSessionRecords(before time.Time) (int64, error) {
	result := cs.db.Table(cs.runtimeTable((&FlightSessionRecord{}).TableName())).
		Where("state NOT IN ?", []FlightSessionState{FlightSessionStateExpired, FlightSessionStateClosed}).
		Where("expires_at <= ?", before).
		Updates(map[string]any{
			"state":      FlightSessionStateExpired,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return 0, fmt.Errorf("expire flight session records: %w", result.Error)
	}
	return result.RowsAffected, nil
}

func (cs *ConfigStore) countActiveWorkers(tx *gorm.DB, where ...any) (int64, error) {
	var count int64
	activeStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
		WorkerStateDraining,
	}
	query := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).Where("state IN ?", activeStates)
	if len(where) > 0 {
		if clauseStr, ok := where[0].(string); ok {
			query = query.Where(clauseStr, where[1:]...)
		}
	}
	if err := query.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (cs *ConfigStore) countNeutralWarmWorkers(tx *gorm.DB) (int64, error) {
	var count int64
	if err := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("org_id = ''").
		Where("state IN ?", []WorkerState{WorkerStateIdle, WorkerStateSpawning}).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func advisoryLockKey(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

// UpsertFlightSessionRecord inserts or updates a durable Flight reconnect row.
func (cs *ConfigStore) UpsertFlightSessionRecord(record *FlightSessionRecord) error {
	if err := cs.db.Table(cs.runtimeTable(record.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "session_token"}},
		DoUpdates: clause.AssignmentColumns([]string{"username", "org_id", "worker_id", "owner_epoch", "cp_instance_id", "state", "expires_at", "last_seen_at", "updated_at"}),
	}).Create(record).Error; err != nil {
		return fmt.Errorf("upsert flight session record: %w", err)
	}
	return nil
}

// GetFlightSessionRecord returns a durable Flight reconnect row by session token.
func (cs *ConfigStore) GetFlightSessionRecord(sessionToken string) (*FlightSessionRecord, error) {
	var record FlightSessionRecord
	err := cs.db.Table(cs.runtimeTable(record.TableName())).First(&record, "session_token = ?", sessionToken).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("get flight session record: %w", err)
	}
	return &record, nil
}

func (cs *ConfigStore) TouchFlightSessionRecord(sessionToken string, lastSeenAt time.Time) error {
	result := cs.db.Table(cs.runtimeTable((&FlightSessionRecord{}).TableName())).
		Where("session_token = ?", sessionToken).
		Updates(map[string]any{
			"last_seen_at": lastSeenAt,
			"updated_at":   time.Now(),
		})
	if result.Error != nil {
		return fmt.Errorf("touch flight session record: %w", result.Error)
	}
	return nil
}

func (cs *ConfigStore) CloseFlightSessionRecord(sessionToken string, closedAt time.Time) error {
	result := cs.db.Table(cs.runtimeTable((&FlightSessionRecord{}).TableName())).
		Where("session_token = ?", sessionToken).
		Updates(map[string]any{
			"state":        FlightSessionStateClosed,
			"last_seen_at": closedAt,
			"updated_at":   time.Now(),
		})
	if result.Error != nil {
		return fmt.Errorf("close flight session record: %w", result.Error)
	}
	return nil
}

// Reload forces an immediate config reload from the database.
func (cs *ConfigStore) Reload() error {
	newSnap, err := cs.load()
	if err != nil {
		return err
	}

	cs.mu.Lock()
	oldSnap := cs.snapshot
	cs.snapshot = newSnap
	callbacks := make([]func(old, new *Snapshot), len(cs.onChange))
	copy(callbacks, cs.onChange)
	cs.mu.Unlock()

	for _, fn := range callbacks {
		fn(oldSnap, newSnap)
	}
	return nil
}
