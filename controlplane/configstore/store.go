package configstore

import (
	"context"
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

// Snapshot holds a point-in-time copy of all config data for fast lookups.
type Snapshot struct {
	Orgs         map[string]*OrgConfig
	UserOrg      map[string]string // username -> org name
	UserPassword map[string]string // username -> password
	Global       GlobalConfig
	DuckLake     DuckLakeConfig
	RateLimit    RateLimitConfig
	QueryLog     QueryLogConfig
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

	slog.Info("Config store connected.", "orgs", len(snap.Orgs), "users", len(snap.UserOrg))
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
		Orgs:         make(map[string]*OrgConfig),
		UserOrg:      make(map[string]string),
		UserPassword: make(map[string]string),
		Global:       global,
		DuckLake:     duckLake,
		RateLimit:    rateLimit,
		QueryLog:     queryLog,
	}

	for _, o := range orgs {
		oc := &OrgConfig{
			Name:         o.Name,
			MaxWorkers:   o.MaxWorkers,
			MemoryBudget: o.MemoryBudget,
			IdleTimeoutS: o.IdleTimeoutS,
			Users:        make(map[string]string),
			Warehouse:    copyManagedWarehouseConfig(o.Warehouse),
		}
		for _, u := range o.Users {
			oc.Users[u.Username] = u.Password
			snap.UserOrg[u.Username] = o.Name
			snap.UserPassword[u.Username] = u.Password
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

// ValidateUser checks username/password against the cached snapshot.
// Passwords are compared using bcrypt. Returns the org name and whether auth succeeded.
func (cs *ConfigStore) ValidateUser(username, password string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return "", false
	}
	storedHash, ok := cs.snapshot.UserPassword[username]
	if !ok {
		// Spend time on a dummy bcrypt compare to avoid timing leaks on username enumeration.
		_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$000000000000000000000000000000000000000000000000000000"), []byte(password))
		return "", false
	}
	if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)); err != nil {
		return "", false
	}
	return cs.snapshot.UserOrg[username], true
}

// HashPassword hashes a plaintext password using bcrypt.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

// OrgForUser returns the org name for a user, or "" if not found.
func (cs *ConfigStore) OrgForUser(username string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return ""
	}
	return cs.snapshot.UserOrg[username]
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

// UpsertWorkerRecord inserts or updates a runtime worker row.
func (cs *ConfigStore) UpsertWorkerRecord(record *WorkerRecord) error {
	if err := cs.db.Table(cs.runtimeTable(record.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "worker_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"pod_name", "pod_uid", "state", "org_id", "owner_cp_instance_id", "owner_epoch", "lease_expires_at", "activation_started_at", "last_heartbeat_at", "retire_reason", "updated_at"}),
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
// incrementing owner_epoch.
func (cs *ConfigStore) ClaimIdleWorker(ownerCPInstanceID, orgID string, leaseExpiresAt time.Time) (*WorkerRecord, error) {
	var claimed *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
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
				"lease_expires_at":     leaseExpiresAt,
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

// CreateSpawningWorkerSlot creates a durable spawning worker row under advisory-lock
// protected org/global capacity checks. A nil result means capacity blocked the spawn.
func (cs *ConfigStore) CreateSpawningWorkerSlot(ownerCPInstanceID, orgID string, ownerEpoch int64, leaseExpiresAt time.Time, podNamePrefix string, maxOrgWorkers, maxGlobalWorkers int) (*WorkerRecord, error) {
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
		if err := tx.Raw("SELECT COALESCE(MAX(worker_id), 0) + 1 FROM "+cs.runtimeTable((&WorkerRecord{}).TableName())).Scan(&workerID).Error; err != nil {
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
			LeaseExpiresAt:    leaseExpiresAt,
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

func (cs *ConfigStore) countActiveWorkers(tx *gorm.DB, where ...any) (int64, error) {
	var count int64
	activeStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
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

func advisoryLockKey(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

// UpsertFlightSessionRecord inserts or updates a durable Flight reconnect row.
func (cs *ConfigStore) UpsertFlightSessionRecord(record *FlightSessionRecord) error {
	if err := cs.db.Table(cs.runtimeTable(record.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "session_token"}},
		DoUpdates: clause.AssignmentColumns([]string{"org_id", "worker_id", "owner_epoch", "cp_instance_id", "state", "expires_at", "last_seen_at", "updated_at"}),
	}).Create(record).Error; err != nil {
		return fmt.Errorf("upsert flight session record: %w", err)
	}
	return nil
}

// GetFlightSessionRecord returns a durable Flight reconnect row by session token.
func (cs *ConfigStore) GetFlightSessionRecord(sessionToken string) (*FlightSessionRecord, error) {
	var record FlightSessionRecord
	if err := cs.db.Table(cs.runtimeTable(record.TableName())).First(&record, "session_token = ?", sessionToken).Error; err != nil {
		return nil, fmt.Errorf("get flight session record: %w", err)
	}
	return &record, nil
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
