package configstore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
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
	Orgs               map[string]*OrgConfig
	DatabaseOrg        map[string]string     // database name -> org ID
	OrgUserPassword    map[OrgUserKey]string // (orgID, username) -> bcrypt hash
	OrgUserPassthrough map[OrgUserKey]bool   // (orgID, username) -> passthrough flag
	Global             GlobalConfig
	DuckLake           DuckLakeConfig
	RateLimit          RateLimitConfig
	QueryLog           QueryLogConfig
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
		Orgs:               make(map[string]*OrgConfig),
		DatabaseOrg:        make(map[string]string),
		OrgUserPassword:    make(map[OrgUserKey]string),
		OrgUserPassthrough: make(map[OrgUserKey]bool),
		Global:             global,
		DuckLake:           duckLake,
		RateLimit:          rateLimit,
		QueryLog:           queryLog,
	}

	for _, o := range orgs {
		oc := &OrgConfig{
			Name:                o.Name,
			DatabaseName:        o.DatabaseName,
			MaxWorkers:          o.MaxWorkers,
			MemoryBudget:        o.MemoryBudget,
			IdleTimeoutS:        o.IdleTimeoutS,
			WorkerCPURequest:    o.WorkerCPURequest,
			WorkerMemoryRequest: o.WorkerMemoryRequest,
			Users:               make(map[string]string),
			Warehouse:           copyManagedWarehouseConfig(o.Warehouse),
		}
		if o.DatabaseName != "" {
			snap.DatabaseOrg[o.DatabaseName] = o.Name
		}
		for _, u := range o.Users {
			oc.Users[u.Username] = u.Password
			key := OrgUserKey{OrgID: o.Name, Username: u.Username}
			snap.OrgUserPassword[key] = u.Password
			if u.Passthrough {
				snap.OrgUserPassthrough[key] = true
			}
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

// IsOrgUserPassthrough reports whether the given (org, user) is configured to
// bypass the PostgreSQL compatibility layer. Returns false for unknown users —
// callers must validate credentials separately before trusting this.
func (cs *ConfigStore) IsOrgUserPassthrough(orgID, username string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return false
	}
	return cs.snapshot.OrgUserPassthrough[OrgUserKey{OrgID: orgID, Username: username}]
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

// GeneratePassword returns a cryptographically random 32-byte URL-safe password.
func GeneratePassword() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// CreateOrgUser creates a new user for the given org.
func (cs *ConfigStore) CreateOrgUser(orgID, username, passwordHash string) error {
	user := OrgUser{
		OrgID:    orgID,
		Username: username,
		Password: passwordHash,
	}
	return cs.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "org_id"}, {Name: "username"}},
		DoUpdates: clause.AssignmentColumns([]string{"password", "updated_at"}),
	}).Create(&user).Error
}

// UpdateOrgUserPassword updates the password hash for an existing user.
func (cs *ConfigStore) UpdateOrgUserPassword(orgID, username, passwordHash string) error {
	result := cs.db.Model(&OrgUser{}).
		Where("org_id = ? AND username = ?", orgID, username).
		Update("password", passwordHash)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("user %q not found in org %q", username, orgID)
	}
	return nil
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

// ListLiveControlPlaneInstanceIDs returns the IDs of control-plane instances
// that are not yet expired — i.e. either currently active OR draining (still
// alive, waiting on in-flight queries to finish before SIGTERM completes).
// Used by the K8s pool's startup orphan sweep to distinguish "owned by a CP
// that is still serving traffic" from "owned by a dead CP".
//
// Including draining CPs is critical: a draining CP's worker pods are still
// running queries that haven't finished yet, and treating them as orphans
// would kill those queries mid-flight.
func (cs *ConfigStore) ListLiveControlPlaneInstanceIDs() ([]string, error) {
	var ids []string
	if err := cs.db.Table(cs.runtimeTable((&ControlPlaneInstance{}).TableName())).
		Where("state <> ?", ControlPlaneInstanceStateExpired).
		Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list live control plane instance ids: %w", err)
	}
	return ids, nil
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
		DoUpdates: clause.AssignmentColumns([]string{"pod_name", "image", "state", "org_id", "owner_cp_instance_id", "owner_epoch", "activation_started_at", "last_heartbeat_at", "retire_reason", "s3_credentials_expires_at", "updated_at"}),
	}).Create(record).Error; err != nil {
		return fmt.Errorf("upsert worker record: %w", err)
	}
	return nil
}

// ListWorkerRecordsByStatesBefore returns worker rows in any of the given
// states whose updated_at is at or before the given cutoff. The age filter is
// what makes this safe to use against in-flight spawns: callers pass a cutoff
// well in the past (e.g. now - 30s) so a row that another CP is currently
// touching will not appear in the result.
func (cs *ConfigStore) ListWorkerRecordsByStatesBefore(states []WorkerState, updatedBefore time.Time) ([]WorkerRecord, error) {
	if len(states) == 0 {
		return nil, nil
	}
	var workers []WorkerRecord
	if err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("state IN ?", states).
		Where("updated_at <= ?", updatedBefore).
		Order("worker_id ASC").
		Find(&workers).Error; err != nil {
		return nil, fmt.Errorf("list worker records by state before: %w", err)
	}
	return workers, nil
}

// GetWorkerRecord returns a runtime worker row by worker id. Returns
// (nil, nil) when no row matches — "not found" is a normal state for
// callers like cleanupOrphanedWorkerPods that need to distinguish between
// a known terminal row and no row at all. Any other DB error is wrapped
// and returned so callers can log and retry on the next tick.
func (cs *ConfigStore) GetWorkerRecord(workerID int) (*WorkerRecord, error) {
	var record WorkerRecord
	err := cs.db.Table(cs.runtimeTable(record.TableName())).First(&record, "worker_id = ?", workerID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get worker record: %w", err)
	}
	return &record, nil
}

// ClaimIdleWorker atomically claims one idle worker row for a control-plane instance.
// The selected row is locked with SKIP LOCKED and transitioned to reserved while
// incrementing owner_epoch. When maxOrgWorkers is set, org claims are serialized
// under the same advisory lock used for spawn-slot allocation.
func (cs *ConfigStore) ClaimIdleWorker(ownerCPInstanceID, orgID, image string, maxOrgWorkers int) (*WorkerRecord, error) {
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
		query := tx.Table(cs.runtimeTable(current.TableName())).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state = ?", WorkerStateIdle)

		if image != "" {
			query = query.Where("image = ?", image)
		}

		err := query.Order("worker_id ASC").Take(&current).Error
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

// RetireHotIdleWorker atomically transitions a worker from hot_idle to retired.
// Returns true if the transition happened, false if the worker was no longer hot_idle
// (e.g. it was reclaimed by another CP pod between the list query and this call).
func (cs *ConfigStore) RetireHotIdleWorker(workerID int) (bool, error) {
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND state = ?", workerID, WorkerStateHotIdle).
		Updates(map[string]any{
			"state":         WorkerStateRetired,
			"retire_reason": "hot_idle_ttl_expired",
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("retire hot-idle worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// RetireIdleWorker atomically transitions a worker from idle to retired.
// Returns true if the transition happened, false if the worker was no longer
// idle (e.g. claimed by another CP between the list query and this call).
func (cs *ConfigStore) RetireIdleWorker(workerID int, reason string) (bool, error) {
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND state = ?", workerID, WorkerStateIdle).
		Updates(map[string]any{
			"state":         WorkerStateRetired,
			"retire_reason": reason,
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("retire idle worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// RetireOrphanWorker is the orphan-cleanup counterpart to
// RetireIdleOrHotIdleWorker. Whereas the latter only handles `idle` /
// `hot_idle`, this method transitions a worker to `retired` from any
// active state (spawning, idle, reserved, activating, hot, hot_idle,
// draining). That breadth is safe in the orphan path because by the time
// orphan cleanup picks up the row, no live CP could still be acting on
// it (the owner CP is expired or absent) — so we can short-circuit the
// state-machine guards and just terminate the row.
//
// Returns true if a row transitioned, false if the row was already
// terminal (`retired` / `lost`) or no row matches the given worker_id.
func (cs *ConfigStore) RetireOrphanWorker(workerID int, reason string) (bool, error) {
	cleanupStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
		WorkerStateDraining,
	}
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND state IN ?", workerID, cleanupStates).
		Updates(map[string]any{
			"state":         WorkerStateRetired,
			"retire_reason": reason,
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("retire orphan worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// ListWorkersDueForCredentialRefresh returns workers owned by the given CP
// whose S3 credentials are about to expire (or have already expired) and
// therefore need a refresh. The cutoff defines "soon": typically the
// scheduler passes (now + half the STS session duration), so a worker is
// picked up well before its session token actually goes invalid.
//
// NULL s3_credentials_expires_at is treated as "due immediately". This
// covers two cases: warm-pool rows that haven't been activated yet (these
// have no creds, so the predicate is irrelevant — they're filtered out by
// the state set anyway since neutral idle workers shouldn't carry creds),
// and pre-migration rows that existed before this column was introduced
// (these get refreshed eagerly so we converge to the new state).
//
// Only active states are considered: retired/lost/draining(-out) rows
// don't need creds. We include `idle` deliberately — an idle worker that
// belongs to an org (post-activation) still has live creds in DuckDB and
// will need them on the next session.
func (cs *ConfigStore) ListWorkersDueForCredentialRefresh(ownerCPInstanceID string, cutoff time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	credEligibleStates := []WorkerState{
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
	}
	err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("owner_cp_instance_id = ?", ownerCPInstanceID).
		Where("state IN ?", credEligibleStates).
		// Org-bound rows only: a neutral warm row (org_id='') hasn't been
		// activated, so it has no STS-brokered creds yet.
		Where("org_id <> ''").
		Where("s3_credentials_expires_at IS NULL OR s3_credentials_expires_at <= ?", cutoff).
		Order("s3_credentials_expires_at ASC NULLS FIRST, worker_id ASC").
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list workers due for credential refresh: %w", err)
	}
	return workers, nil
}

// BumpWorkerEpoch atomically increments owner_epoch on a worker we
// already own, returning the new epoch. Used by the credential-refresh
// scheduler before re-sending ActivateTenant with rotated STS creds: the
// worker's reuseExistingActivation guard requires payload.OwnerEpoch >
// current, so the scheduler bumps here, applies the new epoch on the
// in-memory ManagedWorker, then dispatches the activation. If another CP
// has already taken over the row (different owner_cp_instance_id or
// already-bumped epoch), this returns ErrWorkerOwnerEpochMismatch and the
// caller drops the in-flight refresh.
func (cs *ConfigStore) BumpWorkerEpoch(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64) (int64, error) {
	var newEpoch int64
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
			Where("worker_id = ? AND owner_cp_instance_id = ? AND owner_epoch = ?",
				workerID, ownerCPInstanceID, expectedOwnerEpoch).
			Updates(map[string]any{
				"owner_epoch": gorm.Expr("owner_epoch + 1"),
				"updated_at":  time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrWorkerOwnerEpochMismatch
		}
		var current WorkerRecord
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", workerID).
			Take(&current).Error; err != nil {
			return err
		}
		newEpoch = current.OwnerEpoch
		return nil
	})
	if err != nil {
		return 0, err
	}
	return newEpoch, nil
}

// MarkCredentialsRefreshed conditionally stamps a new S3 credential
// expiration onto a worker row. The update only takes effect when the
// caller is still the owner (same owner_cp_instance_id and owner_epoch);
// any drift means another CP took over the worker and the caller's just-
// minted creds are stale — we discard them rather than trample. Returns
// true when the row was updated.
func (cs *ConfigStore) MarkCredentialsRefreshed(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, expiresAt time.Time) (bool, error) {
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND owner_cp_instance_id = ? AND owner_epoch = ?", workerID, ownerCPInstanceID, expectedOwnerEpoch).
		Updates(map[string]any{
			"s3_credentials_expires_at": expiresAt,
			"updated_at":                time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("mark credentials refreshed for worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// RetireIdleOrHotIdleWorker atomically transitions a worker from idle or hot_idle
// to retired. Returns true if the transition happened, false if the worker was
// in some other state (e.g. claimed/activating/hot).
func (cs *ConfigStore) RetireIdleOrHotIdleWorker(workerID int, reason string) (bool, error) {
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND state IN ?", workerID, []WorkerState{WorkerStateIdle, WorkerStateHotIdle}).
		Updates(map[string]any{
			"state":         WorkerStateRetired,
			"retire_reason": reason,
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("retire idle/hot_idle worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// MarkWorkerDraining atomically transitions a worker into the draining state
// if and only if it is still owned by the caller and not already terminal. It
// returns true when the transition happened.
//
// Used by ShutdownAll to fence a worker before issuing its K8s pod delete: no
// other CP can claim the worker once it's draining (ClaimIdleWorker and
// ClaimHotIdleWorker filter on state=idle and state=hot_idle respectively),
// so the pod-delete/DB-retire chain can proceed without a claim race. If the
// CP then crashes before the final retired transition, ListOrphanedWorkers
// includes draining rows whose owner CP has expired, so orphan cleanup
// retires the worker and deletes the pod.
//
// The ownerCPInstanceID guard prevents a stale CP from moving a worker that
// has already been taken over by a successor.
func (cs *ConfigStore) MarkWorkerDraining(workerID int, ownerCPInstanceID string) (bool, error) {
	drainableStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
	}
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND owner_cp_instance_id = ? AND state IN ?", workerID, ownerCPInstanceID, drainableStates).
		Updates(map[string]any{
			"state":      WorkerStateDraining,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("mark worker %d draining: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// RetireDrainingWorker atomically transitions a draining worker to retired.
// Returns true if the transition happened, false if the worker was no longer
// in draining (e.g. already retired by an orphan sweep after a CP restart).
func (cs *ConfigStore) RetireDrainingWorker(workerID int, reason string) (bool, error) {
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND state = ?", workerID, WorkerStateDraining).
		Updates(map[string]any{
			"state":         WorkerStateRetired,
			"retire_reason": reason,
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("retire draining worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
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
func (cs *ConfigStore) CreateSpawningWorkerSlot(ownerCPInstanceID, orgID, image string, ownerEpoch int64, podNamePrefix string, maxOrgWorkers, maxGlobalWorkers int) (*WorkerRecord, error) {
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
			Image:             image,
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
func (cs *ConfigStore) CreateNeutralWarmWorkerSlot(ownerCPInstanceID, podNamePrefix, image string, targetWarmWorkers, maxGlobalWorkers int) (*WorkerRecord, error) {
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
			Image:             image,
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

// ListOrphanedWorkers returns workers in active states that no live CP
// is responsible for any longer. Three independent failure modes are
// covered, joined by OR:
//
//  1. The owning CP row exists and has been marked expired at least
//     `before` ago. This is the canonical case — a CP died, the
//     liveness janitor flipped its row to expired, the orphan grace
//     elapsed, and now the worker is fair game.
//  2. owner_cp_instance_id is empty / NULL and the worker hasn't
//     heartbeat since `before`. Observed in production: rows whose
//     owner string was lost end up invisible to (1)'s INNER JOIN and
//     accumulate forever, blocking warm-pool replenishment because
//     countNeutralWarmWorkers still counts them. The stale-heartbeat
//     guard avoids racing the spawn path's create-then-stamp window.
//  3. owner_cp_instance_id is set but no matching cp_instances row
//     exists at all (hard-deleted somehow), and again the heartbeat
//     is stale. Same shape as (2), different cause.
//
// Retired/lost rows are deliberately excluded — their pods are already
// gone (or are reconciled by K8sWorkerPool.cleanupOrphanedWorkerPods).
// Re-listing terminal rows here would loop the janitor on K8s 404s.
//
// The join switched from INNER to LEFT in Apr 2026 to handle (2)/(3);
// the original implementation only handled (1).
//
// Apr 2026 also added an exclusion for workers with reclaimable Flight
// sessions: a row with at least one flight_session_records entry in
// active or reconnecting state is spared from orphan retirement so a
// customer reconnecting by session token can still pick up their query
// (see TakeOverWorker). Once the session record itself becomes terminal
// (expired/closed via ExpireFlightSessionRecords), the worker is
// retired normally on the next sweep.
//
// See TestListOrphanedWorkers* in tests/configstore for the regression
// fixtures.
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
	}
	reclaimableSessionStates := []FlightSessionState{
		FlightSessionStateActive,
		FlightSessionStateReconnecting,
	}
	workerTable := cs.runtimeTable((&WorkerRecord{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	flightTable := cs.runtimeTable((&FlightSessionRecord{}).TableName())
	err := cs.db.Table(workerTable+" AS w").
		Select("w.*").
		Joins("LEFT JOIN "+cpTable+" AS cp ON cp.id = w.owner_cp_instance_id").
		Where("w.state IN ?", cleanupStates).
		Where(
			// (1) owner CP exists and is expired long enough ago
			"(cp.state = ? AND cp.expired_at IS NOT NULL AND cp.expired_at <= ?) "+
				// (2) owner string is empty/NULL and heartbeat is stale
				"OR (NULLIF(w.owner_cp_instance_id, '') IS NULL AND w.last_heartbeat_at <= ?) "+
				// (3) owner string is set but no matching CP row, heartbeat stale
				"OR (cp.id IS NULL AND NULLIF(w.owner_cp_instance_id, '') IS NOT NULL AND w.last_heartbeat_at <= ?)",
			ControlPlaneInstanceStateExpired, before,
			before,
			before,
		).
		// Spare workers with at least one reclaimable Flight session: a
		// retire here would kill the customer's mid-flight query at the
		// moment they reconnect by session token. Bounded by
		// ExpireFlightSessionRecords — once the session record is moved
		// to a terminal state, the worker is no longer protected.
		Where("NOT EXISTS (SELECT 1 FROM "+flightTable+" AS f "+
			"WHERE f.worker_id = w.worker_id AND f.state IN ?)",
			reclaimableSessionStates).
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
