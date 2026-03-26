package configstore

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// OrgUserKey is the composite key for org-scoped user lookups.
type OrgUserKey struct {
	OrgID    string
	Username string
}

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
	db           *gorm.DB
	mu           sync.RWMutex
	snapshot     *Snapshot
	pollInterval time.Duration
	onChange     []func(old, new *Snapshot)
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

	// Ensure singleton rows exist with defaults
	db.FirstOrCreate(&GlobalConfig{}, GlobalConfig{ID: 1})
	db.FirstOrCreate(&DuckLakeConfig{}, DuckLakeConfig{ID: 1})
	db.FirstOrCreate(&RateLimitConfig{}, RateLimitConfig{ID: 1})
	db.FirstOrCreate(&QueryLogConfig{}, QueryLogConfig{ID: 1})

	cs := &ConfigStore{
		db:           db,
		pollInterval: pollInterval,
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

// DB exposes the GORM database for direct CRUD operations (used by admin API).
func (cs *ConfigStore) DB() *gorm.DB {
	return cs.db
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
