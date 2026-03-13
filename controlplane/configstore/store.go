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

// Snapshot holds a point-in-time copy of all config data for fast lookups.
type Snapshot struct {
	Teams        map[string]*TeamConfig
	UserTeam     map[string]string // username -> team name
	UserPassword map[string]string // username -> password
	Global       GlobalConfig
	DuckLake     DuckLakeConfig
	RateLimit    RateLimitConfig
	QueryLog     QueryLogConfig
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

	// Auto-migrate all models
	if err := db.AutoMigrate(
		&Team{},
		&TeamUser{},
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

	slog.Info("Config store connected.", "teams", len(snap.Teams), "users", len(snap.UserTeam))
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
	var teams []Team
	if err := cs.db.Preload("Users").Find(&teams).Error; err != nil {
		return nil, fmt.Errorf("load teams: %w", err)
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
		Teams:        make(map[string]*TeamConfig),
		UserTeam:     make(map[string]string),
		UserPassword: make(map[string]string),
		Global:       global,
		DuckLake:     duckLake,
		RateLimit:    rateLimit,
		QueryLog:     queryLog,
	}

	for _, t := range teams {
		tc := &TeamConfig{
			Name:         t.Name,
			MaxWorkers:   t.MaxWorkers,
			MinWorkers:   t.MinWorkers,
			MemoryBudget: t.MemoryBudget,
			IdleTimeoutS: t.IdleTimeoutS,
			Users:        make(map[string]string),
		}
		for _, u := range t.Users {
			tc.Users[u.Username] = u.Password
			snap.UserTeam[u.Username] = t.Name
			snap.UserPassword[u.Username] = u.Password
		}
		snap.Teams[t.Name] = tc
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
// Passwords are compared using bcrypt. Returns the team name and whether auth succeeded.
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
	return cs.snapshot.UserTeam[username], true
}

// HashPassword hashes a plaintext password using bcrypt.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

// TeamForUser returns the team name for a user, or "" if not found.
func (cs *ConfigStore) TeamForUser(username string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return ""
	}
	return cs.snapshot.UserTeam[username]
}

// OnChange registers a callback that fires when the config snapshot changes.
func (cs *ConfigStore) OnChange(fn func(old, new *Snapshot)) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.onChange = append(cs.onChange, fn)
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
