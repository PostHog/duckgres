package duckdbservice

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/posthog/duckgres/server"
)

// ErrLeaseExpired is returned when a session is requested on a worker whose
// activation lease has passed.
var ErrLeaseExpired = fmt.Errorf("activation lease has expired")

// timeNow is the clock used for lease checks; tests can override it.
var timeNow = time.Now

// ActivationPayload carries the tenant-specific runtime that is delivered to a
// neutral shared warm worker over the control-plane RPC channel.
type ActivationPayload struct {
	OrgID          string                `json:"org_id"`
	LeaseExpiresAt time.Time             `json:"lease_expires_at"`
	DuckLake       server.DuckLakeConfig `json:"ducklake"`
}

type activatedTenantRuntime struct {
	payload ActivationPayload
	db      *sql.DB
}

func (p *SessionPool) currentActivation() *activatedTenantRuntime {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.activation == nil {
		return nil
	}
	current := *p.activation
	return &current
}

func (p *SessionPool) activateTenant(payload ActivationPayload) error {
	if !p.sharedWarmMode {
		return fmt.Errorf("tenant activation is not enabled for this worker")
	}
	if strings.TrimSpace(payload.OrgID) == "" {
		return fmt.Errorf("org_id is required")
	}

	p.mu.RLock()
	current := p.activation
	p.mu.RUnlock()
	if current != nil {
		if reflect.DeepEqual(current.payload, payload) {
			return nil
		}
		return fmt.Errorf("worker already activated for org %q", current.payload.OrgID)
	}

	cfg := p.cfg
	cfg.DuckLake = payload.DuckLake

	<-p.warmupDone

	p.mu.Lock()
	db := p.warmupDB
	if db == nil {
		if p.fallbackDB == nil {
			var err error
			p.fallbackDB, err = p.createDBConnection(p.sharedWarmupConfig(), p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
			if err != nil {
				p.mu.Unlock()
				return fmt.Errorf("create activation-ready runtime: %w", err)
			}
		}
		db = p.fallbackDB
		p.warmupDB = db
	}
	p.mu.Unlock()

	if err := p.activateDBConnection(db, cfg, p.duckLakeSem, "duckgres"); err != nil {
		return fmt.Errorf("activate tenant runtime: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activation != nil {
		if reflect.DeepEqual(p.activation.payload, payload) {
			return nil
		}
		return fmt.Errorf("worker already activated for org %q", p.activation.payload.OrgID)
	}

	p.activation = &activatedTenantRuntime{
		payload: payload,
		db:      db,
	}
	return nil
}

// isLeaseExpired reports whether the activation lease has passed. A zero
// LeaseExpiresAt is treated as "no expiry" so that callers who omit the
// field (e.g. non-shared-warm paths) are unaffected.
func (a *activatedTenantRuntime) isLeaseExpired() bool {
	if a == nil {
		return false
	}
	if a.payload.LeaseExpiresAt.IsZero() {
		return false
	}
	return timeNow().After(a.payload.LeaseExpiresAt)
}

func (p *SessionPool) currentSessionConfig() (server.Config, error) {
	if !p.sharedWarmMode {
		return p.cfg, nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.activation == nil {
		return server.Config{}, fmt.Errorf("worker is not activated")
	}
	if p.activation.isLeaseExpired() {
		return server.Config{}, ErrLeaseExpired
	}

	cfg := p.cfg
	cfg.DuckLake = p.activation.payload.DuckLake
	return cfg, nil
}

func (p *SessionPool) sharedWarmupConfig() server.Config {
	cfg := p.cfg
	if p.sharedWarmMode {
		cfg.DuckLake = server.DuckLakeConfig{}
	}
	return cfg
}

func (p *SessionPool) activeSharedDB() *sql.DB {
	if !p.sharedWarmMode {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.activation == nil || p.activation.isLeaseExpired() {
		return nil
	}
	return p.activation.db
}

func sharedWarmWorkerEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(getenv("DUCKGRES_SHARED_WARM_WORKER"))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

var getenv = func(key string) string {
	return ""
}

func init() {
	getenv = lookupEnv
}

func lookupEnv(key string) string {
	return os.Getenv(key)
}
