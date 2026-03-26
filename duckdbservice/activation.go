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

// ActivationPayload carries the tenant-specific runtime that is delivered to a
// neutral shared warm worker over the control-plane RPC channel.
type ActivationPayload struct {
	server.WorkerControlMetadata
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
	if payload.OwnerEpoch < 0 {
		return fmt.Errorf("owner_epoch must be non-negative")
	}

	p.mu.RLock()
	current := p.activation
	currentOwnerEpoch := p.ownerEpoch
	currentOwnerCPInstanceID := p.ownerCPInstanceID
	currentWorkerID := p.workerID
	p.mu.RUnlock()
	if currentWorkerID > 0 && payload.WorkerID != currentWorkerID {
		return fmt.Errorf("stale worker_id %d (current %d)", payload.WorkerID, currentWorkerID)
	}
	if current != nil {
		if !sameTenantActivationRuntime(current.payload, payload) {
			return fmt.Errorf("worker already activated for org %q", current.payload.OrgID)
		}
		if reflect.DeepEqual(current.payload, payload) {
			return nil
		}
		if payload.OwnerEpoch <= currentOwnerEpoch {
			return fmt.Errorf("same-tenant takeover requires newer owner epoch %d (current %d)", payload.OwnerEpoch, currentOwnerEpoch)
		}
		if p.reuseExistingActivation(payload) {
			return nil
		}
		return fmt.Errorf("worker already activated for org %q", current.payload.OrgID)
	}
	if currentOwnerCPInstanceID == "" {
		if payload.OwnerEpoch <= currentOwnerEpoch {
			return fmt.Errorf("stale owner epoch %d (current %d)", payload.OwnerEpoch, currentOwnerEpoch)
		}
	} else if payload.OwnerEpoch <= currentOwnerEpoch {
		return fmt.Errorf("stale owner epoch %d (current %d)", payload.OwnerEpoch, currentOwnerEpoch)
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
	if p.workerID > 0 && payload.WorkerID != p.workerID {
		return fmt.Errorf("stale worker_id %d (current %d)", payload.WorkerID, p.workerID)
	}
	if payload.OwnerEpoch <= p.ownerEpoch {
		return fmt.Errorf("stale owner epoch %d (current %d)", payload.OwnerEpoch, p.ownerEpoch)
	}
	if p.activation != nil {
		if sameTenantActivationRuntime(p.activation.payload, payload) && reflect.DeepEqual(p.activation.payload, payload) {
			return nil
		}
		return fmt.Errorf("worker already activated for org %q", p.activation.payload.OrgID)
	}

	p.activation = &activatedTenantRuntime{
		payload: payload,
		db:      db,
	}
	p.ownerEpoch = payload.OwnerEpoch
	p.ownerCPInstanceID = payload.CPInstanceID
	p.workerID = payload.WorkerID
	return nil
}

func (p *SessionPool) reuseExistingActivation(payload ActivationPayload) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reuseExistingActivationLocked(payload)
}

func (p *SessionPool) reuseExistingActivationLocked(payload ActivationPayload) bool {
	if p.activation == nil {
		return false
	}
	current := p.activation.payload
	if !sameTenantActivationRuntime(current, payload) {
		return false
	}
	if !reflect.DeepEqual(current, payload) && payload.OwnerEpoch <= current.OwnerEpoch {
		return false
	}
	p.activation.payload = payload
	p.ownerEpoch = payload.OwnerEpoch
	p.ownerCPInstanceID = payload.CPInstanceID
	p.workerID = payload.WorkerID
	return true
}

func sameTenantActivationRuntime(current, next ActivationPayload) bool {
	return current.OrgID == next.OrgID && reflect.DeepEqual(current.DuckLake, next.DuckLake)
}

func (p *SessionPool) validateControlMetadata(meta server.WorkerControlMetadata) error {
	if !p.sharedWarmMode {
		return nil
	}
	if meta.OwnerEpoch < 0 {
		return fmt.Errorf("owner_epoch must be non-negative")
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.workerID > 0 && meta.WorkerID != 0 && meta.WorkerID != p.workerID {
		return fmt.Errorf("stale worker_id %d (current %d)", meta.WorkerID, p.workerID)
	}
	if p.activation == nil && p.ownerCPInstanceID == "" {
		return nil
	}
	if meta.OwnerEpoch != p.ownerEpoch {
		return fmt.Errorf("stale owner epoch %d (current %d)", meta.OwnerEpoch, p.ownerEpoch)
	}
	if p.ownerCPInstanceID != "" && meta.CPInstanceID != p.ownerCPInstanceID {
		return fmt.Errorf("stale cp_instance_id %q (current %q)", meta.CPInstanceID, p.ownerCPInstanceID)
	}
	if p.workerID > 0 && meta.WorkerID != p.workerID {
		return fmt.Errorf("stale worker_id %d (current %d)", meta.WorkerID, p.workerID)
	}
	return nil
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
	if p.activation == nil {
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
