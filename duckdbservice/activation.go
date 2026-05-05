package duckdbservice

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/posthog/duckgres/server"
)

// ActivationPayload carries the tenant-specific runtime that is delivered to a
// neutral shared warm worker over the control-plane RPC channel.
type ActivationPayload struct {
	server.WorkerControlMetadata
	OrgID    string                `json:"org_id"`
	DuckLake server.DuckLakeConfig `json:"ducklake"`
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
	overrideS3EndpointForCacheProxy(&cfg.DuckLake)

	<-p.warmupDone

	p.mu.Lock()
	db := p.warmupDB
	if db == nil {
		if p.fallbackDB == nil {
			pair, err := p.createDBPair(p.sharedWarmupConfig(), p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
			if err != nil {
				p.mu.Unlock()
				return fmt.Errorf("create activation-ready runtime: %w", err)
			}
			p.fallbackDB = pair.Main
			p.activePair = pair
			p.controlDB = pair.Control
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
	// Idempotent same-payload check FIRST, before the epoch guard. This is
	// the dup-call race: two concurrent ActivateTenant goroutines can both
	// pass Phase 1 (currentOwnerEpoch=0), both run Phase 2 (the slow
	// activateDBConnection), then race Phase 3. Whichever lands second
	// observes p.ownerEpoch already bumped to its own payload value and,
	// without this early-return, would fall through to the epoch guard
	// below and return "stale owner epoch N (current N)" — even though
	// the activation it intended to commit is *byte-identical* to the one
	// already committed. The CP would then treat the failure as fatal and
	// retire a perfectly-good worker (rollout-race incident on dev
	// 2026-05-05).
	//
	// Genuine takeover races (different cp_instance_ids / different
	// payloads) still fall through to the epoch guard and are arbitrated
	// by "highest epoch wins" as before.
	if p.activation != nil &&
		sameTenantActivationRuntime(p.activation.payload, payload) &&
		reflect.DeepEqual(p.activation.payload, payload) {
		return nil
	}
	if payload.OwnerEpoch <= p.ownerEpoch {
		return fmt.Errorf("stale owner epoch %d (current %d)", payload.OwnerEpoch, p.ownerEpoch)
	}
	if p.activation != nil {
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
	// Phase 1 (locked): inspect current activation, decide whether to refresh,
	// and capture everything we need to perform the refresh outside the lock.
	p.mu.Lock()
	if p.activation == nil {
		p.mu.Unlock()
		return false
	}
	current := p.activation.payload
	if !sameTenantActivationRuntime(current, payload) {
		p.mu.Unlock()
		return false
	}
	if !reflect.DeepEqual(current, payload) && payload.OwnerEpoch <= current.OwnerEpoch {
		p.mu.Unlock()
		return false
	}

	needsRefresh := false
	if p.activation.db != nil && payload.DuckLake.ObjectStore != "" &&
		!reflect.DeepEqual(current.DuckLake, payload.DuckLake) {
		needsRefresh = s3CredentialsChanged(current.DuckLake, payload.DuckLake)
		if !needsRefresh {
			// For aws_sdk/credential_chain, the underlying IAM credentials
			// may have expired even though the payload fields haven't changed.
			provider := server.S3ProviderForConfig(payload.DuckLake)
			needsRefresh = provider == "aws_sdk" || provider == "credential_chain"
		}
	}

	// Pick the connection used to swap the secret. controlDB is a side
	// connection sharing the same DuckDB instance via *duckdb.Connector, with
	// its own independent pool — so the CREATE OR REPLACE SECRET below never
	// queues behind a long-running client query on the main DB. Fall back to
	// the activation DB only when controlDB hasn't been wired (older/test
	// paths that bypass createDBPair).
	refreshDB := p.controlDB
	if refreshDB == nil {
		refreshDB = p.activation.db
	}
	refreshFn := p.refreshS3Secret
	sem := p.duckLakeSem
	p.mu.Unlock()

	// Phase 2 (unlocked): perform the slow I/O. Holding p.mu across this is a
	// bug — RefreshS3Secret runs a CREATE OR REPLACE SECRET which can block
	// indefinitely if the active client query is monopolising a single-conn
	// *sql.DB, and any concurrent health check needs p.mu.RLock to snapshot
	// sessions for stall detection. Releasing the lock here keeps the gRPC
	// health check responsive even when the secret rotation has to wait.
	if needsRefresh {
		if refreshFn == nil {
			refreshFn = server.RefreshS3Secret
		}
		if err := refreshFn(refreshDB, payload.DuckLake, sem); err != nil {
			slog.Warn("Failed to refresh S3 credentials on hot-idle reuse.", "org", payload.OrgID, "error", err)
			return false
		}
	}

	// Phase 3 (locked): re-validate state and commit. The activation may have
	// been concurrently reset, taken over by a higher epoch, or migrated away
	// while we were running RefreshS3Secret without the lock — re-check before
	// stamping the new payload over it.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activation == nil {
		return false
	}
	live := p.activation.payload
	if !sameTenantActivationRuntime(live, payload) {
		return false
	}
	if !reflect.DeepEqual(live, payload) && payload.OwnerEpoch <= live.OwnerEpoch {
		return false
	}

	p.activation.payload = payload
	p.ownerEpoch = payload.OwnerEpoch
	p.ownerCPInstanceID = payload.CPInstanceID
	p.workerID = payload.WorkerID
	return true
}

// s3CredentialsChanged returns true if S3 credentials differ between configs.
func s3CredentialsChanged(a, b server.DuckLakeConfig) bool {
	return a.S3AccessKey != b.S3AccessKey ||
		a.S3SecretKey != b.S3SecretKey ||
		a.S3SessionToken != b.S3SessionToken
}

// sameTenantActivationRuntime compares all structural DuckLake fields except
// short-lived credentials (S3AccessKey, S3SecretKey, S3SessionToken) which
// rotate on every STS AssumeRole call. This allows hot-idle reclaim to match
// even when credentials have been refreshed, while still catching actual
// config changes (endpoint, region, object store, etc.).
func sameTenantActivationRuntime(current, next ActivationPayload) bool {
	if current.OrgID != next.OrgID {
		return false
	}
	a, b := current.DuckLake, next.DuckLake
	return a.MetadataStore == b.MetadataStore &&
		a.ObjectStore == b.ObjectStore &&
		a.DataPath == b.DataPath &&
		a.DeltaCatalogEnabled == b.DeltaCatalogEnabled &&
		a.DeltaCatalogPath == b.DeltaCatalogPath &&
		a.S3Provider == b.S3Provider &&
		a.S3Endpoint == b.S3Endpoint &&
		a.S3Region == b.S3Region &&
		a.S3UseSSL == b.S3UseSSL &&
		a.S3URLStyle == b.S3URLStyle &&
		a.S3Chain == b.S3Chain &&
		a.S3Profile == b.S3Profile &&
		a.Migrate == b.Migrate &&
		reflect.DeepEqual(a.DataInliningRowLimit, b.DataInliningRowLimit) &&
		a.CheckpointInterval == b.CheckpointInterval
}

func (p *SessionPool) validateControlMetadata(meta server.WorkerControlMetadata) error {
	// Epoch and CP instance ID checks are intentionally omitted here.
	// Worker ownership is already serialized by the config store's transactional
	// ClaimIdleWorker / ClaimHotIdleWorker / TakeOverWorker operations, and a
	// worker's org assignment never changes after activation (hot → hot-idle
	// stays on the same org until TTL expiry). Validating epoch/cpInstanceID
	// on every health check caused cascading worker kills during CP rolling
	// updates: the fresh CP starts with epoch 0 while workers remember a
	// higher epoch from the previous CP, so all health checks fail and all
	// workers get deleted.
	if !p.sharedWarmMode {
		return nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.workerID > 0 && meta.WorkerID != 0 && meta.WorkerID != p.workerID {
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
	overrideS3EndpointForCacheProxy(&cfg.DuckLake)
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
