//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/server"
)

// OrgStack holds the isolated worker pool and session manager for an org.
type OrgStack struct {
	Config     *configstore.OrgConfig
	Pool       WorkerPool
	Sessions   *SessionManager
	Rebalancer *MemoryRebalancer
	cancel     context.CancelFunc
}

// OrgRouter manages per-org stacks, creating/destroying them as config changes.
type OrgRouter struct {
	mu                    sync.RWMutex
	orgs                  map[string]*OrgStack
	configStore           *configstore.ConfigStore
	baseCfg               K8sWorkerPoolConfig
	sharedPool            *K8sWorkerPool
	globalCfg             ControlPlaneConfig
	srv                   *server.Server
	stsBroker             *STSBroker
	userSecrets           *CPUserSecretManager
	resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error)
	nextWorkerID          atomic.Int32
	sharedCancel          context.CancelFunc

	// migrating tracks which orgs have a DuckLake migration in progress.
	// During migration, new connections for the org are rejected with a
	// retry-friendly error instead of timing out waiting for a worker.
	migrating sync.Map // orgID (string) → struct{}
}

// NewOrgRouter creates an OrgRouter from the initial config snapshot.
func NewOrgRouter(store *configstore.ConfigStore, baseCfg K8sWorkerPoolConfig, globalCfg ControlPlaneConfig, srv *server.Server, stsBroker *STSBroker, userSecrets *CPUserSecretManager, resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error)) (*OrgRouter, error) {
	tr := &OrgRouter{
		orgs:                  make(map[string]*OrgStack),
		configStore:           store,
		baseCfg:               baseCfg,
		globalCfg:             globalCfg,
		srv:                   srv,
		stsBroker:             stsBroker,
		userSecrets:           userSecrets,
		resolveDucklingStatus: resolveDucklingStatus,
	}

	sharedCfg := baseCfg
	sharedCfg.OrgID = ""
	sharedCfg.WorkerIDGenerator = func() int {
		return int(tr.nextWorkerID.Add(1))
	}
	sharedCfg.RuntimeStore = store

	sharedPoolIface, err := CreateK8sPool(sharedCfg)
	if err != nil {
		return nil, err
	}
	sharedPool, ok := sharedPoolIface.(*K8sWorkerPool)
	if !ok {
		return nil, fmt.Errorf("expected shared K8s pool, got %T", sharedPoolIface)
	}
	tr.sharedPool = sharedPool

	sharedCtx, sharedCancel := context.WithCancel(context.Background())
	tr.sharedCancel = sharedCancel
	go tr.sharedPool.HealthCheckLoop(sharedCtx, tr.globalCfg.HealthCheckInterval, tr.onSharedWorkerCrash, tr.onSharedWorkerProgress)

	snap := store.Snapshot()
	for _, tc := range snap.Orgs {
		// Only create stacks for orgs with ready warehouses (or no warehouse at all for backwards compat)
		if tc.Warehouse != nil && tc.Warehouse.State != configstore.ManagedWarehouseStateReady {
			slog.Info("Skipping org stack creation (warehouse not ready).", "org", tc.Name, "state", tc.Warehouse.State)
			continue
		}
		if _, err := tr.createOrgStack(tc); err != nil {
			slog.Error("Failed to create org stack.", "org", tc.Name, "error", err)
			continue
		}
	}

	return tr, nil
}

// createOrgStack creates an isolated pool + session manager for an org.
func (tr *OrgRouter) createOrgStack(tc *configstore.OrgConfig) (*OrgStack, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Per-org worker cap. 0 = unbounded (the cluster autoscaler / node capacity
	// is the only ceiling). There is no global/cluster-default fallback.
	maxWorkers := tc.MaxWorkers

	pool := NewOrgReservedPool(tr.sharedPool, tc.Name, maxWorkers, workerImageForOrg(tc, tr.baseCfg.WorkerImage), tr.stsBroker)
	activator := NewSharedWorkerActivator(tr.sharedPool, tr.stsBroker, tr.globalCfg.DuckLakeDefaultSpecVersion, func(orgID string) (*configstore.OrgConfig, error) {
		snap := tr.configStore.Snapshot()
		if snap == nil {
			return nil, fmt.Errorf("config snapshot unavailable for org %s", orgID)
		}
		org, ok := snap.Orgs[orgID]
		if !ok {
			return nil, fmt.Errorf("org %s not found in config snapshot", orgID)
		}
		return org, nil
	})
	activator.resolveDucklingStatus = tr.resolveDucklingStatus
	activator.setMigrating = tr.SetMigrating
	activator.clearMigrating = tr.ClearMigrating
	pool.activateReservedWorker = activator.ActivateReservedWorker
	// In K8s mode, DuckDB auto-detects memory from the container's cgroup limits.
	// Pass 0/false to disable budget-based rebalancing.
	rebalancer := NewMemoryRebalancer(0, 0, nil, false)
	sessions := NewOrgSessionManager(pool, rebalancer, tc.Name)
	if tr.userSecrets != nil {
		sessions.SetUserSecretLoader(tr.userSecrets.SessionSecretLoader(tc.Name))
	}
	sessions.SetResourceLimitsProvider(tr.resourceLimitsForOrg(tc.Name))
	sessions.SetRequestedVCPUsResolver(func(profile *WorkerProfile) (int, error) {
		return requestedWorkerVCPUs(profile, tr.baseCfg.WorkerCPURequest)
	})
	sessions.SetConnectionLimiter(NewRuntimeOrgConnectionLimiter(tr.configStore, tc.Name, tr.baseCfg.CPInstanceID, tr.globalCfg.WorkerQueueTimeout))
	rebalancer.SetSessionLister(sessions)

	// Periodic per-org metrics emission
	orgID := tc.Name
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sessionCount := sessions.SessionCount()
				observeOrgSessionsActive(orgID, sessionCount)
			}
		}
	}()

	stack := &OrgStack{
		Config:     tc,
		Pool:       pool,
		Sessions:   sessions,
		Rebalancer: rebalancer,
		cancel:     cancel,
	}

	tr.mu.Lock()
	tr.orgs[tc.Name] = stack
	tr.mu.Unlock()

	slog.Info("Org stack created.", "org", tc.Name, "max_workers", maxWorkers)
	_ = ctx // keep linter happy
	return stack, nil
}

func (tr *OrgRouter) resourceLimitsForOrg(orgID string) func(username string) configstore.OrgResourceLimits {
	return func(username string) configstore.OrgResourceLimits {
		snap := tr.configStore.Snapshot()
		if snap == nil {
			return configstore.OrgResourceLimits{}
		}
		limits := configstore.OrgResourceLimits{}
		if org, ok := snap.Orgs[orgID]; ok && org != nil {
			limits.OrgMaxVCPUs = org.MaxVCPUs
		}
		limits.UserMaxVCPUs = snap.OrgUserMaxVCPUs[configstore.OrgUserKey{OrgID: orgID, Username: username}]
		return limits
	}
}

// DestroyOrgStack drains and cleans up an org's resources.
func (tr *OrgRouter) DestroyOrgStack(orgID string) {
	tr.mu.Lock()
	stack, ok := tr.orgs[orgID]
	if !ok {
		tr.mu.Unlock()
		return
	}
	delete(tr.orgs, orgID)
	tr.mu.Unlock()

	slog.Info("Destroying org stack.", "org", orgID)
	stack.cancel()
	if stack.Sessions != nil {
		stack.Sessions.DestroyAllSessions()
	}
	stack.Pool.ShutdownAll()
	if stack.Rebalancer != nil {
		stack.Rebalancer.Stop()
	}
}

// StackForOrg resolves an orgID directly to its org stack.
func (tr *OrgRouter) StackForOrg(orgID string) (*OrgStack, bool) {
	tr.mu.RLock()
	stack, ok := tr.orgs[orgID]
	tr.mu.RUnlock()
	return stack, ok
}

func (tr *OrgRouter) IcebergConfigForOrg(orgID string) (server.IcebergConfig, bool) {
	tr.mu.RLock()
	stack, ok := tr.orgs[orgID]
	tr.mu.RUnlock()
	if !ok || stack == nil || stack.Config == nil || stack.Config.Warehouse == nil {
		return server.IcebergConfig{}, false
	}

	src := stack.Config.Warehouse.Iceberg
	cfg := server.IcebergConfig{
		Enabled:                   src.Enabled,
		Backend:                   src.Backend,
		Namespace:                 src.Namespace,
		Region:                    src.Region,
		LakekeeperEndpoint:        src.LakekeeperEndpoint,
		LakekeeperWarehouse:       src.LakekeeperWarehouse,
		LakekeeperClientID:        src.LakekeeperClientID,
		LakekeeperOAuth2ServerURI: src.LakekeeperOAuth2ServerURI,
	}
	return cfg, true
}

// SetMigrating marks an org as having a DuckLake migration in progress.
func (tr *OrgRouter) SetMigrating(orgID string) {
	tr.migrating.Store(orgID, struct{}{})
	slog.Info("DuckLake migration started for org.", "org", orgID)
}

// ClearMigrating marks an org's DuckLake migration as complete.
func (tr *OrgRouter) ClearMigrating(orgID string) {
	tr.migrating.Delete(orgID)
	slog.Info("DuckLake migration completed for org.", "org", orgID)
}

// IsMigrating returns true if the org has a DuckLake migration in progress.
func (tr *OrgRouter) IsMigrating(orgID string) bool {
	_, ok := tr.migrating.Load(orgID)
	return ok
}

// HandleConfigChange reconciles org stacks when the config snapshot changes.
func (tr *OrgRouter) HandleConfigChange(old, new *configstore.Snapshot) {
	// Detect new orgs or orgs whose warehouse just became ready
	for name, tc := range new.Orgs {
		oldTC, existed := old.Orgs[name]

		// Skip orgs with warehouses that aren't ready
		if tc.Warehouse != nil && tc.Warehouse.State != configstore.ManagedWarehouseStateReady {
			// If warehouse is being deleted, destroy existing stack
			if tc.Warehouse.State == configstore.ManagedWarehouseStateDeleting ||
				tc.Warehouse.State == configstore.ManagedWarehouseStateDeleted {
				tr.mu.RLock()
				_, hasStack := tr.orgs[name]
				tr.mu.RUnlock()
				if hasStack {
					slog.Info("Warehouse deprovisioning, destroying stack.", "org", name)
					tr.DestroyOrgStack(name)
				}
			}
			continue
		}

		tr.mu.RLock()
		_, hasStack := tr.orgs[name]
		tr.mu.RUnlock()

		if !existed && !hasStack {
			// Brand new org -- create stack
			slog.Info("New org detected, creating stack.", "org", name)
			if _, err := tr.createOrgStack(tc); err != nil {
				slog.Error("Failed to create org stack on config change.", "org", name, "error", err)
			}
		} else if existed && !hasStack {
			// Existing org whose warehouse just became ready
			warehouseJustReady := oldTC.Warehouse != nil &&
				oldTC.Warehouse.State != configstore.ManagedWarehouseStateReady &&
				tc.Warehouse != nil &&
				tc.Warehouse.State == configstore.ManagedWarehouseStateReady
			noWarehouse := tc.Warehouse == nil

			if warehouseJustReady || noWarehouse {
				slog.Info("Org warehouse ready, creating stack.", "org", name)
				if _, err := tr.createOrgStack(tc); err != nil {
					slog.Error("Failed to create org stack on config change.", "org", name, "error", err)
				}
			}
		}
	}

	// Detect removed orgs
	for name := range old.Orgs {
		if _, exists := new.Orgs[name]; !exists {
			slog.Info("Org removed, destroying stack.", "org", name)
			tr.DestroyOrgStack(name)
		}
	}

	// Refresh existing org stacks and update worker limits when needed.
	for name, newTC := range new.Orgs {
		oldTC, existed := old.Orgs[name]
		if !existed {
			continue
		}
		limitsChanged := oldTC.MaxWorkers != newTC.MaxWorkers
		resourceLimitChanged := oldTC.MaxVCPUs != newTC.MaxVCPUs
		floorChanged := oldTC.DefaultWorkerMinHotIdle != newTC.DefaultWorkerMinHotIdle
		imageChanged := workerImageForOrg(oldTC, tr.baseCfg.WorkerImage) != workerImageForOrg(newTC, tr.baseCfg.WorkerImage)

		tr.mu.Lock()
		if stack, ok := tr.orgs[name]; ok {
			stack.Config = newTC
			if resourceLimitChanged {
				slog.Info("Org resource limit changed.", "org", name,
					"old_max_vcpus", oldTC.MaxVCPUs, "new_max_vcpus", newTC.MaxVCPUs)
			}
			if limitsChanged {
				slog.Info("Org config changed.", "org", name,
					"old_max_workers", oldTC.MaxWorkers, "new_max_workers", newTC.MaxWorkers)
				// 0 = unbounded; no global/cluster-default fallback.
				stack.Pool.SetMaxWorkers(newTC.MaxWorkers)
			}
			if floorChanged {
				slog.Info("Org default hot-idle floor changed.", "org", name,
					"old_default_worker_min_hot_idle", oldTC.DefaultWorkerMinHotIdle,
					"new_default_worker_min_hot_idle", newTC.DefaultWorkerMinHotIdle)
			}
			if imageChanged {
				image := workerImageForOrg(newTC, tr.baseCfg.WorkerImage)
				slog.Info("Org worker image changed.", "org", name, "image", image)
				if pool, ok := stack.Pool.(interface{ SetWorkerImage(string) }); ok {
					pool.SetWorkerImage(image)
				}
			}
		}
		tr.mu.Unlock()
	}
}

func workerImageForOrg(tc *configstore.OrgConfig, fallback string) string {
	if tc != nil && tc.Warehouse != nil && tc.Warehouse.Image != "" {
		return tc.Warehouse.Image
	}
	return fallback
}

// AllStacks returns a snapshot of all org stacks for admin API usage.
func (tr *OrgRouter) AllStacks() map[string]*OrgStack {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	result := make(map[string]*OrgStack, len(tr.orgs))
	for k, v := range tr.orgs {
		result[k] = v
	}
	return result
}

// ShutdownAll shuts down all org stacks.
func (tr *OrgRouter) ShutdownAll() {
	tr.mu.Lock()
	orgs := make(map[string]*OrgStack, len(tr.orgs))
	for k, v := range tr.orgs {
		orgs[k] = v
	}
	tr.orgs = make(map[string]*OrgStack)
	tr.mu.Unlock()

	for name, stack := range orgs {
		slog.Info("Shutting down org stack.", "org", name)
		stack.cancel()
		if stack.Sessions != nil {
			stack.Sessions.DestroyAllSessions()
		}
		stack.Pool.ShutdownAll()
		if stack.Rebalancer != nil {
			stack.Rebalancer.Stop()
		}
	}

	if tr.sharedCancel != nil {
		tr.sharedCancel()
	}
	if tr.sharedPool != nil {
		tr.sharedPool.ShutdownAll()
	}
}

// ReleaseIdleHotWorkers parks this CP's idle (zero-session) Hot workers into
// hot_idle so the TTL reaper can reclaim them, instead of letting them linger
// for the whole (possibly unbounded) drain wait. All workers live in the shared
// pool; per-org reserved pools are slices of it. Returns the number parked.
func (tr *OrgRouter) ReleaseIdleHotWorkers() int {
	if tr.sharedPool == nil {
		return 0
	}
	return tr.sharedPool.ReleaseIdleHotWorkers(LifecycleOriginDrainReleaseIdle)
}

func (tr *OrgRouter) onSharedWorkerCrash(workerID int) {
	stack, orgID, ok := tr.stackForWorker(workerID)
	if !ok {
		return
	}

	observeOrgWorkerCrash(orgID)
	stack.Sessions.OnWorkerCrash(workerID, func(pid int32) {
		slog.Warn("Session orphaned by worker crash.", "org", orgID, "pid", pid, "worker", workerID)
	})
}

func (tr *OrgRouter) onSharedWorkerProgress(workerID int, progress map[string]*SessionProgress) {
	stack, _, ok := tr.stackForWorker(workerID)
	if !ok {
		return
	}
	stack.Sessions.UpdateProgress(workerID, progress)
}

func (tr *OrgRouter) stackForWorker(workerID int) (*OrgStack, string, bool) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	for orgID, stack := range tr.orgs {
		if stack.Sessions.SessionCountForWorker(workerID) > 0 {
			return stack, orgID, true
		}
	}
	return nil, "", false
}
