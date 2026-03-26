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
	resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error)
	nextWorkerID          atomic.Int32
	sharedCancel          context.CancelFunc

	// migrating tracks which orgs have a DuckLake migration in progress.
	// During migration, new connections for the org are rejected with a
	// retry-friendly error instead of timing out waiting for a worker.
	migrating sync.Map // orgID (string) → struct{}
}

// NewOrgRouter creates an OrgRouter from the initial config snapshot.
func NewOrgRouter(store *configstore.ConfigStore, baseCfg K8sWorkerPoolConfig, globalCfg ControlPlaneConfig, srv *server.Server, stsBroker *STSBroker, resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error)) (*OrgRouter, error) {
	tr := &OrgRouter{
		orgs:                  make(map[string]*OrgStack),
		configStore:           store,
		baseCfg:               baseCfg,
		globalCfg:             globalCfg,
		srv:                   srv,
		stsBroker:             stsBroker,
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

	tr.reconcileWarmCapacity(snap)

	return tr, nil
}

// createOrgStack creates an isolated pool + session manager for an org.
func (tr *OrgRouter) createOrgStack(tc *configstore.OrgConfig) (*OrgStack, error) {
	ctx, cancel := context.WithCancel(context.Background())

	maxWorkers := tc.MaxWorkers
	if maxWorkers == 0 {
		maxWorkers = tr.baseCfg.MaxWorkers
	}

	pool := NewOrgReservedPool(tr.sharedPool, tc.Name, maxWorkers, tr.stsBroker)
	activator := NewSharedWorkerActivator(tr.sharedPool, tr.stsBroker, func(orgID string) (*configstore.OrgConfig, error) {
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
	sessions := NewSessionManager(pool, rebalancer)
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
	stack.Pool.ShutdownAll()
	if stack.Rebalancer != nil {
		stack.Rebalancer.Stop()
	}
}

// StackForUser resolves a username to its org stack.
func (tr *OrgRouter) StackForUser(username string) (*OrgStack, bool) {
	orgID := tr.configStore.OrgForUser(username)
	if orgID == "" {
		return nil, false
	}

	tr.mu.RLock()
	stack, ok := tr.orgs[orgID]
	tr.mu.RUnlock()
	return stack, ok
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

// IsMigratingForUser resolves a username to its org and checks migration state.
func (tr *OrgRouter) IsMigratingForUser(username string) (bool, string) {
	orgID := tr.configStore.OrgForUser(username)
	if orgID == "" {
		return false, ""
	}
	return tr.IsMigrating(orgID), orgID
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

		tr.mu.Lock()
		if stack, ok := tr.orgs[name]; ok {
			stack.Config = newTC
			if limitsChanged {
				slog.Info("Org config changed.", "org", name,
					"old_max_workers", oldTC.MaxWorkers, "new_max_workers", newTC.MaxWorkers)
				maxWorkers := newTC.MaxWorkers
				if maxWorkers == 0 {
					maxWorkers = tr.baseCfg.MaxWorkers
				}
				stack.Pool.SetMaxWorkers(maxWorkers)
			}
		}
		tr.mu.Unlock()
	}

	tr.reconcileWarmCapacity(new)
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

func (tr *OrgRouter) reconcileWarmCapacity(snap *configstore.Snapshot) {
	if tr.sharedPool == nil || snap == nil {
		return
	}

	target := tr.globalCfg.K8s.SharedWarmTarget
	if target < 0 {
		target = 0
	}

	tr.sharedPool.SetWarmCapacityTarget(target)
	if target > 0 {
		observeOrgWorkerSpawn("shared")
		if err := tr.sharedPool.SpawnMinWorkers(target); err != nil {
			slog.Warn("Failed to reconcile shared warm capacity.", "target", target, "error", err)
		}
	}
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
