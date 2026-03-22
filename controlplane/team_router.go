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
	"github.com/posthog/duckgres/server"
)

// TeamStack holds the isolated worker pool and session manager for a team.
type TeamStack struct {
	Config     *configstore.TeamConfig
	Pool       WorkerPool
	Sessions   *SessionManager
	Rebalancer *MemoryRebalancer
	cancel     context.CancelFunc
}

// TeamRouter manages per-team stacks, creating/destroying them as config changes.
type TeamRouter struct {
	mu           sync.RWMutex
	teams        map[string]*TeamStack
	configStore  *configstore.ConfigStore
	baseCfg      K8sWorkerPoolConfig
	sharedPool   *K8sWorkerPool
	globalCfg    ControlPlaneConfig
	srv          *server.Server
	nextWorkerID atomic.Int32
	sharedCancel context.CancelFunc
}

// NewTeamRouter creates a TeamRouter from the initial config snapshot.
func NewTeamRouter(store *configstore.ConfigStore, baseCfg K8sWorkerPoolConfig, globalCfg ControlPlaneConfig, srv *server.Server) (*TeamRouter, error) {
	tr := &TeamRouter{
		teams:       make(map[string]*TeamStack),
		configStore: store,
		baseCfg:     baseCfg,
		globalCfg:   globalCfg,
		srv:         srv,
	}

	sharedCfg := baseCfg
	sharedCfg.TeamName = ""
	sharedCfg.WorkerIDGenerator = func() int {
		return int(tr.nextWorkerID.Add(1))
	}

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
	for _, tc := range snap.Teams {
		if _, err := tr.createTeamStack(tc); err != nil {
			slog.Error("Failed to create team stack.", "team", tc.Name, "error", err)
			continue
		}
	}

	tr.reconcileWarmCapacity(snap)

	return tr, nil
}

// createTeamStack creates an isolated pool + session manager for a team.
func (tr *TeamRouter) createTeamStack(tc *configstore.TeamConfig) (*TeamStack, error) {
	ctx, cancel := context.WithCancel(context.Background())

	maxWorkers := tc.MaxWorkers
	if maxWorkers == 0 {
		maxWorkers = tr.baseCfg.MaxWorkers
	}

	memoryBudget := tr.baseCfg.MemoryBudget
	if tc.MemoryBudget != "" {
		memoryBudget = int64(server.ParseMemoryBytes(tc.MemoryBudget))
	}

	pool := NewTeamReservedWorkerPool(tr.sharedPool, tc.Name, maxWorkers)

	rebalancer := NewMemoryRebalancer(uint64(memoryBudget), 0, nil, tr.globalCfg.MemoryRebalance)
	sessions := NewSessionManager(pool, rebalancer)
	rebalancer.SetSessionLister(sessions)

	// Periodic per-team metrics emission
	teamName := tc.Name
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sessionCount := sessions.SessionCount()
				observeTeamSessionsActive(teamName, sessionCount)
			}
		}
	}()

	stack := &TeamStack{
		Config:     tc,
		Pool:       pool,
		Sessions:   sessions,
		Rebalancer: rebalancer,
		cancel:     cancel,
	}

	tr.mu.Lock()
	tr.teams[tc.Name] = stack
	tr.mu.Unlock()

	slog.Info("Team stack created.", "team", tc.Name, "max_workers", maxWorkers)
	_ = ctx // keep linter happy
	return stack, nil
}

// DestroyTeamStack drains and cleans up a team's resources.
func (tr *TeamRouter) DestroyTeamStack(teamName string) {
	tr.mu.Lock()
	stack, ok := tr.teams[teamName]
	if !ok {
		tr.mu.Unlock()
		return
	}
	delete(tr.teams, teamName)
	tr.mu.Unlock()

	slog.Info("Destroying team stack.", "team", teamName)
	stack.cancel()
	stack.Pool.ShutdownAll()
	if stack.Rebalancer != nil {
		stack.Rebalancer.Stop()
	}
}

// StackForUser resolves a username to its team stack.
func (tr *TeamRouter) StackForUser(username string) (*TeamStack, bool) {
	teamName := tr.configStore.TeamForUser(username)
	if teamName == "" {
		return nil, false
	}

	tr.mu.RLock()
	stack, ok := tr.teams[teamName]
	tr.mu.RUnlock()
	return stack, ok
}

// HandleConfigChange reconciles team stacks when the config snapshot changes.
func (tr *TeamRouter) HandleConfigChange(old, new *configstore.Snapshot) {
	// Detect new teams
	for name, tc := range new.Teams {
		if _, existed := old.Teams[name]; !existed {
			slog.Info("New team detected, creating stack.", "team", name)
			if _, err := tr.createTeamStack(tc); err != nil {
				slog.Error("Failed to create team stack on config change.", "team", name, "error", err)
			}
		}
	}

	// Detect removed teams
	for name := range old.Teams {
		if _, exists := new.Teams[name]; !exists {
			slog.Info("Team removed, destroying stack.", "team", name)
			tr.DestroyTeamStack(name)
		}
	}

	// Detect changed team limits (update in-place)
	for name, newTC := range new.Teams {
		oldTC, existed := old.Teams[name]
		if !existed {
			continue
		}
		if oldTC.MaxWorkers != newTC.MaxWorkers || oldTC.MemoryBudget != newTC.MemoryBudget {
			slog.Info("Team config changed.", "team", name,
				"old_max_workers", oldTC.MaxWorkers, "new_max_workers", newTC.MaxWorkers)
			tr.mu.Lock()
			if stack, ok := tr.teams[name]; ok {
				stack.Config = newTC
				// Propagate MaxWorkers to the pool so it enforces the new limit
				maxWorkers := newTC.MaxWorkers
				if maxWorkers == 0 {
					maxWorkers = tr.baseCfg.MaxWorkers
				}
				stack.Pool.SetMaxWorkers(maxWorkers)
			}
			tr.mu.Unlock()
		}
	}

	tr.reconcileWarmCapacity(new)
}

// AllStacks returns a snapshot of all team stacks for admin API usage.
func (tr *TeamRouter) AllStacks() map[string]*TeamStack {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	result := make(map[string]*TeamStack, len(tr.teams))
	for k, v := range tr.teams {
		result[k] = v
	}
	return result
}

// ShutdownAll shuts down all team stacks.
func (tr *TeamRouter) ShutdownAll() {
	tr.mu.Lock()
	teams := make(map[string]*TeamStack, len(tr.teams))
	for k, v := range tr.teams {
		teams[k] = v
	}
	tr.teams = make(map[string]*TeamStack)
	tr.mu.Unlock()

	for name, stack := range teams {
		slog.Info("Shutting down team stack.", "team", name)
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

func (tr *TeamRouter) reconcileWarmCapacity(snap *configstore.Snapshot) {
	if tr.sharedPool == nil || snap == nil {
		return
	}

	target := tr.globalCfg.K8s.SharedWarmTarget
	if target < 0 {
		target = 0
	}

	tr.sharedPool.SetWarmCapacityTarget(target)
	if target > 0 {
		observeTeamWorkerSpawn("shared")
		if err := tr.sharedPool.SpawnMinWorkers(target); err != nil {
			slog.Warn("Failed to reconcile shared warm capacity.", "target", target, "error", err)
		}
	}
}

func (tr *TeamRouter) onSharedWorkerCrash(workerID int) {
	stack, teamName, ok := tr.stackForWorker(workerID)
	if !ok {
		return
	}

	observeTeamWorkerCrash(teamName)
	stack.Sessions.OnWorkerCrash(workerID, func(pid int32) {
		slog.Warn("Session orphaned by worker crash.", "team", teamName, "pid", pid, "worker", workerID)
	})
}

func (tr *TeamRouter) onSharedWorkerProgress(workerID int, progress map[string]*SessionProgress) {
	stack, _, ok := tr.stackForWorker(workerID)
	if !ok {
		return
	}
	stack.Sessions.UpdateProgress(workerID, progress)
}

func (tr *TeamRouter) stackForWorker(workerID int) (*TeamStack, string, bool) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	for teamName, stack := range tr.teams {
		if stack.Sessions.SessionCountForWorker(workerID) > 0 {
			return stack, teamName, true
		}
	}
	return nil, "", false
}
