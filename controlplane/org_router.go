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
	draining              atomic.Bool
	sharedCancel          context.CancelFunc
	projectReaderChange   func(orgID, username string)
	admissionReclaimer    admissionReclaimer
	terminal              bool // protected by mu; no stack operation may start after this is set
	stackOps              sync.WaitGroup
	orgStackMutations     map[string]chan struct{} // protected by mu; serializes one org generation through teardown/publication
	shutdownOnce          sync.Once

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
	tr.admissionReclaimer = NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxReservations: globalCfg.AdmissionReclaimerMaxReservations,
	})

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
	if tc == nil {
		return nil, fmt.Errorf("org config is required")
	}
	finishCreation, err := tr.beginOrgStackCreation(tc.Name)
	if err != nil {
		return nil, err
	}
	defer finishCreation()

	// A config callback may have waited behind an older mutation. Resolve the
	// authoritative config only after acquiring the per-org slot so stale
	// callbacks cannot construct a generation the current snapshot rejects.
	latest, state := tr.latestOrgStackState(tc.Name, &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{tc.Name: tc},
	})
	if state != orgStackEnsurePresent {
		return nil, fmt.Errorf("current config does not allow an org stack for org %s", tc.Name)
	}

	return tr.createOrgStackWhileMutationHeld(latest)
}

// createOrgStackWhileMutationHeld constructs and publishes one org generation.
// The caller must hold the org's mutation slot from before config selection
// through this function's return.
func (tr *OrgRouter) createOrgStackWhileMutationHeld(tc *configstore.OrgConfig) (*OrgStack, error) {

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
	sessions.SetConnectionLimiter(NewRuntimeOrgConnectionLimiter(
		tr.configStore,
		tc.Name,
		tr.baseCfg.CPInstanceID,
		tr.globalCfg.WorkerQueueTimeout,
		tr.admissionReclaimer,
	))
	rebalancer.SetSessionLister(sessions)

	stack := &OrgStack{
		Config:     tc,
		Pool:       pool,
		Sessions:   sessions,
		Rebalancer: rebalancer,
		cancel:     cancel,
	}

	publishResult, publishErr := tr.publishOrgStackCandidate(tc.Name, stack)
	if publishErr != nil {
		shutdownUnpublishedOrgStack(stack)
		return nil, publishErr
	}
	switch publishResult {
	case orgStackPublishAccepted:
	case orgStackPublishRejectedTerminal:
		shutdownUnpublishedOrgStack(stack)
		return nil, fmt.Errorf("org router is shutting down")
	case orgStackPublishRejectedDuplicate:
		shutdownUnpublishedOrgStack(stack)
		return nil, fmt.Errorf("org stack already exists for org %s", tc.Name)
	case orgStackPublishRejectedConfig:
		shutdownUnpublishedOrgStack(stack)
		return nil, fmt.Errorf("current config does not allow an org stack for org %s", tc.Name)
	}

	// Start candidate-owned background work only after successful publication.
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

	slog.Info("Org stack created.", "org", tc.Name, "max_workers", stack.Config.MaxWorkers)
	return stack, nil
}

// beginOrgStackOperation registers a create/destroy operation while holding the
// same mutex that guards the terminal shutdown transition. This ordering makes
// stackOps.Wait safe: once terminal is set, no later WaitGroup.Add can race it.
func (tr *OrgRouter) beginOrgStackOperation() (func(), bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.terminal {
		return nil, false
	}
	tr.stackOps.Add(1)
	return tr.stackOps.Done, true
}

// beginOrgStackMutation holds one per-org generation slot through either
// construction/publication or removal/teardown. OrgReservedPool.ShutdownAll is
// intentionally org-scoped rather than pool-instance-scoped, so allowing old
// teardown to overlap replacement construction could retire the replacement's
// workers.
func (tr *OrgRouter) beginOrgStackMutation(orgID string) (func(), error) {
	finishOperation, ok := tr.beginOrgStackOperation()
	if !ok {
		return nil, fmt.Errorf("org router is shutting down")
	}

	for {
		tr.mu.Lock()
		if tr.terminal {
			tr.mu.Unlock()
			finishOperation()
			return nil, fmt.Errorf("org router is shutting down")
		}
		if tr.orgStackMutations == nil {
			tr.orgStackMutations = make(map[string]chan struct{})
		}
		if wait, exists := tr.orgStackMutations[orgID]; exists {
			tr.mu.Unlock()
			<-wait
			continue
		}

		done := make(chan struct{})
		tr.orgStackMutations[orgID] = done
		tr.mu.Unlock()

		var once sync.Once
		return func() {
			once.Do(func() {
				tr.mu.Lock()
				delete(tr.orgStackMutations, orgID)
				close(done)
				tr.mu.Unlock()
				finishOperation()
			})
		}, nil
	}
}

// beginOrgStackCreation acquires the per-org generation slot and verifies that
// no published stack exists before the caller constructs any org-scoped pool.
func (tr *OrgRouter) beginOrgStackCreation(orgID string) (func(), error) {
	finish, err := tr.beginOrgStackMutation(orgID)
	if err != nil {
		return nil, err
	}

	tr.mu.RLock()
	_, exists := tr.orgs[orgID]
	terminal := tr.terminal
	tr.mu.RUnlock()
	if terminal {
		finish()
		return nil, fmt.Errorf("org router is shutting down")
	}
	if exists {
		finish()
		return nil, fmt.Errorf("org stack already exists for org %s", orgID)
	}
	return finish, nil
}

type orgStackPublishResult uint8

const (
	orgStackPublishAccepted orgStackPublishResult = iota
	orgStackPublishRejectedDuplicate
	orgStackPublishRejectedTerminal
	orgStackPublishRejectedConfig
)

// publishOrgStackCandidate validates and publishes a fully built, unpublished
// candidate atomically with respect to ConfigStore snapshot replacement. The
// caller owns candidate cleanup on every rejected result.
func (tr *OrgRouter) publishOrgStackCandidate(orgID string, stack *OrgStack) (orgStackPublishResult, error) {
	if stack == nil || stack.Config == nil {
		return orgStackPublishRejectedConfig, fmt.Errorf("org stack candidate config is required")
	}

	result := orgStackPublishRejectedConfig
	var validationErr error
	validateAndPublish := func(snapshot *configstore.Snapshot) {
		tc, state := orgStackStateFromSnapshot(orgID, snapshot)
		if state != orgStackEnsurePresent {
			validationErr = fmt.Errorf("current config does not allow an org stack for org %s", orgID)
			return
		}
		if tc.Name != orgID {
			validationErr = fmt.Errorf("org config name %q does not match org %q", tc.Name, orgID)
			return
		}

		// The candidate is still private under the org mutation slot. Bring the
		// fields captured during construction forward to the config protected by
		// this read lock before making the generation visible.
		pool, ok := stack.Pool.(*OrgReservedPool)
		if !ok {
			validationErr = fmt.Errorf("org stack candidate has unexpected pool type %T", stack.Pool)
			return
		}
		stack.Config = tc
		pool.maxWorkers = tc.MaxWorkers
		pool.image = workerImageForOrg(tc, tr.baseCfg.WorkerImage)
		result = tr.publishOrgStack(orgID, stack)
	}

	if tr.configStore != nil {
		tr.configStore.WithSnapshot(validateAndPublish)
	} else {
		validateAndPublish(&configstore.Snapshot{
			Orgs: map[string]*configstore.OrgConfig{orgID: stack.Config},
		})
	}
	return result, validationErr
}

// publishOrgStack closes the creation lifecycle before making a late-created
// stack visible when the router is already draining. Holding mu across the
// drain/terminal checks and publication closes the races with BeginDrain's and
// ShutdownAll's stack snapshots. Publication is insert-only; a rejected
// candidate remains owned by the registered creator, which must release only
// its unpublished resources before completing the operation.
func (tr *OrgRouter) publishOrgStack(orgID string, stack *OrgStack) orgStackPublishResult {
	tr.mu.Lock()
	if tr.terminal {
		tr.mu.Unlock()
		return orgStackPublishRejectedTerminal
	}
	if _, ok := tr.orgs[orgID]; ok {
		tr.mu.Unlock()
		return orgStackPublishRejectedDuplicate
	}
	if tr.draining.Load() && stack != nil && stack.Sessions != nil {
		stack.Sessions.BeginDrain()
	}
	tr.orgs[orgID] = stack
	tr.mu.Unlock()
	return orgStackPublishAccepted
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

type orgStackReconcileState uint8

const (
	// Preserve the current presence for transitional warehouse states. A
	// running stack remains available while resharding, but a CP that starts in
	// the middle of provisioning or resharding must not create a new one.
	orgStackPreserve orgStackReconcileState = iota
	orgStackEnsurePresent
	orgStackEnsureAbsent
)

// latestOrgStackState reads the ConfigStore snapshot after the caller has
// acquired the per-org mutation slot. ConfigStore publishes immutable snapshot
// pointers before invoking callbacks, so this makes callbacks converge on the
// latest published state even when callback executions overlap or reorder.
// fallback keeps direct unit construction and routers without a ConfigStore
// source-compatible with the callback snapshot they were given.
func (tr *OrgRouter) latestOrgStackState(orgID string, fallback *configstore.Snapshot) (*configstore.OrgConfig, orgStackReconcileState) {
	snapshot := fallback
	if tr.configStore != nil {
		snapshot = tr.configStore.Snapshot()
	}
	return orgStackStateFromSnapshot(orgID, snapshot)
}

func orgStackStateFromSnapshot(orgID string, snapshot *configstore.Snapshot) (*configstore.OrgConfig, orgStackReconcileState) {
	if snapshot == nil {
		return nil, orgStackPreserve
	}

	tc, exists := snapshot.Orgs[orgID]
	if !exists || tc == nil {
		return nil, orgStackEnsureAbsent
	}
	if tc.Warehouse == nil || tc.Warehouse.State == configstore.ManagedWarehouseStateReady {
		return tc, orgStackEnsurePresent
	}
	if tc.Warehouse.State == configstore.ManagedWarehouseStateDeleting ||
		tc.Warehouse.State == configstore.ManagedWarehouseStateDeleted {
		return tc, orgStackEnsureAbsent
	}
	return tc, orgStackPreserve
}

// reconcileOrgStack serializes every callback for an org, then decides against
// the latest published config while holding that slot. Every callback enters
// this path even when its own old/new snapshots appear to require no action;
// this is what lets a newer ready callback repair an older removal that was
// already in flight.
func (tr *OrgRouter) reconcileOrgStack(orgID string, fallback *configstore.Snapshot) error {
	finishMutation, err := tr.beginOrgStackMutation(orgID)
	if err != nil {
		return err
	}
	defer finishMutation()

	tc, state := tr.latestOrgStackState(orgID, fallback)
	tr.mu.RLock()
	stack, hasStack := tr.orgs[orgID]
	tr.mu.RUnlock()

	switch state {
	case orgStackEnsureAbsent:
		if !hasStack {
			return nil
		}
		tr.removeOrgStackWhileMutationHeld(orgID, stack)
		return nil
	case orgStackEnsurePresent:
		if !hasStack {
			_, err := tr.createOrgStackWhileMutationHeld(tc)
			return err
		}
	case orgStackPreserve:
		if !hasStack || tc == nil {
			return nil
		}
	}

	tr.refreshOrgStackWhileMutationHeld(orgID, stack, tc)
	return nil
}

func (tr *OrgRouter) refreshOrgStackWhileMutationHeld(orgID string, stack *OrgStack, tc *configstore.OrgConfig) {
	if stack == nil || tc == nil {
		return
	}

	tr.mu.Lock()
	current, ok := tr.orgs[orgID]
	if !ok || current != stack {
		tr.mu.Unlock()
		return
	}
	oldTC := stack.Config
	stack.Config = tc
	tr.mu.Unlock()

	limitsChanged := oldTC == nil || oldTC.MaxWorkers != tc.MaxWorkers
	resourceLimitChanged := oldTC == nil || oldTC.MaxVCPUs != tc.MaxVCPUs
	floorChanged := oldTC == nil || oldTC.DefaultWorkerMinHotIdle != tc.DefaultWorkerMinHotIdle
	oldImage := tr.baseCfg.WorkerImage
	if oldTC != nil {
		oldImage = workerImageForOrg(oldTC, tr.baseCfg.WorkerImage)
	}
	newImage := workerImageForOrg(tc, tr.baseCfg.WorkerImage)
	imageChanged := oldTC == nil || oldImage != newImage

	if resourceLimitChanged {
		oldMaxVCPUs := 0
		if oldTC != nil {
			oldMaxVCPUs = oldTC.MaxVCPUs
		}
		slog.Info("Org resource limit changed.", "org", orgID,
			"old_max_vcpus", oldMaxVCPUs, "new_max_vcpus", tc.MaxVCPUs)
	}
	if limitsChanged && stack.Pool != nil {
		oldMaxWorkers := 0
		if oldTC != nil {
			oldMaxWorkers = oldTC.MaxWorkers
		}
		slog.Info("Org config changed.", "org", orgID,
			"old_max_workers", oldMaxWorkers, "new_max_workers", tc.MaxWorkers)
		// 0 = unbounded; no global/cluster-default fallback.
		stack.Pool.SetMaxWorkers(tc.MaxWorkers)
	}
	if floorChanged {
		oldFloor := 0
		if oldTC != nil {
			oldFloor = oldTC.DefaultWorkerMinHotIdle
		}
		slog.Info("Org default hot-idle floor changed.", "org", orgID,
			"old_default_worker_min_hot_idle", oldFloor,
			"new_default_worker_min_hot_idle", tc.DefaultWorkerMinHotIdle)
	}
	if imageChanged {
		slog.Info("Org worker image changed.", "org", orgID, "image", newImage)
		if pool, ok := stack.Pool.(interface{ SetWorkerImage(string) }); ok {
			pool.SetWorkerImage(newImage)
		}
	}
}

func (tr *OrgRouter) removeOrgStackWhileMutationHeld(orgID string, expected *OrgStack) {
	tr.mu.Lock()
	stack, ok := tr.orgs[orgID]
	if !ok || (expected != nil && stack != expected) {
		tr.mu.Unlock()
		return
	}
	delete(tr.orgs, orgID)
	tr.mu.Unlock()

	slog.Info("Destroying org stack.", "org", orgID)
	shutdownOrgStack(stack)
}

// DestroyOrgStack drains and cleans up an org's resources.
func (tr *OrgRouter) DestroyOrgStack(orgID string) {
	finishMutation, err := tr.beginOrgStackMutation(orgID)
	if err != nil {
		return
	}
	defer finishMutation()

	tr.removeOrgStackWhileMutationHeld(orgID, nil)
}

func shutdownOrgStack(stack *OrgStack) {
	if stack == nil {
		return
	}
	if stack.cancel != nil {
		stack.cancel()
	}
	if stack.Sessions != nil {
		stack.Sessions.DestroyAllSessions()
	}
	if stack.Pool != nil {
		stack.Pool.ShutdownAll()
	}
	if stack.Rebalancer != nil {
		stack.Rebalancer.Stop()
	}
}

// shutdownUnpublishedOrgStack releases only resources owned by a candidate
// that never became visible. Stack construction does not acquire workers or
// create sessions. In particular, it must not call the org-scoped pool
// ShutdownAll, which could retire workers owned by a published generation.
func shutdownUnpublishedOrgStack(stack *OrgStack) {
	if stack == nil {
		return
	}
	if stack.cancel != nil {
		stack.cancel()
	}
	if stack.Sessions != nil {
		stack.Sessions.BeginDrain()
	}
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
	for key := range changedProjectReaderUsers(old, new) {
		if tr.srv != nil {
			tr.srv.DrainUserConnections(key.OrgID, key.Username)
		}
		tr.mu.RLock()
		onProjectReaderChange := tr.projectReaderChange
		tr.mu.RUnlock()
		if onProjectReaderChange != nil {
			onProjectReaderChange(key.OrgID, key.Username)
		}
		tr.mu.RLock()
		stack := tr.orgs[key.OrgID]
		tr.mu.RUnlock()
		if stack != nil && stack.Sessions != nil {
			stack.Sessions.DestroySessionsForUser(key.Username)
		}
	}

	orgIDs := make(map[string]struct{}, len(old.Orgs)+len(new.Orgs))
	for name := range old.Orgs {
		orgIDs[name] = struct{}{}
	}
	for name, tc := range new.Orgs {
		orgIDs[name] = struct{}{}
		oldTC, existed := old.Orgs[name]
		if tr.srv != nil && tc.Warehouse != nil && tc.Warehouse.State == configstore.ManagedWarehouseStateResharding &&
			(!existed || oldTC == nil || oldTC.Warehouse == nil || oldTC.Warehouse.State != configstore.ManagedWarehouseStateResharding) {
			drained := tr.srv.DrainOrgConnections(name)
			slog.Info("Warehouse resharding, draining PostgreSQL connections at their next idle boundary.",
				"org", name, "connections", drained)
		}
	}

	// Every affected org enters the keyed reconciler, including apparent
	// no-ops. Callback snapshots describe why a callback fired, but only the
	// ConfigStore's latest published snapshot decides the resulting stack.
	for name := range orgIDs {
		if err := tr.reconcileOrgStack(name, new); err != nil {
			slog.Error("Failed to reconcile org stack on config change.", "org", name, "error", err)
		}
	}
}

func (tr *OrgRouter) setProjectReaderChangeHandler(handler func(orgID, username string)) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.projectReaderChange = handler
}

func changedProjectReaderUsers(old, new *configstore.Snapshot) map[configstore.OrgUserKey]struct{} {
	changed := make(map[configstore.OrgUserKey]struct{})
	keys := make(map[configstore.OrgUserKey]struct{}, len(old.OrgUserAccess)+len(new.OrgUserAccess))
	for key := range old.OrgUserAccess {
		keys[key] = struct{}{}
	}
	for key := range new.OrgUserAccess {
		keys[key] = struct{}{}
	}

	for key := range keys {
		oldAccess := old.OrgUserAccess[key]
		newAccess := new.OrgUserAccess[key]
		if oldAccess.Mode != configstore.OrgUserAccessModeProjectReader && newAccess.Mode != configstore.OrgUserAccessModeProjectReader {
			continue
		}
		if !sameProjectReaderAccess(oldAccess, newAccess) ||
			old.OrgUserPassword[key] != new.OrgUserPassword[key] ||
			old.OrgUserDisabled[key] != new.OrgUserDisabled[key] ||
			!sameProjectReaderTeam(old, new, key.OrgID, oldAccess.TeamID, newAccess.TeamID) {
			changed[key] = struct{}{}
		}
	}
	return changed
}

func sameProjectReaderAccess(a, b configstore.OrgUserAccessConfig) bool {
	if a.Mode != b.Mode || (a.TeamID == nil) != (b.TeamID == nil) {
		return false
	}
	return a.TeamID == nil || *a.TeamID == *b.TeamID
}

func sameProjectReaderTeam(old, new *configstore.Snapshot, orgID string, oldTeamID, newTeamID *int64) bool {
	if oldTeamID == nil || newTeamID == nil || *oldTeamID != *newTeamID {
		return oldTeamID == nil && newTeamID == nil
	}
	oldTeam, oldFound := projectReaderTeam(old, orgID, *oldTeamID)
	newTeam, newFound := projectReaderTeam(new, orgID, *newTeamID)
	return oldFound == newFound && (!oldFound || sameOrgTeam(oldTeam, newTeam))
}

func sameOrgTeam(a, b configstore.OrgTeamConfig) bool {
	return a.TeamID == b.TeamID &&
		a.SchemaName == b.SchemaName &&
		a.Enabled == b.Enabled &&
		a.IsBillingTeam == b.IsBillingTeam &&
		sameOptionalString(a.EventsTableName, b.EventsTableName) &&
		sameOptionalString(a.PersonsTableName, b.PersonsTableName) &&
		sameOptionalString(a.SchemaDataImportsName, b.SchemaDataImportsName)
}

func sameOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func projectReaderTeam(snapshot *configstore.Snapshot, orgID string, teamID int64) (configstore.OrgTeamConfig, bool) {
	org := snapshot.Orgs[orgID]
	if org == nil {
		return configstore.OrgTeamConfig{}, false
	}
	for _, team := range org.Teams {
		if team.TeamID == teamID {
			return team, true
		}
	}
	return configstore.OrgTeamConfig{}, false
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

// BeginDrain stops new and in-progress session creation for every org while
// leaving established sessions intact so they can drain naturally.
func (tr *OrgRouter) BeginDrain() {
	tr.draining.Store(true)
	stacks := tr.AllStacks()
	for _, stack := range stacks {
		if stack.Sessions != nil {
			stack.Sessions.BeginDrain()
		}
	}
}

// ShutdownAll shuts down all org stacks.
func (tr *OrgRouter) ShutdownAll() {
	tr.shutdownOnce.Do(tr.shutdownAll)
}

func (tr *OrgRouter) shutdownAll() {
	tr.draining.Store(true)
	tr.mu.Lock()
	tr.terminal = true
	tr.mu.Unlock()

	// Once terminal is set no new stack operation can register. Join every
	// create/destroy that linearized before it before taking ownership of the
	// remaining published stacks, so shutdown never races another teardown of
	// the same org generation.
	tr.stackOps.Wait()

	tr.mu.Lock()
	orgs := make(map[string]*OrgStack, len(tr.orgs))
	for k, v := range tr.orgs {
		orgs[k] = v
	}
	tr.orgs = make(map[string]*OrgStack)
	tr.mu.Unlock()

	for name, stack := range orgs {
		slog.Info("Shutting down org stack.", "org", name)
		shutdownOrgStack(stack)
	}

	if tr.sharedCancel != nil {
		tr.sharedCancel()
	}
	if tr.sharedPool != nil {
		tr.sharedPool.ShutdownAll()
	}
	if tr.admissionReclaimer != nil {
		timeout := tr.globalCfg.ShutdownTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if err := tr.admissionReclaimer.DrainAndClose(ctx); err != nil {
			slog.Warn("Admission reclaimer did not fully drain during control-plane shutdown.", "error", err)
		}
		cancel()
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
