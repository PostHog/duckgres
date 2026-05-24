//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/provisioning"
	"github.com/posthog/duckgres/server"
)

// orgRouterAdapter wraps OrgRouter to implement both OrgRouterInterface
// (for the control plane) and admin.OrgStackInfo (for the admin API).
type orgRouterAdapter struct {
	router *OrgRouter
}

// defaultHotIdleTTL is how long a hot-idle worker retains its org assignment
// before being retired. During this window, any CP pod can reclaim it for the
// same org without re-activation.
const defaultHotIdleTTL = 5 * time.Minute

func (a *orgRouterAdapter) StackForOrg(orgID string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	stack, ok := a.router.StackForOrg(orgID)
	if !ok {
		return nil, nil, nil, false
	}
	return stack.Pool, stack.Sessions, stack.Rebalancer, true
}

func (a *orgRouterAdapter) IsMigratingForOrg(orgID string) bool {
	return a.router.IsMigrating(orgID)
}

func (a *orgRouterAdapter) SetWarmCapacityTarget(n int) {
	a.router.sharedPool.SetWarmCapacityTarget(n)
	if n <= 0 {
		a.router.sharedPool.SetPerImageWarmTargets(nil)
	}
}

func (a *orgRouterAdapter) ShutdownAll() {
	a.router.ShutdownAll()
}

func (a *orgRouterAdapter) AllOrgStats() []admin.OrgStatus {
	stacks := a.router.AllStacks()
	stats := make([]admin.OrgStatus, 0, len(stacks))
	for name, stack := range stacks {
		sessionCount := stack.Sessions.SessionCount()
		stats = append(stats, admin.OrgStatus{
			Name:           name,
			ActiveSessions: sessionCount,
			MaxWorkers:     stack.Config.MaxWorkers,
			MemoryBudget:   stack.Config.MemoryBudget,
		})
		// Emit per-org Prometheus metrics
		observeOrgSessionsActive(name, sessionCount)
	}
	return stats
}

func (a *orgRouterAdapter) AllWorkerStatuses() []admin.WorkerStatus {
	stacks := a.router.AllStacks()
	var result []admin.WorkerStatus
	for name, stack := range stacks {
		sessions := stack.Sessions.AllSessions()
		sessionsByWorker := make(map[int]int)
		for _, s := range sessions {
			sessionsByWorker[s.WorkerID]++
		}
		activeCount := 0
		idleCount := 0
		for wID, count := range sessionsByWorker {
			status := "active"
			if count == 0 {
				status = "idle"
				idleCount++
			} else {
				activeCount++
			}
			result = append(result, admin.WorkerStatus{
				ID:             wID,
				Org:            name,
				ActiveSessions: count,
				Status:         status,
			})
		}
		// Emit per-org worker Prometheus metrics
		observeOrgWorkersActive(name, activeCount)
		observeOrgWorkersIdle(name, idleCount)
	}
	return result
}

func (a *orgRouterAdapter) AllSessionStatuses() []admin.SessionStatus {
	stacks := a.router.AllStacks()
	var result []admin.SessionStatus
	for name, stack := range stacks {
		for _, s := range stack.Sessions.AllSessions() {
			result = append(result, admin.SessionStatus{
				PID:      s.PID,
				WorkerID: s.WorkerID,
				Org:      name,
				Protocol: s.Protocol,
			})
		}
	}
	return result
}

// Compile-time checks.
var _ OrgRouterInterface = (*orgRouterAdapter)(nil)
var _ admin.OrgStackInfo = (*orgRouterAdapter)(nil)

// SetupMultiTenant initializes the config store, org router, and API server.
// Called from RunControlPlane when --config-store is set with remote backend.
// Returns the API server for graceful shutdown.
func SetupMultiTenant(
	cfg ControlPlaneConfig,
	srv *server.Server,
	memBudget uint64,
	maxWorkers int,
	isHealthy func() bool,
) (ConfigStoreInterface, OrgRouterInterface, *http.Server, *ControlPlaneRuntimeTracker, *JanitorLeaderManager, error) {
	pollInterval := cfg.ConfigPollInterval
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	store, err := configstore.NewConfigStore(cfg.ConfigStoreConn, pollInterval)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	namespace, err := resolveK8sNamespace(cfg.K8s.WorkerNamespace)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	cpID := cfg.K8s.ControlPlaneID
	if cpID == "" {
		cpID = os.Getenv("POD_NAME")
	}
	if cpID == "" {
		cpID, _ = os.Hostname()
	}
	podUID := os.Getenv("POD_UID")
	if podUID == "" {
		podUID = cpID
	}
	bootID := make([]byte, 16)
	if _, err := rand.Read(bootID); err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("generate control plane boot id: %w", err)
	}
	bootIDHex := hex.EncodeToString(bootID)
	cpInstanceID := makeControlPlaneInstanceID(podUID, bootIDHex)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:             namespace,
		CPID:                  cpID,
		CPInstanceID:          cpInstanceID,
		WorkerImage:           cfg.K8s.WorkerImage,
		WorkerPort:            cfg.K8s.WorkerPort,
		SecretName:            cfg.K8s.WorkerSecret,
		ConfigMap:             cfg.K8s.WorkerConfigMap,
		MaxWorkers:            maxWorkers,
		IdleTimeout:           cfg.WorkerIdleTimeout,
		ConfigPath:            cfg.ConfigPath,
		ImagePullPolicy:       cfg.K8s.ImagePullPolicy,
		ServiceAccount:        cfg.K8s.ServiceAccount,
		WorkerCPURequest:      cfg.K8s.WorkerCPURequest,
		WorkerMemoryRequest:   cfg.K8s.WorkerMemoryRequest,
		WorkerNodeSelector:    parseNodeSelector(cfg.K8s.WorkerNodeSelector),
		WorkerTolerationKey:   cfg.K8s.WorkerTolerationKey,
		WorkerTolerationValue: cfg.K8s.WorkerTolerationValue,
		WorkerExclusiveNode:   cfg.K8s.WorkerExclusiveNode,
		ResolveOrgConfig: func(orgID string) (*configstore.OrgConfig, error) {
			snap := store.Snapshot()
			if snap == nil {
				return nil, fmt.Errorf("config snapshot unavailable")
			}
			org, ok := snap.Orgs[orgID]
			if !ok {
				return nil, fmt.Errorf("org %s not found in snapshot", orgID)
			}
			return org, nil
		},
	}

	// Initialize STS broker for credential brokering (best-effort)
	var stsBroker *STSBroker
	if cfg.K8s.AWSRegion != "" {
		var err error
		stsBroker, err = NewSTSBroker(context.Background(), cfg.K8s.AWSRegion)
		if err != nil {
			slog.Warn("STS broker unavailable, workers will use pod identity for S3.", "error", err)
		}
	}

	// Initialize Duckling CR resolver for reading infrastructure details from Crossplane CRs (best-effort)
	var resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error)
	dc, dcErr := provisioner.NewDucklingClient()
	if dcErr != nil {
		slog.Warn("Duckling client unavailable, will use config store for infrastructure details.", "error", dcErr)
	} else {
		resolveDucklingStatus = func(ctx context.Context, orgID string) (*provisioner.DucklingStatus, error) {
			return dc.Get(ctx, orgID)
		}
	}

	router, err := NewOrgRouter(store, baseCfg, cfg, srv, stsBroker, resolveDucklingStatus)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	adpt := &orgRouterAdapter{router: router}
	runtimeTracker := NewControlPlaneRuntimeTracker(
		store,
		cpInstanceID,
		cpID,
		podUID,
		bootIDHex,
		5*time.Second,
	)
	janitor := NewControlPlaneJanitor(store, 5*time.Second, 20*time.Second)
	janitor.maxDrainTimeout = cfg.HandoverDrainTimeout
	janitor.hotIdleTTL = defaultHotIdleTTL
	janitor.warmCapacityMissBucketTTL = cfg.K8s.WarmCapacityDemandTTL
	// Per-worker transitions (orphan retire, stuck reaper, hot-idle TTL)
	// all flow through this lifecycle service. The legacy retireWorker /
	// retireOrphanWorker / retireLocalWorker / deleteRetiredWorker
	// lambda chain is gone — the snapshot/lease-typed API is the only
	// path now.
	janitor.lifecycle = router.sharedPool.lifecycle
	lastWarmCapacityTargets := map[string]int{}
	var lastWarmCapacityRecentMisses []configstore.WarmCapacityMissAggregate
	var lastWarmCapacityWorkerStats []configstore.WarmCapacityWorkerStats
	lastWarmCapacityGlobalCapBlocked := false
	janitor.reconcileWarmCapacity = func() {
		snap := store.Snapshot()
		if snap == nil {
			return
		}
		baseTargets := router.computeBaseWarmCapacityTargets(snap)
		targetSnapshot, err := computeEffectiveWarmCapacityTargetSnapshot(
			baseTargets,
			store,
			cfg.K8s,
			janitor.now(),
		)
		if err != nil {
			slog.Warn("Janitor failed to read dynamic warm-capacity demand; reconciling base warm targets only.", "error", err)
		}
		observeWarmCapacityRecentMisses(targetSnapshot.RecentMisses, lastWarmCapacityRecentMisses)
		lastWarmCapacityRecentMisses = cloneWarmCapacityMissAggregates(targetSnapshot.RecentMisses)
		observeWarmCapacityTargets(targetSnapshot.BaseTargets, targetSnapshot.EffectiveTargets, cfg.K8s.MaxWorkers, lastWarmCapacityTargets)
		if stats, statsErr := listWarmCapacityWorkerStats(store); statsErr != nil {
			slog.Warn("Janitor failed to read warm-capacity worker stats.", "error", statsErr)
		} else {
			observeWarmCapacityWorkerStats(stats, lastWarmCapacityWorkerStats)
			lastWarmCapacityWorkerStats = cloneWarmCapacityWorkerStats(stats)
		}
		logWarmCapacityTargetChanges(lastWarmCapacityTargets, targetSnapshot.BaseTargets, targetSnapshot.EffectiveTargets)
		lastWarmCapacityTargets = cloneWarmCapacityTargets(targetSnapshot.EffectiveTargets)

		globalCapBlocked := warmCapacityGlobalCapBlocksDemand(targetSnapshot.BaseTargets, targetSnapshot.EffectiveTargets, targetSnapshot.RecentMisses, cfg.K8s)
		if globalCapBlocked && !lastWarmCapacityGlobalCapBlocked {
			slog.Info("Global worker cap prevents dynamic warm capacity.", "max_workers", cfg.K8s.MaxWorkers, "base_target_total", sumIntMap(targetSnapshot.BaseTargets), "effective_target_total", sumIntMap(targetSnapshot.EffectiveTargets))
		}
		lastWarmCapacityGlobalCapBlocked = globalCapBlocked

		targets := targetSnapshot.EffectiveTargets
		router.sharedPool.SetPerImageWarmTargets(targets)
		reconcileWarmCapacityImageTargets(router.sharedPool, targets)
	}
	janitor.retireMismatchedVersionWorker = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		router.sharedPool.RetireOneMismatchedVersionWorker(ctx)
	}
	janitor.cleanupOrphanedWorkerPods = func() {
		// Pods and secrets each get their own 30s deadline so a slow
		// pod-list (large namespace) can't starve the secret reaper
		// behind it, and vice versa.
		podCtx, podCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if n := router.sharedPool.cleanupOrphanedWorkerPods(podCtx, 2*time.Minute); n > 0 {
			slog.Info("Stranded worker pods reconciled.", "count", n)
		}
		podCancel()

		// Sibling reconciler that catches a secret created without a
		// pod (spawn crashed between createSecret and createPod). The
		// pod-cleanup loop above only iterates pods, so this is the
		// only place that reclaims those orphans.
		secretCtx, secretCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if n := router.sharedPool.cleanupOrphanedWorkerSecrets(secretCtx, 2*time.Minute); n > 0 {
			slog.Info("Stranded worker RPC secrets reconciled.", "count", n)
		}
		secretCancel()
	}

	// Scheduler-side activator: a single SharedWorkerActivator instance
	// that the credential-refresh tick uses to re-broker STS sessions for
	// any worker we own. It's distinct from the per-org activators created
	// inside createOrgStack (those are wired into the worker-claim path);
	// this one operates across all orgs by looking each up in the snapshot.
	refreshActivator := NewSharedWorkerActivator(
		router.sharedPool,
		stsBroker,
		cfg.DuckLakeDefaultSpecVersion,
		func(orgID string) (*configstore.OrgConfig, error) {
			snap := store.Snapshot()
			if snap == nil {
				return nil, fmt.Errorf("config snapshot unavailable for org %s", orgID)
			}
			org, ok := snap.Orgs[orgID]
			if !ok {
				return nil, fmt.Errorf("org %s not found in config snapshot", orgID)
			}
			return org, nil
		},
	)
	if refreshActivator != nil {
		refreshActivator.resolveDucklingStatus = resolveDucklingStatus
	}

	// Half the configured STS session duration: a worker due for refresh
	// gets picked up well before its current session token actually goes
	// stale, with a full half-life of slack to retry transient STS / RPC
	// failures on subsequent ticks.
	const credentialRefreshLookahead = stsSessionDuration / 2

	// Per-CP scheduler — runs on every CP regardless of leader status, since
	// each CP refreshes only the workers it owns (filtered by cpInstanceID in
	// the SQL). Running this on the janitor leader only would leave workers
	// owned by non-leader CPs to expire naturally, breaking long-running
	// queries.
	if refreshActivator != nil {
		scheduler := &credentialRefreshScheduler{
			interval:     5 * time.Second,
			lookahead:    credentialRefreshLookahead,
			cpInstanceID: cpInstanceID,
			store:        store,
			workerByID:   router.sharedPool.Worker,
			refresh:      refreshActivator.RefreshCredentials,
		}
		go scheduler.Run(context.Background())
	}
	janitorLeader, err := NewJanitorLeaderManager(namespace, cpInstanceID, janitor)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Start provisioning controller (best-effort — K8s API may not be available locally)
	provCtrl, err := provisioner.NewController(store, 10*time.Second)
	if err != nil {
		slog.Warn("Provisioning controller unavailable.", "error", err)
	} else {
		// Opt-in: enable the per-org Lakekeeper provisioning branch. Off by
		// default so existing deploys are unaffected. Best-effort — if the
		// K8s client can't be built we log and leave the controller running
		// without the Lakekeeper branch (S3-Tables warehouses still work).
		if lakekeeperProvisionerEnabled() {
			if k8sClient, lkErr := provisioner.NewLakekeeperK8sClient(); lkErr != nil {
				slog.Warn("Lakekeeper provisioner enabled but K8s client unavailable; skipping.", "error", lkErr)
			} else {
				lkProv := provisioner.NewLakekeeperProvisioner(store, k8sClient)
				provCtrl.WithLakekeeperProvisioner(lkProv, newLakekeeperInputsResolver(resolveDucklingStatus))
				slog.Info("Lakekeeper provisioner enabled (allowall + NetworkPolicy mode).")
			}
		}
		go provCtrl.Run(context.Background())
	}

	// Register config change handler
	store.OnChange(router.HandleConfigChange)

	// Start polling
	store.Start(context.Background())

	// Resolve admin bearer token
	internalSecret := cfg.InternalSecret
	if internalSecret == "" {
		tokenBytes := make([]byte, 32)
		if _, err := rand.Read(tokenBytes); err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("generate internal secret: %w", err)
		}
		internalSecret = hex.EncodeToString(tokenBytes)
		slog.Info("Generated internal secret; set --internal-secret or DUCKGRES_INTERNAL_SECRET explicitly to avoid rotation on restart.")
	}

	// Set up API server (admin + provisioning + dashboard on :8080).
	// The existing metrics server on :9090 stays running separately.
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// Health endpoint (unauthenticated, used by K8s probes)
	engine.GET("/health", newHealthHandler(isHealthy))

	// Authenticated API
	api := engine.Group("/api/v1", admin.APIAuthMiddleware(internalSecret))
	admin.RegisterAPI(api, store, adpt)
	provisioning.RegisterAPI(api, provisioning.NewGormStore(store))

	// Dashboard
	admin.RegisterDashboard(engine, internalSecret)

	apiServer := &http.Server{
		Addr:    ":8080",
		Handler: engine,
	}
	go func() {
		slog.Info("Starting API server.", "addr", apiServer.Addr)
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("API server error.", "error", err)
		}
	}()

	return store, adpt, apiServer, runtimeTracker, janitorLeader, nil
}

type warmCapacityMissAggregateLister interface {
	ListWarmCapacityMissesSince(since time.Time, reasons ...configstore.WorkerClaimMissReason) ([]configstore.WarmCapacityMissAggregate, error)
}

type warmCapacityWorkerStatsLister interface {
	ListWarmCapacityWorkerStats() ([]configstore.WarmCapacityWorkerStats, error)
}

type warmCapacityTargetSnapshot struct {
	BaseTargets      map[string]int
	EffectiveTargets map[string]int
	RecentMisses     []configstore.WarmCapacityMissAggregate
}

func computeEffectiveWarmCapacityTargets(baseTargets map[string]int, lister warmCapacityMissAggregateLister, cfg K8sConfig, now time.Time) (map[string]int, error) {
	snap, err := computeEffectiveWarmCapacityTargetSnapshot(baseTargets, lister, cfg, now)
	return snap.EffectiveTargets, err
}

func computeEffectiveWarmCapacityTargetSnapshot(baseTargets map[string]int, lister warmCapacityMissAggregateLister, cfg K8sConfig, now time.Time) (warmCapacityTargetSnapshot, error) {
	snap := warmCapacityTargetSnapshot{
		BaseTargets:      sanitizeWarmCapacityTargets(baseTargets),
		EffectiveTargets: sanitizeWarmCapacityTargets(baseTargets),
	}
	dynamicCfg := dynamicWarmCapacityConfigFromK8s(cfg)
	if !dynamicCfg.Enabled {
		return snap, nil
	}
	if lister == nil {
		return snap, nil
	}
	window := cfg.WarmCapacityMissWindow
	if window <= 0 {
		window = DefaultWarmCapacityMissWindow
	}
	if now.IsZero() {
		now = time.Now()
	}
	aggregates, err := lister.ListWarmCapacityMissesSince(now.Add(-window), configstore.WorkerClaimMissReasonNoIdle)
	if err != nil {
		return snap, err
	}
	snap.RecentMisses = aggregates
	snap.EffectiveTargets = computeDynamicWarmCapacityTargets(snap.BaseTargets, aggregates, dynamicCfg)
	return snap, nil
}

func listWarmCapacityWorkerStats(lister warmCapacityWorkerStatsLister) ([]configstore.WarmCapacityWorkerStats, error) {
	if lister == nil {
		return nil, nil
	}
	return lister.ListWarmCapacityWorkerStats()
}

func logWarmCapacityTargetChanges(previous, baseTargets, effectiveTargets map[string]int) {
	for _, image := range warmCapacityTargetScopes(previous, baseTargets, effectiveTargets) {
		effective := positiveMapValue(effectiveTargets, image)
		if positiveMapValue(previous, image) == effective {
			continue
		}
		base := positiveMapValue(baseTargets, image)
		demand := effective - base
		if demand < 0 {
			demand = 0
		}
		slog.Info("Warm capacity target changed.",
			"scope", warmCapacityImageScope(image),
			"base_target", base,
			"demand_target", demand,
			"effective_target", effective,
		)
	}
}

func cloneWarmCapacityTargets(targets map[string]int) map[string]int {
	out := make(map[string]int, len(targets))
	for image, target := range targets {
		if strings.TrimSpace(image) == "" || target <= 0 {
			continue
		}
		out[image] = target
	}
	return out
}

func cloneWarmCapacityMissAggregates(aggregates []configstore.WarmCapacityMissAggregate) []configstore.WarmCapacityMissAggregate {
	if len(aggregates) == 0 {
		return nil
	}
	out := make([]configstore.WarmCapacityMissAggregate, len(aggregates))
	copy(out, aggregates)
	return out
}

func cloneWarmCapacityWorkerStats(stats []configstore.WarmCapacityWorkerStats) []configstore.WarmCapacityWorkerStats {
	if len(stats) == 0 {
		return nil
	}
	out := make([]configstore.WarmCapacityWorkerStats, len(stats))
	copy(out, stats)
	return out
}

func warmCapacityGlobalCapBlocksDemand(baseTargets, effectiveTargets map[string]int, aggregates []configstore.WarmCapacityMissAggregate, cfg K8sConfig) bool {
	if cfg.MaxWorkers <= 0 || len(aggregates) == 0 {
		return false
	}
	effectiveTotal := sumIntMap(effectiveTargets)
	if effectiveTotal < cfg.MaxWorkers {
		return false
	}
	dynamicCfg := dynamicWarmCapacityConfigFromK8s(cfg)
	if !dynamicCfg.Enabled {
		return false
	}
	dynamicCfg.MaxWorkers = 0
	uncappedTargets := computeDynamicWarmCapacityTargets(baseTargets, aggregates, dynamicCfg)
	return sumIntMap(uncappedTargets) > effectiveTotal
}

func reconcileWarmCapacityImageTargets(pool *K8sWorkerPool, targets map[string]int) {
	if pool == nil || len(targets) == 0 {
		return
	}
	var wg sync.WaitGroup
	for image, count := range targets {
		if count <= 0 {
			continue
		}
		wg.Add(1)
		go func(image string, count int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := pool.SpawnMinWorkersForImage(ctx, image, count); err != nil {
				slog.Warn("Janitor failed to reconcile image warm capacity.", "image", image, "target", count, "error", err)
			}
		}(image, count)
	}
	wg.Wait()
}

func resolveK8sNamespace(namespace string) (string, error) {
	if namespace != "" {
		return namespace, nil
	}
	ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", fmt.Errorf("k8s namespace not set and auto-detection failed: %w", err)
	}
	return string(ns), nil
}

func newHealthHandler(isHealthy func() bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if isHealthy != nil && !isHealthy() {
			c.String(http.StatusServiceUnavailable, "unhealthy")
			return
		}
		c.String(http.StatusOK, "ok")
	}
}

// parseNodeSelector parses a JSON string into a map[string]string.
// Returns nil if the input is empty or invalid.
func parseNodeSelector(s string) map[string]string {
	if s == "" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		slog.Warn("Invalid DUCKGRES_K8S_WORKER_NODE_SELECTOR JSON, ignoring.", "error", err)
		return nil
	}
	return m
}

func makeControlPlaneInstanceID(podUID, bootIDHex string) string {
	if podUID == "" {
		podUID = "cp"
	}
	if len(bootIDHex) > 16 {
		bootIDHex = bootIDHex[:16]
	}
	return controlPlaneIDLabelValue(podUID + "-" + bootIDHex)
}
