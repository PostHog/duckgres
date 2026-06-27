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

// effectiveDefaultWorkerTTL resolves the janitor's hot-idle retention: the
// operator default TTL (DUCKGRES_K8S_WORKER_DEFAULT_TTL →
// K8sConfig.WorkerDefaultTTL) when set, otherwise the single built-in
// defaultWorkerTTL (1m — the same fallback sized-but-no-ttl requests get at
// profile resolution, so there is exactly ONE default TTL however a worker
// came to have no explicit one). The full per-request precedence is:
// client GUC > org default > deployment default TTL > built-in 1m.
func effectiveDefaultWorkerTTL(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return defaultWorkerTTL
}

func (a *orgRouterAdapter) StackForOrg(orgID string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	stack, ok := a.router.StackForOrg(orgID)
	if !ok {
		return nil, nil, nil, false
	}
	return stack.Pool, stack.Sessions, stack.Rebalancer, true
}

func (a *orgRouterAdapter) IcebergConfigForOrg(orgID string) (server.IcebergConfig, bool) {
	return a.router.IcebergConfigForOrg(orgID)
}

func (a *orgRouterAdapter) IsMigratingForOrg(orgID string) bool {
	return a.router.IsMigrating(orgID)
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
		for wID, count := range sessionsByWorker {
			status := "active"
			if count == 0 {
				status = "idle"
			}
			result = append(result, admin.WorkerStatus{
				ID:             wID,
				Org:            name,
				ActiveSessions: count,
				Status:         status,
			})
		}
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

	// Per-user persistent secret manager (CREATE PERSISTENT SECRET). With no
	// key configured the manager still loads so DROP can clean up stale rows,
	// but persistence is disabled with a clear client-facing error.
	userSecrets, err := NewCPUserSecretManager(store, cfg.UserSecretKey)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	server.SetUserSecretManager(srv, userSecrets)
	if cfg.UserSecretKey == "" {
		slog.Info("User persistent secrets disabled (DUCKGRES_USER_SECRET_KEY not set).")
	} else {
		slog.Info("User persistent secrets enabled.")
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
		Namespace:                    namespace,
		CPID:                         cpID,
		CPInstanceID:                 cpInstanceID,
		WorkerImage:                  cfg.K8s.WorkerImage,
		WorkerPort:                   cfg.K8s.WorkerPort,
		SecretName:                   cfg.K8s.WorkerSecret,
		ConfigMap:                    cfg.K8s.WorkerConfigMap,
		MaxWorkers:                   maxWorkers,
		IdleTimeout:                  cfg.WorkerIdleTimeout,
		ConfigPath:                   cfg.ConfigPath,
		ImagePullPolicy:              cfg.K8s.ImagePullPolicy,
		ServiceAccount:               cfg.K8s.ServiceAccount,
		WorkerCPURequest:             cfg.K8s.WorkerCPURequest,
		WorkerMemoryRequest:          cfg.K8s.WorkerMemoryRequest,
		WorkerNodeSelector:           parseNodeSelector(cfg.K8s.WorkerNodeSelector),
		WorkerTolerationKey:          cfg.K8s.WorkerTolerationKey,
		WorkerTolerationValue:        cfg.K8s.WorkerTolerationValue,
		WorkerPriorityClassName:      cfg.K8s.WorkerPriorityClassName,
		HeadroomNodes:                cfg.K8s.HeadroomNodes,
		HeadroomPercent:              cfg.K8s.HeadroomPercent,
		PlaceholderImage:             cfg.K8s.PlaceholderImage,
		PlaceholderPriorityClassName: cfg.K8s.PlaceholderPriorityClassName,
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

	router, err := NewOrgRouter(store, baseCfg, cfg, srv, stsBroker, userSecrets, resolveDucklingStatus)
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
	janitor.hotIdleTTL = effectiveDefaultWorkerTTL(cfg.K8s.WorkerDefaultTTL)
	// Per-worker transitions (orphan retire, stuck reaper, hot-idle TTL)
	// all flow through this lifecycle service. The legacy retireWorker /
	// retireOrphanWorker / retireLocalWorker / deleteRetiredWorker
	// lambda chain is gone — the snapshot/lease-typed API is the only
	// path now.
	janitor.lifecycle = router.sharedPool.lifecycle
	// Reset leader-owned cluster-wide gauges when this janitor stops
	// being the leader, so stale per-image counts from this CP don't
	// linger in Prometheus while a peer takes over.
	janitor.onStop = resetLeaderOwnedClusterMetrics
	var lastWorkerLifecycleStats []configstore.WorkerLifecycleStats
	// Refresh the per-image worker lifecycle gauges (Hot / HotIdle / Draining /
	// … counts). Workers are spawned on demand and reused while hot-idle until
	// their TTL — there is no warm pool to reconcile, so this is pure
	// observability, leader-only.
	janitor.observeWorkerLifecycle = func() {
		stats, err := listWorkerLifecycleStats(store)
		if err != nil {
			slog.Warn("Janitor failed to read worker lifecycle stats.", "error", err)
			return
		}
		observeWorkerLifecycleStats(stats, lastWorkerLifecycleStats)
		lastWorkerLifecycleStats = cloneWorkerLifecycleStats(stats)
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
	janitor.hotIdleFloor = func(snap configstore.WorkerSnapshot) int {
		snapshot := store.Snapshot()
		if snapshot == nil {
			return 0
		}
		org, ok := snapshot.Orgs[snap.OrgID()]
		if !ok || org == nil {
			return 0
		}
		if snap.Image() != workerImageForOrg(org, baseCfg.WorkerImage) {
			return 0
		}
		profile, _, err := resolveOrgDefaultWorkerProfile(cfg.K8s, org)
		if err != nil {
			return 0
		}
		profileCPU, profileMemory := profile.Parts()
		if snap.ProfileCPU() != profileCPU || snap.ProfileMemory() != profileMemory {
			return 0
		}
		return org.DefaultWorkerMinHotIdle
	}
	// Node-headroom controller: keep low-priority placeholder pods as warm,
	// preemptible spare capacity so worker spawns schedule immediately.
	// Leader-only (runs on the janitor tick). Always wired — reconcileHeadroom
	// itself decides the target (constant HeadroomNodes, legacy HeadroomPercent,
	// or disabled). It must run even when disabled so a pool that had headroom
	// turned off converges its placeholders to zero instead of stranding them.
	janitor.reconcileHeadroom = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		router.sharedPool.reconcileHeadroom(ctx)
	}

	// Per-CP fallback hot-idle reaper: runs on EVERY replica, independent of the
	// janitor leader lease, retiring this replica's own expired hot-idle workers.
	// Backstops the leader-only janitor reaper above so a wedged/absent leader
	// can no longer let idle worker pods (and their r6gd nodes) accumulate
	// fleet-wide. Reuses the same TTL, floor, lifecycle and store as the janitor;
	// the fenced CAS makes concurrent leader/fallback retires safe.
	fallbackReaper := &perCPHotIdleReaper{
		store:        store,
		lifecycle:    router.sharedPool.lifecycle,
		cpInstanceID: cpInstanceID,
		hotIdleTTL:   janitor.hotIdleTTL,
		hotIdleFloor: janitor.hotIdleFloor,
		interval:     time.Minute,
	}
	go fallbackReaper.Run(context.Background())

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
		// Env suffix for CP-owned s3bucket naming: drives the backfill of
		// spec.dataStore.bucketName onto existing ready ducklings. Empty leaves
		// it disabled (composition keeps deriving).
		provCtrl.WithBucketSuffix(cfg.DucklingBucketSuffix)
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
	adminTokens := admin.NewTokenSet(internalSecret, cfg.InternalSecretFallbacks)
	if n := len(cfg.InternalSecretFallbacks); n > 0 {
		// Count only — never log the secret values.
		slog.Info("Internal secret rotation fallbacks active.", "fallback_count", n)
	}

	// Set up API server (admin + provisioning + dashboard on :8080).
	// The existing metrics server on :9090 stays running separately.
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// Health endpoint (unauthenticated, used by K8s probes)
	engine.GET("/health", newHealthHandler(isHealthy))

	// Authenticated API
	api := engine.Group("/api/v1", admin.APIAuthMiddleware(adminTokens))
	admin.RegisterAPI(api, store, adpt)
	provisioning.RegisterAPI(api, provisioning.NewGormStore(store), cfg.DucklingBucketSuffix)

	// Dashboard
	admin.RegisterDashboard(engine, adminTokens)

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

type workerLifecycleStatsLister interface {
	ListWorkerLifecycleStats() ([]configstore.WorkerLifecycleStats, error)
}

func listWorkerLifecycleStats(lister workerLifecycleStatsLister) ([]configstore.WorkerLifecycleStats, error) {
	if lister == nil {
		return nil, nil
	}
	return lister.ListWorkerLifecycleStats()
}

func cloneWorkerLifecycleStats(stats []configstore.WorkerLifecycleStats) []configstore.WorkerLifecycleStats {
	if len(stats) == 0 {
		return nil
	}
	out := make([]configstore.WorkerLifecycleStats, len(stats))
	copy(out, stats)
	return out
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
