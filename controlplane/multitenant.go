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
	"k8s.io/client-go/kubernetes"
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

func (a *orgRouterAdapter) IsMigratingForOrg(orgID string) bool {
	return a.router.IsMigrating(orgID)
}

func (a *orgRouterAdapter) ShutdownAll() {
	a.router.ShutdownAll()
}

func (a *orgRouterAdapter) ReleaseIdleHotWorkers() int {
	return a.router.ReleaseIdleHotWorkers()
}

func (a *orgRouterAdapter) AllOrgStats() []admin.OrgStatus {
	stacks := a.router.AllStacks()
	stats := make([]admin.OrgStatus, 0, len(stacks))
	for name, stack := range stacks {
		sessionCount := stack.Sessions.SessionCount()
		st := admin.OrgStatus{
			Name:           name,
			ActiveSessions: sessionCount,
			MaxWorkers:     stack.Config.MaxWorkers,
		}
		// Workers this CP has assigned to the org (cap-counting; excludes
		// hot-idle). Per-CP local view — the admin /status fan-out sums it
		// across replicas (mergeOrgStats) for a cluster-wide per-org count.
		if rp, ok := stack.Pool.(*OrgReservedPool); ok {
			st.Workers = rp.WorkerCount()
		}
		stats = append(stats, st)
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
			ws := admin.WorkerStatus{
				ID:             wID,
				Org:            name,
				ActiveSessions: count,
				Status:         status,
			}
			// Pod-shape (cpu/memory/ttl) of the session-holding worker; empty/zero
			// for the default profile or a worker no longer in the pool.
			if profile, ok := stack.Sessions.WorkerProfile(wID); ok {
				ws.CPU = profile.CPU
				ws.Memory = profile.Memory
				ws.TTLSeconds = int(profile.TTL.Seconds())
			}
			result = append(result, ws)
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
				User:     s.Username,
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
	isHealthy func() bool,
) (ConfigStoreInterface, OrgRouterInterface, *http.Server, *ControlPlaneRuntimeTracker, *JanitorLeaderManager, *computeMeter, error) {
	pollInterval := cfg.ConfigPollInterval
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	store, err := configstore.NewConfigStore(cfg.ConfigStoreConn, pollInterval)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	// Per-user persistent secret manager (CREATE PERSISTENT SECRET). With no
	// key configured the manager still loads so DROP can clean up stale rows,
	// but persistence is disabled with a clear client-facing error.
	userSecrets, err := NewCPUserSecretManager(store, cfg.UserSecretKey)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	server.SetUserSecretManager(srv, userSecrets)
	if cfg.UserSecretKey == "" {
		slog.Info("User persistent secrets disabled (DUCKGRES_USER_SECRET_KEY not set).")
	} else {
		slog.Info("User persistent secrets enabled.")
	}

	namespace, err := resolveK8sNamespace(cfg.K8s.WorkerNamespace)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
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
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("generate control plane boot id: %w", err)
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
		return nil, nil, nil, nil, nil, nil, err
	}

	adpt := &orgRouterAdapter{router: router}
	runtimeTracker := NewControlPlaneRuntimeTracker(
		store,
		cpInstanceID,
		cpID,
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
		orphanGrace:  janitor.orphanGrace, // same cutoff as the leader orphan sweep
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
		return nil, nil, nil, nil, nil, nil, err
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
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("generate internal secret: %w", err)
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

	// Admin UI dependencies. Prometheus URL is an env-only K8s knob (set by the
	// chart); see config_resolution.go / CLAUDE.md. The admin role for an SSO
	// caller is resolved per-request from the operators table (managed under
	// Admin → Operators); the break-glass internal-secret path is independent.
	//
	// There is no bootstrap seed. The first SSO login auto-provisions a
	// create-only VIEWER row for the caller (so an operator appears in the
	// config store just by logging in), and the role is then read back. To make
	// the first admin: log in over break-glass (internal-secret → admin) and
	// patch your auto-provisioned row to admin under Admin → Operators.
	resolve := func(email string) admin.Role {
		role, err := store.OperatorRole(email)
		if err != nil {
			slog.Warn("admin: operator role lookup failed", "email", email, "error", err)
		}
		if role == "" {
			// Auto-provision (create-only, never clobbers an existing role) so the
			// caller has a row to be promoted from. Best-effort: a failure here
			// must not block authentication — the caller simply stays viewer.
			if err := store.SeedOperator(email, string(admin.RoleViewer)); err != nil {
				slog.Warn("admin: operator auto-provision failed", "email", email, "error", err)
			}
		}
		if role == "admin" {
			return admin.RoleAdmin
		}
		return admin.RoleViewer
	}
	auditStore, err := admin.NewAuditStore(store.DB())
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("init admin audit store: %w", err)
	}
	metricsProxy := admin.NewMetricsProxy(os.Getenv("DUCKGRES_PROMETHEUS_URL"))
	clusterInfo := &clusterInfoProvider{router: router, store: store, srv: srv, selfCPID: cpInstanceID}
	imp := &impersonator{router: router}
	// Cross-CP live-state aggregation: live sessions/queries are per-CP in
	// memory, so a single replica only sees its own slice. The fetcher fans the
	// read out to peer CP pods so the dashboard shows cluster-wide numbers.
	var liveFetcher admin.PeerFetcher
	if router.sharedPool != nil && router.sharedPool.clientset != nil {
		liveFetcher = newClusterPeerFetcher(
			router.sharedPool.clientset, router.sharedPool.namespace,
			router.sharedPool.cpID, internalSecret, 8080,
		)
	}

	// Authenticated API. AuthMiddleware resolves the caller (internal-secret →
	// admin, else ALB/Cognito SSO → viewer/admin). AuditMiddleware records every
	// mutation. RoleGate enforces viewer/admin (mutations + the audit log read
	// require admin).
	// RoleGate blocks viewer mutations (method-based); the audit-log read
	// self-gates via RequireAdmin at its route (no brittle path coupling here).
	api := engine.Group("/api/v1",
		admin.AuthMiddleware(adminTokens, resolve),
		admin.AuditMiddleware(auditStore),
		admin.RoleGate(),
	)
	admin.RegisterAPI(api, store, adpt, liveFetcher)
	provisioning.RegisterAPI(api, provisioning.NewGormStore(store), cfg.DucklingBucketSuffix)
	// Node-overview topology reads reuse the shared K8s pool's in-cluster
	// clientset (nil when there's no shared pool — leaves those routes off).
	var clusterClient kubernetes.Interface
	if router.sharedPool != nil {
		clusterClient = router.sharedPool.clientset
	}
	admin.RegisterExtras(api, admin.Extras{
		Store:         store,
		Live:          clusterInfo,
		Users:         store,
		Fetcher:       liveFetcher,
		Impersonator:  imp,
		Audit:         auditStore,
		Metrics:       metricsProxy,
		ClusterClient: clusterClient,
	})

	// Live Duckling drift finder. Reuse the in-cluster Duckling client built
	// above (dc); pass nil when it's unavailable so the endpoint degrades to
	// {"available": false} rather than 500ing. A typed-nil *DucklingClient must
	// not be boxed into the interface, so only assign when dc is usable.
	var ducklingChecker admin.DucklingChecker
	if dcErr == nil && dc != nil {
		ducklingChecker = dc
	}
	admin.RegisterDucklingsDrift(api, store, ducklingChecker)

	// Live per-Duckling metadata-store assignment (which cnpg shard each
	// tenant landed on) for the org overview/detail pages. Same nil-degrade
	// contract as the drift finder.
	var ducklingMetadata admin.DucklingMetadataLister
	if dcErr == nil && dc != nil {
		ducklingMetadata = ducklingMetadataAdapter{dc: dc}
	}
	admin.RegisterDucklingsMetadata(api, ducklingMetadata)

	// Break-glass internal-secret login (the SPA owns "/" and app routes).
	admin.RegisterLogin(engine, adminTokens)

	// Embedded React SPA (served unauthenticated; all data is under /api/v1).
	if err := admin.RegisterUI(engine); err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("register admin UI: %w", err)
	}

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

	// Compute-usage metering (managed-warehouse billing). Enabled only when both
	// ingest URL and token are configured; otherwise the meter is nil and every
	// call site no-ops (queries are NEVER failed on its account). The flusher
	// runs on every CP pod (cross-pod UPSERT-increment); the drain loop is
	// leader-only, co-located under the janitor lease so exactly one CP ships.
	var meter *computeMeter
	if cfg.BillingMeteringEnabled() {
		meter = newComputeMeter(store)
		go meter.Run(context.Background())

		drainer := newComputeDrainer(store, newComputeCaptureClient(cfg.BillingIngestURL, cfg.BillingIngestToken))
		if janitorLeader != nil {
			janitorLeader.AttachLeaderLoop(drainer.Run)
		}
		slog.Info("Managed-warehouse compute-usage metering enabled.", "ingest_url", cfg.BillingIngestURL)
	} else {
		slog.Info("Managed-warehouse compute-usage metering disabled (DUCKGRES_BILLING_INGEST_URL/TOKEN not both set).")
	}

	return store, adpt, apiServer, runtimeTracker, janitorLeader, meter, nil
}

// ducklingMetadataAdapter converts provisioner.CRMetadataStore into the
// admin package's DucklingMetadataStore so admin does not import provisioner
// (same decoupling as DucklingChecker, which needs no adapter only because
// its method signatures carry no provisioner types).
type ducklingMetadataAdapter struct {
	dc *provisioner.DucklingClient
}

func (a ducklingMetadataAdapter) CRMetadataStores(ctx context.Context) (map[string]admin.DucklingMetadataStore, error) {
	stores, err := a.dc.CRMetadataStores(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]admin.DucklingMetadataStore, len(stores))
	for name, ms := range stores {
		out[name] = admin.DucklingMetadataStore{Kind: ms.Type, Endpoint: ms.Endpoint}
	}
	return out, nil
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
