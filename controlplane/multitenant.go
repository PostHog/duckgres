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

func (a *orgRouterAdapter) StackForUser(username string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	stack, ok := a.router.StackForUser(username)
	if !ok {
		return nil, nil, nil, false
	}
	return stack.Pool, stack.Sessions, stack.Rebalancer, true
}

func (a *orgRouterAdapter) StackForOrg(orgID string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	stack, ok := a.router.StackForOrg(orgID)
	if !ok {
		return nil, nil, nil, false
	}
	return stack.Pool, stack.Sessions, stack.Rebalancer, true
}

func (a *orgRouterAdapter) IsMigratingForUser(username string) (bool, string) {
	return a.router.IsMigratingForUser(username)
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
	cpInstanceID := podUID + ":" + bootIDHex

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
	janitor.retireWorker = func(record configstore.WorkerRecord, reason string) {
		router.sharedPool.retireClaimedWorker(&record, reason)
	}
	janitor.reconcileWarmCapacity = func() {
		target := router.sharedPool.WarmCapacityTarget()
		if target <= 0 {
			return
		}
		observeOrgWorkerSpawn("shared")
		if err := router.sharedPool.SpawnMinWorkers(target); err != nil {
			slog.Warn("Janitor failed to reconcile shared warm capacity.", "target", target, "error", err)
		}
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
		slog.Info("Generated internal secret (pass via --internal-secret or DUCKGRES_INTERNAL_SECRET to set explicitly).", "secret", internalSecret)
	}

	// Set up API server (admin + provisioning + dashboard on :8080).
	// The existing metrics server on :9090 stays running separately.
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// Health endpoint (unauthenticated, used by K8s probes)
	engine.GET("/health", newHealthHandler(runtimeTracker.Draining))

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

func newHealthHandler(isDraining func() bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if isDraining != nil && isDraining() {
			c.String(http.StatusServiceUnavailable, "draining")
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
