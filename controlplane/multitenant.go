//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/provisioning"
	"github.com/posthog/duckgres/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
			})
		}
	}
	return result
}

// Compile-time checks.
var _ OrgRouterInterface = (*orgRouterAdapter)(nil)
var _ admin.OrgStackInfo = (*orgRouterAdapter)(nil)

// SetupMultiTenant initializes the config store, org router, admin server, and provisioning server.
// Called from RunControlPlane when --config-store is set with remote backend.
// Returns the admin server and provisioning server for graceful shutdown.
func SetupMultiTenant(
	cfg ControlPlaneConfig,
	srv *server.Server,
	memBudget uint64,
	maxWorkers int,
) (ConfigStoreInterface, OrgRouterInterface, []*http.Server, error) {
	pollInterval := cfg.ConfigPollInterval
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	store, err := configstore.NewConfigStore(cfg.ConfigStoreConn, pollInterval)
	if err != nil {
		return nil, nil, nil, err
	}

	provisioningPort := cfg.ProvisioningPort
	if provisioningPort == 0 {
		provisioningPort = 9091
	}

	baseCfg := K8sWorkerPoolConfig{
		Namespace:            cfg.K8s.WorkerNamespace,
		CPID:                 cfg.K8s.ControlPlaneID,
		WorkerImage:          cfg.K8s.WorkerImage,
		WorkerPort:           cfg.K8s.WorkerPort,
		SecretName:           cfg.K8s.WorkerSecret,
		ConfigMap:            cfg.K8s.WorkerConfigMap,
		MaxWorkers:           maxWorkers,
		IdleTimeout:          cfg.WorkerIdleTimeout,
		ConfigPath:           cfg.ConfigPath,
		ImagePullPolicy:      cfg.K8s.ImagePullPolicy,
		ServiceAccount:       cfg.K8s.ServiceAccount,
		MemoryBudget:         int64(memBudget),
		SharedWarmActivation: cfg.K8s.SharedWarmWorkers,
	}

	router, err := NewOrgRouter(store, baseCfg, cfg, srv)
	if err != nil {
		return nil, nil, nil, err
	}

	adpt := &orgRouterAdapter{router: router}

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
	adminToken := cfg.AdminToken
	if adminToken == "" {
		tokenBytes := make([]byte, 32)
		if _, err := rand.Read(tokenBytes); err != nil {
			return nil, nil, nil, fmt.Errorf("generate admin token: %w", err)
		}
		adminToken = hex.EncodeToString(tokenBytes)
		slog.Info("Generated admin API token (pass via --admin-token or DUCKGRES_ADMIN_TOKEN to set explicitly).", "token", adminToken)
	}

	// Set up Gin admin server (replaces the simple metrics server)
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// Existing endpoints (unauthenticated)
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))
	engine.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// Admin API (authenticated)
	api := engine.Group("/api/v1", admin.APIAuthMiddleware(adminToken))
	admin.RegisterAPI(api, store, adpt)

	// Dashboard
	admin.RegisterDashboard(engine, adminToken)

	adminServer := &http.Server{
		Addr:    ":9090",
		Handler: engine,
	}
	go func() {
		slog.Info("Starting admin server with dashboard.", "addr", adminServer.Addr)
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("Admin server error.", "error", err)
		}
	}()

	// Set up provisioning API server (separate from admin — production-facing)
	provToken := cfg.ProvisioningToken
	if provToken == "" {
		provToken = adminToken // fall back to admin token if not set
	}

	provEngine := gin.New()
	provEngine.Use(gin.Recovery())
	provEngine.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})
	provAPI := provEngine.Group("/api/v1", admin.APIAuthMiddleware(provToken))
	provisioning.RegisterAPI(provAPI, provisioning.NewGormStore(store))

	provServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", provisioningPort),
		Handler: provEngine,
	}
	go func() {
		slog.Info("Starting provisioning API server.", "addr", provServer.Addr)
		if err := provServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("Provisioning API server error.", "error", err)
		}
	}()

	return store, adpt, []*http.Server{adminServer, provServer}, nil
}
