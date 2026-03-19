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
	"github.com/posthog/duckgres/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// teamRouterAdapter wraps TeamRouter to implement both TeamRouterInterface
// (for the control plane) and admin.TeamStackInfo (for the admin API).
type teamRouterAdapter struct {
	router *TeamRouter
}

func (a *teamRouterAdapter) StackForUser(username string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	stack, ok := a.router.StackForUser(username)
	if !ok {
		return nil, nil, nil, false
	}
	return stack.Pool, stack.Sessions, stack.Rebalancer, true
}

func (a *teamRouterAdapter) ShutdownAll() {
	a.router.ShutdownAll()
}

func (a *teamRouterAdapter) AllTeamStats() []admin.TeamStatus {
	stacks := a.router.AllStacks()
	stats := make([]admin.TeamStatus, 0, len(stacks))
	for name, stack := range stacks {
		sessionCount := stack.Sessions.SessionCount()
		stats = append(stats, admin.TeamStatus{
			Name:           name,
			ActiveSessions: sessionCount,
			MaxWorkers:     stack.Config.MaxWorkers,
			MinWorkers:     stack.Config.MinWorkers,
			MemoryBudget:   stack.Config.MemoryBudget,
		})
		// Emit per-team Prometheus metrics
		observeTeamSessionsActive(name, sessionCount)
	}
	return stats
}

func (a *teamRouterAdapter) AllWorkerStatuses() []admin.WorkerStatus {
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
				Team:           name,
				ActiveSessions: count,
				Status:         status,
			})
		}
		// Emit per-team worker Prometheus metrics
		observeTeamWorkersActive(name, activeCount)
		observeTeamWorkersIdle(name, idleCount)
	}
	return result
}

func (a *teamRouterAdapter) AllSessionStatuses() []admin.SessionStatus {
	stacks := a.router.AllStacks()
	var result []admin.SessionStatus
	for name, stack := range stacks {
		for _, s := range stack.Sessions.AllSessions() {
			result = append(result, admin.SessionStatus{
				PID:      s.PID,
				WorkerID: s.WorkerID,
				Team:     name,
			})
		}
	}
	return result
}

// Compile-time checks.
var _ TeamRouterInterface = (*teamRouterAdapter)(nil)
var _ admin.TeamStackInfo = (*teamRouterAdapter)(nil)

// SetupMultiTenant initializes the config store, team router, and Gin admin server.
// Called from RunControlPlane when --config-store is set with remote backend.
func buildMultiTenantBasePoolConfig(cfg ControlPlaneConfig, memBudget uint64, maxWorkers int) K8sWorkerPoolConfig {
	return K8sWorkerPoolConfig{
		Namespace:       cfg.K8s.WorkerNamespace,
		CPID:            cfg.K8s.ControlPlaneID,
		WorkerImage:     cfg.K8s.WorkerImage,
		WorkerPort:      cfg.K8s.WorkerPort,
		SecretName:      cfg.K8s.WorkerSecret,
		MaxWorkers:      maxWorkers,
		IdleTimeout:     cfg.WorkerIdleTimeout,
		ConfigPath:      cfg.ConfigPath,
		ImagePullPolicy: cfg.K8s.ImagePullPolicy,
		ServiceAccount:  cfg.K8s.ServiceAccount,
		MemoryBudget:    int64(memBudget),
	}
}

func SetupMultiTenant(
	cfg ControlPlaneConfig,
	srv *server.Server,
	memBudget uint64,
	maxWorkers int,
) (ConfigStoreInterface, TeamRouterInterface, *http.Server, error) {
	pollInterval := cfg.ConfigPollInterval
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	store, err := configstore.NewConfigStore(cfg.ConfigStoreConn, pollInterval)
	if err != nil {
		return nil, nil, nil, err
	}

	baseCfg := buildMultiTenantBasePoolConfig(cfg, memBudget, maxWorkers)

	router, err := NewTeamRouter(store, baseCfg, cfg, srv)
	if err != nil {
		return nil, nil, nil, err
	}

	adpt := &teamRouterAdapter{router: router}

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

	return store, adpt, adminServer, nil
}
