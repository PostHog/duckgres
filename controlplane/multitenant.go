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
) (ConfigStoreInterface, OrgRouterInterface, *http.Server, error) {
	pollInterval := cfg.ConfigPollInterval
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	store, err := configstore.NewConfigStore(cfg.ConfigStoreConn, pollInterval)
	if err != nil {
		return nil, nil, nil, err
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

	router, err := NewOrgRouter(store, baseCfg, cfg, srv, stsBroker)
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
	internalSecret := cfg.InternalSecret
	if internalSecret == "" {
		tokenBytes := make([]byte, 32)
		if _, err := rand.Read(tokenBytes); err != nil {
			return nil, nil, nil, fmt.Errorf("generate internal secret: %w", err)
		}
		internalSecret = hex.EncodeToString(tokenBytes)
		slog.Info("Generated internal secret (pass via --internal-secret or DUCKGRES_INTERNAL_SECRET to set explicitly).", "secret", internalSecret)
	}

	// Set up API server (admin + provisioning + dashboard on :8080).
	// The existing metrics server on :9090 stays running separately.
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	// Health endpoint (also available on :9090 via metrics server)
	engine.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

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

	return store, adpt, apiServer, nil
}
