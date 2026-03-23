package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cloudflare/tableflip"
	"github.com/posthog/duckgres/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ControlPlaneConfig extends server.Config with control-plane-specific settings.
type ControlPlaneConfig struct {
	server.Config

	Process ProcessConfig

	SocketDir            string
	ConfigPath           string // Path to config file, passed to workers
	HealthCheckInterval  time.Duration
	WorkerQueueTimeout   time.Duration // How long to wait for an available worker slot (default: 5m)
	WorkerIdleTimeout    time.Duration // How long to keep an idle worker alive (default: 5m)
	HandoverDrainTimeout time.Duration // How long to wait for connections to drain during upgrade (default: 24h)
	MetricsServer        *http.Server  // Optional metrics server to shut down during upgrade

	// WorkerBackend selects the worker management backend.
	// "process" (default): workers are local child processes communicating over Unix sockets.
	// "remote": Kubernetes-backed multitenant workers communicating over TCP.
	//           Requires ConfigStoreConn and a binary built with -tags kubernetes.
	WorkerBackend string

	// K8s contains Kubernetes-specific configuration. Only used for remote
	// multitenant mode.
	K8s K8sConfig

	// ConfigStoreConn is the PostgreSQL connection string for the config store.
	// Required when WorkerBackend == "remote".
	ConfigStoreConn string

	// ConfigPollInterval is how often to poll the config store for changes.
	// Default: 30s.
	ConfigPollInterval time.Duration

	// AdminToken is the bearer token required for admin API requests.
	// When empty, a random token is generated and logged at startup.
	AdminToken string
}

type ProcessConfig struct {
	MinWorkers int
	MaxWorkers int
}

// K8sConfig holds Kubernetes worker backend configuration.
type K8sConfig struct {
	WorkerImage      string // Container image for worker pods (required)
	WorkerNamespace  string // K8s namespace (default: auto-detect from service account)
	ControlPlaneID   string // Unique CP identifier for labeling worker pods (default: os.Hostname())
	WorkerPort       int    // gRPC port on worker pods (default: 8816)
	WorkerSecret     string // K8s Secret name containing bearer token
	WorkerConfigMap  string // ConfigMap name for duckgres.yaml
	ImagePullPolicy  string // Image pull policy for worker pods (e.g., "Never", "IfNotPresent", "Always")
	ServiceAccount   string // ServiceAccount name for worker pods (default: "default")
	MaxWorkers       int    // Global cap for the shared K8s worker pool (0 = auto-derived)
	SharedWarmTarget int    // Neutral shared warm-worker target for K8s multi-tenant mode (0 = disabled)
}

// ControlPlane manages the TCP listener and routes connections to Flight SQL workers.
// The control plane owns client connections end-to-end: TLS, authentication,
// PostgreSQL wire protocol, and SQL transpilation all happen here. Workers are
// thin DuckDB execution engines reachable via Arrow Flight SQL over Unix sockets.
type ControlPlane struct {
	cfg             ControlPlaneConfig
	pool            WorkerPool      // non-nil in single-tenant process mode
	sessions        *SessionManager // non-nil in single-tenant process mode
	flight          *FlightIngress
	rebalancer      *MemoryRebalancer
	srv             *server.Server // Minimal server for cancel request routing
	rateLimiter     *server.RateLimiter
	tlsConfig       *tls.Config
	pgListener      net.Listener
	upgrader        *tableflip.Upgrader
	parentPID       int // tableflip parent PID (0 if first generation)
	activeConns     int64
	closed          bool
	closeMu         sync.Mutex
	wg              sync.WaitGroup
	reloading       atomic.Bool            // guards against concurrent upgrade from double SIGUSR1
	upgradeDraining atomic.Bool            // true after upgrade succeeded; SIGTERM should exit immediately
	acmeManager     *server.ACMEManager    // ACME manager for Let's Encrypt HTTP-01 (nil when using static certs)
	acmeDNSManager  *server.ACMEDNSManager // ACME manager for DNS-01 (nil when not using DNS challenges)

	// Multi-tenant fields (non-nil in remote multitenant mode)
	orgRouter   OrgRouterInterface
	configStore ConfigStoreInterface
}

// ConfigStoreInterface abstracts the config store for the control plane.
// Defined here to avoid circular imports with the configstore package.
type ConfigStoreInterface interface {
	ValidateUser(username, password string) (orgID string, ok bool)
	OrgForUser(username string) string
}

// OrgRouterInterface abstracts the org router for the control plane.
type OrgRouterInterface interface {
	StackForUser(username string) (pool WorkerPool, sessions *SessionManager, rebalancer *MemoryRebalancer, ok bool)
	ShutdownAll()
}

// RunControlPlane is the entry point for the control plane process.
func RunControlPlane(cfg ControlPlaneConfig) {
	// Apply defaults
	if cfg.SocketDir == "" {
		cfg.SocketDir = "/var/run/duckgres"
	}
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 2 * time.Second
	}
	if cfg.WorkerQueueTimeout == 0 {
		cfg.WorkerQueueTimeout = 5 * time.Minute
	}
	if cfg.WorkerIdleTimeout == 0 {
		cfg.WorkerIdleTimeout = 5 * time.Minute
	}
	if cfg.HandoverDrainTimeout == 0 {
		cfg.HandoverDrainTimeout = 24 * time.Hour
	}

	// Enforce secure defaults for control-plane mode.
	if err := validateControlPlaneSecurity(cfg); err != nil {
		slog.Error("Invalid control-plane security configuration.", "error", err)
		os.Exit(1)
	}
	if err := validateWorkerBackendConfig(cfg); err != nil {
		slog.Error("Invalid worker backend configuration.", "error", err)
		os.Exit(1)
	}

	isK8s := cfg.WorkerBackend == "remote"

	// --- tableflip upgrader for zero-downtime restarts ---
	// tableflip handles process spawning, listener FD inheritance, and the
	// parent/child ready/exit lifecycle. This replaces the bespoke handover
	// protocol (SCM_RIGHTS, JSON messages, handover socket).
	upg, err := tableflip.New(tableflip.Options{
		UpgradeTimeout: 30 * time.Second,
	})
	if err != nil {
		slog.Error("Failed to create tableflip upgrader.", "error", err)
		os.Exit(1)
	}

	if !isK8s {
		// Create socket directory.
		if err := os.MkdirAll(cfg.SocketDir, 0755); err != nil {
			slog.Error("Failed to create socket directory.", "error", err)
			os.Exit(1)
		}
	}

	// Configure TLS: ACME DNS-01, ACME HTTP-01, or static certificate files
	var tlsCfg *tls.Config
	var acmeMgr *server.ACMEManager
	var acmeDNSMgr *server.ACMEDNSManager
	if cfg.ACMEDomain != "" && cfg.ACMEDNSProvider != "" {
		mgr, err := server.NewACMEDNSManager(cfg.ACMEDomain, cfg.ACMEEmail, cfg.ACMEDNSZoneID, cfg.ACMECacheDir)
		if err != nil {
			slog.Error("Failed to start ACME DNS manager.", "error", err)
			os.Exit(1)
		}
		acmeDNSMgr = mgr
		tlsCfg = mgr.TLSConfig()
	} else if cfg.ACMEDomain != "" {
		mgr, err := server.NewACMEManager(cfg.ACMEDomain, cfg.ACMEEmail, cfg.ACMECacheDir, ":80")
		if err != nil {
			slog.Error("Failed to start ACME manager.", "error", err)
			os.Exit(1)
		}
		acmeMgr = mgr
		tlsCfg = mgr.TLSConfig()
	} else {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			slog.Error("Failed to load TLS certificates.", "error", err)
			os.Exit(1)
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	// Inherit or bind the PG TCP listener. tableflip returns the inherited
	// listener from the parent process on upgrade, or creates a fresh one.
	pgAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	pgLn, err := upg.Listen("tcp", pgAddr)
	if err != nil {
		slog.Error("Failed to listen.", "addr", pgAddr, "error", err)
		os.Exit(1)
	}

	// Save the tableflip parent PID before it potentially exits and we get
	// reparented to init. We need this to kill a stuck parent during future
	// upgrades (tableflip requires the parent to exit before the child can
	// do its own upgrade).
	var parentPID int
	if upg.HasParent() {
		parentPID = os.Getppid()
		slog.Info("Upgrade complete, inherited PG listener.", "addr", pgLn.Addr().String())
	}

	if !isK8s {
		// Verify socket directory is writable. On upgrade, the directory
		// may have gone read-only (EROFS under systemd ProtectSystem=strict);
		// PreBindSockets is non-fatal in that case, and workers will use the
		// /tmp fallback via effectiveSocketDir.
		if !upg.HasParent() {
			if err := checkSocketDirWritable(cfg.SocketDir); err != nil {
				slog.Error("Socket directory is not writable. Control plane will not be able to create worker sockets.", "dir", cfg.SocketDir, "error", err)
				os.Exit(1)
			}
		}
	}

	// Use default rate limit config if not specified
	if cfg.RateLimit.MaxFailedAttempts == 0 {
		cfg.RateLimit = server.DefaultRateLimitConfig()
	}

	// Initialize memory rebalancer. Every session gets the full memory budget
	// (75% of system RAM by default). DuckDB spills to disk/swap if needed.
	memBudget := server.ParseMemoryBytes(cfg.MemoryBudget)

	// Use a temporary rebalancer to auto-detect the budget and derive
	// backend-specific default max_workers if not explicitly set.
	tempRebalancer := NewMemoryRebalancer(memBudget, 0, nil, false)
	memBudget = tempRebalancer.memoryBudget // capture auto-detected value

	processMaxWorkers := cfg.Process.MaxWorkers
	if processMaxWorkers == 0 {
		processMaxWorkers = tempRebalancer.DefaultMaxWorkers()
	}
	k8sMaxWorkers := cfg.K8s.MaxWorkers
	if k8sMaxWorkers == 0 {
		k8sMaxWorkers = tempRebalancer.DefaultMaxWorkers()
	}

	rebalancer := NewMemoryRebalancer(memBudget, 0, nil, cfg.MemoryRebalance)

	if !isK8s && cfg.Process.MaxWorkers == 0 {
		slog.Info("Derived process.max_workers from memory budget.",
			"process_max_workers", processMaxWorkers,
			"memory_budget", formatBytes(memBudget))
	}
	if isK8s && cfg.K8s.MaxWorkers == 0 {
		slog.Info("Derived k8s.max_workers from memory budget.",
			"k8s_max_workers", k8sMaxWorkers,
			"memory_budget", formatBytes(memBudget))
	}

	processMinWorkers := cfg.Process.MinWorkers
	if processMinWorkers > processMaxWorkers {
		slog.Warn("process.min_workers exceeds process.max_workers; capping to process.max_workers.",
			"process_min_workers", processMinWorkers,
			"process_max_workers", processMaxWorkers)
		processMinWorkers = processMaxWorkers
	}

	k8sSharedWarmTarget := cfg.K8s.SharedWarmTarget
	if isK8s && k8sSharedWarmTarget > k8sMaxWorkers {
		slog.Warn("k8s.shared_warm_target exceeds k8s.max_workers; capping to k8s.max_workers.",
			"k8s_shared_warm_target", k8sSharedWarmTarget,
			"k8s_max_workers", k8sMaxWorkers)
		k8sSharedWarmTarget = k8sMaxWorkers
		cfg.K8s.SharedWarmTarget = k8sSharedWarmTarget
	}

	// Create a minimal server for cancel request routing
	srv := &server.Server{}
	server.InitMinimalServer(srv, cfg.Config, nil)

	// Initialize query logger (non-fatal on error)
	if ql, err := server.NewQueryLogger(cfg.Config); err != nil {
		slog.Warn("Failed to initialize query log, continuing without it.", "error", err)
	} else if ql != nil {
		server.SetQueryLogger(srv, ql)
	}

	cp := &ControlPlane{
		cfg:            cfg,
		srv:            srv,
		rateLimiter:    server.NewRateLimiter(cfg.RateLimit),
		tlsConfig:      tlsCfg,
		pgListener:     pgLn,
		upgrader:       upg,
		parentPID:      parentPID,
		acmeManager:    acmeMgr,
		acmeDNSManager: acmeDNSMgr,
	}

	// Multi-tenant mode: config store + per-org pools (K8s remote backend only)
	if cfg.WorkerBackend == "remote" {
		store, adapter, adminSrv, err := SetupMultiTenant(cfg, srv, memBudget, k8sMaxWorkers)
		if err != nil {
			slog.Error("Failed to set up multi-tenant config store.", "error", err)
			os.Exit(1)
		}
		cp.configStore = store
		cp.orgRouter = adapter
		// Replace the simple metrics server with the Gin admin server
		if cfg.MetricsServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = cfg.MetricsServer.Shutdown(ctx)
			cancel()
		}
		cfg.MetricsServer = adminSrv
		cp.cfg = cfg
		_ = store // keep linter happy
	} else {
		// Single-tenant mode: one shared process pool + session manager
		procPool := NewFlightWorkerPool(cfg.SocketDir, cfg.ConfigPath, processMinWorkers, processMaxWorkers)
		procPool.idleTimeout = cfg.WorkerIdleTimeout

		// Pre-bind worker sockets. On upgrade with EROFS, this may fail —
		// that's OK, workers will fall back to effectiveSocketDir (/tmp).
		if processMaxWorkers > 0 {
			if err := procPool.PreBindSockets(processMaxWorkers); err != nil {
				if upg.HasParent() {
					slog.Warn("Failed to pre-bind worker sockets (will use dynamic sockets).", "error", err)
				} else {
					slog.Error("Failed to pre-bind worker sockets.", "error", err)
					os.Exit(1)
				}
			}
		}
		pool := WorkerPool(procPool)

		sessions := NewSessionManager(pool, rebalancer)

		// Wire the circular dependency: rebalancer needs sessions to iterate,
		// sessions needs rebalancer to trigger rebalance on create/destroy.
		rebalancer.SetSessionLister(sessions)

		cp.pool = pool
		cp.sessions = sessions
		cp.rebalancer = rebalancer

		// Wire progress lookup so pg_stat_activity can show query progress.
		server.SetProgressFn(srv, func(pid int32) (pct float64, rows, totalRows uint64, stalled bool) {
			sp := sessions.GetProgress(pid)
			if sp == nil {
				return -1, 0, 0, false
			}
			return sp.Percentage, sp.Rows, sp.TotalRows, sp.Stalled
		})

		warmTarget := processMinWorkers
		if warmTarget > 0 {
			if err := pool.SpawnMinWorkers(warmTarget); err != nil {
				slog.Error("Failed to spawn min workers.", "error", err)
				os.Exit(1)
			}
		}

		// Start health check loop with crash notification and progress caching.
		onCrash := func(workerID int) {
			sessions.OnWorkerCrash(workerID, func(pid int32) {
				slog.Warn("Session orphaned by worker crash.", "pid", pid, "worker", workerID)
			})
		}
		onProgress := func(workerID int, progress map[string]*SessionProgress) {
			sessions.UpdateProgress(workerID, progress)
		}
		go pool.HealthCheckLoop(makeShutdownCtx(), cfg.HealthCheckInterval, onCrash, onProgress)
	}

	// Flight ingress is created AFTER upgrade so the old CP can keep
	// serving Flight SQL clients until it shuts down its listener in
	// drainAfterUpgrade. This minimizes the Flight unavailability
	// window to just the brief port rebind, rather than the entire
	// upgrade + pre-warm duration.
	cp.startFlightIngress()

	// Handle SIGUSR1 for graceful upgrade (process mode only)
	if !isK8s {
		go func() {
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGUSR1)
			for range sig {
				cp.handleUpgrade()
			}
		}()
	}

	// Handle SIGTERM/SIGINT for shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		s := <-sig
		if cp.upgradeDraining.Load() {
			slog.Info("Received shutdown signal during upgrade drain, exiting immediately.", "signal", s)
			cp.pool.ShutdownAll()
			os.Exit(0)
		}
		slog.Info("Received shutdown signal.", "signal", s)
		cp.shutdown()
		os.Exit(0)
	}()

	// Signal readiness to tableflip (completes the upgrade handshake with the
	// parent process, if any) and to systemd.
	if upg.HasParent() {
		// We're the new process after an upgrade. Tell systemd our PID before
		// signaling Ready to the parent — the parent may exit shortly after.
		if err := sdNotify(fmt.Sprintf("MAINPID=%d\nREADY=1", os.Getpid())); err != nil {
			slog.Warn("sd_notify MAINPID+READY failed.", "error", err)
		}
	} else {
		if err := sdNotify("READY=1"); err != nil {
			slog.Warn("sd_notify READY failed.", "error", err)
		}
	}
	if err := upg.Ready(); err != nil {
		slog.Error("Failed to signal readiness.", "error", err)
		os.Exit(1)
	}

	slog.Info("Control plane listening.",
		"pg_addr", cp.pgListener.Addr().String(),
		"flight_addr", cp.flightAddr(),
		"worker_backend", cfg.WorkerBackend,
		"process_min_workers", processMinWorkers,
		"process_max_workers", processMaxWorkers,
		"k8s_max_workers", k8sMaxWorkers,
		"k8s_shared_warm_target", k8sSharedWarmTarget,
		"worker_queue_timeout", cfg.WorkerQueueTimeout,
		"memory_budget", formatBytes(rebalancer.memoryBudget),
		"memory_rebalance", cfg.MemoryRebalance)

	// Accept loop in background
	go cp.acceptLoop()

	// Block until a successful upgrade causes tableflip to signal exit.
	// SIGTERM/SIGINT are handled above and call os.Exit directly.
	<-upg.Exit()

	// A successful upgrade completed — drain in-flight connections and exit.
	cp.drainAfterUpgrade()
}

func makeShutdownCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()
	return ctx
}

func (cp *ControlPlane) acceptLoop() {
	for {
		conn, err := cp.pgListener.Accept()
		if err != nil {
			cp.closeMu.Lock()
			closed := cp.closed
			cp.closeMu.Unlock()
			if closed {
				// Block here instead of returning. Returning would cause
				// RunControlPlane() → main() to exit, killing all in-flight
				// connection goroutines before the drain completes.
				// The upgrade drain or shutdown handler will call os.Exit(0)
				// after draining connections.
				select {}
			}
			slog.Error("Accept error.", "error", err)
			continue
		}

		// Enable TCP keepalive
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}

		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.handleConnection(conn)
		}()
	}
}

func createSessionWithRegisteredCancel(
	srv *server.Server,
	timeout time.Duration,
	key server.BackendKey,
	createFn func(context.Context) (int32, *server.FlightExecutor, error),
) (int32, *server.FlightExecutor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	srv.RegisterQuery(key, cancel)
	defer srv.UnregisterQuery(key)

	return createFn(ctx)
}

func (cp *ControlPlane) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()
	slog.Info("Connection accepted.", "remote_addr", remoteAddr)

	releaseRateLimit, msg := server.BeginRateLimitedAuthAttempt(cp.rateLimiter, remoteAddr)
	if msg != "" {
		slog.Warn("Connection rejected.", "remote_addr", remoteAddr, "reason", msg)
		_ = conn.Close()
		return
	}
	defer releaseRateLimit()

	// Set a startup read timeout to prevent goroutine leaks from clients
	// that connect but never send data (e.g., load balancer TCP health checks).
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		slog.Error("Failed to set startup deadline.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}

	// Read startup message to determine SSL vs cancel.
	// readStartupFromRaw handles GSSENC probes by replying 'N' and continuing.
	params, err := readStartupFromRaw(conn)
	if err != nil {
		if err == io.EOF || errors.Is(err, io.EOF) {
			slog.Debug("Client closed connection before sending startup message.", "remote_addr", remoteAddr)
		} else {
			slog.Error("Failed to read startup.", "remote_addr", remoteAddr, "error", err)
		}
		_ = conn.Close()
		return
	}

	// Handle cancel request
	if params.cancelRequest {
		key := server.BackendKey{Pid: params.cancelPid, SecretKey: params.cancelSecretKey}
		cp.srv.CancelQuery(key)
		_ = conn.Close()
		return
	}

	// Clear the startup read deadline before proceeding to TLS (which sets its own).
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		slog.Error("Failed to clear startup deadline.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}

	// Require SSL
	if !params.sslRequest {
		slog.Warn("Connection rejected: SSL required.", "remote_addr", remoteAddr)
		server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
		_ = conn.Close()
		return
	}

	// Send 'S' to indicate SSL support
	if _, err := conn.Write([]byte("S")); err != nil {
		slog.Error("Failed to send SSL response.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}

	atomic.AddInt64(&cp.activeConns, 1)
	defer atomic.AddInt64(&cp.activeConns, -1)

	// TLS handshake
	tlsConn := tls.Server(conn, cp.tlsConfig)
	if err := tlsConn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		slog.Error("Failed to set TLS deadline.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}
	if err := tlsConn.Handshake(); err != nil {
		slog.Error("TLS handshake failed.", "remote_addr", remoteAddr, "error", err)
		_ = tlsConn.Close()
		return
	}
	slog.Info("TLS connection established.", "remote_addr", remoteAddr)
	defer func() { _ = tlsConn.Close() }()

	if err := tlsConn.SetDeadline(time.Time{}); err != nil {
		slog.Error("Failed to clear TLS deadline.", "remote_addr", remoteAddr, "error", err)
		return
	}

	reader := bufio.NewReader(tlsConn)
	writer := bufio.NewWriter(tlsConn)

	// Read startup message (user/database)
	startupParams, err := server.ReadStartupMessage(reader)
	if err != nil {
		slog.Error("Failed to read startup message.", "remote_addr", remoteAddr, "error", err)
		return
	}

	username := startupParams["user"]
	database := startupParams["database"]
	applicationName := startupParams["application_name"]

	if username == "" {
		server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "no user specified")
		_ = writer.Flush()
		return
	}

	// Request password
	if err := server.WriteAuthCleartextPassword(writer); err != nil {
		slog.Error("Failed to request password.", "remote_addr", remoteAddr, "error", err)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush writer.", "remote_addr", remoteAddr, "error", err)
		return
	}

	// Read password response
	msgType, body, err := server.ReadMessage(reader)
	if err != nil {
		slog.Error("Failed to read password message.", "remote_addr", remoteAddr, "error", err)
		return
	}

	if msgType != 'p' {
		server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "expected password message")
		_ = writer.Flush()
		return
	}

	password := string(bytes.TrimRight(body, "\x00"))

	// Authenticate: use config store (multi-tenant) or YAML users (single-tenant)
	if cp.configStore != nil {
		orgID, ok := cp.configStore.ValidateUser(username, password)
		if !ok {
			slog.Warn("Authentication failed.", "user", username, "remote_addr", remoteAddr)
			banned := server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
			if banned {
				slog.Warn("IP banned after too many failed auth attempts.", "remote_addr", remoteAddr)
			}
			_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
			_ = writer.Flush()
			return
		}
		_ = orgID // used for routing below
	} else {
		if !server.ValidateUserPassword(cp.cfg.Users, username, password) {
			slog.Warn("Authentication failed.", "user", username, "remote_addr", remoteAddr)
			banned := server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
			if banned {
				slog.Warn("IP banned after too many failed auth attempts.", "remote_addr", remoteAddr)
			}
			_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
			_ = writer.Flush()
			return
		}
	}

	// Send auth OK
	if err := server.WriteAuthOK(writer); err != nil {
		slog.Error("Failed to send auth OK.", "remote_addr", remoteAddr, "error", err)
		return
	}

	server.RecordSuccessfulAuthAttempt(cp.rateLimiter, remoteAddr)
	slog.Info("User authenticated.", "user", username, "remote_addr", remoteAddr)

	// Resolve the session manager and rebalancer for this connection.
	// In multi-tenant mode, each org has its own stack.
	var sessions *SessionManager
	var rebalancer *MemoryRebalancer
	if cp.orgRouter != nil {
		_, sess, rebal, ok := cp.orgRouter.StackForUser(username)
		if !ok {
			_ = server.WriteErrorResponse(writer, "FATAL", "28000", "no org configured for user")
			_ = writer.Flush()
			return
		}
		sessions = sess
		rebalancer = rebal
	} else {
		sessions = cp.sessions
		rebalancer = cp.rebalancer
	}

	// Feed initial parameters and backend key data to the client IMMEDIATELY.
	// This keeps JDBC drivers happy while we perform the slow worker acquisition.
	pid := sessions.ReservePID()
	secretKey := server.GenerateSecretKey()

	// Use a temporary clientConn just to send initial params
	tmpCC := server.NewClientConn(cp.srv, nil, nil, writer, username, database, applicationName, nil, pid, secretKey, -1)
	defer server.CancelClientConn(tmpCC)
	server.SendInitialParams(tmpCC)
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush initial params.", "remote_addr", remoteAddr, "error", err)
		return
	}

	// Create session on a worker. The timeout controls how long we wait in the
	// worker queue when all slots are occupied.
	// Pass resource limits to be applied immediately by the worker (one RPC).
	var (
		memLimit string
		threads  int
	)
	if rebalancer != nil {
		memLimit = rebalancer.MemoryLimit()
		threads = rebalancer.PerSessionThreads()
	}

	_, executor, err := createSessionWithRegisteredCancel(
		cp.srv,
		cp.cfg.WorkerQueueTimeout,
		server.BackendKey{Pid: pid, SecretKey: secretKey},
		func(ctx context.Context) (int32, *server.FlightExecutor, error) {
			return sessions.CreateSession(ctx, username, pid, memLimit, threads)
		},
	)
	if err != nil {
		slog.Error("Failed to create session.", "user", username, "remote_addr", remoteAddr, "error", err)
		if errors.Is(err, context.Canceled) {
			_ = server.WriteErrorResponse(writer, "FATAL", "57014", "canceling authentication due to user request")
		} else {
			_ = server.WriteErrorResponse(writer, "FATAL", "53300", "too many connections")
		}
		_ = writer.Flush()
		return
	}
	defer sessions.DestroySession(pid)

	// Register the TCP connection so OnWorkerCrash can close it to unblock
	// the message loop if the backing worker dies.
	sessions.SetConnCloser(pid, tlsConn)

	// Create real clientConn with FlightExecutor and worker assignment
	workerID := sessions.WorkerIDForPID(pid)
	cc := server.NewClientConn(cp.srv, tlsConn, reader, writer, username, database, applicationName, executor, pid, secretKey, workerID)

	// Send ReadyForQuery to signal that the handshake is complete
	if err := server.WriteReadyForQuery(writer, 'I'); err != nil {
		slog.Error("Failed to send ReadyForQuery.", "remote_addr", remoteAddr, "error", err)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush writer.", "remote_addr", remoteAddr, "error", err)
		return
	}

	// Run message loop
	if err := server.RunMessageLoop(cc); err != nil {
		slog.Error("Message loop error.", "user", username, "remote_addr", remoteAddr, "error", err)
		return
	}

	slog.Info("Client disconnected.", "user", username, "remote_addr", remoteAddr)
}

// startupResult holds the parsed initial startup message.
type startupResult struct {
	sslRequest      bool
	gssRequest      bool
	cancelRequest   bool
	cancelPid       int32
	cancelSecretKey int32
}

// readStartupFromRaw reads the startup message from a raw (unbuffered) connection.
// It handles GSSENCRequest negotiation (up to maxNegotiationRounds) before returning.
func readStartupFromRaw(conn net.Conn) (startupResult, error) {
	// A legitimate client sends at most one GSSENCRequest followed by SSLRequest.
	// Cap iterations to prevent a malicious client from looping indefinitely.
	const maxNegotiationRounds = 3

	for range maxNegotiationRounds {
		// Read length (4 bytes)
		var length int32
		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			return startupResult{}, fmt.Errorf("read length: %w", err)
		}

		if length < 8 || length > 10000 {
			return startupResult{}, fmt.Errorf("invalid startup message length: %d", length)
		}

		remaining := make([]byte, length-4)
		if _, err := fullRead(conn, remaining); err != nil {
			return startupResult{}, fmt.Errorf("read body: %w", err)
		}

		protocolVersion := binary.BigEndian.Uint32(remaining[:4])

		// SSL request
		if protocolVersion == 80877103 {
			return startupResult{sslRequest: true}, nil
		}

		// Cancel request
		if protocolVersion == 80877102 && len(remaining) >= 12 {
			pid := int32(binary.BigEndian.Uint32(remaining[4:8]))
			key := int32(binary.BigEndian.Uint32(remaining[8:12]))
			return startupResult{cancelRequest: true, cancelPid: pid, cancelSecretKey: key}, nil
		}

		// GSSENCRequest (PostgreSQL 12+, JDBC driver gssEncMode=prefer)
		// Respond with 'N' (GSSAPI encryption not supported) and re-read.
		// The client will follow up with SSLRequest on the same connection.
		// The startup read deadline set in handleConnection covers all rounds.
		if protocolVersion == 80877104 {
			slog.Debug("GSSENCRequest received, declining.", "remote_addr", conn.RemoteAddr())
			if _, err := conn.Write([]byte("N")); err != nil {
				return startupResult{}, fmt.Errorf("write GSSENC decline: %w", err)
			}
			continue
		}

		return startupResult{}, fmt.Errorf("unexpected protocol version: %d", protocolVersion)
	}

	return startupResult{}, fmt.Errorf("too many negotiation rounds")
}

// readStartupWithGSSFallback accepts a GSSAPI probe, rejects it with 'N',
// and keeps reading startup packets on the same connection so clients can
// continue with SSLRequest/startup without reconnecting.
func readStartupWithGSSFallback(conn net.Conn) (startupResult, error) {
	for i := 0; i < 4; i++ {
		params, err := readStartupFromRaw(conn)
		if err != nil {
			return startupResult{}, err
		}

		if !params.gssRequest {
			return params, nil
		}

		if _, err := conn.Write([]byte{'N'}); err != nil {
			return startupResult{}, fmt.Errorf("write GSSAPI rejection: %w", err)
		}
	}

	return startupResult{}, fmt.Errorf("too many GSSAPI startup requests")
}

func fullRead(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (cp *ControlPlane) shutdown() {
	cp.closeMu.Lock()
	cp.closed = true
	cp.closeMu.Unlock()

	if cp.pgListener != nil {
		_ = cp.pgListener.Close()
	}
	if cp.flight != nil {
		cp.flight.Shutdown()
		cp.flight = nil
	}

	// Wait for in-flight connections to finish
	slog.Info("Waiting for connections to drain...")
	cp.wg.Wait()

	slog.Info("Shutting down workers...")
	if cp.orgRouter != nil {
		cp.orgRouter.ShutdownAll()
	} else if cp.pool != nil {
		cp.pool.ShutdownAll()
	}

	if cp.rebalancer != nil {
		cp.rebalancer.Stop()
	}

	// Shut down ACME managers if active
	if cp.acmeManager != nil {
		if err := cp.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown error.", "error", err)
		}
	}
	if cp.acmeDNSManager != nil {
		if err := cp.acmeDNSManager.Close(); err != nil {
			slog.Warn("ACME DNS manager shutdown error.", "error", err)
		}
	}

	cp.stopQueryLogger()

	slog.Info("Control plane shutdown complete.")
}

func (cp *ControlPlane) stopQueryLogger() {
	if cp.srv != nil && cp.srv.QueryLogger() != nil {
		cp.srv.QueryLogger().Stop()
	}
}

// handleUpgrade triggers a graceful upgrade via tableflip: stops subsystems
// that need exclusive ports, calls upg.Upgrade() to spawn the child process
// with inherited FDs, and on success lets the main goroutine proceed to drain.
func (cp *ControlPlane) handleUpgrade() {
	if !cp.reloading.CompareAndSwap(false, true) {
		slog.Warn("SIGUSR1 ignored, upgrade already in progress.")
		return
	}

	slog.Info("Received SIGUSR1, starting graceful upgrade.")

	// Stop metrics server before spawning the replacement so it can bind
	// to the same port.
	if cp.cfg.MetricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := cp.cfg.MetricsServer.Shutdown(ctx); err != nil {
			slog.Warn("Metrics server shutdown failed.", "error", err)
		}
		cancel()
	}

	// Stop ACME managers so the new CP can bind port 80 (HTTP-01) or
	// manage DNS records. Nil out after close so drainAfterUpgrade
	// doesn't double-close.
	if cp.acmeManager != nil {
		if err := cp.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown failed.", "error", err)
		}
		cp.acmeManager = nil
	}
	if cp.acmeDNSManager != nil {
		if err := cp.acmeDNSManager.Close(); err != nil {
			slog.Warn("ACME DNS manager shutdown failed.", "error", err)
		}
		cp.acmeDNSManager = nil
	}

	// Flight ingress is NOT stopped here — the old CP keeps serving Flight
	// SQL clients during the upgrade. It is shut down in drainAfterUpgrade.

	if err := sdNotify("RELOADING=1"); err != nil {
		slog.Warn("sd_notify RELOADING failed.", "error", err)
	}

	// tableflip.Upgrade() spawns the child process with all Listen'd FDs
	// inherited, then blocks until the child calls Ready() or times out.
	if err := cp.upgrader.Upgrade(); err != nil {
		// tableflip requires the parent process to have fully exited before
		// the child can perform its own upgrade. If the parent is stuck
		// draining long-lived connections, kill it and retry.
		if strings.Contains(err.Error(), "parent hasn't exited") && cp.killStuckParent() {
			if retryErr := cp.upgrader.Upgrade(); retryErr == nil {
				slog.Info("Upgrade succeeded after terminating stuck parent.")
				cp.upgradeDraining.Store(true)
				cp.reloading.Store(false)
				return
			} else {
				err = retryErr
			}
		}

		slog.Error("Upgrade failed, recovering.", "error", err)
		cp.reloading.Store(false)
		if err := sdNotify("READY=1"); err != nil {
			slog.Warn("sd_notify READY (recovery) failed.", "error", err)
		}
		cp.recoverAfterFailedReload()
		return
	}

	slog.Info("Upgrade succeeded, child process is ready. Draining connections.")
	cp.upgradeDraining.Store(true)
	cp.reloading.Store(false)
	// upg.Exit() channel closes now, triggering drainAfterUpgrade in the main goroutine.
}

// killStuckParent sends SIGTERM (then SIGKILL) to the tableflip parent process
// that failed to exit after the previous upgrade. Returns true if the parent
// was found and terminated (or was already dead).
func (cp *ControlPlane) killStuckParent() bool {
	if cp.parentPID <= 1 {
		return false
	}

	// Verify the parent is still alive.
	if err := syscall.Kill(cp.parentPID, 0); err != nil {
		slog.Info("Tableflip parent already exited.", "parent_pid", cp.parentPID)
		return true
	}

	slog.Warn("Tableflip parent still alive, sending SIGTERM to unblock upgrade.", "parent_pid", cp.parentPID)
	_ = syscall.Kill(cp.parentPID, syscall.SIGTERM)

	// Wait up to 15 seconds for graceful exit.
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := syscall.Kill(cp.parentPID, 0); err != nil {
				slog.Info("Tableflip parent exited after SIGTERM.", "parent_pid", cp.parentPID)
				return true
			}
		case <-deadline:
			slog.Warn("Tableflip parent did not exit after SIGTERM, sending SIGKILL.", "parent_pid", cp.parentPID)
			_ = syscall.Kill(cp.parentPID, syscall.SIGKILL)
			time.Sleep(1 * time.Second)
			return true
		}
	}
}

// drainAfterUpgrade is called after a successful tableflip upgrade. It stops
// accepting new connections, waits for in-flight connections to finish, shuts
// down workers, and exits.
func (cp *ControlPlane) drainAfterUpgrade() {
	// Shut down Flight ingress now that the new CP has started.
	if cp.flight != nil {
		cp.flight.Shutdown()
		cp.flight = nil
	}

	// Stop accepting new connections. The new CP has its own listener copy
	// (inherited via tableflip), so closing ours doesn't affect it.
	cp.closeMu.Lock()
	cp.closed = true
	cp.closeMu.Unlock()
	_ = cp.pgListener.Close()

	// Wait for in-flight connections to finish (with timeout)
	drainDone := make(chan struct{})
	go func() {
		cp.wg.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		slog.Info("All connections drained after upgrade.")
	case <-time.After(cp.cfg.HandoverDrainTimeout):
		slog.Warn("Upgrade drain timeout, forcing exit.", "timeout", cp.cfg.HandoverDrainTimeout)
	}

	// Shut down workers
	if cp.orgRouter != nil {
		cp.orgRouter.ShutdownAll()
	} else if cp.pool != nil {
		cp.pool.ShutdownAll()
	}
	cp.stopQueryLogger()

	// Give systemd time to process the MAINPID notification before we exit.
	if os.Getenv("NOTIFY_SOCKET") != "" {
		time.Sleep(2 * time.Second)
	}

	slog.Info("Old control plane exiting after upgrade.")
	os.Exit(0)
}

// startFlightIngress creates and starts the Flight SQL ingress listener.
// During upgrade, the old CP's Flight listener may still be shutting down,
// so we retry port binding briefly before giving up.
func (cp *ControlPlane) startFlightIngress() {
	if cp.cfg.FlightPort <= 0 {
		return
	}

	// Flight ingress requires a single session manager (not multi-tenant).
	if cp.sessions == nil {
		slog.Info("Flight ingress disabled in multi-tenant mode.")
		return
	}

	flightCfg := FlightIngressConfig{
		SessionIdleTTL:     cp.cfg.FlightSessionIdleTTL,
		SessionReapTick:    cp.cfg.FlightSessionReapInterval,
		HandleIdleTTL:      cp.cfg.FlightHandleIdleTTL,
		SessionTokenTTL:    cp.cfg.FlightSessionTokenTTL,
		WorkerQueueTimeout: cp.cfg.WorkerQueueTimeout,
	}

	var flightIngress *FlightIngress
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		flightIngress, err = NewFlightIngress(cp.cfg.Host, cp.cfg.FlightPort, cp.tlsConfig, cp.cfg.Users, cp.sessions, cp.rateLimiter, flightCfg)
		if err == nil {
			break
		}
		if attempt < 9 {
			slog.Warn("Flight ingress port not yet available, retrying...", "attempt", attempt+1, "error", err)
			time.Sleep(500 * time.Millisecond)
		}
	}
	if err != nil {
		slog.Error("Failed to initialize Flight ingress, continuing without Flight SQL.", "error", err)
		return
	}

	cp.flight = flightIngress
	cp.flight.Start()
}

// recoverAfterFailedReload restores all subsystems that were shut down in the
// SIGUSR1 handler when the new CP fails to complete the upgrade.
func (cp *ControlPlane) recoverAfterFailedReload() {
	cp.recoverMetricsAfterFailedReload()
	cp.recoverACMEAfterFailedReload()
	// Flight ingress is NOT shut down in the SIGUSR1 handler (it keeps
	// serving during upgrade), so no recovery is needed here.
}

func (cp *ControlPlane) recoverMetricsAfterFailedReload() {
	if cp.cfg.MetricsServer == nil {
		return
	}

	// The old http.Server is in a "closed" state after Shutdown() and cannot
	// be restarted. Create a fresh one on the same address.
	addr := cp.cfg.MetricsServer.Addr
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	newSrv := &http.Server{Addr: addr, Handler: mux}
	cp.cfg.MetricsServer = newSrv
	go func() {
		slog.Info("Recovered metrics server after reload failure.", "addr", addr)
		if err := newSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("Recovered metrics server error.", "error", err)
		}
	}()
}

func (cp *ControlPlane) recoverACMEAfterFailedReload() {
	if cp.cfg.ACMEDomain == "" {
		return
	}

	if cp.cfg.ACMEDNSProvider != "" {
		// DNS-01 mode
		mgr, err := server.NewACMEDNSManager(cp.cfg.ACMEDomain, cp.cfg.ACMEEmail, cp.cfg.ACMEDNSZoneID, cp.cfg.ACMECacheDir)
		if err != nil {
			slog.Error("Failed to recover ACME DNS manager after reload failure.", "error", err)
			return
		}
		cp.acmeDNSManager = mgr
		cp.tlsConfig = mgr.TLSConfig()
		slog.Info("Recovered ACME DNS manager after reload failure.")
		return
	}

	// HTTP-01 mode
	mgr, err := server.NewACMEManager(cp.cfg.ACMEDomain, cp.cfg.ACMEEmail, cp.cfg.ACMECacheDir, ":80")
	if err != nil {
		slog.Error("Failed to recover ACME manager after reload failure.", "error", err)
		return
	}
	cp.acmeManager = mgr
	cp.tlsConfig = mgr.TLSConfig()
	slog.Info("Recovered ACME manager after reload failure.")
}

func (cp *ControlPlane) flightAddr() string {
	if cp.flight == nil {
		return "disabled"
	}
	return cp.flight.Addr()
}
