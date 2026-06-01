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
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/netkeepalive"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/ducklake"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/flightsqlingress"
	"github.com/posthog/duckgres/server/sessionmeta"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ControlPlaneConfig extends server.Config with control-plane-specific settings.
type ControlPlaneConfig struct {
	server.Config

	Process ProcessConfig

	SocketDir            string
	ConfigPath           string // Path to config file, passed to workers
	HealthCheckInterval  time.Duration
	WorkerQueueTimeout   time.Duration // How long to wait for an available worker/org connection slot (default: 60s)
	WorkerIdleTimeout    time.Duration // How long to keep an idle worker alive (default: 5m)
	RetireOnSessionEnd   bool          // When true, process workers are retired immediately after their last session ends.
	HandoverDrainTimeout time.Duration // How long to wait for connections to drain during upgrade. 0 = unbounded (wait until k8s SIGKILL via terminationGracePeriodSeconds). Default: 0 in remote mode (so a CP rolling out doesn't kill in-flight customer queries at a self-imposed wall — see drainAndShutdown), 24h in process mode.
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

	// InternalSecret is the shared secret for API authentication.
	// When empty, a random secret is generated and logged at startup.
	InternalSecret string

	// SNIRoutingMode controls hostname-based org routing. Values:
	//   "" or "off"   - SNI is ignored; legacy database-param routing only
	//                   (default).
	//   "passthrough" - Managed Postgres SNI must resolve to the same org as
	//                   the requested startup database; when startup database is
	//                   empty, use the SNI-derived database fallback. Legacy
	//                   hostnames still fall back with a warn log.
	//   "enforce"     - Reject connections whose SNI doesn't match a managed
	//                   suffix. Explicit startup database still takes
	//                   priority when SNI is present, but must resolve to the
	//                   same org as the managed hostname.
	// Unknown values behave like "off" in the connection path.
	// Only consulted in multi-tenant control-plane builds (kubernetes tag);
	// other builds always behave as "off".
	SNIRoutingMode string

	// ManagedHostnameSuffixes lists DNS suffixes (each starting with a dot) for
	// managed tenant hostnames. When SNI matches one of these suffixes, the
	// single-label prefix is resolved as hostname_alias, database_name, or org
	// name. Postgres still honors an explicit startup database, but only when it
	// resolves to the same org as the managed hostname.
	ManagedHostnameSuffixes []string

	// DuckLakeDefaultSpecVersion is the global default DuckLake spec version
	// used for migration checks when an org doesn't specify an override.
	DuckLakeDefaultSpecVersion string
}

type ProcessConfig struct {
	MinWorkers int
	MaxWorkers int
}

// K8sConfig holds Kubernetes worker backend configuration.
type K8sConfig struct {
	WorkerImage                     string        // Container image for worker pods (required)
	WorkerNamespace                 string        // K8s namespace (default: auto-detect from service account)
	ControlPlaneID                  string        // Unique CP identifier for labeling worker pods (default: os.Hostname())
	WorkerPort                      int           // gRPC port on worker pods (default: 8816)
	WorkerSecret                    string        // Base name for per-worker K8s Secrets containing RPC bearer token and TLS material
	WorkerConfigMap                 string        // ConfigMap name for duckgres.yaml
	ImagePullPolicy                 string        // Image pull policy for worker pods (e.g., "Never", "IfNotPresent", "Always")
	ServiceAccount                  string        // Neutral ServiceAccount name for worker pods (default: "duckgres-worker")
	MaxWorkers                      int           // Global cap for the shared K8s worker pool (0 = unbounded; cluster autoscaler is the natural ceiling)
	SharedWarmTarget                int           // Neutral shared warm-worker target for K8s multi-tenant mode (0 = disabled)
	DynamicWarmCapacityEnabled      bool          // Enable configstore-driven dynamic warm-capacity target computation
	WarmCapacityMissWindow          time.Duration // Window of recent no-idle misses that contributes to dynamic targets
	WarmCapacityMissesPerWorker     int           // Number of recent misses that translate to one extra warm worker
	WarmCapacityDemandTTL           time.Duration // Retention TTL for warm-capacity miss buckets
	WarmCapacityDynamicImageCeiling int           // Max dynamic extra warm workers per image (0 = unlimited)
	WarmCapacityDynamicTotalCeiling int           // Max dynamic extra warm workers across images (0 = unlimited)
	WorkerCPURequest                string        // CPU request for worker pods (e.g., "500m")
	WorkerMemoryRequest             string        // Memory request for worker pods (e.g., "1Gi")
	WorkerNodeSelector              string        // JSON map for worker pod nodeSelector (e.g., '{"posthog.com/nodepool":"workers"}')
	WorkerTolerationKey             string        // Taint key for worker pod NoSchedule toleration
	WorkerTolerationValue           string        // Taint value for worker pod NoSchedule toleration
	WorkerExclusiveNode             bool          // One worker per node via pod anti-affinity
	AWSRegion                       string        // AWS region for STS client
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
	flightListener  net.Listener
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

	// isRemoteBackend is true when workers run as separate K8s pods (remote backend).
	isRemoteBackend bool

	// Multi-tenant fields (non-nil in remote multitenant mode)
	orgRouter      OrgRouterInterface
	configStore    ConfigStoreInterface
	apiServer      *http.Server // API server on :8080 (shut down on graceful exit)
	runtimeTracker *ControlPlaneRuntimeTracker
	janitorLeader  *JanitorLeaderManager
}

// ConfigStoreInterface abstracts the config store for the control plane.
// Defined here to avoid circular imports with the configstore package.
type ConfigStoreInterface interface {
	ResolveDatabase(database string) (orgID string)
	DatabaseNameForSNIPrefix(prefix string) string // translates SNI hostname prefix → canonical database_name (alias-aware)
	// ResolveSNIPrefix maps a managed hostname prefix to its org and database.
	// It accepts hostname_alias, database_name, and DNS-safe org names.
	ResolveSNIPrefix(prefix string) (orgID, databaseName string)
	ResolvePostgresConnection(startupDatabase, sniPrefix string, useManagedSNI bool, username, password string) configstore.PostgresConnectionResolution
	ValidateOrgUser(orgID, username, password string) bool
	// ValidateOrgUserAndGetPassthrough does both lookups against the same
	// snapshot — the auth path needs both, and a single read closes the
	// window where the snapshot could swap between two separate calls.
	// passthrough is always false when valid is false.
	ValidateOrgUserAndGetPassthrough(orgID, username, password string) (valid, passthrough bool)
	// OrgWarehouseStatus reports an org's current warehouse provisioning state so
	// connection-time errors can distinguish "no such org" from "warehouse not
	// ready yet". Returns (state, orgExists). state is "" when the org has no
	// warehouse row (legacy single-tenant orgs); otherwise it is the lifecycle
	// string (pending/provisioning/ready/failed/deleting/deleted).
	OrgWarehouseStatus(orgID string) (state string, orgExists bool)
	UpsertFlightSessionRecord(record *configstore.FlightSessionRecord) error
	GetFlightSessionRecord(sessionToken string) (*configstore.FlightSessionRecord, error)
	TouchFlightSessionRecord(sessionToken string, lastSeenAt time.Time) error
	CloseFlightSessionRecord(sessionToken string, closedAt time.Time) error
}

// OrgRouterInterface abstracts the org router for the control plane.
type OrgRouterInterface interface {
	StackForOrg(orgID string) (pool WorkerPool, sessions *SessionManager, rebalancer *MemoryRebalancer, ok bool)
	IcebergConfigForOrg(orgID string) (server.IcebergConfig, bool)
	IsMigratingForOrg(orgID string) bool
	SetWarmCapacityTarget(n int)
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
		cfg.WorkerQueueTimeout = 60 * time.Second
	}
	if cfg.WorkerIdleTimeout == 0 {
		cfg.WorkerIdleTimeout = 5 * time.Minute
	}
	if cfg.SessionInitTimeout == 0 {
		cfg.SessionInitTimeout = server.DefaultSessionInitTimeout
	}
	if cfg.HandoverDrainTimeout == 0 {
		// Remote mode: no internal drain timeout. The CP waits for active
		// sessions to finish for as long as it takes; k8s
		// terminationGracePeriodSeconds is the only hard wall. The
		// previous 15m default cut off in-flight customer queries that
		// just happened to be running at the wall (see the worker-40761
		// incident) — moving the wall doesn't fix that race, removing it
		// does. Process mode keeps 24h since there's no k8s safety net.
		if cfg.WorkerBackend == "remote" {
			cfg.HandoverDrainTimeout = 0
		} else {
			cfg.HandoverDrainTimeout = 24 * time.Hour
		}
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

	var flightLn net.Listener
	if cfg.FlightPort > 0 {
		flightAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.FlightPort)
		flightLn, err = upg.Listen("tcp", flightAddr)
		if err != nil {
			slog.Error("Failed to listen.", "addr", flightAddr, "error", err)
			os.Exit(1)
		}
	}

	// Save the tableflip parent PID before it potentially exits and we get
	// reparented to init. We need this to kill a stuck parent during future
	// upgrades (tableflip requires the parent to exit before the child can
	// do its own upgrade).
	var parentPID int
	if upg.HasParent() {
		parentPID = os.Getppid()
		slog.Info("Upgrade complete, inherited PG listener.", "addr", pgLn.Addr().String())
		if flightLn != nil {
			slog.Info("Upgrade complete, inherited Flight listener.", "addr", flightLn.Addr().String())
		}
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
	// process-mode default max_workers if not explicitly set. The K8s
	// backend does NOT derive max_workers from this budget — worker pods
	// run on separate nodes, so the CP's RAM tells us nothing about how
	// many worker pods the cluster can host. For K8s mode, MaxWorkers=0
	// means "unbounded" and the cluster's NodePool/autoscaler is the
	// natural ceiling.
	tempRebalancer := NewMemoryRebalancer(memBudget, 0, nil, false)
	memBudget = tempRebalancer.memoryBudget // capture auto-detected value

	processMaxWorkers := cfg.Process.MaxWorkers
	if processMaxWorkers == 0 {
		processMaxWorkers = tempRebalancer.DefaultMaxWorkers()
	}
	// K8s max_workers: 0 = unbounded (no cap derived from CP memory).
	k8sMaxWorkers := cfg.K8s.MaxWorkers

	rebalancer := NewMemoryRebalancer(memBudget, 0, nil, cfg.MemoryRebalance)

	if !isK8s && cfg.Process.MaxWorkers == 0 {
		slog.Info("Derived process.max_workers from memory budget.",
			"process_max_workers", processMaxWorkers,
			"memory_budget", formatBytes(memBudget))
	}
	if isK8s && k8sMaxWorkers == 0 {
		slog.Info("k8s.max_workers unset; worker pool is unbounded — cluster autoscaler (e.g. Karpenter) is the ceiling.")
	}

	processMinWorkers := cfg.Process.MinWorkers
	if processMinWorkers > processMaxWorkers {
		slog.Warn("process.min_workers exceeds process.max_workers; capping to process.max_workers.",
			"process_min_workers", processMinWorkers,
			"process_max_workers", processMaxWorkers)
		processMinWorkers = processMaxWorkers
	}

	k8sSharedWarmTarget := cfg.K8s.SharedWarmTarget
	// Only cap the warm target if k8sMaxWorkers is an actual upper bound
	// (>0). When k8sMaxWorkers == 0 the pool is unbounded and the warm
	// target stands on its own.
	if isK8s && k8sMaxWorkers > 0 && k8sSharedWarmTarget > k8sMaxWorkers {
		slog.Warn("k8s.shared_warm_target exceeds k8s.max_workers; capping to k8s.max_workers.",
			"k8s_shared_warm_target", k8sSharedWarmTarget,
			"k8s_max_workers", k8sMaxWorkers)
		k8sSharedWarmTarget = k8sMaxWorkers
		cfg.K8s.SharedWarmTarget = k8sSharedWarmTarget
	}

	// In remote (multitenant) mode the global DuckLake.MetadataStore is empty
	// because metadata stores are per-org (loaded from configstore), but every
	// worker is DuckLake-backed. Force the transpiler into DuckLake mode so
	// DDL stripping (e.g. ALTER TABLE ADD PRIMARY KEY → no-op) and the other
	// DuckLake-aware transforms still run.
	if cfg.WorkerBackend == "remote" {
		cfg.AlwaysDuckLake = true
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
		cfg:             cfg,
		srv:             srv,
		rateLimiter:     server.NewRateLimiter(cfg.RateLimit),
		tlsConfig:       tlsCfg,
		pgListener:      pgLn,
		flightListener:  flightLn,
		upgrader:        upg,
		parentPID:       parentPID,
		acmeManager:     acmeMgr,
		acmeDNSManager:  acmeDNSMgr,
		isRemoteBackend: cfg.WorkerBackend == "remote",
	}

	// Multi-tenant mode: config store + per-org pools (K8s remote backend only)
	if cfg.WorkerBackend == "remote" {
		store, adapter, apiServer, runtimeTracker, janitorLeader, err := SetupMultiTenant(cfg, srv, memBudget, k8sMaxWorkers, cp.healthReady)
		if err != nil {
			slog.Error("Failed to set up multi-tenant config store.", "error", err)
			os.Exit(1)
		}
		cp.configStore = store
		cp.orgRouter = adapter
		cp.apiServer = apiServer
		cp.runtimeTracker = runtimeTracker
		cp.janitorLeader = janitorLeader
		cp.cfg = cfg
		_ = store // keep linter happy
		if cp.runtimeTracker != nil {
			if err := cp.runtimeTracker.Start(context.Background()); err != nil {
				slog.Error("Failed to start control-plane runtime tracker.", "error", err)
				os.Exit(1)
			}
		}
		if cp.janitorLeader != nil {
			if err := cp.janitorLeader.Start(context.Background()); err != nil {
				slog.Error("Failed to start janitor leader election.", "error", err)
				os.Exit(1)
			}
		}
	} else {
		// Single-tenant mode: one shared process pool + session manager
		procPool := NewFlightWorkerPool(cfg.SocketDir, cfg.ConfigPath, processMinWorkers, processMaxWorkers)
		procPool.idleTimeout = cfg.WorkerIdleTimeout
		procPool.retireOnSessionEnd = cfg.RetireOnSessionEnd

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
		// Check if DuckLake migration is needed (fast PG version query, <1s).
		// If so, set the migrate flag immediately so workers use AUTOMATIC_MIGRATION
		// TRUE and skip their own slow backup+check. The backup runs asynchronously
		// as a safety net — it must not block startup or systemd will kill us
		// (TimeoutStartSec=180 is shorter than the backup of large metadata stores).
		if cfg.DuckLake.MetadataStore != "" {
			targetVersion := cfg.DuckLakeDefaultSpecVersion
			if targetVersion == "" {
				targetVersion = ducklake.DefaultSpecVersion
			}
			if needed, ver, err := ducklake.CheckMigrationVersion(cfg.DuckLake, targetVersion); err != nil {
				slog.Warn("DuckLake migration version check failed, workers will check independently.", "error", err)
			} else if needed {
				slog.Info("DuckLake migration needed, workers will use AUTOMATIC_MIGRATION.", "from", ver, "to", targetVersion)
				procPool.ducklakeMigrate = true
				// Run backup asynchronously — it's a safety net, not a gate.
				go func() {
					if err := ducklake.BackupMetadata(cfg.DuckLake, cfg.DataDir); err != nil {
						slog.Warn("DuckLake metadata backup failed (migration will still proceed).", "error", err)
					} else {
						slog.Info("DuckLake metadata backup completed.")
					}
				}()
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

	// Flight ingress is created after worker/session wiring, but the TCP
	// listener itself is pre-bound via tableflip so upgrades inherit the
	// socket instead of racing a re-bind on the old process's 8815 listener.
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
		if cp.runtimeTracker != nil {
			if err := cp.runtimeTracker.MarkDraining(); err != nil {
				slog.Warn("Failed to mark control plane draining.", "error", err)
			}
		}
		if cp.janitorLeader != nil {
			cp.janitorLeader.Stop()
		}
		if isK8s {
			cp.drainAndShutdown(cp.cfg.HandoverDrainTimeout)
		} else {
			cp.shutdown()
		}
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
		"session_init_timeout", cfg.SessionInitTimeout,
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

		netkeepalive.TuneAcceptedConn(conn)

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
	createFn func(context.Context) (int32, *flightclient.FlightExecutor, error),
) (int32, *flightclient.FlightExecutor, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	srv.RegisterQuery(key, cancel)
	defer srv.UnregisterQuery(key)

	return createFn(ctx)
}

func sessionCreationErrorResponse(err error) (code string, message string) {
	var capacityErr *WarmCapacityExhaustedError
	switch {
	case errors.As(err, &capacityErr):
		return "53300", warmCapacityMissPolicyForReason(capacityErr.missReason()).sqlMessage(capacityErr.RetryAfter)
	case errors.Is(err, context.Canceled):
		return "57014", "canceling authentication due to user request"
	case errors.Is(err, context.DeadlineExceeded):
		return "53300", "timed out waiting for an available worker"
	case errors.Is(err, ErrTooManyConnections):
		return "53300", "too many connections"
	default:
		return "58000", fmt.Sprintf("failed to create session: %v", err)
	}
}

// SNI routing modes (values for ControlPlaneConfig.SNIRoutingMode).
const (
	SNIRoutingOff         = "off"         // ignore SNI entirely; identity can no longer be resolved
	SNIRoutingPassthrough = "passthrough" // require managed SNI but warn on legacy hostnames
	SNIRoutingEnforce     = "enforce"     // default: require a managed SNI hostname that resolves to an org
)

type postgresSNIResolution struct {
	sniPrefix     string
	isManaged     bool
	useManagedSNI bool
}

func (cp *ControlPlane) resolvePostgresSNI(mode, sni string) postgresSNIResolution {
	sniPrefix, isManaged := cp.extractOrgFromSNI(sni)
	return postgresSNIResolution{
		sniPrefix:     sniPrefix,
		isManaged:     isManaged,
		useManagedSNI: postgresSNIRoutingModeEnabled(mode) && isManaged,
	}
}

func postgresSNIRoutingModeEnabled(mode string) bool {
	return mode == SNIRoutingPassthrough || mode == SNIRoutingEnforce
}

// managedHostnameHint formats the configured ManagedHostnameSuffixes into a
// "<org-id>.dw.<env>.postwh.com" string suitable for user-facing error
// messages and migration warnings. The leading dot of each suffix is
// preserved so the result is a syntactically valid hostname template.
//
// Examples:
//   - [".dw.dev.postwh.com"]                       → "<org-id>.dw.dev.postwh.com"
//   - [".dw.us.postwh.com", ".dw.eu.postwh.com"]   → "<org-id>.dw.us.postwh.com or <org-id>.dw.eu.postwh.com"
//   - []                                           → "<org-id>.<managed-suffix>" (generic, indicates misconfig)
func (cp *ControlPlane) managedHostnameHint() string {
	suffixes := cp.cfg.ManagedHostnameSuffixes
	if len(suffixes) == 0 {
		return "<org-id>.<managed-suffix>"
	}
	parts := make([]string, len(suffixes))
	for i, s := range suffixes {
		parts[i] = "<org-id>" + s
	}
	return strings.Join(parts, " or ")
}

func (cp *ControlPlane) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()
	slog.Info("Connection accepted.", "remote_addr", remoteAddr)
	server.IncrementOpenConnections()
	defer server.DecrementOpenConnections()

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
		if params.startupMessage {
			// Client sent a v3.0 startup without negotiating SSL (e.g. sslmode=disable).
			// Send a PostgreSQL error so the client sees a clear message.
			_ = server.WriteErrorResponse(conn, "FATAL", "28000", "SSL/TLS connection required. Connect with sslmode=require or higher.")
		}
		slog.Warn("Connection rejected: SSL required.", "remote_addr", remoteAddr)
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

	// Honor a client-supplied connect-time search_path from the startup
	// `options` parameter (libpq `options=-c search_path=...`, PGOPTIONS, or
	// pgjdbc `currentSchema`), so a session can pick its default catalog at
	// connect (e.g. iceberg.public). Sanitized here at the trust boundary;
	// empty/invalid falls back to the worker's default search_path.
	var clientSearchPath string
	if raw := server.ParseStartupOptions(startupParams["options"])["search_path"]; raw != "" {
		if sp, ok := server.SanitizeSearchPath(raw); ok {
			clientSearchPath = sp
		} else {
			slog.Warn("Ignoring unsafe client search_path option.", "remote_addr", remoteAddr, "search_path", raw)
		}
	}

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

	// Authenticate.
	// In multi-tenant mode the org is resolved solely from the managed hostname
	// (SNI); the user is authenticated within that org. The startup `database`
	// param no longer identifies the org — it selects which attached catalog
	// (ducklake/iceberg) the session defaults to.
	var (
		orgID            string
		passthroughUser  bool
		defaultCatalog   string
		requestedCatalog string // "" | "ducklake" | "iceberg" (validated below)
	)
	if cp.configStore != nil {
		sni := tlsConn.ConnectionState().ServerName
		sniResolution := cp.resolvePostgresSNI(cp.cfg.SNIRoutingMode, sni)
		if cp.cfg.SNIRoutingMode != SNIRoutingEnforce && cp.cfg.SNIRoutingMode != SNIRoutingPassthrough {
			// Identity now comes solely from the managed hostname. The legacy
			// database→org routing is gone, so an org cannot be resolved without
			// SNI routing enabled. Warn loudly — this is a misconfiguration.
			slog.Warn("Postgres connection: SNI routing disabled but identity now requires a managed hostname; set sni_routing_mode=enforce.",
				"mode", cp.cfg.SNIRoutingMode, "remote_addr", remoteAddr, "user", username, "application_name", applicationName)
		}
		if !sniResolution.isManaged {
			hint := cp.managedHostnameHint()
			slog.Warn("Postgres connection rejected: SNI does not match a managed hostname.",
				"sni", sni, "expected", hint, "remote_addr", remoteAddr, "user", username, "application_name", applicationName)
			_ = server.WriteErrorResponse(writer, "FATAL", "08006",
				fmt.Sprintf("this server requires connecting via %s", hint))
			_ = writer.Flush()
			return
		}

		resolution := cp.configStore.ResolvePostgresConnection(database, sniResolution.sniPrefix, sniResolution.useManagedSNI, username, password)
		if resolution.SNIResolved {
			observeSNIRoutingResolution("postgres", resolution.SNIAliasUsed)
		}
		if !resolution.SNIResolved {
			slog.Warn("Postgres connection rejected: managed hostname does not resolve to a known organization.",
				"sni", sni, "sni_prefix", sniResolution.sniPrefix, "remote_addr", remoteAddr, "user", username, "application_name", applicationName)
			_ = server.WriteErrorResponse(writer, "FATAL", "08006",
				fmt.Sprintf("this server requires connecting via %s", cp.managedHostnameHint()))
			_ = writer.Flush()
			return
		}
		if !resolution.CatalogValid {
			// The startup `database` is now a catalog selector; only
			// "ducklake"/"iceberg"/empty are valid. No logical-name masking.
			slog.Warn("Postgres connection rejected: requested database is not a selectable catalog.",
				"database", database, "org", resolution.OrgID, "remote_addr", remoteAddr, "user", username)
			_ = server.WriteErrorResponse(writer, "FATAL", "3D000",
				fmt.Sprintf("database %q does not exist (connect with \"ducklake\" or \"iceberg\")", database))
			_ = writer.Flush()
			return
		}
		if !resolution.Valid {
			slog.Warn("Authentication failed.", "user", username, "org", resolution.OrgID, "database", database, "remote_addr", remoteAddr)
			banned := server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
			if banned {
				slog.Warn("IP banned after too many failed auth attempts.", "remote_addr", remoteAddr)
			}
			_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
			_ = writer.Flush()
			return
		}
		orgID = resolution.OrgID
		passthroughUser = resolution.Passthrough
		defaultCatalog = resolution.DefaultCatalog
		requestedCatalog = resolution.EffectiveCatalog
		// `database` is finalized post-session to the real catalog the session
		// defaults to (once worker attachment is known), so logs and the
		// current_database() macro surface the actual catalog.
	} else {
		// Single-tenant: static users map
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
		// Reject connections during DuckLake migration to prevent queries from
		// hitting a partially-migrated catalog. The client gets a clear error
		// and can retry after the migration completes.
		if cp.orgRouter.IsMigratingForOrg(orgID) {
			slog.Info("Connection rejected during DuckLake migration.", "user", username, "org", orgID, "remote_addr", remoteAddr)
			_ = server.WriteErrorResponse(writer, "FATAL", "57P03",
				"DuckLake catalog upgrade in progress for your organization, please retry in a few moments")
			_ = writer.Flush()
			return
		}

		_, sess, rebal, ok := cp.orgRouter.StackForOrg(orgID)
		if !ok {
			// Distinguish "no such org" from "warehouse still provisioning". The
			// org stack only exists for warehouses in state=ready; before that,
			// the stack absence is expected and the client should be told to
			// retry rather than receive a misleading auth-style error.
			whState, orgExists := cp.configStore.OrgWarehouseStatus(orgID)
			switch {
			case !orgExists:
				_ = server.WriteErrorResponse(writer, "FATAL", "28000", "no org configured for user")
			case whState == "" || whState == string(configstore.ManagedWarehouseStateReady):
				// Org exists, warehouse says ready, but stack hasn't been built yet — a
				// transient race the router will resolve. Tell client to retry.
				_ = server.WriteErrorResponse(writer, "FATAL", "57P03",
					"warehouse is starting up, please retry in a few seconds")
			case whState == string(configstore.ManagedWarehouseStateFailed):
				_ = server.WriteErrorResponse(writer, "FATAL", "57P03",
					"warehouse provisioning failed; contact support")
			case whState == string(configstore.ManagedWarehouseStateDeleting) ||
				whState == string(configstore.ManagedWarehouseStateDeleted):
				_ = server.WriteErrorResponse(writer, "FATAL", "57P03",
					"warehouse is being deleted")
			default:
				// pending / provisioning
				_ = server.WriteErrorResponse(writer, "FATAL", "57P03",
					"warehouse is still provisioning, please retry in a few minutes")
			}
			_ = writer.Flush()
			return
		}
		sessions = sess
		rebalancer = rebal
	} else {
		sessions = cp.sessions
		rebalancer = cp.rebalancer
	}
	if cp.isDraining() {
		_ = server.WriteErrorResponse(writer, "FATAL", "57P03", "control plane is draining, retry shortly")
		_ = writer.Flush()
		return
	}

	// Feed initial parameters and backend key data to the client IMMEDIATELY.
	// This keeps JDBC drivers happy while we perform the slow worker acquisition.
	pid := sessions.ReservePID()
	secretKey := server.GenerateSecretKey()

	// Use a temporary clientConn just to send initial params
	tmpCC := server.NewClientConn(cp.srv, nil, nil, writer, username, orgID, database, applicationName, nil, pid, secretKey, -1, "")
	defer server.CancelClientConn(tmpCC)
	server.SendInitialParams(tmpCC)
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush initial params.", "remote_addr", remoteAddr, "error", err)
		return
	}

	// Derive DuckDB resource limits for the session.
	// For remote workers (K8s pods), derive from the worker pod's resource
	// spec — the CP's own resources are irrelevant. For local process workers,
	// use the rebalancer which derives from the CP's system resources.
	var (
		memLimit string
		threads  int
	)
	if cp.isRemoteBackend {
		memLimit, threads = cp.workerDuckDBLimits()
	} else if rebalancer != nil {
		memLimit = rebalancer.MemoryLimit()
		threads = rebalancer.PerSessionThreads()
	}

	_, executor, err := createSessionWithRegisteredCancel(
		cp.srv,
		cp.cfg.WorkerQueueTimeout,
		server.BackendKey{Pid: pid, SecretKey: secretKey},
		func(ctx context.Context) (int32, *flightclient.FlightExecutor, error) {
			return sessions.CreateSession(ctx, username, pid, memLimit, threads)
		},
	)
	if err != nil {
		slog.Error("Failed to create session.", "user", username, "remote_addr", remoteAddr, "error", err)
		code, message := sessionCreationErrorResponse(err)
		_ = server.WriteErrorResponse(writer, "FATAL", code, message)
		_ = writer.Flush()
		return
	}
	// Worker is now assigned — capture identity for log correlation.
	workerID := sessions.WorkerIDForPID(pid)
	workerPod := sessions.WorkerPodNameForPID(pid)
	if orgID != "" {
		observeOrgSessionsActive(orgID, sessions.SessionCount())
	}
	defer func() {
		sessions.DestroySession(pid)
		if orgID != "" {
			observeOrgSessionsActive(orgID, sessions.SessionCount())
		}
	}()

	// Probe which catalogs the worker actually attached for this session, then
	// resolve the real catalog the session defaults to. The startup `database`
	// selected "ducklake"/"iceberg"/"" (default); fail closed (3D000) if the
	// requested catalog isn't attached.
	attachCtx, attachCancel := context.WithTimeout(context.Background(), cp.cfg.SessionInitTimeout)
	duckLakeAttached, dlErr := sessionmeta.HasAttachedCatalog(attachCtx, executor, physicalDuckLakeCatalog)
	icebergAttached, icErr := sessionmeta.HasAttachedCatalog(attachCtx, executor, physicalIcebergCatalog)
	attachCancel()
	probeErr := dlErr
	if probeErr == nil {
		probeErr = icErr
	}
	if probeErr != nil {
		slog.Error("Failed to detect attached catalogs.", "user", username, "org", orgID, "remote_addr", remoteAddr, "error", probeErr, "worker", workerID, "worker_pod", workerPod)
		_ = server.WriteErrorResponse(writer, "FATAL", "XX000", "failed to detect attached catalogs")
		_ = writer.Flush()
		return
	}
	var effectiveCatalog string
	if cp.configStore != nil {
		var ok bool
		effectiveCatalog, ok = resolveEffectiveCatalog(requestedCatalog, defaultCatalog, duckLakeAttached, icebergAttached)
		if !ok {
			slog.Warn("Postgres connection rejected: requested catalog is not available for this connection.",
				"requested", requestedCatalog, "org", orgID, "ducklake_attached", duckLakeAttached, "iceberg_attached", icebergAttached, "remote_addr", remoteAddr, "user", username)
			msg := "no catalog is available for this connection"
			if requestedCatalog != "" {
				msg = fmt.Sprintf("database %q does not exist", requestedCatalog)
			}
			_ = server.WriteErrorResponse(writer, "FATAL", "3D000", msg)
			_ = writer.Flush()
			return
		}
	} else {
		// Single-tenant (process backend / static users): de-mask to the real
		// attached catalog when present; otherwise keep the client's database name
		// (plain DuckDB, no masking concern). No catalog-selection rejection here.
		switch {
		case duckLakeAttached:
			effectiveCatalog = physicalDuckLakeCatalog
		case icebergAttached:
			effectiveCatalog = physicalIcebergCatalog
		default:
			effectiveCatalog = database
		}
	}
	// `database` now reflects the real catalog the session defaults to — this is
	// what drives the current_database() macro/pg_database view and what logs and
	// observability surface.
	database = effectiveCatalog

	// Passthrough users skip pg_catalog initialization and the catalog USE
	// rewriting — they bypass the PG compatibility layer entirely. They still
	// need their selected catalog as the session default, though: without one the
	// worker session stays in DuckDB's empty in-memory catalog (see the
	// passthrough branch below).
	if !passthroughUser {
		initCtx, initCancel := context.WithTimeout(context.Background(), cp.cfg.SessionInitTimeout)
		if err := sessionmeta.InitSessionDatabaseMetadata(initCtx, executor, effectiveCatalog); err != nil {
			initCancel()
			slog.Error("Failed to initialize session database metadata.", "user", username, "org", orgID, "database", database, "remote_addr", remoteAddr, "error", err, "worker", workerID, "worker_pod", workerPod)
			_ = server.WriteErrorResponse(writer, "FATAL", "XX000", "failed to initialize session database metadata")
			_ = writer.Flush()
			return
		}
		initCancel()

		// Apply the effective connect-time session default AFTER metadata init.
		// It must run here, not on the worker at session create:
		// InitSessionDatabaseMetadata's defer resets the catalog/search_path, so an
		// earlier value would be clobbered. A client-supplied search_path is
		// best-effort; the configured catalog (Iceberg) fails closed because
		// silently falling back would route the user to the wrong catalog.
		if cmd, source := effectiveSessionDefaultCommand(clientSearchPath, effectiveCatalog); cmd != "" {
			spCtx, spCancel := context.WithTimeout(context.Background(), cp.cfg.SessionInitTimeout)
			_, err := executor.ExecContext(spCtx, cmd)
			spCancel()
			if err != nil {
				if source == sessionDefaultSourceConfiguredCatalog {
					slog.Error("Failed to apply session default catalog.", "user", username, "org", orgID, "catalog", effectiveCatalog, "error", err)
					_ = server.WriteErrorResponse(writer, "FATAL", "XX000", "failed to apply default catalog")
					_ = writer.Flush()
					return
				}
				slog.Warn("Failed to apply client connect-time search_path; using default.", "user", username, "org", orgID, "search_path", clientSearchPath, "error", err)
			}
		}
	} else {
		// Passthrough: no pg_catalog views and no rewriting, but the session must
		// still land in its selected catalog instead of the empty in-memory one.
		// Standalone passthrough does this via server.setDuckLakeDefault/
		// setIcebergDefault; the remote-worker path issues the equivalent here.
		if clientSearchPath != "" {
			slog.Warn("Ignoring client connect-time search_path for passthrough session.", "user", username, "org", orgID, "search_path", clientSearchPath, "remote_addr", remoteAddr)
		}
		if cmd := passthroughSessionDefaultCatalogCommand(effectiveCatalog); cmd != "" {
			initCtx, initCancel := context.WithTimeout(context.Background(), cp.cfg.SessionInitTimeout)
			_, err := executor.ExecContext(initCtx, cmd)
			initCancel()
			if err != nil {
				slog.Error("Failed to apply passthrough session default catalog.", "user", username, "org", orgID, "command", cmd, "error", err, "worker", workerID, "worker_pod", workerPod)
				_ = server.WriteErrorResponse(writer, "FATAL", "XX000", "failed to apply default catalog")
				_ = writer.Flush()
				return
			}
		}
	}

	// Register the TCP connection so OnWorkerCrash can close it to unblock
	// the message loop if the backing worker dies.
	sessions.SetConnCloser(pid, tlsConn)

	// Create real clientConn with FlightExecutor and worker assignment
	cc := server.NewClientConn(cp.srv, tlsConn, reader, writer, username, orgID, database, applicationName, executor, pid, secretKey, workerID, workerPod)
	if cp.orgRouter != nil && orgID != "" {
		if icebergCfg, ok := cp.orgRouter.IcebergConfigForOrg(orgID); ok {
			server.SetConnectionIcebergConfig(cc, icebergCfg)
		}
	}
	// Catalog USE rewriting (expanding bare `USE ducklake`/`USE iceberg` to the
	// reliable two-part target) is a non-passthrough feature; passthrough sessions
	// talk raw DuckDB, so keep it disabled for them. Enabled whenever either
	// catalog is attached.
	server.SetCatalogUseRewrite(cc, (duckLakeAttached || icebergAttached) && !passthroughUser)
	server.SetPassthrough(cc, passthroughUser)
	if orgID != "" {
		observeOrgPgSessionAccepted(orgID, passthroughUser)
	}

	// Send ReadyForQuery to signal that the handshake is complete
	if err := server.WriteReadyForQuery(writer, 'I'); err != nil {
		slog.Error("Failed to send ReadyForQuery.", "remote_addr", remoteAddr, "error", err, "worker", workerID, "worker_pod", workerPod)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush writer.", "remote_addr", remoteAddr, "error", err, "worker", workerID, "worker_pod", workerPod)
		return
	}

	// Run message loop
	if err := server.RunMessageLoop(cc); err != nil {
		slog.Error("Message loop error.", "user", username, "remote_addr", remoteAddr, "error", err, "worker", workerID, "worker_pod", workerPod)
		return
	}

	slog.Info("Client disconnected.", "user", username, "remote_addr", remoteAddr, "worker", workerID, "worker_pod", workerPod)
}

// workerDuckDBLimits derives DuckDB memory_limit and threads from the worker
// pod's K8s resource spec. Uses 75% of the worker's memory limit for DuckDB
// and the full CPU request as thread count. Returns empty/zero if worker
// resources are not configured (DuckDB will then auto-detect on the worker).
func (cp *ControlPlane) workerDuckDBLimits() (memLimit string, threads int) {
	memReq := cp.cfg.K8s.WorkerMemoryRequest
	if memReq != "" {
		memBytes := parseK8sMemory(memReq)
		if memBytes > 0 {
			duckdbBytes := memBytes * 3 / 4 // 75% of worker memory for DuckDB
			const gb = 1024 * 1024 * 1024
			const mb = 1024 * 1024
			if duckdbBytes >= gb {
				memLimit = fmt.Sprintf("%dGB", duckdbBytes/gb)
			} else {
				memLimit = fmt.Sprintf("%dMB", duckdbBytes/mb)
			}
		}
	}

	cpuReq := cp.cfg.K8s.WorkerCPURequest
	if cpuReq != "" {
		threads = parseK8sCPU(cpuReq)
	}

	return memLimit, threads
}

// parseK8sMemory parses a Kubernetes memory string (e.g., "360Gi", "8Gi", "512Mi", "4GB")
// into bytes. Supports both IEC (Ki/Mi/Gi/Ti) and SI (KB/MB/GB/TB) units.
func parseK8sMemory(s string) uint64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	// Try k8s IEC notation first (Ki, Mi, Gi, Ti)
	units := []struct {
		suffix     string
		multiplier uint64
	}{
		{"Ti", 1024 * 1024 * 1024 * 1024},
		{"Gi", 1024 * 1024 * 1024},
		{"Mi", 1024 * 1024},
		{"Ki", 1024},
	}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			var v float64
			if _, err := fmt.Sscanf(strings.TrimSuffix(s, u.suffix), "%f", &v); err == nil && v > 0 {
				return uint64(v * float64(u.multiplier))
			}
			return 0
		}
	}

	// Fall back to DuckDB/SI notation (KB, MB, GB, TB)
	return server.ParseMemoryBytes(s)
}

// parseK8sCPU parses a Kubernetes CPU string (e.g., "46", "46000m", "500m")
// into a whole thread count. Millicores below 1000 round down to 0.
func parseK8sCPU(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if strings.HasSuffix(s, "m") {
		var millicores int
		if _, err := fmt.Sscanf(strings.TrimSuffix(s, "m"), "%d", &millicores); err != nil {
			return 0
		}
		return millicores / 1000
	}
	var cores int
	if _, err := fmt.Sscanf(s, "%d", &cores); err != nil {
		return 0
	}
	return cores
}

// startupResult holds the parsed initial startup message.
type startupResult struct {
	sslRequest      bool
	gssRequest      bool
	startupMessage  bool // true when the client sent a v3.0 startup (no SSL negotiation)
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

		// Protocol version 3.0 — client sent a startup message without
		// negotiating SSL first (e.g. sslmode=disable).
		if protocolVersion == 196608 {
			return startupResult{startupMessage: true}, nil
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
	cp.stopAcceptingPGConnections()
	if cp.flight != nil {
		cp.flight.Shutdown()
		cp.flight = nil
	}
	if cp.janitorLeader != nil {
		cp.janitorLeader.Stop()
	}

	// Wait for in-flight connections to finish
	slog.Info("Waiting for connections to drain...")
	cp.wg.Wait()

	cp.shutdownRuntimeResources()
}

func (cp *ControlPlane) drainAndShutdown(timeout time.Duration) {
	// Stop spawning warm workers immediately so we don't create pods that
	// outlive this CP instance and block scheduling for the replacement.
	if cp.orgRouter != nil {
		cp.orgRouter.SetWarmCapacityTarget(0)
	}
	cp.stopAcceptingPGConnections()
	if cp.flight != nil {
		cp.flight.BeginDrain()
	}
	if timeout > 0 {
		slog.Info("Waiting for planned shutdown drain.", "timeout", timeout)
	} else {
		slog.Info("Waiting for planned shutdown drain (unbounded — k8s SIGKILL is the wall).")
	}
	if cp.waitForDrain(timeout) {
		slog.Info("All pgwire connections and Flight sessions drained before shutdown.")
	} else {
		slog.Warn("Planned shutdown drain timeout exceeded, forcing shutdown.", "timeout", timeout)
	}
	if cp.flight != nil {
		cp.flight.Shutdown()
		cp.flight = nil
	}
	cp.shutdownRuntimeResources()
}

func (cp *ControlPlane) stopAcceptingPGConnections() {
	cp.closeMu.Lock()
	cp.closed = true
	cp.closeMu.Unlock()

	if cp.pgListener != nil {
		_ = cp.pgListener.Close()
	}
}

// waitForDrain blocks until both the pgwire and Flight server report
// zero in-flight work, or the timeout fires. timeout == 0 means
// unbounded — k8s terminationGracePeriodSeconds becomes the only wall.
// Returns true on clean drain, false on timeout.
func (cp *ControlPlane) waitForDrain(timeout time.Duration) bool {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	pgDone := make(chan struct{})
	go func() {
		cp.wg.Wait()
		close(pgDone)
	}()

	flightDone := make(chan bool, 1)
	go func() {
		if cp.flight != nil {
			flightDone <- cp.flight.WaitForZeroSessions(ctx)
			return
		}
		flightDone <- true
	}()

	pgClosed := false
	flightClosed := false
	for !pgClosed || !flightClosed {
		select {
		case <-ctx.Done():
			return false
		case <-pgDone:
			pgClosed = true
		case drained := <-flightDone:
			if !drained {
				return false
			}
			flightClosed = true
		}
	}
	return true
}

func (cp *ControlPlane) shutdownRuntimeResources() {
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

func (cp *ControlPlane) isDraining() bool {
	return cp.runtimeTracker != nil && cp.runtimeTracker.Draining()
}

func (cp *ControlPlane) healthReady() bool {
	if cp == nil {
		return false
	}
	if cp.isDraining() {
		return false
	}
	if cp.cfg.FlightPort <= 0 {
		return true
	}
	return cp.flight != nil && cp.flight.Healthy()
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
	if cp.apiServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := cp.apiServer.Shutdown(ctx); err != nil {
			slog.Warn("API server shutdown failed.", "error", err)
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
// cpFlightCredentialValidator authenticates Flight SQL clients in
// multi-tenant mode. Identity is derived solely from the managed hostname
// (SNI): the org is resolved from the SNI prefix and the user is authenticated
// within that org. Flight has no `database` param, so there is no catalog
// selection here — the per-user default catalog applies.
type cpFlightCredentialValidator struct {
	cp *ControlPlane
}

func (v *cpFlightCredentialValidator) ValidateCredentials(username, password string) bool {
	return v.ValidateCredentialsForSNI("", username, password)
}

// ValidateCredentialsForSNI authenticates (username, password) against the org
// the connection's managed hostname (SNI) resolves to. It does NOT stash the
// resolved org anywhere keyed by username — session routing re-derives the org
// from the same SNI at create time (orgRoutedSessionProvider.resolveOrg), so the
// authenticated principal stays bound to this connection's hostname rather than
// a shared username→org map that two tenants could collide on.
func (v *cpFlightCredentialValidator) ValidateCredentialsForSNI(sni, username, password string) bool {
	cp := v.cp
	sniPrefix, isManaged := cp.extractOrgFromSNI(sni)
	if !isManaged {
		// A username alone can collide across orgs, so identity requires a
		// managed hostname — there is no username-scan fallback.
		slog.Warn("Flight auth rejected: SNI does not match a managed hostname.",
			"sni", sni, "expected", cp.managedHostnameHint(), "user", username)
		return false
	}
	orgID, dbname := cp.configStore.ResolveSNIPrefix(sniPrefix)
	if orgID == "" {
		slog.Warn("Flight client SNI references unknown org.",
			"sni", sni, "sni_prefix", sniPrefix, "user", username)
		return false
	}
	observeSNIRoutingResolution("flight", dbname != sniPrefix)
	return cp.configStore.ValidateOrgUser(orgID, username, password)
}

// flightOrgFromContext resolves the org for a Flight session from the request
// context's SNI (the managed hostname). Used by orgRoutedSessionProvider to bind
// each session to its connection's org, mirroring the auth-time resolution.
func (cp *ControlPlane) flightOrgFromContext(ctx context.Context) (string, bool) {
	return cp.resolveFlightOrgFromSNI(flightsqlingress.SNIFromContext(ctx))
}

// resolveFlightOrgFromSNI maps a TLS ServerName to its org, returning ok=false
// for unmanaged hostnames or prefixes that resolve to no org.
func (cp *ControlPlane) resolveFlightOrgFromSNI(sni string) (orgID string, ok bool) {
	prefix, isManaged := cp.extractOrgFromSNI(sni)
	if !isManaged {
		return "", false
	}
	orgID, _ = cp.configStore.ResolveSNIPrefix(prefix)
	return orgID, orgID != ""
}

func (cp *ControlPlane) startFlightIngress() {
	if cp.cfg.FlightPort <= 0 {
		return
	}

	var validator flightsqlingress.CredentialValidator
	var provider flightsqlingress.SessionProvider

	switch {
	case cp.configStore != nil && cp.orgRouter != nil:
		// Multi-tenant: auth via config store, sessions routed per-org. The
		// managed hostname (SNI) is authoritative for org identity at both auth
		// and session-create time; there is no username-keyed routing state.
		orgProvider := &orgRoutedSessionProvider{
			orgRouter:   cp.orgRouter,
			configStore: cp.configStore,
			pidSession:  make(map[int32]flightOwnedSession),
			resolveOrg:  cp.flightOrgFromContext,
		}
		validator = &cpFlightCredentialValidator{cp: cp}
		provider = orgProvider
	case cp.sessions != nil:
		// Single-tenant: static users map, single session manager.
		validator = &flightsqlingress.MapCredentialValidator{Users: cp.cfg.Users}
		provider = &flightSessionProvider{sm: cp.sessions}
	default:
		slog.Warn("Flight ingress disabled: no session manager or config store available.")
		return
	}

	flightCfg := FlightIngressConfig{
		SessionIdleTTL:     cp.cfg.FlightSessionIdleTTL,
		SessionReapTick:    cp.cfg.FlightSessionReapInterval,
		HandleIdleTTL:      cp.cfg.FlightHandleIdleTTL,
		SessionTokenTTL:    cp.cfg.FlightSessionTokenTTL,
		WorkerQueueTimeout: cp.cfg.WorkerQueueTimeout,
	}

	var (
		flightIngress *FlightIngress
		err           error
	)
	if cp.flightListener != nil {
		flightIngress, err = NewFlightIngressFromListener(cp.flightListener, cp.tlsConfig, validator, provider, cp.rateLimiter, flightCfg)
	} else {
		flightIngress, err = NewFlightIngress(cp.cfg.Host, cp.cfg.FlightPort, cp.tlsConfig, validator, provider, cp.rateLimiter, flightCfg)
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
