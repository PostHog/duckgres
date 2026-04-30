package server

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx" driver for direct PostgreSQL connections
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// processStartTime is captured at process init, used to distinguish server vs child process uptime.
var processStartTime = time.Now()

// processVersion is set from main() via SetProcessVersion. Defaults to "dev".
var processVersion = "dev"

// startupReadTimeout bounds pre-TLS startup negotiation reads to avoid stalled
// clients pinning connection goroutines indefinitely.
var startupReadTimeout = 30 * time.Second

var bundledDuckDBExtensionsDir = "/app/extensions"

var bundledExtensionBootstrap struct {
	mu     sync.Mutex
	byPath map[string]error
}

// SetProcessVersion sets the version string for this process. Called from main().
func SetProcessVersion(v string) { processVersion = v }

// ProcessVersion returns the version string for this process.
func ProcessVersion() string { return processVersion }

func bootstrapBundledExtensions(dataDir string) error {
	extDir := filepath.Join(dataDir, "extensions")

	bundledExtensionBootstrap.mu.Lock()
	defer bundledExtensionBootstrap.mu.Unlock()
	if bundledExtensionBootstrap.byPath == nil {
		bundledExtensionBootstrap.byPath = make(map[string]error)
	}
	if err, ok := bundledExtensionBootstrap.byPath[extDir]; ok {
		return err
	}

	err := seedBundledExtensions(bundledDuckDBExtensionsDir, extDir)
	bundledExtensionBootstrap.byPath[extDir] = err
	return err
}

// BootstrapBundledExtensions eagerly seeds bundled extension binaries into the
// configured extension_directory cache once per data directory.
func BootstrapBundledExtensions(dataDir string) error {
	return bootstrapBundledExtensions(dataDir)
}

func setExtensionDirectory(db *sql.DB, dataDir string) error {
	extDir := filepath.Join(dataDir, "extensions")
	if _, err := db.Exec(fmt.Sprintf("SET extension_directory = '%s'", extDir)); err != nil {
		return fmt.Errorf("set extension_directory %s: %w", extDir, err)
	}

	return nil
}

// passwordPattern matches password=<value> or password: <value> with quoted or unquoted values.
var passwordPattern = regexp.MustCompile(`(?i)(password\s*[=:]\s*)("[^"]*"|[^\s"]+)`)

var connectionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_connections_open",
	Help: "Number of currently open client connections",
})

// IncrementOpenConnections increments the open connections gauge.
// Used by the control plane which handles connections separately from the standalone server.
func IncrementOpenConnections() { connectionsGauge.Inc() }

// DecrementOpenConnections decrements the open connections gauge.
func DecrementOpenConnections() { connectionsGauge.Dec() }

var queryDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_query_duration_seconds",
	Help:    "Query execution duration in seconds",
	Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 18000, 36000},
}, []string{"org"})

var queryErrorsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_query_errors_total",
	Help: "Total number of failed queries",
}, []string{"org"})

var authFailuresCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_auth_failures_total",
	Help: "Total number of authentication failures",
})

var rateLimitRejectsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_rate_limit_rejects_total",
	Help: "Total number of connections rejected due to rate limiting",
})

var rateLimitedIPsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_rate_limited_ips",
	Help: "Number of currently rate-limited IP addresses",
})

var queryCancellationsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_query_cancellations_total",
	Help: "Total number of queries cancelled via cancel request",
})

var ducklakeConflictTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_ducklake_conflict_total",
	Help: "Total number of DuckLake transaction conflicts encountered",
})

var ducklakeConflictRetriesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_ducklake_conflict_retries_total",
	Help: "Total number of DuckLake transaction conflict retry attempts",
})

var ducklakeConflictRetrySuccessesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_ducklake_conflict_retry_successes_total",
	Help: "Total number of DuckLake transaction conflict retries that succeeded",
})

var ducklakeConflictRetriesExhaustedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_ducklake_conflict_retries_exhausted_total",
	Help: "Total number of DuckLake transaction conflicts where all retries were exhausted",
})

var s3BytesReadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_s3_bytes_read_total",
	Help: "Total bytes read from S3 by DuckDB",
}, []string{"org"})

var scanWallSecondsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_scan_wall_seconds",
	Help:    "Estimated wall-clock scan time per query",
	Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
}, []string{"org"})

var scanRowsPerSecondHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "duckgres_scan_rows_per_second",
	Help: "Scan throughput: estimated wall-clock rows scanned per second. High values (>1e10) indicate buffer pool/cache hits.",
	// Range spans S3 cold reads (1e5-1e8) through in-memory cache hits (1e9-1e12).
	Buckets: []float64{1e5, 5e5, 1e6, 5e6, 1e7, 5e7, 1e8, 5e8, 1e9, 1e10, 1e11, 1e12},
}, []string{"org"})

// BackendKey uniquely identifies a backend connection for cancel requests
type BackendKey struct {
	Pid       int32
	SecretKey int32
}

// RedactSecrets replaces password=<value> (and password: <value>) patterns with
// password=[REDACTED] for safe logging and error reporting. It handles both quoted
// and unquoted values. It does not currently redact other secret types (tokens, keys).
func RedactSecrets(s string) string {
	return passwordPattern.ReplaceAllString(s, "${1}[REDACTED]")
}

func redactConnectionString(connStr string) string {
	return RedactSecrets(connStr)
}

type Config struct {
	Host string
	Port int
	// FlightPort enables Arrow Flight SQL ingress on the control plane.
	// 0 disables Flight ingress.
	FlightPort int

	// FlightSessionIdleTTL controls how long an idle Flight auth session is kept
	// before being reaped.
	FlightSessionIdleTTL time.Duration

	// FlightSessionReapInterval controls how frequently idle Flight auth sessions
	// are scanned and reaped.
	FlightSessionReapInterval time.Duration

	// FlightHandleIdleTTL controls stale prepared/query handle cleanup inside a
	// Flight auth session.
	FlightHandleIdleTTL time.Duration

	// FlightSessionTokenTTL controls the absolute lifetime of issued
	// x-duckgres-session tokens. Expired tokens are rejected and require
	// a fresh bootstrap request.
	FlightSessionTokenTTL time.Duration
	DataDir               string
	Users                 map[string]string // username -> password

	// TLS configuration (required unless ACME is configured)
	TLSCertFile string // Path to TLS certificate file
	TLSKeyFile  string // Path to TLS private key file

	// ACME/Let's Encrypt configuration (alternative to static TLS cert/key)
	ACMEDomain   string // Domain for ACME certificate (e.g., "decisive-mongoose-wine.us.duckgres.com")
	ACMEEmail    string // Contact email for Let's Encrypt notifications
	ACMECacheDir string // Directory for cached certificates (default: "./certs/acme")

	// ACME DNS-01 challenge configuration (for private/internal interfaces)
	// When ACMEDNSProvider is set, DNS-01 challenges are used instead of HTTP-01.
	// This allows certificate issuance for hosts without public port 80 access.
	ACMEDNSProvider string // DNS provider for ACME DNS-01 challenges (currently only "route53")
	ACMEDNSZoneID   string // Route53 hosted zone ID for DNS-01 challenges

	// Rate limiting configuration
	RateLimit RateLimitConfig

	// Extensions to load on database initialization
	Extensions []string

	// DuckLake configuration
	DuckLake DuckLakeConfig

	// Graceful shutdown timeout (default: 30s)
	ShutdownTimeout time.Duration

	// IdleTimeout is the maximum time a connection can be idle before being closed.
	// This prevents accumulation of zombie connections from clients that disconnect
	// uncleanly. Default: 24 hours. Set to a negative value (e.g., -1) to disable.
	IdleTimeout time.Duration

	// FilePersistence stores DuckDB data in <DataDir>/<username>.duckdb instead of :memory:.
	// DuckDB memory-maps the file and serves queries from RAM, so performance is similar
	// to in-memory mode while data persists across connections and restarts.
	FilePersistence bool

	// ProcessIsolation enables spawning each client connection in a separate OS process.
	// This prevents DuckDB C++ crashes from taking down the entire server.
	// When enabled, rate limiting and cancel requests are handled by the parent process,
	// while TLS, authentication, and query execution happen in child processes.
	ProcessIsolation bool

	// MemoryLimit is the DuckDB memory_limit per session (e.g., "4GB").
	// If empty, auto-detected from system memory.
	MemoryLimit string

	// Threads is the DuckDB threads per session.
	// If zero, defaults to runtime.NumCPU().
	Threads int

	// MemoryBudget is the total memory available for all DuckDB sessions (e.g., "24GB").
	// Used in control-plane mode for dynamic per-session memory allocation.
	// If empty, defaults to 75% of system RAM.
	MemoryBudget string

	// MemoryRebalance enables dynamic per-connection memory reallocation in control-plane mode.
	// When enabled, the memory budget is redistributed across all active sessions on every
	// connect/disconnect. When disabled (default), each session gets a static allocation
	// of budget/max_workers at creation time.
	MemoryRebalance bool

	// PassthroughUsers are users that bypass the SQL transpiler and pg_catalog initialization.
	// Queries from these users go directly to DuckDB without any PostgreSQL compatibility layer.
	PassthroughUsers map[string]bool

	// QueryLog configures the DuckLake query log (system.query_log table).
	QueryLog QueryLogConfig
}

// QueryLogConfig configures the query log feature.
type QueryLogConfig struct {
	Enabled              bool
	FlushInterval        time.Duration
	BatchSize            int
	CompactInterval      time.Duration
	DataInliningRowLimit int
}

// DuckLakeConfig configures DuckLake catalog attachment
type DuckLakeConfig struct {
	// MetadataStore is the connection string for the DuckLake metadata database
	// Format: "postgres:host=<host> user=<user> password=<password> dbname=<db>"
	MetadataStore string

	// DisableMetadataThreadLocalCache disables postgres_scanner thread-local
	// connection caching for the hidden DuckLake metadata pool as early as
	// possible, before ATTACH creates that pool. This trades some warm-reuse
	// performance for a lower retained metadata-connection footprint.
	// Nil means use the server default (enabled).
	DisableMetadataThreadLocalCache *bool

	// ObjectStore is the S3-compatible storage path for DuckLake data files
	// Format: "s3://bucket/path/" for S3/MinIO
	// If not specified, uses DataPath for local storage
	ObjectStore string

	// DataPath is the local file system path for DuckLake data files
	// Used when ObjectStore is not set (for local/non-S3 storage)
	DataPath string

	// DeltaCatalogEnabled attaches the DuckDB Delta extension catalog at worker
	// startup/activation in addition to DuckLake. DeltaCatalogPath defaults to a
	// sibling delta/ prefix at the DuckLake object-store root when omitted.
	DeltaCatalogEnabled bool
	DeltaCatalogPath    string

	// S3 credential provider: "config" (explicit credentials) or "credential_chain" (AWS SDK chain)
	// Default: "config" if S3AccessKey is set, otherwise "credential_chain"
	S3Provider string

	// S3 configuration for "config" provider (explicit credentials for MinIO or S3)
	S3Endpoint     string // e.g., "localhost:9000" for MinIO
	S3AccessKey    string // S3 access key ID
	S3SecretKey    string // S3 secret access key
	S3SessionToken string // STS session token for temporary credentials
	S3Region       string // S3 region (default: us-east-1)
	S3UseSSL       bool   // Use HTTPS for S3 connections (default: false for MinIO)
	S3URLStyle     string // "path" or "vhost" (default: "path" for MinIO compatibility)

	// S3 configuration for "credential_chain" provider (AWS SDK credential chain)
	// Chain specifies which credential sources to check, semicolon-separated
	// Options: env, config, sts, sso, instance, process
	// Default: checks all sources in AWS SDK order
	S3Chain   string // e.g., "env;config" to check env vars then config files
	S3Profile string // AWS profile name to use (for "config" chain)

	// HTTPProxy routes DuckDB httpfs traffic through a forward HTTP proxy.
	// When set, DuckDB signs S3 requests for the real S3 hostname and sends them
	// through the proxy as plain HTTP (requires S3UseSSL=false). Used by the
	// local cache proxy DaemonSet for NVMe caching.
	HTTPProxy string

	// CheckpointInterval controls how often DuckLake CHECKPOINT runs.
	// CHECKPOINT performs full catalog maintenance: expire snapshots,
	// merge adjacent files, rewrite data files, and clean up orphaned files.
	// Set to 0 to disable. Default: 24h.
	CheckpointInterval time.Duration

	// DataInliningRowLimit controls the maximum number of rows to inline
	// in DuckLake metadata instead of writing to Parquet files.
	// Default: 0 (disabled). Set to a positive value to enable inlining.
	DataInliningRowLimit *int

	// Migrate is set by the control plane after running the migration check.
	// When true, AttachDuckLake uses AUTOMATIC_MIGRATION TRUE without
	// re-running the version check. This avoids redundant backups and
	// long-running checks in worker processes.
	Migrate bool `json:"migrate,omitempty" yaml:"-"`

	// SpecVersion is the target DuckLake spec version for this connection.
	// When empty, the worker uses its own built-in default.
	SpecVersion string `json:"spec_version,omitempty" yaml:"-"`

	// ViaPgBouncer is set by the control plane when the DuckLake metadata
	// connection is routed through a network-level pooler (e.g. PgBouncer)
	// rather than direct to Postgres. When true, the worker disables the
	// postgres_scanner in-process pool via `SET GLOBAL pg_pool_max_connections = 0`.
	// See duckdb/ducklake#1031: behind a network pooler, client-side pooling
	// is redundant and prevents the pooler from reclaiming idle connections.
	ViaPgBouncer bool `json:"via_pgbouncer,omitempty" yaml:"-"`
}

// fileDBEntry tracks a shared *sql.DB for file-persistence mode.
// One entry per user file; multiple PG connections share the pool via pinned *sql.Conn.
type fileDBEntry struct {
	db          *sql.DB
	refs        int
	stopRefresh func() // credential refresh goroutine
}

type Server struct {
	cfg         Config
	listener    net.Listener
	tlsConfig   *tls.Config
	rateLimiter *RateLimiter
	wg          sync.WaitGroup
	closed      bool
	closeMu     sync.Mutex
	activeConns int64 // atomic counter for active connections

	// duckLakeSem serializes DuckLake attachment to avoid write-write conflicts.
	// Using a channel instead of mutex allows for timeout on acquisition.
	duckLakeSem chan struct{}

	// Query cancellation tracking (used in non-isolated mode)
	activeQueries   map[BackendKey]context.CancelFunc
	activeQueriesMu sync.RWMutex

	// Child process tracking (used when ProcessIsolation is enabled)
	childTracker *ChildTracker

	// External query cancel channel (used in child worker processes)
	// When this channel is closed, all active queries should be cancelled.
	// This is used to propagate SIGUSR1 from signal handler to query execution.
	externalCancelCh <-chan struct{}

	// ACME manager for Let's Encrypt certificates (nil when using static certs)
	acmeManager    *ACMEManager
	acmeDNSManager *ACMEDNSManager

	// Connection registry for pg_stat_activity
	connsMu sync.RWMutex
	conns   map[int32]*clientConn

	// Query logger for DuckLake system.query_log
	queryLogger *QueryLogger

	// Per-user shared DB pool for file persistence mode.
	// Each user gets one *sql.DB; PG connections share it via pinned *sql.Conn.
	fileDBsMu sync.Mutex
	fileDBs   map[string]*fileDBEntry

	// DuckLake checkpoint scheduler
	checkpointer *DuckLakeCheckpointer

	// Progress lookup function for pg_stat_activity.
	// In control plane mode, returns cached progress from worker health checks.
	// Nil in standalone mode.
	progressFn func(pid int32) (pct float64, rows, totalRows uint64, stalled bool)
}

func New(cfg Config) (*Server, error) {
	// Apply default rate limit config for any unset fields
	defaults := DefaultRateLimitConfig()
	if cfg.RateLimit.MaxFailedAttempts == 0 {
		cfg.RateLimit.MaxFailedAttempts = defaults.MaxFailedAttempts
	}
	if cfg.RateLimit.FailedAttemptWindow == 0 {
		cfg.RateLimit.FailedAttemptWindow = defaults.FailedAttemptWindow
	}
	if cfg.RateLimit.BanDuration == 0 {
		cfg.RateLimit.BanDuration = defaults.BanDuration
	}
	if cfg.RateLimit.MaxConnectionsPerIP == 0 {
		cfg.RateLimit.MaxConnectionsPerIP = defaults.MaxConnectionsPerIP
	}
	if cfg.RateLimit.MaxConnections == 0 {
		cfg.RateLimit.MaxConnections = defaults.MaxConnections
	}

	// Use default shutdown timeout if not specified
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}

	// Use default idle timeout if not specified (24 hours)
	// Negative value means explicitly disabled (set to 0)
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 24 * time.Hour
	} else if cfg.IdleTimeout < 0 {
		cfg.IdleTimeout = 0
	}

	if cfg.ACMEDNSProvider != "" && cfg.ACMEDomain == "" {
		return nil, errors.New("ACME DNS provider requires ACME domain")
	}
	if cfg.ACMEDNSProvider != "" && cfg.ACMEDNSProvider != "route53" {
		return nil, fmt.Errorf("unsupported ACME DNS provider %q (only \"route53\" is supported)", cfg.ACMEDNSProvider)
	}

	s := &Server{
		cfg:           cfg,
		rateLimiter:   NewRateLimiter(cfg.RateLimit),
		activeQueries: make(map[BackendKey]context.CancelFunc),
		duckLakeSem:   make(chan struct{}, 1),
		conns:         make(map[int32]*clientConn),
		fileDBs:       make(map[string]*fileDBEntry),
	}

	// Configure TLS: ACME DNS-01, ACME HTTP-01, or static certificate files
	if cfg.ACMEDomain != "" && cfg.ACMEDNSProvider != "" {
		// DNS-01 challenge mode (for private/internal interfaces)
		mgr, err := NewACMEDNSManager(cfg.ACMEDomain, cfg.ACMEEmail, cfg.ACMEDNSZoneID, cfg.ACMECacheDir)
		if err != nil {
			return nil, fmt.Errorf("failed to start ACME DNS manager: %w", err)
		}
		s.acmeDNSManager = mgr
		s.tlsConfig = mgr.TLSConfig()
		slog.Info("TLS enabled via ACME DNS-01.", "domain", cfg.ACMEDomain, "provider", cfg.ACMEDNSProvider)
	} else if cfg.ACMEDomain != "" {
		// HTTP-01 challenge mode (requires port 80)
		mgr, err := NewACMEManager(cfg.ACMEDomain, cfg.ACMEEmail, cfg.ACMECacheDir, ":80")
		if err != nil {
			return nil, fmt.Errorf("failed to start ACME manager: %w", err)
		}
		s.acmeManager = mgr
		s.tlsConfig = mgr.TLSConfig()
		slog.Info("TLS enabled via ACME/Let's Encrypt.", "domain", cfg.ACMEDomain)
	} else {
		// Static certificate files
		if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
			return nil, fmt.Errorf("TLS certificate and key are required (or configure --acme-domain)")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificates: %w", err)
		}
		s.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		slog.Info("TLS enabled.", "cert_file", cfg.TLSCertFile)
	}

	// Initialize child tracker if process isolation is enabled
	if cfg.ProcessIsolation {
		s.childTracker = NewChildTracker()
		slog.Info("Process isolation enabled. Each connection will spawn a child process.")
	}

	slog.Info("Rate limiting enabled.", "max_failed_attempts", cfg.RateLimit.MaxFailedAttempts, "window", cfg.RateLimit.FailedAttemptWindow, "ban_duration", cfg.RateLimit.BanDuration)
	if cfg.IdleTimeout > 0 {
		slog.Info("Idle timeout enabled.", "timeout", cfg.IdleTimeout)
	} else {
		slog.Info("Idle timeout disabled.")
	}

	// Run DuckLake migration check before initializing query logger and checkpointer,
	// since they both attach DuckLake and need to know if migration is required.
	if cfg.DuckLake.MetadataStore != "" {
		ensureDuckLakeMigrationCheck(cfg.DuckLake, cfg.DataDir)
	}

	if err := bootstrapBundledExtensions(cfg.DataDir); err != nil {
		return nil, fmt.Errorf("failed to bootstrap bundled DuckDB extensions: %w", err)
	}

	// Initialize query logger (non-fatal on error)
	if ql, err := NewQueryLogger(cfg); err != nil {
		slog.Warn("Failed to initialize query log, continuing without it.", "error", err)
	} else if ql != nil {
		s.queryLogger = ql
	}

	// Initialize DuckLake checkpoint scheduler (non-fatal on error)
	if cp, err := NewDuckLakeCheckpointer(cfg); err != nil {
		slog.Warn("Failed to initialize DuckLake checkpoint scheduler, continuing without it.", "error", err)
	} else if cp != nil {
		s.checkpointer = cp
	}

	return s, nil
}

func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.closeMu.Lock()
			closed := s.closed
			s.closeMu.Unlock()
			if closed {
				return nil
			}
			slog.Error("Accept error.", "error", err)
			continue
		}

		// Enable TCP keepalive to detect dead connections
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

func (s *Server) Close() error {
	s.closeMu.Lock()
	s.closed = true
	s.closeMu.Unlock()

	// Stop accepting new connections
	if s.listener != nil {
		_ = s.listener.Close()
	}

	// Check if there are active connections
	activeConns := atomic.LoadInt64(&s.activeConns)
	if activeConns > 0 {
		slog.Info("Waiting for active connections to finish.", "count", activeConns)
	}

	// If process isolation is enabled, signal children to terminate
	if s.cfg.ProcessIsolation && s.childTracker != nil {
		childCount := s.childTracker.Count()
		if childCount > 0 {
			slog.Info("Signaling child processes to terminate.", "count", childCount)
			s.childTracker.SignalAll(syscall.SIGTERM)
		}
	}

	// Wait for connections with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		// Also wait for child processes if isolation is enabled
		if s.cfg.ProcessIsolation && s.childTracker != nil {
			<-s.childTracker.WaitAll()
		}
		close(done)
	}()

	select {
	case <-done:
		slog.Info("All connections closed gracefully.")
	case <-time.After(s.cfg.ShutdownTimeout):
		slog.Warn("Shutdown timeout exceeded, force closing remaining connections.", "timeout", s.cfg.ShutdownTimeout)
		// Force kill remaining children
		if s.cfg.ProcessIsolation && s.childTracker != nil {
			s.childTracker.SignalAll(syscall.SIGKILL)
		}
	}

	// Shut down ACME managers if active
	if s.acmeManager != nil {
		if err := s.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown error.", "error", err)
		}
	}
	if s.acmeDNSManager != nil {
		if err := s.acmeDNSManager.Close(); err != nil {
			slog.Warn("ACME DNS manager shutdown error.", "error", err)
		}
	}

	// Stop query logger (drains remaining entries)
	if s.queryLogger != nil {
		s.queryLogger.Stop()
	}

	// Stop DuckLake checkpoint scheduler
	if s.checkpointer != nil {
		s.checkpointer.Stop()
	}

	// Database connections are now closed by each clientConn when it terminates
	slog.Info("Shutdown complete.")
	return nil
}

// Shutdown performs a graceful shutdown with the given context
func (s *Server) Shutdown(ctx context.Context) error {
	s.closeMu.Lock()
	s.closed = true
	s.closeMu.Unlock()

	// Stop accepting new connections
	if s.listener != nil {
		_ = s.listener.Close()
	}

	// Check if there are active connections
	activeConns := atomic.LoadInt64(&s.activeConns)
	if activeConns > 0 {
		slog.Info("Waiting for active connections to finish.", "count", activeConns)
	}

	// If process isolation is enabled, signal children to terminate
	if s.cfg.ProcessIsolation && s.childTracker != nil {
		childCount := s.childTracker.Count()
		if childCount > 0 {
			slog.Info("Signaling child processes to terminate.", "count", childCount)
			s.childTracker.SignalAll(syscall.SIGTERM)
		}
	}

	// Wait for connections with context
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		// Also wait for child processes if isolation is enabled
		if s.cfg.ProcessIsolation && s.childTracker != nil {
			<-s.childTracker.WaitAll()
		}
		close(done)
	}()

	select {
	case <-done:
		slog.Info("All connections closed gracefully.")
	case <-ctx.Done():
		slog.Warn("Shutdown context cancelled, force closing remaining connections.")
		// Force kill remaining children
		if s.cfg.ProcessIsolation && s.childTracker != nil {
			s.childTracker.SignalAll(syscall.SIGKILL)
		}
	}

	// Shut down ACME managers if active
	if s.acmeManager != nil {
		if err := s.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown error.", "error", err)
		}
	}
	if s.acmeDNSManager != nil {
		if err := s.acmeDNSManager.Close(); err != nil {
			slog.Warn("ACME DNS manager shutdown error.", "error", err)
		}
	}

	// Database connections are now closed by each clientConn when it terminates
	slog.Info("Shutdown complete.")
	return nil
}

// ActiveConnections returns the number of active connections
func (s *Server) ActiveConnections() int64 {
	return atomic.LoadInt64(&s.activeConns)
}

// RegisterQuery registers a cancel function for a backend key.
// This allows the query to be cancelled via a cancel request from another connection.
func (s *Server) RegisterQuery(key BackendKey, cancel context.CancelFunc) {
	s.activeQueriesMu.Lock()
	s.activeQueries[key] = cancel
	s.activeQueriesMu.Unlock()
}

// UnregisterQuery removes the cancel function for a backend key.
// This should be called when a query completes (successfully or with error).
func (s *Server) UnregisterQuery(key BackendKey) {
	s.activeQueriesMu.Lock()
	delete(s.activeQueries, key)
	s.activeQueriesMu.Unlock()
}

// CancelQuery cancels a running query by its backend key.
// Returns true if a query was found and cancelled, false otherwise.
func (s *Server) CancelQuery(key BackendKey) bool {
	s.activeQueriesMu.RLock()
	cancel, ok := s.activeQueries[key]
	s.activeQueriesMu.RUnlock()

	if ok && cancel != nil {
		cancel()
		queryCancellationsCounter.Inc()
		slog.Info("Query cancelled via cancel request.", "pid", key.Pid, "secret_key", key.SecretKey)
		return true
	}
	return false
}

// initConnsMap initializes the connection registry map.
// This is a separate method to work around cases where a local variable
// named "clientConn" shadows the type name (e.g., in worker.go).
func (s *Server) initConnsMap() {
	s.conns = make(map[int32]*clientConn)
}

// registerConn adds a client connection to the registry for pg_stat_activity.
func (s *Server) registerConn(c *clientConn) {
	s.connsMu.Lock()
	s.conns[c.pid] = c
	s.connsMu.Unlock()
}

// unregisterConn removes a client connection from the registry.
func (s *Server) unregisterConn(pid int32) {
	s.connsMu.Lock()
	delete(s.conns, pid)
	s.connsMu.Unlock()
}

// listConns returns a snapshot of all registered client connections.
func (s *Server) listConns() []*clientConn {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	conns := make([]*clientConn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	return conns
}

// createDBConnection creates a DuckDB connection for a client session.
// This is a thin wrapper around CreateDBConnection using the server's config.
func (s *Server) createDBConnection(username string) (*sql.DB, error) {
	return CreateDBConnection(s.cfg, s.duckLakeSem, username, processStartTime, processVersion)
}

// acquireFileDB returns a shared *sql.DB for the given user, creating one if needed.
// The caller must call releaseFileDB when the connection is no longer needed.
func (s *Server) acquireFileDB(username string, passthrough bool) (*sql.DB, error) {
	s.fileDBsMu.Lock()
	defer s.fileDBsMu.Unlock()

	if entry, ok := s.fileDBs[username]; ok {
		entry.refs++
		return entry.db, nil
	}

	var db *sql.DB
	var err error
	if passthrough {
		db, err = CreatePassthroughDBConnection(s.cfg, s.duckLakeSem, username, processStartTime, processVersion)
	} else {
		db, err = CreateDBConnection(s.cfg, s.duckLakeSem, username, processStartTime, processVersion)
	}
	if err != nil {
		return nil, err
	}

	// openBaseDB sets MaxOpenConns(1) for single-session use; override for shared pool.
	db.SetMaxOpenConns(0) // unlimited
	db.SetMaxIdleConns(4)

	stopRefresh := StartCredentialRefresh(db, s.cfg.DuckLake)

	s.fileDBs[username] = &fileDBEntry{
		db:          db,
		refs:        1,
		stopRefresh: stopRefresh,
	}
	return db, nil
}

// releaseFileDB decrements the ref count for a user's shared DB.
// When the last reference is released, the DB is closed and removed from the pool.
func (s *Server) releaseFileDB(username string) {
	s.fileDBsMu.Lock()
	defer s.fileDBsMu.Unlock()

	entry, ok := s.fileDBs[username]
	if !ok {
		return
	}
	entry.refs--
	if entry.refs <= 0 {
		if entry.stopRefresh != nil {
			entry.stopRefresh()
		}
		_ = entry.db.Close()
		delete(s.fileDBs, username)
	}
}

// openBaseDB creates and configures a DuckDB connection with threads, memory
// limit, temp directory, extensions, and cache_httpfs settings.
// This shared setup is used by both regular and passthrough connections.
//
// When DataDir is set, the database is file-backed at <DataDir>/<username>.duckdb.
// DuckDB memory-maps the file and serves queries from RAM (like Redis with AOF),
// so performance is equivalent to in-memory while data persists across restarts.
// When DataDir is empty, falls back to a pure in-memory database.
func openBaseDB(cfg Config, username string) (*sql.DB, error) {
	// allow_unsigned_extensions is a startup-only DuckDB config — it must be
	// in the DSN, not via SET.
	dsn := ":memory:?allow_unsigned_extensions=true"
	if cfg.FilePersistence && cfg.DataDir != "" && username != "" {
		if strings.ContainsAny(username, "/\\") || strings.Contains(username, "..") {
			return nil, fmt.Errorf("invalid username for file persistence: %q (contains path separator or ..)", username)
		}
		if err := os.MkdirAll(cfg.DataDir, 0750); err != nil {
			return nil, fmt.Errorf("failed to create data directory %s: %w", cfg.DataDir, err)
		}
		dsn = filepath.Join(cfg.DataDir, username+".duckdb") + "?allow_unsigned_extensions=true"
		slog.Info("Opening file-backed DuckDB.", "path", dsn)
	}
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Single connection per client session. This is the isolation boundary:
	// DuckDB connections share a single catalog (tables, views, credentials),
	// so concurrent sessions on the same DB would see each other's data.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Verify connection
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	// Set DuckDB threads
	threads := cfg.Threads
	if threads == 0 {
		threads = runtime.NumCPU() * 2
	}
	if _, err := db.Exec(fmt.Sprintf("SET threads = %d", threads)); err != nil {
		slog.Warn("Failed to set DuckDB threads.", "threads", threads, "error", err)
	} else {
		slog.Debug("Set DuckDB threads.", "threads", threads)
	}

	// Set DuckDB memory limit
	memLimit := cfg.MemoryLimit
	if memLimit == "" {
		memLimit = autoMemoryLimit()
	}
	if _, err := db.Exec(fmt.Sprintf("SET memory_limit = '%s'", memLimit)); err != nil {
		slog.Warn("Failed to set DuckDB memory_limit.", "memory_limit", memLimit, "error", err)
	} else {
		slog.Debug("Set DuckDB memory_limit.", "memory_limit", memLimit)
	}

	// Set temp directory to a subdirectory under DataDir to ensure DuckDB has a
	// writable location for intermediate results. This prevents "Read-only file system"
	// errors in containerized or restricted environments.
	tempDir := filepath.Join(cfg.DataDir, "tmp")
	if _, err := db.Exec(fmt.Sprintf("SET temp_directory = '%s'", tempDir)); err != nil {
		slog.Warn("Failed to set DuckDB temp_directory.", "temp_directory", tempDir, "error", err)
	} else {
		slog.Debug("Set DuckDB temp_directory.", "temp_directory", tempDir)
	}

	if err := setExtensionDirectory(db, cfg.DataDir); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to configure extension_directory: %w", err)
	}

	// Load configured extensions
	if err := LoadExtensions(db, cfg.Extensions); err != nil {
		slog.Warn("Failed to load some extensions.", "user", username, "error", err)
	}

	// Enable query profiling so per-query operator timing can be extracted
	// and attached to OTEL trace spans. Standard mode adds sub-1% overhead
	// (just clock_gettime per operator boundary).
	// Output goes to a fixed temp file; in K8s mode the worker reads it
	// after each query and sends it to the control plane via gRPC trailer.
	if _, err := db.Exec("SET enable_profiling = 'json'"); err != nil {
		slog.Warn("Failed to enable DuckDB profiling.", "error", err)
	}
	if _, err := db.Exec("SET profiling_mode = 'detailed'"); err != nil {
		slog.Warn("Failed to set DuckDB profiling mode.", "error", err)
	}
	if _, err := db.Exec("SET profiling_output = '/tmp/duckgres-profiling.json'"); err != nil {
		slog.Warn("Failed to set DuckDB profiling output path.", "error", err)
	}

	// Configure cache_httpfs cache directory if the extension is loaded.
	// cache_httpfs wraps httpfs with a local disk cache, avoiding repeated S3/HTTP downloads.
	if hasCacheHTTPFS(cfg.Extensions) {
		cacheDir := filepath.Join(cfg.DataDir, "cache")
		if err := os.MkdirAll(cacheDir, 0750); err != nil {
			slog.Warn("Failed to create cache_httpfs cache directory.", "cache_directory", cacheDir, "error", err)
		} else if _, err := db.Exec(fmt.Sprintf("SET cache_httpfs_cache_directory = '%s/'", cacheDir)); err != nil {
			// NOTE: cache directory path comes from trusted server config (DataDir), not user input.
			slog.Warn("Failed to set cache_httpfs cache directory.", "cache_directory", cacheDir, "error", err)
		} else {
			slog.Debug("Set cache_httpfs cache directory.", "cache_directory", cacheDir)
		}
	}

	return db, nil
}

func seedBundledExtensions(srcRoot, dstRoot string) error {
	srcRoot = filepath.Clean(srcRoot)
	dstRoot = filepath.Clean(dstRoot)

	info, err := os.Stat(srcRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat bundled extensions dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("bundled extensions path %s is not a directory", srcRoot)
	}
	if err := os.MkdirAll(dstRoot, 0o750); err != nil {
		return fmt.Errorf("mkdir extension directory %s: %w", dstRoot, err)
	}

	return filepath.Walk(srcRoot, func(path string, walkInfo os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == srcRoot {
			return nil
		}

		rel, err := filepath.Rel(srcRoot, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dstRoot, rel)

		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if walkInfo != nil {
			info = walkInfo
		}
		if info.IsDir() {
			return os.MkdirAll(dstPath, 0o750)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o750); err != nil {
			return err
		}
		if _, err := os.Stat(dstPath); err == nil {
			if !shouldRefreshBundledExtension(path) {
				return nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return copyFile(path, dstPath, info.Mode().Perm())
	})
}

func shouldRefreshBundledExtension(srcPath string) bool {
	return filepath.Base(srcPath) == "postgres_scanner.duckdb_extension"
}

func copyFile(srcPath, dstPath string, mode os.FileMode) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	tmpFile, err := os.CreateTemp(filepath.Dir(dstPath), ".bundled-extension-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if err := tmpFile.Chmod(mode); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if _, err := io.Copy(tmpFile, srcFile); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, dstPath)
}

// CreateDBConnection creates a DuckDB connection for a client session.
// Uses in-memory database as an anchor for DuckLake attachment (actual data lives in RDS/S3).
// This is a standalone function so it can be reused by both the server and control plane workers.
// serverStartTime is the time the top-level server process started (may differ from processStartTime
// in process isolation mode where each child has its own processStartTime).
// serverVersion is the version of the top-level server/control-plane process.
func CreateDBConnection(cfg Config, duckLakeSem chan struct{}, username string, serverStartTime time.Time, serverVersion string) (*sql.DB, error) {
	db, err := openBaseDB(cfg, username)
	if err != nil {
		return nil, err
	}

	if err := ConfigureDBConnection(db, cfg, duckLakeSem, username, serverStartTime, serverVersion); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// ConfigureDBConnection initializes an existing DuckDB connection with pg_catalog,
// information_schema, and DuckLake catalog attachment.
func ConfigureDBConnection(db *sql.DB, cfg Config, duckLakeSem chan struct{}, username string, serverStartTime time.Time, serverVersion string) error {
	// Initialize pg_catalog schema for PostgreSQL compatibility
	// Must be done BEFORE attaching DuckLake so macros are created in memory.main,
	// not in the DuckLake catalog (which doesn't support macro storage).
	if err := initPgCatalog(db, serverStartTime, processStartTime, serverVersion, processVersion); err != nil {
		slog.Warn("Failed to initialize pg_catalog.", "user", username, "error", err)
		// Continue anyway - basic queries will still work
	}

	// Register ClickHouse SQL macros (chsql compat)
	initClickHouseMacros(db)

	// Attach DuckLake catalog if configured (but don't set as default yet)
	duckLakeMode := false
	if err := AttachDuckLake(db, cfg.DuckLake, duckLakeSem, cfg.DataDir); err != nil {
		// If DuckLake was explicitly configured, fail the connection.
		// Silent fallback to local DB causes schema/table mismatches.
		if cfg.DuckLake.MetadataStore != "" {
			return fmt.Errorf("DuckLake configured but attachment failed: %w", err)
		}
		// DuckLake not configured, this warning is just informational
		slog.Warn("Failed to attach DuckLake.", "user", username, "error", err)
	} else if cfg.DuckLake.MetadataStore != "" {
		duckLakeMode = true

		// Recreate pg_class_full to source from DuckLake metadata instead of DuckDB's pg_catalog.
		// This ensures consistent PostgreSQL-compatible OIDs across all pg_class queries.
		if err := recreatePgClassForDuckLake(db); err != nil {
			slog.Warn("Failed to recreate pg_class_full for DuckLake.", "error", err)
			// Non-fatal: continue with DuckDB-based pg_class_full
		}

		// Recreate pg_namespace to source from DuckLake metadata.
		// This ensures OIDs match pg_class_full for JOINs (e.g., Metabase table discovery).
		if err := recreatePgNamespaceForDuckLake(db); err != nil {
			slog.Warn("Failed to recreate pg_namespace for DuckLake.", "error", err)
			// Non-fatal: continue with DuckDB-based pg_namespace
		}
	}
	if err := AttachDeltaCatalog(db, cfg.DuckLake, duckLakeSem); err != nil {
		if cfg.DuckLake.DeltaCatalogEnabled {
			return fmt.Errorf("delta catalog configured but attachment failed: %w", err)
		}
		slog.Warn("Failed to attach Delta catalog.", "user", username, "error", err)
	}

	// Initialize information_schema compatibility views in memory.main
	// Must be done AFTER attaching DuckLake (so views can reference ducklake.information_schema)
	// but BEFORE setting DuckLake as default (so views are created in memory.main, not ducklake.main)
	if err := initInformationSchema(db, duckLakeMode); err != nil {
		slog.Warn("Failed to initialize information_schema.", "user", username, "error", err)
		// Continue anyway - basic queries will still work
	}

	// Now set DuckLake as the default catalog so all user queries use it
	if duckLakeMode {
		if err := setDuckLakeDefault(db); err != nil {
			return fmt.Errorf("failed to set DuckLake as default: %w", err)
		}
	}

	return nil
}

// ActivateDBConnection applies tenant-specific DuckLake runtime to an already
// initialized generic DuckDB connection used by a shared warm worker.
func ActivateDBConnection(db *sql.DB, cfg Config, duckLakeSem chan struct{}, username string) error {
	if cfg.DuckLake.MetadataStore == "" {
		return fmt.Errorf("tenant activation requires ducklake metadata_store")
	}

	if err := AttachDuckLake(db, cfg.DuckLake, duckLakeSem, cfg.DataDir); err != nil {
		return fmt.Errorf("DuckLake configured but attachment failed: %w", err)
	}
	if err := AttachDeltaCatalog(db, cfg.DuckLake, duckLakeSem); err != nil {
		return fmt.Errorf("delta catalog configured but attachment failed: %w", err)
	}

	if err := recreatePgClassForDuckLake(db); err != nil {
		slog.Warn("Failed to recreate pg_class_full for DuckLake during activation.", "user", username, "error", err)
	}
	if err := recreatePgNamespaceForDuckLake(db); err != nil {
		slog.Warn("Failed to recreate pg_namespace for DuckLake during activation.", "user", username, "error", err)
	}
	if err := initInformationSchema(db, true); err != nil {
		slog.Warn("Failed to initialize information_schema during activation.", "user", username, "error", err)
	}
	if err := setDuckLakeDefault(db); err != nil {
		return fmt.Errorf("failed to set DuckLake as default: %w", err)
	}

	return nil
}

// CreatePassthroughDBConnection creates a DuckDB connection without pg_catalog
// or information_schema initialization. DuckLake is still attached if configured
// so passthrough users can access the same data. This is used for passthrough users
// who send DuckDB-native SQL and don't need the PostgreSQL compatibility layer.
func CreatePassthroughDBConnection(cfg Config, duckLakeSem chan struct{}, username string, serverStartTime time.Time, serverVersion string) (*sql.DB, error) {
	db, err := openBaseDB(cfg, username)
	if err != nil {
		return nil, err
	}

	// Utility macros (uptime, version) are useful for all connections.
	initUtilityMacros(db, serverStartTime, processStartTime, serverVersion, processVersion)

	// Register ClickHouse SQL macros (chsql compat)
	initClickHouseMacros(db)

	// Attach DuckLake catalog if configured (same data, no pg_catalog views)
	if err := AttachDuckLake(db, cfg.DuckLake, duckLakeSem, cfg.DataDir); err != nil {
		if cfg.DuckLake.MetadataStore != "" {
			_ = db.Close()
			return nil, fmt.Errorf("DuckLake configured but attachment failed: %w", err)
		}
		slog.Warn("Failed to attach DuckLake.", "user", username, "error", err)
	} else if cfg.DuckLake.MetadataStore != "" {
		if err := setDuckLakeDefault(db); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to set DuckLake as default: %w", err)
		}
	}
	if err := AttachDeltaCatalog(db, cfg.DuckLake, duckLakeSem); err != nil {
		if cfg.DuckLake.DeltaCatalogEnabled {
			_ = db.Close()
			return nil, fmt.Errorf("delta catalog configured but attachment failed: %w", err)
		}
		slog.Warn("Failed to attach Delta catalog.", "user", username, "error", err)
	}

	return db, nil
}

// parseExtensionName splits an extension string into its name and install command.
// For "cache_httpfs FROM community", returns ("cache_httpfs", "cache_httpfs FROM community").
// For "ducklake", returns ("ducklake", "ducklake").
func parseExtensionName(ext string) (name, installCmd string) {
	if idx := strings.Index(strings.ToUpper(ext), " FROM "); idx != -1 {
		return strings.TrimSpace(ext[:idx]), ext
	}
	return ext, ext
}

// LoadExtensions installs and loads DuckDB extensions.
// This is a standalone function so it can be reused by control plane workers.
// Extension strings can include a source, e.g. "cache_httpfs FROM community".
// INSTALL uses the full string; LOAD uses just the extension name.
//
// NOTE: Extension names come from trusted server config, not user input.
func LoadExtensions(db *sql.DB, extensions []string) error {
	if len(extensions) == 0 {
		return nil
	}

	var lastErr error
	for _, ext := range extensions {
		name, installCmd := parseExtensionName(ext)

		if shouldInstallExtension(name) {
			// First install the extension (downloads if needed). Bundled extensions
			// are preseeded into the extension cache and INSTALL can overwrite that
			// bundled binary with DuckDB's repository copy.
			if _, err := db.Exec("INSTALL " + installCmd); err != nil {
				slog.Warn("Failed to install extension.", "extension", installCmd, "error", err)
				lastErr = err
				continue
			}
		}

		// Then load it into the current session
		if _, err := db.Exec("LOAD " + name); err != nil {
			slog.Warn("Failed to load extension.", "extension", name, "error", err)
			lastErr = err
			continue
		}

		slog.Info("Loaded extension.", "extension", name)
	}

	return lastErr
}

func shouldInstallExtension(name string) bool {
	return !hasBundledExtensionBinary(name)
}

func hasBundledExtensionBinary(name string) bool {
	matches, err := filepath.Glob(filepath.Join(bundledDuckDBExtensionsDir, "*", "*", name+".duckdb_extension"))
	return err == nil && len(matches) > 0
}

func boolPtr(v bool) *bool { return &v }

func duckLakeDisableMetadataThreadLocalCacheEnabled(dlCfg DuckLakeConfig) bool {
	if dlCfg.DisableMetadataThreadLocalCache == nil {
		return true
	}
	return *dlCfg.DisableMetadataThreadLocalCache
}

func buildDuckLakePreAttachStatements(dlCfg DuckLakeConfig) []string {
	var statements []string
	if duckLakeDisableMetadataThreadLocalCacheEnabled(dlCfg) {
		statements = append(statements, "SET GLOBAL pg_pool_enable_thread_local_cache = false")
	}
	if dlCfg.ViaPgBouncer {
		statements = append(statements, "SET GLOBAL pg_pool_max_connections = 0")
	}
	return statements
}

type duckLakeSQLExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func isMissingDuckLakePoolSettingError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unrecognized configuration parameter")
}

func applyDuckLakePreAttachSettingsWith(db duckLakeSQLExecer, loadPostgresScanner func() error, dlCfg DuckLakeConfig) error {
	statements := buildDuckLakePreAttachStatements(dlCfg)
	if len(statements) == 0 {
		return nil
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			if isMissingDuckLakePoolSettingError(err) {
				if loadErr := loadPostgresScanner(); loadErr != nil {
					slog.Warn("DuckLake pre-attach pool setting unavailable; continuing without it.",
						"statement", stmt, "error", loadErr)
					continue
				}
				if _, retryErr := db.Exec(stmt); retryErr != nil {
					if isMissingDuckLakePoolSettingError(retryErr) {
						slog.Warn("DuckLake pre-attach pool setting still unavailable after loading postgres_scanner; continuing without it.",
							"statement", stmt, "error", retryErr)
						continue
					}
					return fmt.Errorf("apply DuckLake pre-attach setting %q after loading postgres_scanner: %w", stmt, retryErr)
				}
				continue
			}
			return fmt.Errorf("apply DuckLake pre-attach setting %q: %w", stmt, err)
		}
	}
	return nil
}

func applyDuckLakePreAttachSettings(db *sql.DB, dlCfg DuckLakeConfig) error {
	return applyDuckLakePreAttachSettingsWith(db, func() error {
		return LoadExtensions(db, []string{"postgres_scanner"})
	}, dlCfg)
}

func configureDuckLakeMetadataPool(db duckLakeSQLExecer) {
	_, err := db.Exec(`SELECT * FROM postgres_configure_pool(
		catalog_name := '__ducklake_metadata_ducklake',
		enable_reaper_thread := true,
		idle_timeout_millis := 60000,
		max_lifetime_millis := 600000
	)`)
	if err != nil {
		slog.Warn("Failed to configure DuckLake metadata pg pool.", "error", err)
	}
}

// hasCacheHTTPFS checks if cache_httpfs is in the extensions list.
func hasCacheHTTPFS(extensions []string) bool {
	for _, ext := range extensions {
		name, _ := parseExtensionName(ext)
		if name == "cache_httpfs" {
			return true
		}
	}
	return false
}

// AttachDuckLake attaches a DuckLake catalog if configured (but does NOT set it as default).
// Call setDuckLakeDefault after creating per-connection views in memory.main.
// This is a standalone function so it can be reused by control plane workers.
// dataDir is used for writing migration backup files if a schema upgrade is needed.
func AttachDuckLake(db *sql.DB, dlCfg DuckLakeConfig, sem chan struct{}, dataDir string) error {
	if dlCfg.MetadataStore == "" {
		return nil // DuckLake not configured
	}

	// In control-plane mode, the CP runs the migration check and sets
	// dlCfg.Migrate=true before sending the activation payload to workers.
	// Workers skip the check entirely to avoid redundant backups and
	// health-check timeouts during long backup operations.
	// In standalone mode, the check runs here (once per process).
	if !dlCfg.Migrate {
		ensureDuckLakeMigrationCheck(dlCfg, dataDir)
		if val, ok := dlMigrations.Load(dlCfg.MetadataStore); ok {
			state := val.(*migrationState)
			if state.err != nil {
				return fmt.Errorf("DuckLake migration check failed: %w", state.err)
			}
		}
	}

	// Serialize DuckLake attachment to avoid race conditions where multiple
	// connections try to attach simultaneously, causing errors like
	// "database with name '__ducklake_metadata_ducklake' already exists".
	// Use a 30-second timeout to prevent connections from hanging indefinitely
	// if attachment is slow (e.g., network latency to metadata store).
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for DuckLake attachment lock")
	}

	// Check if DuckLake catalog is already attached
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'ducklake'").Scan(&count)
	if err == nil && count > 0 {
		// Already attached
		return nil
	}

	// Create S3 secret if using object store
	// - With explicit credentials (S3AccessKey set) or custom endpoint
	// - With credential_chain or aws_sdk provider (for AWS S3)
	if dlCfg.ObjectStore != "" {
		needsSecret := dlCfg.S3Endpoint != "" ||
			dlCfg.S3AccessKey != "" ||
			dlCfg.S3Provider == "credential_chain" ||
			dlCfg.S3Provider == "aws_sdk" ||
			dlCfg.S3Chain != "" ||
			dlCfg.S3Profile != ""

		if needsSecret {
			if err := createS3Secret(db, dlCfg); err != nil {
				return fmt.Errorf("failed to create S3 secret: %w", err)
			}
		}
	}

	// Route httpfs traffic through a forward HTTP proxy (cache proxy DaemonSet).
	// DuckDB keeps SigV4 for the real S3 hostname; the proxy forwards the signed
	// request verbatim, so the proxy needs no AWS credentials.
	//
	// Use SET GLOBAL http_proxy (not a scoped HTTP secret) — DuckDB's S3
	// extension doesn't honor HTTP-secret SCOPE for S3 URLs, so proxying must
	// be global. The proxy itself CONNECT-tunnels non-bucket HTTPS traffic
	// (e.g. read_parquet('https://...')) and only caches DuckLake bucket URLs.
	//
	// Set BEFORE the ATTACH so the proxy is in effect for the initial catalog
	// read (some settings don't propagate to DuckLake's subcatalogs post-attach,
	// same gotcha as pg_pool_max_connections).
	if dlCfg.HTTPProxy != "" {
		// Force plaintext HTTP + path-style at the session level in addition to
		// the S3 secret's USE_SSL/URL_STYLE — DuckDB observed to ignore secret
		// settings and tunnel via HTTPS CONNECT when the endpoint looks like AWS
		// S3, which the proxy can't cache (encrypted tunnel).
		for _, stmt := range []string{
			fmt.Sprintf("SET GLOBAL http_proxy = '%s'", dlCfg.HTTPProxy),
			"SET GLOBAL s3_use_ssl = false",
			"SET GLOBAL s3_url_style = 'path'",
		} {
			if _, err := db.Exec(stmt); err != nil {
				slog.Warn("Failed to set httpfs proxy config.", "stmt", stmt, "error", err)
			}
		}
		slog.Info("Routed httpfs traffic through forward HTTP proxy.", "proxy", dlCfg.HTTPProxy)
	}

	// Warn if metadata store appears to connect via pgbouncer.
	// pgbouncer's connection lifecycle management (idle timeout, server_lifetime, etc.)
	// can kill connections that DuckLake's internal metadata database depends on,
	// causing cascading failures during long queries.
	if strings.Contains(dlCfg.MetadataStore, " port=6432") ||
		strings.Contains(dlCfg.MetadataStore, ":6432/") ||
		strings.HasSuffix(dlCfg.MetadataStore, ":6432") {
		slog.Warn("DuckLake metadata store appears to connect via pgbouncer (port 6432). " +
			"This can cause connection drops during long queries. " +
			"Consider connecting directly to PostgreSQL instead.")
	}

	// Build the ATTACH statement.
	// See: https://ducklake.select/docs/stable/duckdb/usage/connecting
	if err := applyDuckLakePreAttachSettings(db, dlCfg); err != nil {
		return err
	}
	migrate := dlCfg.Migrate || duckLakeMigrationNeeded(dlCfg.MetadataStore)
	attachStmt := buildDuckLakeAttachStmt(dlCfg, migrate)

	dataPath := dlCfg.ObjectStore
	if dataPath == "" {
		dataPath = dlCfg.DataPath
	}
	if migrate {
		targetVersion := dlCfg.SpecVersion
		if targetVersion == "" {
			targetVersion = DefaultDuckLakeSpecVersion
		}
		slog.Info("Attaching DuckLake catalog with automatic migration.",
			"from", duckLakeMigrationCheckedVersion(dlCfg.MetadataStore), "to", targetVersion,
			"metadata", redactConnectionString(dlCfg.MetadataStore))
	} else if dataPath != "" {
		slog.Info("Attaching DuckLake catalog with data path.",
			"metadata", redactConnectionString(dlCfg.MetadataStore), "data", dataPath)
	} else {
		slog.Info("Attaching DuckLake catalog.", "metadata", redactConnectionString(dlCfg.MetadataStore))
	}

	_, attachSpan := tracer.Start(context.Background(), "duckgres.ducklake_attach")
	if err := retryOnTransientAttach(func() error {
		_, err := db.Exec(attachStmt)
		return err
	}); err != nil {
		attachSpan.End()
		return fmt.Errorf("failed to attach DuckLake: %w", err)
	}
	attachSpan.End()

	slog.Info("Attached DuckLake catalog successfully.")

	// Set DuckLake max retry count to handle concurrent connections
	// DuckLake uses optimistic concurrency - when multiple connections commit
	// simultaneously, they may conflict on snapshot IDs. Default of 10 is too low
	// for tools like Fivetran that open many concurrent connections.
	if _, err := db.Exec("SET ducklake_max_retry_count = 100"); err != nil {
		slog.Warn("Failed to set ducklake_max_retry_count.", "error", err)
		// Don't fail - this is not critical, DuckLake will use its default
	}

	// Reclaim idle metadata connections. DuckDB 1.5.2 / DuckLake 1.0 enabled
	// thread-local connection caching for postgres_scanner by default but ships
	// with reaper_thread=off and idle/lifetime timeouts=0, so every connection
	// a worker thread ever caches stays pinned forever — producing a steady-state
	// spike in metadata RDS connections post-upgrade. Enabling the reaper with a
	// 60s idle timeout reclaims idle cached connections while keeping the warm-
	// connection latency benefit for active workers; the 10-min max lifetime is
	// a belt-and-braces cap against stuck connections (NAT churn, pgbouncer kills).
	//
	// postgres_configure_pool() reconfigures the pool that ATTACH already created;
	// SET GLOBAL only affects pools created after it runs, so would be a no-op here.
	// See: https://github.com/duckdb/ducklake/issues/1031 and
	// https://github.com/duckdb/duckdb-postgres/pull/430
	configureDuckLakeMetadataPool(db)

	// Ensure performance indexes exist on the DuckLake metadata tables.
	// Run in a goroutine so it doesn't block the DuckLake semaphore or
	// delay connection setup. Uses atomic flag to retry on transient failures.
	// See: https://github.com/duckdb/ducklake/issues/859
	go ensureDuckLakeMetadataIndexes(dlCfg)

	return nil
}

// AttachDeltaCatalog attaches the configured Delta Lake catalog/table alongside
// DuckLake. It reuses the DuckLake S3 secret settings so Delta scans can access
// the same object store credentials.
func AttachDeltaCatalog(db *sql.DB, dlCfg DuckLakeConfig, sem chan struct{}) error {
	if !dlCfg.DeltaCatalogEnabled {
		return nil
	}
	catalogPath := deltaCatalogPath(dlCfg)
	if catalogPath == "" {
		return fmt.Errorf("delta catalog path is empty")
	}

	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for Delta catalog attachment lock")
	}

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'delta'").Scan(&count)
	if err == nil && count > 0 {
		return nil
	}

	if err := LoadExtensions(db, []string{"delta"}); err != nil {
		return fmt.Errorf("load delta extension: %w", err)
	}

	if deltaCatalogNeedsS3Secret(catalogPath, dlCfg) {
		if err := createS3Secret(db, dlCfg); err != nil {
			return fmt.Errorf("failed to create S3 secret: %w", err)
		}
	}

	attachStmt := buildDeltaCatalogAttachStmt(dlCfg)
	slog.Info("Attaching Delta catalog.", "path", catalogPath)
	if _, err := db.Exec(attachStmt); err != nil {
		return fmt.Errorf("failed to attach Delta catalog: %w", err)
	}
	slog.Info("Attached Delta catalog successfully.", "path", catalogPath)
	return nil
}

func deltaCatalogNeedsS3Secret(catalogPath string, dlCfg DuckLakeConfig) bool {
	if !strings.Contains(catalogPath, "://") {
		return false
	}
	provider := S3ProviderForConfig(dlCfg)
	return dlCfg.S3Endpoint != "" ||
		dlCfg.S3AccessKey != "" ||
		provider == "credential_chain" ||
		provider == "aws_sdk" ||
		dlCfg.S3Chain != "" ||
		dlCfg.S3Profile != ""
}

// duckLakeIndexDone tracks whether metadata indexes have been successfully created.
// Uses atomic.Bool instead of sync.Once so transient failures can be retried.
var duckLakeIndexDone atomic.Bool

// duckLakeIndexMu serializes concurrent index creation attempts.
var duckLakeIndexMu sync.Mutex

// duckLakeMetadataIndex pairs an index name with its CREATE statement so the
// fast-path existence check and the slow-path creation loop stay in sync.
type duckLakeMetadataIndex struct {
	name string
	stmt string
}

// duckLakeMetadataIndexes lists indexes that improve DuckDB postgres scanner
// performance. The scanner uses COPY with ctid batches and pushes down filters,
// but without indexes each batch requires a sequential scan.
var duckLakeMetadataIndexes = []duckLakeMetadataIndex{
	// Critical: ducklake_file_column_stats is often the largest table (millions of rows).
	// Filter pushdown CTEs query by (table_id, column_id) on every query.
	{
		name: "idx_ducklake_file_col_stats_tbl_col",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_file_col_stats_tbl_col ON ducklake_file_column_stats (table_id, column_id)",
	},
	// Catalog loading queries (GetCatalogForSnapshot) filter by snapshot ranges.
	{
		name: "idx_ducklake_tag_object_snap",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_tag_object_snap ON ducklake_tag (object_id, begin_snapshot, end_snapshot)",
	},
	{
		name: "idx_ducklake_col_tag_tbl_col_snap",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_col_tag_tbl_col_snap ON ducklake_column_tag (table_id, column_id, begin_snapshot, end_snapshot)",
	},
	{
		name: "idx_ducklake_table_snap",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_table_snap ON ducklake_table (begin_snapshot, end_snapshot)",
	},
	{
		name: "idx_ducklake_column_tbl_snap",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_column_tbl_snap ON ducklake_column (table_id, begin_snapshot, end_snapshot, column_order)",
	},
	// File and stats queries.
	{
		name: "idx_ducklake_data_file_tbl_snap",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_data_file_tbl_snap ON ducklake_data_file (table_id, begin_snapshot, end_snapshot)",
	},
	{
		name: "idx_ducklake_delete_file_tbl_snap",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_delete_file_tbl_snap ON ducklake_delete_file (table_id, begin_snapshot, end_snapshot)",
	},
	{
		name: "idx_ducklake_table_stats_tbl",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_table_stats_tbl ON ducklake_table_stats (table_id)",
	},
	{
		name: "idx_ducklake_table_col_stats_tbl",
		stmt: "CREATE INDEX IF NOT EXISTS idx_ducklake_table_col_stats_tbl ON ducklake_table_column_stats (table_id)",
	},
}

// ensureDuckLakeMetadataIndexes connects directly to the DuckLake PostgreSQL
// metadata store and creates indexes that dramatically improve query planning
// performance. This is non-fatal — if it fails, DuckLake still works, just slower.
// Retries on subsequent AttachDuckLake calls until it succeeds.
func ensureDuckLakeMetadataIndexes(dlCfg DuckLakeConfig) {
	if duckLakeIndexDone.Load() {
		return
	}

	// Only relevant for PostgreSQL metadata stores.
	if !strings.HasPrefix(dlCfg.MetadataStore, "postgres:") {
		return
	}

	// Serialize concurrent attempts (multiple connections attaching simultaneously).
	duckLakeIndexMu.Lock()
	defer duckLakeIndexMu.Unlock()

	// Double-check after acquiring the lock.
	if duckLakeIndexDone.Load() {
		return
	}

	// Strip the "postgres:" DuckLake protocol prefix to get a standard libpq connection string.
	connStr := strings.TrimPrefix(dlCfg.MetadataStore, "postgres:")

	// pgx/stdlib accepts libpq key=value format directly.
	pgDB, err := sql.Open("pgx", connStr)
	if err != nil {
		slog.Warn("Failed to open connection for DuckLake metadata indexes.", "error", err)
		return
	}
	defer func() { _ = pgDB.Close() }()

	// Fast path: a single pg_indexes lookup avoids 9 CREATE INDEX round-trips
	// when all expected indexes already exist. Each CREATE INDEX IF NOT EXISTS
	// is a no-op at the storage layer but still costs a server round-trip; under
	// pgbouncer transaction pooling that round-trip can take 1-2s during burst
	// load (server-conn handover + TLS handshake to RDS). Collapsing the check
	// to one round-trip cuts the post-attach window from ~16s to a few hundred
	// ms in the steady state.
	expectedNames := make([]string, len(duckLakeMetadataIndexes))
	for i, ix := range duckLakeMetadataIndexes {
		expectedNames[i] = ix.name
	}
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
	var present int
	err = pgDB.QueryRowContext(checkCtx, "SELECT count(*) FROM pg_indexes WHERE indexname = ANY($1)", expectedNames).Scan(&present)
	checkCancel()
	if err == nil && present == len(duckLakeMetadataIndexes) {
		duckLakeIndexDone.Store(true)
		slog.Info("DuckLake metadata indexes already present; skipped ensure.", "verified", present, "total", len(duckLakeMetadataIndexes))
		return
	}

	// Slow path: create any missing indexes.
	// Use a generous timeout — CREATE INDEX on large tables (e.g., ducklake_file_column_stats
	// at 1.2 GB) can take minutes on first run.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := pgDB.PingContext(ctx); err != nil {
		slog.Warn("Failed to connect to DuckLake metadata store for index creation.", "error", err)
		return
	}

	created := 0
	for _, ix := range duckLakeMetadataIndexes {
		if _, err := pgDB.ExecContext(ctx, ix.stmt); err != nil {
			slog.Warn("Failed to create DuckLake metadata index.", "statement", ix.stmt, "error", err)
			// Continue — create as many indexes as possible
		} else {
			created++
		}
	}

	if created == len(duckLakeMetadataIndexes) {
		duckLakeIndexDone.Store(true)
	}
	slog.Info("Ensured DuckLake metadata indexes.", "created_or_verified", created, "total", len(duckLakeMetadataIndexes))
}

// setDuckLakeDefault sets the DuckLake catalog as the default so all queries use it.
// This should be called AFTER creating per-connection views in memory.main.
func setDuckLakeDefault(db *sql.DB) error {
	if _, err := db.Exec("USE ducklake"); err != nil {
		return fmt.Errorf("failed to set DuckLake as default catalog: %w", err)
	}
	slog.Info("Set DuckLake as default catalog.")
	return nil
}

// createS3Secret creates a DuckDB secret for S3/MinIO access.
// This is a standalone function so it can be reused by control plane workers.
// Supports three providers:
//   - "config": explicit credentials (for MinIO or when you have access keys)
//   - "credential_chain": DuckDB's built-in credential chain (does NOT support EKS Pod Identity)
//   - "aws_sdk": Go AWS SDK credential fetch → explicit config secret (supports EKS Pod Identity)
//
// Note: Caller must hold duckLakeSem to avoid race conditions.
// See: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
func createS3Secret(db *sql.DB, dlCfg DuckLakeConfig) error {
	// Check if secret already exists to avoid unnecessary creation
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM duckdb_secrets() WHERE name = 'ducklake_s3'").Scan(&count)
	if err == nil && count > 0 {
		return nil // Secret already exists
	}

	// Determine provider: use credential_chain if explicitly set or if no access key provided
	provider := S3ProviderForConfig(dlCfg)

	var secretStmt string

	switch provider {
	case "aws_sdk":
		// Use Go AWS SDK to fetch credentials (supports EKS Pod Identity, IRSA, etc.)
		var err error
		secretStmt, err = buildAWSSdkSecret(context.Background(), dlCfg)
		if err != nil {
			return fmt.Errorf("aws_sdk credential fetch failed: %w", err)
		}
		slog.Info("Creating S3 secret with aws_sdk provider (Go SDK credentials).")
	case "credential_chain":
		// Use DuckDB's built-in credential chain (does NOT support EKS Pod Identity)
		secretStmt = buildCredentialChainSecret(dlCfg)
		slog.Info("Creating S3 secret with credential_chain provider.")
	default:
		// Use explicit credentials (config provider)
		secretStmt = buildConfigSecret(dlCfg)
		slog.Info("Creating S3 secret with config provider.", "endpoint", dlCfg.S3Endpoint)
	}

	if _, err := db.Exec(secretStmt); err != nil {
		return err
	}

	slog.Info("Created S3 secret successfully.")
	return nil
}

// RefreshS3Secret replaces the DuckDB S3 secret with updated credentials.
// Used when a hot-idle worker is reclaimed and STS credentials have rotated.
// Respects the configured S3 provider (config, aws_sdk, credential_chain).
func RefreshS3Secret(db *sql.DB, dlCfg DuckLakeConfig, duckLakeSem chan struct{}) error {
	if dlCfg.ObjectStore == "" {
		return nil
	}
	if duckLakeSem != nil {
		duckLakeSem <- struct{}{}
		defer func() { <-duckLakeSem }()
	}

	provider := S3ProviderForConfig(dlCfg)
	var secretStmt string
	switch provider {
	case "aws_sdk":
		var err error
		secretStmt, err = buildAWSSdkSecret(context.Background(), dlCfg)
		if err != nil {
			return fmt.Errorf("refresh aws_sdk S3 secret: %w", err)
		}
	case "credential_chain":
		secretStmt = buildCredentialChainSecret(dlCfg)
	default:
		secretStmt = buildConfigSecret(dlCfg)
	}

	// If the previous session left the connection in DuckDB's "Current
	// transaction is aborted" state, the exec will always fail. Issue a
	// ROLLBACK to recover, matching the pattern in StartCredentialRefresh.
	if _, err := db.Exec(secretStmt); err != nil {
		if isTransactionAborted(err) {
			_, _ = db.Exec("ROLLBACK")
			if _, retryErr := db.Exec(secretStmt); retryErr != nil {
				return fmt.Errorf("refresh S3 secret after rollback: %w", retryErr)
			}
		} else {
			return fmt.Errorf("refresh S3 secret: %w", err)
		}
	}
	slog.Debug("Refreshed S3 secret for hot-idle reuse.", "provider", provider)
	return nil
}

// buildConfigSecret builds a CREATE SECRET statement with explicit credentials
func buildConfigSecret(dlCfg DuckLakeConfig) string {
	region := dlCfg.S3Region
	if region == "" {
		region = "us-east-1"
	}

	urlStyle := dlCfg.S3URLStyle
	if urlStyle == "" {
		urlStyle = "path" // Default to path style for MinIO compatibility
	}

	useSSL := "false"
	if dlCfg.S3UseSSL {
		useSSL = "true"
	}

	// Build base secret with explicit credentials
	secret := fmt.Sprintf(`
		CREATE OR REPLACE SECRET ducklake_s3 (
			TYPE s3,
			PROVIDER config,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			URL_STYLE '%s',
			USE_SSL %s`,
		dlCfg.S3AccessKey,
		dlCfg.S3SecretKey,
		region,
		urlStyle,
		useSSL,
	)

	// Add endpoint if specified (for MinIO or custom S3-compatible storage)
	if dlCfg.S3Endpoint != "" {
		secret += fmt.Sprintf(",\n\t\t\tENDPOINT '%s'", dlCfg.S3Endpoint)
	}

	if dlCfg.S3SessionToken != "" {
		secret += fmt.Sprintf(",\n\t\t\tSESSION_TOKEN '%s'", dlCfg.S3SessionToken)
	}

	secret += "\n\t\t)"
	return secret
}

// buildCredentialChainSecret builds a CREATE SECRET statement using AWS SDK credential chain
func buildCredentialChainSecret(dlCfg DuckLakeConfig) string {
	// Start with base credential_chain secret
	secret := `
		CREATE OR REPLACE SECRET ducklake_s3 (
			TYPE s3,
			PROVIDER credential_chain`

	// Add chain if specified (e.g., "env;config" to check specific sources)
	if dlCfg.S3Chain != "" {
		secret += fmt.Sprintf(",\n\t\t\tCHAIN '%s'", dlCfg.S3Chain)
	}

	// Add profile if specified (for config chain)
	if dlCfg.S3Profile != "" {
		secret += fmt.Sprintf(",\n\t\t\tPROFILE '%s'", dlCfg.S3Profile)
	}

	// Add region override if specified
	if dlCfg.S3Region != "" {
		secret += fmt.Sprintf(",\n\t\t\tREGION '%s'", dlCfg.S3Region)
	}

	// Add endpoint if specified (for custom S3-compatible storage)
	if dlCfg.S3Endpoint != "" {
		secret += fmt.Sprintf(",\n\t\t\tENDPOINT '%s'", dlCfg.S3Endpoint)

		// Also set URL style and SSL for custom endpoints
		urlStyle := dlCfg.S3URLStyle
		if urlStyle == "" {
			urlStyle = "path"
		}
		secret += fmt.Sprintf(",\n\t\t\tURL_STYLE '%s'", urlStyle)

		useSSL := "false"
		if dlCfg.S3UseSSL {
			useSSL = "true"
		}
		secret += fmt.Sprintf(",\n\t\t\tUSE_SSL %s", useSSL)
	}

	secret += "\n\t\t)"
	return secret
}

// fetchAWSSDKCredentials uses the Go AWS SDK's default credential chain to retrieve
// temporary credentials. This supports all credential sources that the Go SDK supports,
// including EKS Pod Identity (AWS_CONTAINER_CREDENTIALS_FULL_URI), IRSA, instance
// metadata, environment variables, and config files — unlike DuckDB's built-in
// credential_chain which does not support EKS Pod Identity.
func fetchAWSSDKCredentials(ctx context.Context, region string) (aws.Credentials, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to load AWS config: %w", err)
	}
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}
	return creds, nil
}

// buildAWSSdkSecret fetches credentials via the Go AWS SDK and builds a
// CREATE SECRET statement with PROVIDER config using the explicit temporary credentials.
func buildAWSSdkSecret(ctx context.Context, dlCfg DuckLakeConfig) (string, error) {
	creds, err := fetchAWSSDKCredentials(ctx, dlCfg.S3Region)
	if err != nil {
		return "", err
	}

	region := dlCfg.S3Region
	if region == "" {
		region = "us-east-1"
	}

	secret := fmt.Sprintf(`
		CREATE OR REPLACE SECRET ducklake_s3 (
			TYPE s3,
			PROVIDER config,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s'`,
		creds.AccessKeyID,
		creds.SecretAccessKey,
		region,
	)

	if creds.SessionToken != "" {
		secret += fmt.Sprintf(",\n\t\t\tSESSION_TOKEN '%s'", creds.SessionToken)
	}

	if dlCfg.S3Endpoint != "" {
		secret += fmt.Sprintf(",\n\t\t\tENDPOINT '%s'", dlCfg.S3Endpoint)
		urlStyle := dlCfg.S3URLStyle
		if urlStyle == "" {
			urlStyle = "path"
		}
		secret += fmt.Sprintf(",\n\t\t\tURL_STYLE '%s'", urlStyle)
		useSSL := "false"
		if dlCfg.S3UseSSL {
			useSSL = "true"
		}
		secret += fmt.Sprintf(",\n\t\t\tUSE_SSL %s", useSSL)
	}

	secret += "\n\t\t)"
	return secret, nil
}

// credentialRefreshInterval is how often to refresh S3 credentials for long-lived connections.
// EC2 instance role credentials typically expire after 6 hours. Refreshing every 5 minutes
// ensures fresh credentials are always available without excessive IMDS calls.
var credentialRefreshInterval = 5 * time.Minute

// S3ProviderForConfig returns the effective S3 provider for the given DuckLake config.
func S3ProviderForConfig(dlCfg DuckLakeConfig) string {
	provider := dlCfg.S3Provider
	if provider == "" {
		if dlCfg.S3AccessKey != "" {
			provider = "config"
		} else {
			provider = "credential_chain"
		}
	}
	return provider
}

// needsCredentialRefresh returns true if the DuckLake config uses temporary credentials
// that need periodic refresh (credential_chain or aws_sdk provider with an S3 object store).
func needsCredentialRefresh(dlCfg DuckLakeConfig) bool {
	if dlCfg.ObjectStore == "" {
		return false
	}
	p := S3ProviderForConfig(dlCfg)
	return p == "credential_chain" || p == "aws_sdk" || dlCfg.S3SessionToken != ""
}

// isTransactionAborted returns true if the error indicates DuckDB's connection
// is stuck in an aborted transaction state (requires ROLLBACK to recover).
func isTransactionAborted(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Current transaction is aborted")
}

// sqlExecer is satisfied by both *sql.DB and *sql.Conn, allowing
// StartCredentialRefresh to work with either a connection pool or a pinned connection.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// StartCredentialRefresh starts a background goroutine that periodically refreshes
// S3 credentials for long-lived DuckDB connections using the credential_chain provider.
// This prevents credential expiration when running on EC2 with IAM instance roles,
// STS assume-role, or other temporary credential sources.
//
// The execer parameter accepts either *sql.DB (standalone mode) or *sql.Conn (worker
// mode where the pool's only connection is pinned by the session).
//
// The optional isTxActive callback reports whether the caller currently has an active
// user transaction on this connection. When provided and returning false, aborted
// transaction errors are auto-recovered by issuing ROLLBACK and retrying once.
// When omitted (or returning true), automatic rollback is skipped to avoid rolling
// back caller-owned transactions.
//
// Note: ExecContext serializes behind any running query (pool contention for *sql.DB,
// internal mutex for *sql.Conn). This means credentials are refreshed between queries,
// not during them. A query that runs longer than the credential TTL (~6h for instance
// roles) could still fail if DuckDB makes S3 requests with stale cached credentials.
//
// Returns a stop function that cancels the refresh goroutine. The caller must call
// the stop function when the connection is closed to prevent goroutine leaks.
// If credential refresh is not needed (static credentials, no S3, etc.), returns a no-op.
func StartCredentialRefresh(execer sqlExecer, dlCfg DuckLakeConfig, isTxActive ...func() bool) func() {
	if !needsCredentialRefresh(dlCfg) {
		return func() {}
	}

	var txActiveProbe func() bool
	if len(isTxActive) > 0 {
		txActiveProbe = isTxActive[0]
	}

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(credentialRefreshInterval)
		defer ticker.Stop()
		var consecutiveFailures int
		provider := S3ProviderForConfig(dlCfg)
		for {
			select {
			case <-ticker.C:
				var secretStmt string
				var buildErr error
				if provider == "aws_sdk" {
					secretStmt, buildErr = buildAWSSdkSecret(context.Background(), dlCfg)
					if buildErr != nil {
						slog.Warn("Failed to fetch AWS SDK credentials for refresh.", "error", buildErr)
						continue
					}
				} else {
					secretStmt = buildCredentialChainSecret(dlCfg)
				}
				_, err := execer.ExecContext(context.Background(), secretStmt)

				// If stuck in aborted transaction, only auto-rollback when caller
				// confirms there is no active user transaction.
				if isTransactionAborted(err) {
					switch {
					case txActiveProbe == nil:
						slog.Warn("S3 credential refresh hit aborted transaction; skipping automatic ROLLBACK because transaction state is unknown.")
					case txActiveProbe():
						slog.Warn("S3 credential refresh hit aborted transaction; skipping automatic ROLLBACK while user transaction is active.")
					default:
						slog.Warn("S3 credential refresh hit aborted transaction, issuing ROLLBACK.")
						_, _ = execer.ExecContext(context.Background(), "ROLLBACK")
						_, err = execer.ExecContext(context.Background(), secretStmt)
					}
				}

				if err != nil {
					consecutiveFailures++
					lvl := slog.LevelWarn
					if consecutiveFailures >= 3 {
						lvl = slog.LevelError
					}
					slog.Log(context.Background(), lvl, "Failed to refresh S3 credentials.",
						"error", err, "consecutive_failures", consecutiveFailures)
				} else {
					if consecutiveFailures > 0 {
						slog.Info("S3 credential refresh recovered.", "after_failures", consecutiveFailures)
					}
					consecutiveFailures = 0
					slog.Debug("Refreshed S3 credentials.")
				}
			case <-done:
				return
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()

	// Check rate limiting before doing anything
	if msg := s.rateLimiter.CheckConnection(remoteAddr); msg != "" {
		// Send PostgreSQL error and close
		slog.Warn("Connection rejected.", "remote_addr", remoteAddr, "reason", msg)
		rateLimitRejectsCounter.Inc()
		_ = conn.Close()
		return
	}

	// Register this connection
	if !s.rateLimiter.RegisterConnection(remoteAddr) {
		slog.Warn("Connection rejected: rate limit exceeded.", "remote_addr", remoteAddr)
		rateLimitRejectsCounter.Inc()
		_ = conn.Close()
		return
	}

	// Process isolation mode: handle SSL request and cancel in parent, spawn child for rest
	if s.cfg.ProcessIsolation {
		s.handleConnectionIsolated(conn, remoteAddr)
		return
	}

	// Non-isolated mode: handle everything in the current goroutine
	s.handleConnectionInProcess(conn, remoteAddr)
}

// handleConnectionInProcess handles a connection in the current process (non-isolated mode).
func (s *Server) handleConnectionInProcess(conn net.Conn, remoteAddr net.Addr) {
	slog.Debug("Connection accepted.", "remote_addr", remoteAddr)

	// Track active connections (only after rate limiting passes)
	atomic.AddInt64(&s.activeConns, 1)
	connectionsGauge.Inc()
	defer func() {
		atomic.AddInt64(&s.activeConns, -1)
		connectionsGauge.Dec()
	}()

	// Ensure we unregister when done
	defer func() {
		s.rateLimiter.UnregisterConnection(remoteAddr)
		_ = conn.Close()
	}()

	// Recover from Go-level panics (e.g., from DuckDB CGO boundary).
	// This won't catch C++ fatal signals (SIGABRT/SIGSEGV) — process isolation handles those.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Recovered from panic in connection handler.",
				"remote_addr", remoteAddr, "panic", r)
		}
	}()

	c := &clientConn{
		server: s,
		conn:   conn,
	}

	if err := c.serve(); err != nil {
		slog.Error("Connection error.", "user", c.username, "remote_addr", remoteAddr, "error", err)
	} else {
		slog.Info("Client disconnected.", "user", c.username, "remote_addr", remoteAddr)
	}
}

// handleConnectionIsolated handles a connection with process isolation.
// The parent handles SSL request and cancel requests, then spawns a child process
// for TLS handshake, authentication, and query execution.
func (s *Server) handleConnectionIsolated(conn net.Conn, remoteAddr net.Addr) {
	if err := conn.SetReadDeadline(time.Now().Add(startupReadTimeout)); err != nil {
		slog.Error("Failed to set startup deadline.", "remote_addr", remoteAddr, "error", err)
		s.rateLimiter.UnregisterConnection(remoteAddr)
		_ = conn.Close()
		return
	}

	// IMPORTANT: Use the raw connection (not a buffered reader) for reading the
	// SSL request message. This ensures we don't accidentally buffer data that
	// should be read by the child process after FD passing.
	// The SSL request is a single, small message and the client waits for 'S'
	// before sending the TLS ClientHello, so unbuffered reads are safe here.
	params, err := readStartupMessage(conn)
	if err != nil {
		if err == io.EOF || errors.Is(err, io.EOF) {
			slog.Debug("Client closed connection before sending startup message.", "remote_addr", remoteAddr)
		} else {
			slog.Error("Failed to read startup message.", "remote_addr", remoteAddr, "error", err)
		}
		s.rateLimiter.UnregisterConnection(remoteAddr)
		_ = conn.Close()
		return
	}

	// Handle GSSENCRequest - decline and re-read for SSLRequest.
	// Loop to handle the unlikely case of multiple GSSENCRequests.
	for range 3 {
		if params["__gssenc_request"] != "true" {
			break
		}
		slog.Debug("GSSENCRequest received, declining.", "remote_addr", remoteAddr)
		if _, err := conn.Write([]byte("N")); err != nil {
			slog.Error("Failed to send GSSENC decline.", "remote_addr", remoteAddr, "error", err)
			s.rateLimiter.UnregisterConnection(remoteAddr)
			_ = conn.Close()
			return
		}
		// Re-read: client will send SSLRequest next
		params, err = readStartupMessage(conn)
		if err != nil {
			if err == io.EOF || errors.Is(err, io.EOF) {
				slog.Debug("Client closed connection after GSSENC decline.", "remote_addr", remoteAddr)
			} else {
				slog.Error("Failed to read startup message after GSSENC decline.", "remote_addr", remoteAddr, "error", err)
			}
			s.rateLimiter.UnregisterConnection(remoteAddr)
			_ = conn.Close()
			return
		}
	}

	// Handle cancel request in parent (no child spawn needed)
	if params["__cancel_request"] == "true" {
		s.handleCancelRequestIsolated(params)
		s.rateLimiter.UnregisterConnection(remoteAddr)
		_ = conn.Close()
		return
	}

	// Handle SSL request: send 'S' then spawn child for TLS handshake
	if params["__ssl_request"] == "true" {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			slog.Error("Failed to clear startup deadline.", "remote_addr", remoteAddr, "error", err)
			s.rateLimiter.UnregisterConnection(remoteAddr)
			_ = conn.Close()
			return
		}

		// Send 'S' to indicate we support SSL
		if _, err := conn.Write([]byte("S")); err != nil {
			slog.Error("Failed to send SSL response.", "remote_addr", remoteAddr, "error", err)
			s.rateLimiter.UnregisterConnection(remoteAddr)
			_ = conn.Close()
			return
		}

		// After sending 'S', the client will do TLS handshake and then send the real startup message.
		// We spawn a child process to handle TLS and everything after.
		// Track active connections
		atomic.AddInt64(&s.activeConns, 1)
		connectionsGauge.Inc()

		// Spawn child process - it will do TLS handshake and read the startup message
		child, err := s.spawnChildForTLS(conn)
		if err != nil {
			slog.Error("Failed to spawn child process.", "remote_addr", remoteAddr, "error", err)
			s.rateLimiter.UnregisterConnection(remoteAddr)
			atomic.AddInt64(&s.activeConns, -1)
			connectionsGauge.Dec()
			_ = conn.Close()
			return
		}

		// Register child in tracker
		s.childTracker.Add(child)

		// Monitor child in background (handles cleanup when child exits)
		go func() {
			s.monitorChild(child)
			s.rateLimiter.UnregisterConnection(remoteAddr)
			atomic.AddInt64(&s.activeConns, -1)
			connectionsGauge.Dec()
		}()
	} else {
		// No SSL request - reject connection (TLS is required)
		slog.Warn("Connection rejected: SSL required.", "remote_addr", remoteAddr)
		s.rateLimiter.UnregisterConnection(remoteAddr)
		_ = conn.Close()
		return
	}
}

// handleCancelRequestIsolated handles a cancel request in process isolation mode.
func (s *Server) handleCancelRequestIsolated(params map[string]string) {
	pidStr := params["__cancel_pid"]
	secretKeyStr := params["__cancel_secret_key"]

	if pidStr == "" || secretKeyStr == "" {
		return
	}

	var pid, secretKey int64
	if _, err := fmt.Sscanf(pidStr, "%d", &pid); err != nil {
		return
	}
	if _, err := fmt.Sscanf(secretKeyStr, "%d", &secretKey); err != nil {
		return
	}

	key := BackendKey{Pid: int32(pid), SecretKey: int32(secretKey)}
	s.CancelQueryBySignal(key)
}
