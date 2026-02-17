package server

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
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

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// processStartTime is captured at process init, used to distinguish server vs child process uptime.
var processStartTime = time.Now()

// processVersion is set from main() via SetProcessVersion. Defaults to "dev".
var processVersion = "dev"

// SetProcessVersion sets the version string for this process. Called from main().
func SetProcessVersion(v string) { processVersion = v }

// ProcessVersion returns the version string for this process.
func ProcessVersion() string { return processVersion }

// redactConnectionString removes sensitive information (passwords) from connection strings for logging
var passwordPattern = regexp.MustCompile(`(?i)(password\s*[=:]\s*)([^\s]+)`)

var connectionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_connections_open",
	Help: "Number of currently open client connections",
})

var queryDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_query_duration_seconds",
	Help:    "Query execution duration in seconds",
	Buckets: prometheus.DefBuckets,
})

var queryErrorsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_query_errors_total",
	Help: "Total number of failed queries",
})

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

// BackendKey uniquely identifies a backend connection for cancel requests
type BackendKey struct {
	Pid       int32
	SecretKey int32
}

func redactConnectionString(connStr string) string {
	return passwordPattern.ReplaceAllString(connStr, "${1}[REDACTED]")
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
	DataDir             string
	Users               map[string]string // username -> password

	// TLS configuration (required unless ACME is configured)
	TLSCertFile string // Path to TLS certificate file
	TLSKeyFile  string // Path to TLS private key file

	// ACME/Let's Encrypt configuration (alternative to static TLS cert/key)
	ACMEDomain   string // Domain for ACME certificate (e.g., "decisive-mongoose-wine.us.duckgres.com")
	ACMEEmail    string // Contact email for Let's Encrypt notifications
	ACMECacheDir string // Directory for cached certificates (default: "./certs/acme")

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

	// MaxWorkers is the maximum number of worker processes in control-plane mode.
	// 0 means unlimited.
	MaxWorkers int

	// MinWorkers is the number of pre-warmed worker processes at startup in control-plane mode.
	// 0 means no pre-warming (workers spawn on demand).
	MinWorkers int

	// PassthroughUsers are users that bypass the SQL transpiler and pg_catalog initialization.
	// Queries from these users go directly to DuckDB without any PostgreSQL compatibility layer.
	PassthroughUsers map[string]bool
}

// DuckLakeConfig configures DuckLake catalog attachment
type DuckLakeConfig struct {
	// MetadataStore is the connection string for the DuckLake metadata database
	// Format: "postgres:host=<host> user=<user> password=<password> dbname=<db>"
	MetadataStore string

	// ObjectStore is the S3-compatible storage path for DuckLake data files
	// Format: "s3://bucket/path/" for S3/MinIO
	// If not specified, uses DataPath for local storage
	ObjectStore string

	// DataPath is the local file system path for DuckLake data files
	// Used when ObjectStore is not set (for local/non-S3 storage)
	DataPath string

	// S3 credential provider: "config" (explicit credentials) or "credential_chain" (AWS SDK chain)
	// Default: "config" if S3AccessKey is set, otherwise "credential_chain"
	S3Provider string

	// S3 configuration for "config" provider (explicit credentials for MinIO or S3)
	S3Endpoint  string // e.g., "localhost:9000" for MinIO
	S3AccessKey string // S3 access key ID
	S3SecretKey string // S3 secret access key
	S3Region    string // S3 region (default: us-east-1)
	S3UseSSL    bool   // Use HTTPS for S3 connections (default: false for MinIO)
	S3URLStyle  string // "path" or "vhost" (default: "path" for MinIO compatibility)

	// S3 configuration for "credential_chain" provider (AWS SDK credential chain)
	// Chain specifies which credential sources to check, semicolon-separated
	// Options: env, config, sts, sso, instance, process
	// Default: checks all sources in AWS SDK order
	S3Chain   string // e.g., "env;config" to check env vars then config files
	S3Profile string // AWS profile name to use (for "config" chain)
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
	acmeManager *ACMEManager
}

func New(cfg Config) (*Server, error) {
	// Use default rate limit config if not specified
	if cfg.RateLimit.MaxFailedAttempts == 0 {
		cfg.RateLimit = DefaultRateLimitConfig()
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

	s := &Server{
		cfg:           cfg,
		rateLimiter:   NewRateLimiter(cfg.RateLimit),
		activeQueries: make(map[BackendKey]context.CancelFunc),
		duckLakeSem:   make(chan struct{}, 1),
	}

	// Configure TLS: ACME (Let's Encrypt) or static certificate files
	if cfg.ACMEDomain != "" {
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

	// Shut down ACME HTTP challenge listener if active
	if s.acmeManager != nil {
		if err := s.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown error.", "error", err)
		}
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

	// Shut down ACME HTTP challenge listener if active
	if s.acmeManager != nil {
		if err := s.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown error.", "error", err)
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

// createDBConnection creates a DuckDB connection for a client session.
// This is a thin wrapper around CreateDBConnection using the server's config.
func (s *Server) createDBConnection(username string) (*sql.DB, error) {
	return CreateDBConnection(s.cfg, s.duckLakeSem, username, processStartTime, processVersion)
}

// openBaseDB creates and configures a bare DuckDB in-memory connection with
// threads, memory limit, temp directory, extensions, and cache_httpfs settings.
// This shared setup is used by both regular and passthrough connections.
func openBaseDB(cfg Config, username string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Single connection per client session
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

	// Load configured extensions
	if err := LoadExtensions(db, cfg.Extensions); err != nil {
		slog.Warn("Failed to load some extensions.", "user", username, "error", err)
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

	// Initialize pg_catalog schema for PostgreSQL compatibility
	// Must be done BEFORE attaching DuckLake so macros are created in memory.main,
	// not in the DuckLake catalog (which doesn't support macro storage).
	if err := initPgCatalog(db, serverStartTime, processStartTime, serverVersion, processVersion); err != nil {
		slog.Warn("Failed to initialize pg_catalog.", "user", username, "error", err)
		// Continue anyway - basic queries will still work
	}

	// Attach DuckLake catalog if configured (but don't set as default yet)
	duckLakeMode := false
	if err := AttachDuckLake(db, cfg.DuckLake, duckLakeSem); err != nil {
		// If DuckLake was explicitly configured, fail the connection.
		// Silent fallback to local DB causes schema/table mismatches.
		if cfg.DuckLake.MetadataStore != "" {
			_ = db.Close()
			return nil, fmt.Errorf("DuckLake configured but attachment failed: %w", err)
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
			_ = db.Close()
			return nil, fmt.Errorf("failed to set DuckLake as default: %w", err)
		}
	}

	return db, nil
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

	// Attach DuckLake catalog if configured (same data, no pg_catalog views)
	if err := AttachDuckLake(db, cfg.DuckLake, duckLakeSem); err != nil {
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

		// First install the extension (downloads if needed)
		if _, err := db.Exec("INSTALL " + installCmd); err != nil {
			slog.Warn("Failed to install extension.", "extension", installCmd, "error", err)
			lastErr = err
			continue
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
func AttachDuckLake(db *sql.DB, dlCfg DuckLakeConfig, sem chan struct{}) error {
	if dlCfg.MetadataStore == "" {
		return nil // DuckLake not configured
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
	// - With credential_chain provider (for AWS S3)
	if dlCfg.ObjectStore != "" {
		needsSecret := dlCfg.S3Endpoint != "" ||
			dlCfg.S3AccessKey != "" ||
			dlCfg.S3Provider == "credential_chain" ||
			dlCfg.S3Chain != "" ||
			dlCfg.S3Profile != ""

		if needsSecret {
			if err := createS3Secret(db, dlCfg); err != nil {
				return fmt.Errorf("failed to create S3 secret: %w", err)
			}
		}
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

	// Build the ATTACH statement
	// Format without data path: ATTACH 'ducklake:<metadata_connection>' AS ducklake
	// Format with data path: ATTACH 'ducklake:<metadata_connection>' AS ducklake (DATA_PATH '<path>')
	// See: https://ducklake.select/docs/stable/duckdb/usage/connecting
	var attachStmt string
	dataPath := dlCfg.ObjectStore
	if dataPath == "" {
		dataPath = dlCfg.DataPath
	}
	if dataPath != "" {
		attachStmt = fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake (DATA_PATH '%s')",
			dlCfg.MetadataStore, dataPath)
		slog.Info("Attaching DuckLake catalog with data path.", "metadata", redactConnectionString(dlCfg.MetadataStore), "data", dataPath)
	} else {
		attachStmt = fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake", dlCfg.MetadataStore)
		slog.Info("Attaching DuckLake catalog.", "metadata", redactConnectionString(dlCfg.MetadataStore))
	}

	if _, err := db.Exec(attachStmt); err != nil {
		return fmt.Errorf("failed to attach DuckLake: %w", err)
	}

	slog.Info("Attached DuckLake catalog successfully.")

	// Set DuckLake max retry count to handle concurrent connections
	// DuckLake uses optimistic concurrency - when multiple connections commit
	// simultaneously, they may conflict on snapshot IDs. Default of 10 is too low
	// for tools like Fivetran that open many concurrent connections.
	if _, err := db.Exec("SET ducklake_max_retry_count = 100"); err != nil {
		slog.Warn("Failed to set ducklake_max_retry_count.", "error", err)
		// Don't fail - this is not critical, DuckLake will use its default
	}

	return nil
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
// Supports two providers:
//   - "config": explicit credentials (for MinIO or when you have access keys)
//   - "credential_chain": AWS SDK credential chain (env vars, config files, instance metadata, etc.)
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
	provider := s3ProviderForConfig(dlCfg)

	var secretStmt string

	if provider == "credential_chain" {
		// Use AWS SDK credential chain
		secretStmt = buildCredentialChainSecret(dlCfg)
		slog.Info("Creating S3 secret with credential_chain provider.")
	} else {
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

// credentialRefreshInterval is how often to refresh S3 credentials for long-lived connections.
// EC2 instance role credentials typically expire after 6 hours. Refreshing every 5 minutes
// ensures fresh credentials are always available without excessive IMDS calls.
const credentialRefreshInterval = 5 * time.Minute

// s3ProviderForConfig returns the effective S3 provider for the given DuckLake config.
func s3ProviderForConfig(dlCfg DuckLakeConfig) string {
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
// that need periodic refresh (credential_chain provider with an S3 object store).
func needsCredentialRefresh(dlCfg DuckLakeConfig) bool {
	if dlCfg.ObjectStore == "" {
		return false
	}
	return s3ProviderForConfig(dlCfg) == "credential_chain"
}

// StartCredentialRefresh starts a background goroutine that periodically refreshes
// S3 credentials for long-lived DuckDB connections using the credential_chain provider.
// This prevents credential expiration when running on EC2 with IAM instance roles,
// STS assume-role, or other temporary credential sources.
//
// Note: Because each DuckDB connection uses MaxOpenConns(1), the refresh db.Exec will
// block behind any running query. This means credentials are refreshed between queries,
// not during them. A query that runs longer than the credential TTL (~6h for instance
// roles) could still fail if DuckDB makes S3 requests with the stale cached credentials.
//
// Returns a stop function that cancels the refresh goroutine. The caller must call
// the stop function when the connection is closed to prevent goroutine leaks.
// If credential refresh is not needed (static credentials, no S3, etc.), returns a no-op.
func StartCredentialRefresh(db *sql.DB, dlCfg DuckLakeConfig) func() {
	if !needsCredentialRefresh(dlCfg) {
		return func() {}
	}

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(credentialRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				secretStmt := buildCredentialChainSecret(dlCfg)
				if _, err := db.Exec(secretStmt); err != nil {
					slog.Warn("Failed to refresh S3 credentials.", "error", err)
				} else {
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
		slog.Error("Connection error.", "error", err)
	}
}

// handleConnectionIsolated handles a connection with process isolation.
// The parent handles SSL request and cancel requests, then spawns a child process
// for TLS handshake, authentication, and query execution.
func (s *Server) handleConnectionIsolated(conn net.Conn, remoteAddr net.Addr) {
	// IMPORTANT: Use the raw connection (not a buffered reader) for reading the
	// SSL request message. This ensures we don't accidentally buffer data that
	// should be read by the child process after FD passing.
	// The SSL request is a single, small message and the client waits for 'S'
	// before sending the TLS ClientHello, so unbuffered reads are safe here.
	params, err := readStartupMessage(conn)
	if err != nil {
		slog.Error("Failed to read startup message.", "remote_addr", remoteAddr, "error", err)
		s.rateLimiter.UnregisterConnection(remoteAddr)
		_ = conn.Close()
		return
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
