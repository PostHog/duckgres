package server

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"net"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// redactConnectionString removes sensitive information (passwords) from connection strings for logging
var passwordPattern = regexp.MustCompile(`(?i)(password\s*[=:]\s*)([^\s]+)`)

func redactConnectionString(connStr string) string {
	return passwordPattern.ReplaceAllString(connStr, "${1}[REDACTED]")
}

type Config struct {
	Host    string
	Port    int
	DataDir string
	Users   map[string]string // username -> password

	// TLS configuration (required)
	TLSCertFile string // Path to TLS certificate file
	TLSKeyFile  string // Path to TLS private key file

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
	// uncleanly. Default: 10 minutes. Set to 0 to disable.
	IdleTimeout time.Duration
}

// DuckLakeConfig configures DuckLake catalog attachment
type DuckLakeConfig struct {
	// MetadataStore is the connection string for the DuckLake metadata database
	// Format: "postgres:host=<host> user=<user> password=<password> dbname=<db>"
	MetadataStore string

	// ObjectStore is the S3-compatible storage path for DuckLake data files
	// Format: "s3://bucket/path/" for S3/MinIO
	// If not specified, data is stored alongside the metadata
	ObjectStore string

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
}

func New(cfg Config) (*Server, error) {
	// TLS is required
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("TLS certificate and key are required")
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificates: %w", err)
	}

	// Use default rate limit config if not specified
	if cfg.RateLimit.MaxFailedAttempts == 0 {
		cfg.RateLimit = DefaultRateLimitConfig()
	}

	// Use default shutdown timeout if not specified
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}

	// Use default idle timeout if not specified (10 minutes)
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 10 * time.Minute
	}

	s := &Server{
		cfg:         cfg,
		rateLimiter: NewRateLimiter(cfg.RateLimit),
		tlsConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
		duckLakeSem: make(chan struct{}, 1),
	}

	log.Printf("TLS enabled with certificate: %s", cfg.TLSCertFile)
	log.Printf("Rate limiting enabled: max %d failed attempts in %v, ban duration %v",
		cfg.RateLimit.MaxFailedAttempts, cfg.RateLimit.FailedAttemptWindow, cfg.RateLimit.BanDuration)
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
			log.Printf("Accept error: %v", err)
			continue
		}

		// Enable TCP keepalive to detect dead connections
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(30 * time.Second)
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
		s.listener.Close()
	}

	// Check if there are active connections
	activeConns := atomic.LoadInt64(&s.activeConns)
	if activeConns > 0 {
		log.Printf("Waiting for %d active connection(s) to finish...", activeConns)
	}

	// Wait for connections with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All connections closed gracefully")
	case <-time.After(s.cfg.ShutdownTimeout):
		log.Printf("Shutdown timeout (%v) exceeded, force closing remaining connections", s.cfg.ShutdownTimeout)
	}

	// Database connections are now closed by each clientConn when it terminates
	log.Println("Shutdown complete")
	return nil
}

// Shutdown performs a graceful shutdown with the given context
func (s *Server) Shutdown(ctx context.Context) error {
	s.closeMu.Lock()
	s.closed = true
	s.closeMu.Unlock()

	// Stop accepting new connections
	if s.listener != nil {
		s.listener.Close()
	}

	// Check if there are active connections
	activeConns := atomic.LoadInt64(&s.activeConns)
	if activeConns > 0 {
		log.Printf("Waiting for %d active connection(s) to finish...", activeConns)
	}

	// Wait for connections with context
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All connections closed gracefully")
	case <-ctx.Done():
		log.Printf("Shutdown context cancelled, force closing remaining connections")
	}

	// Database connections are now closed by each clientConn when it terminates
	log.Println("Shutdown complete")
	return nil
}

// ActiveConnections returns the number of active connections
func (s *Server) ActiveConnections() int64 {
	return atomic.LoadInt64(&s.activeConns)
}

// createDBConnection creates a DuckDB connection for a client session.
// Uses in-memory database as an anchor for DuckLake attachment (actual data lives in RDS/S3).
func (s *Server) createDBConnection(username string) (*sql.DB, error) {
	// Create new in-memory connection (DuckLake provides actual storage)
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Single connection per client session
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Verify connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	// Load configured extensions
	if err := s.loadExtensions(db); err != nil {
		log.Printf("Warning: failed to load some extensions for user %q: %v", username, err)
	}

	// Initialize pg_catalog schema for PostgreSQL compatibility
	// Must be done BEFORE attaching DuckLake so macros are created in memory.main,
	// not in the DuckLake catalog (which doesn't support macro storage).
	if err := initPgCatalog(db); err != nil {
		log.Printf("Warning: failed to initialize pg_catalog for user %q: %v", username, err)
		// Continue anyway - basic queries will still work
	}

	// Attach DuckLake catalog if configured (but don't set as default yet)
	duckLakeMode := false
	if err := s.attachDuckLake(db); err != nil {
		// If DuckLake was explicitly configured, fail the connection.
		// Silent fallback to local DB causes schema/table mismatches.
		if s.cfg.DuckLake.MetadataStore != "" {
			db.Close()
			return nil, fmt.Errorf("DuckLake configured but attachment failed: %w", err)
		}
		// DuckLake not configured, this warning is just informational
		log.Printf("Warning: failed to attach DuckLake for user %q: %v", username, err)
	} else if s.cfg.DuckLake.MetadataStore != "" {
		duckLakeMode = true
	}

	// Initialize information_schema compatibility views in memory.main
	// Must be done AFTER attaching DuckLake (so views can reference ducklake.information_schema)
	// but BEFORE setting DuckLake as default (so views are created in memory.main, not ducklake.main)
	if err := initInformationSchema(db, duckLakeMode); err != nil {
		log.Printf("Warning: failed to initialize information_schema for user %q: %v", username, err)
		// Continue anyway - basic queries will still work
	}

	// Now set DuckLake as the default catalog so all user queries use it
	if duckLakeMode {
		if err := setDuckLakeDefault(db); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set DuckLake as default: %w", err)
		}
	}

	return db, nil
}

// loadExtensions installs and loads configured DuckDB extensions
func (s *Server) loadExtensions(db *sql.DB) error {
	if len(s.cfg.Extensions) == 0 {
		return nil
	}

	var lastErr error
	for _, ext := range s.cfg.Extensions {
		// First install the extension (downloads if needed)
		if _, err := db.Exec("INSTALL " + ext); err != nil {
			log.Printf("Warning: failed to install extension %q: %v", ext, err)
			lastErr = err
			continue
		}

		// Then load it into the current session
		if _, err := db.Exec("LOAD " + ext); err != nil {
			log.Printf("Warning: failed to load extension %q: %v", ext, err)
			lastErr = err
			continue
		}

		log.Printf("Loaded extension: %s", ext)
	}

	return lastErr
}

// attachDuckLake attaches a DuckLake catalog if configured (but does NOT set it as default).
// Call setDuckLakeDefault after creating per-connection views in memory.main.
func (s *Server) attachDuckLake(db *sql.DB) error {
	if s.cfg.DuckLake.MetadataStore == "" {
		return nil // DuckLake not configured
	}

	// Serialize DuckLake attachment to avoid race conditions where multiple
	// connections try to attach simultaneously, causing errors like
	// "database with name '__ducklake_metadata_ducklake' already exists".
	// Use a 30-second timeout to prevent connections from hanging indefinitely
	// if attachment is slow (e.g., network latency to metadata store).
	select {
	case s.duckLakeSem <- struct{}{}:
		defer func() { <-s.duckLakeSem }()
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
	if s.cfg.DuckLake.ObjectStore != "" {
		needsSecret := s.cfg.DuckLake.S3Endpoint != "" ||
			s.cfg.DuckLake.S3AccessKey != "" ||
			s.cfg.DuckLake.S3Provider == "credential_chain" ||
			s.cfg.DuckLake.S3Chain != "" ||
			s.cfg.DuckLake.S3Profile != ""

		if needsSecret {
			if err := s.createS3Secret(db); err != nil {
				return fmt.Errorf("failed to create S3 secret: %w", err)
			}
		}
	}

	// Build the ATTACH statement
	// Format without object store: ATTACH 'ducklake:<metadata_connection>' AS ducklake
	// Format with object store: ATTACH 'ducklake:<metadata_connection>' AS ducklake (DATA_PATH '<s3_path>')
	// See: https://ducklake.select/docs/stable/duckdb/usage/connecting
	var attachStmt string
	if s.cfg.DuckLake.ObjectStore != "" {
		attachStmt = fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake (DATA_PATH '%s')",
			s.cfg.DuckLake.MetadataStore, s.cfg.DuckLake.ObjectStore)
		log.Printf("Attaching DuckLake catalog with object store: metadata=%s, data=%s",
			redactConnectionString(s.cfg.DuckLake.MetadataStore), s.cfg.DuckLake.ObjectStore)
	} else {
		attachStmt = fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake", s.cfg.DuckLake.MetadataStore)
		log.Printf("Attaching DuckLake catalog: %s", redactConnectionString(s.cfg.DuckLake.MetadataStore))
	}

	if _, err := db.Exec(attachStmt); err != nil {
		return fmt.Errorf("failed to attach DuckLake: %w", err)
	}

	log.Printf("Attached DuckLake catalog successfully")
	return nil
}

// setDuckLakeDefault sets the DuckLake catalog as the default so all queries use it.
// This should be called AFTER creating per-connection views in memory.main.
func setDuckLakeDefault(db *sql.DB) error {
	if _, err := db.Exec("USE ducklake"); err != nil {
		return fmt.Errorf("failed to set DuckLake as default catalog: %w", err)
	}
	log.Printf("Set DuckLake as default catalog")
	return nil
}

// createS3Secret creates a DuckDB secret for S3/MinIO access
// Supports two providers:
//   - "config": explicit credentials (for MinIO or when you have access keys)
//   - "credential_chain": AWS SDK credential chain (env vars, config files, instance metadata, etc.)
//
// Note: Caller must hold duckLakeSem to avoid race conditions.
// See: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
func (s *Server) createS3Secret(db *sql.DB) error {
	// Check if secret already exists to avoid unnecessary creation
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM duckdb_secrets() WHERE name = 'ducklake_s3'").Scan(&count)
	if err == nil && count > 0 {
		return nil // Secret already exists
	}

	// Determine provider: use credential_chain if explicitly set or if no access key provided
	provider := s.cfg.DuckLake.S3Provider
	if provider == "" {
		if s.cfg.DuckLake.S3AccessKey != "" {
			provider = "config"
		} else {
			provider = "credential_chain"
		}
	}

	var secretStmt string

	if provider == "credential_chain" {
		// Use AWS SDK credential chain
		secretStmt = s.buildCredentialChainSecret()
		log.Printf("Creating S3 secret with credential_chain provider")
	} else {
		// Use explicit credentials (config provider)
		secretStmt = s.buildConfigSecret()
		log.Printf("Creating S3 secret with config provider for endpoint: %s", s.cfg.DuckLake.S3Endpoint)
	}

	if _, err := db.Exec(secretStmt); err != nil {
		return err
	}

	log.Printf("Created S3 secret successfully")
	return nil
}

// buildConfigSecret builds a CREATE SECRET statement with explicit credentials
func (s *Server) buildConfigSecret() string {
	region := s.cfg.DuckLake.S3Region
	if region == "" {
		region = "us-east-1"
	}

	urlStyle := s.cfg.DuckLake.S3URLStyle
	if urlStyle == "" {
		urlStyle = "path" // Default to path style for MinIO compatibility
	}

	useSSL := "false"
	if s.cfg.DuckLake.S3UseSSL {
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
		s.cfg.DuckLake.S3AccessKey,
		s.cfg.DuckLake.S3SecretKey,
		region,
		urlStyle,
		useSSL,
	)

	// Add endpoint if specified (for MinIO or custom S3-compatible storage)
	if s.cfg.DuckLake.S3Endpoint != "" {
		secret += fmt.Sprintf(",\n\t\t\tENDPOINT '%s'", s.cfg.DuckLake.S3Endpoint)
	}

	secret += "\n\t\t)"
	return secret
}

// buildCredentialChainSecret builds a CREATE SECRET statement using AWS SDK credential chain
func (s *Server) buildCredentialChainSecret() string {
	// Start with base credential_chain secret
	secret := `
		CREATE OR REPLACE SECRET ducklake_s3 (
			TYPE s3,
			PROVIDER credential_chain`

	// Add chain if specified (e.g., "env;config" to check specific sources)
	if s.cfg.DuckLake.S3Chain != "" {
		secret += fmt.Sprintf(",\n\t\t\tCHAIN '%s'", s.cfg.DuckLake.S3Chain)
	}

	// Add profile if specified (for config chain)
	if s.cfg.DuckLake.S3Profile != "" {
		secret += fmt.Sprintf(",\n\t\t\tPROFILE '%s'", s.cfg.DuckLake.S3Profile)
	}

	// Add region override if specified
	if s.cfg.DuckLake.S3Region != "" {
		secret += fmt.Sprintf(",\n\t\t\tREGION '%s'", s.cfg.DuckLake.S3Region)
	}

	// Add endpoint if specified (for custom S3-compatible storage)
	if s.cfg.DuckLake.S3Endpoint != "" {
		secret += fmt.Sprintf(",\n\t\t\tENDPOINT '%s'", s.cfg.DuckLake.S3Endpoint)

		// Also set URL style and SSL for custom endpoints
		urlStyle := s.cfg.DuckLake.S3URLStyle
		if urlStyle == "" {
			urlStyle = "path"
		}
		secret += fmt.Sprintf(",\n\t\t\tURL_STYLE '%s'", urlStyle)

		useSSL := "false"
		if s.cfg.DuckLake.S3UseSSL {
			useSSL = "true"
		}
		secret += fmt.Sprintf(",\n\t\t\tUSE_SSL %s", useSSL)
	}

	secret += "\n\t\t)"
	return secret
}

func (s *Server) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()

	// Track active connections
	atomic.AddInt64(&s.activeConns, 1)
	defer atomic.AddInt64(&s.activeConns, -1)

	// Check rate limiting before doing anything
	if msg := s.rateLimiter.CheckConnection(remoteAddr); msg != "" {
		// Send PostgreSQL error and close
		log.Printf("Connection from %s rejected: %s", remoteAddr, msg)
		conn.Close()
		return
	}

	// Register this connection
	if !s.rateLimiter.RegisterConnection(remoteAddr) {
		log.Printf("Connection from %s rejected: rate limit exceeded", remoteAddr)
		conn.Close()
		return
	}

	// Ensure we unregister when done
	defer func() {
		s.rateLimiter.UnregisterConnection(remoteAddr)
		conn.Close()
	}()

	c := &clientConn{
		server: s,
		conn:   conn,
	}

	if err := c.serve(); err != nil {
		log.Printf("Connection error: %v", err)
	}
}
