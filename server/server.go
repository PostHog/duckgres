package server

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

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
	dbs         map[string]*sql.DB // username -> db connection
	dbsMu       sync.RWMutex
	wg          sync.WaitGroup
	closed      bool
	closeMu     sync.Mutex
	activeConns int64 // atomic counter for active connections
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

	s := &Server{
		cfg:         cfg,
		dbs:         make(map[string]*sql.DB),
		rateLimiter: NewRateLimiter(cfg.RateLimit),
		tlsConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
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

	// Close all database connections
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	for _, db := range s.dbs {
		db.Close()
	}
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

	// Close all database connections
	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	for _, db := range s.dbs {
		db.Close()
	}
	log.Println("Shutdown complete")
	return nil
}

// ActiveConnections returns the number of active connections
func (s *Server) ActiveConnections() int64 {
	return atomic.LoadInt64(&s.activeConns)
}

func (s *Server) getOrCreateDB(username string) (*sql.DB, error) {
	s.dbsMu.RLock()
	db, ok := s.dbs[username]
	s.dbsMu.RUnlock()
	if ok {
		return db, nil
	}

	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()

	// Double-check after acquiring write lock
	if db, ok := s.dbs[username]; ok {
		return db, nil
	}

	dbPath := fmt.Sprintf("%s/%s.db", s.cfg.DataDir, username)
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	// Load configured extensions
	if err := s.loadExtensions(db); err != nil {
		log.Printf("Warning: failed to load some extensions for user %q: %v", username, err)
		// Continue anyway - database will still work without the extensions
	}

	// Attach DuckLake catalog if configured
	if err := s.attachDuckLake(db); err != nil {
		log.Printf("Warning: failed to attach DuckLake for user %q: %v", username, err)
		// Continue anyway - database will still work without DuckLake
	}

	// Initialize pg_catalog schema for PostgreSQL compatibility
	if err := initPgCatalog(db); err != nil {
		log.Printf("Warning: failed to initialize pg_catalog for user %q: %v", username, err)
		// Continue anyway - basic queries will still work
	}

	s.dbs[username] = db
	log.Printf("Opened DuckDB database for user %q at %s", username, dbPath)
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

// attachDuckLake attaches a DuckLake catalog if configured
func (s *Server) attachDuckLake(db *sql.DB) error {
	if s.cfg.DuckLake.MetadataStore == "" {
		return nil // DuckLake not configured
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
			s.cfg.DuckLake.MetadataStore, s.cfg.DuckLake.ObjectStore)
	} else {
		attachStmt = fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake", s.cfg.DuckLake.MetadataStore)
		log.Printf("Attaching DuckLake catalog: %s", s.cfg.DuckLake.MetadataStore)
	}

	if _, err := db.Exec(attachStmt); err != nil {
		return fmt.Errorf("failed to attach DuckLake: %w", err)
	}

	log.Printf("Attached DuckLake catalog successfully")
	return nil
}

// createS3Secret creates a DuckDB secret for S3/MinIO access
// Supports two providers:
//   - "config": explicit credentials (for MinIO or when you have access keys)
//   - "credential_chain": AWS SDK credential chain (env vars, config files, instance metadata, etc.)
//
// See: https://duckdb.org/docs/stable/core_extensions/httpfs/s3api
func (s *Server) createS3Secret(db *sql.DB) error {
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
