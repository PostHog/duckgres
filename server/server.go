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

	// Build the ATTACH statement
	// Format: ATTACH 'ducklake:<connection_string>' AS ducklake
	// Example: ATTACH 'ducklake:postgres:host=localhost user=ducklake password=secret dbname=ducklake' AS ducklake
	attachStmt := fmt.Sprintf("ATTACH 'ducklake:%s' AS ducklake", s.cfg.DuckLake.MetadataStore)

	if _, err := db.Exec(attachStmt); err != nil {
		return fmt.Errorf("failed to attach DuckLake: %w", err)
	}

	log.Printf("Attached DuckLake catalog: %s", s.cfg.DuckLake.MetadataStore)
	return nil
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
