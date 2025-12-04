package server

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"log"
	"net"
	"sync"

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
}

// DuckLakeConfig configures DuckLake metadata store and data path
type DuckLakeConfig struct {
	MetadataStore string // e.g., "postgres:dbname=ducklake" or "sqlite:ducklake.db"
	DataPath      string // e.g., "s3://my-bucket/data/" or "/local/path"
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

	if s.listener != nil {
		s.listener.Close()
	}

	s.wg.Wait()

	s.dbsMu.Lock()
	defer s.dbsMu.Unlock()
	for _, db := range s.dbs {
		db.Close()
	}
	return nil
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
	// Format: ATTACH 'ducklake:metadata_store' (DATA_PATH 'data_path')
	attachStmt := fmt.Sprintf("ATTACH 'ducklake:%s'", s.cfg.DuckLake.MetadataStore)
	if s.cfg.DuckLake.DataPath != "" {
		attachStmt += fmt.Sprintf(" (DATA_PATH '%s')", s.cfg.DuckLake.DataPath)
	}

	if _, err := db.Exec(attachStmt); err != nil {
		return fmt.Errorf("failed to attach DuckLake: %w", err)
	}

	log.Printf("Attached DuckLake catalog: %s", s.cfg.DuckLake.MetadataStore)
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()

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
