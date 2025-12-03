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
}

type Server struct {
	cfg       Config
	listener  net.Listener
	tlsConfig *tls.Config
	dbs       map[string]*sql.DB // username -> db connection
	dbsMu     sync.RWMutex
	wg        sync.WaitGroup
	closed    bool
	closeMu   sync.Mutex
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

	s := &Server{
		cfg: cfg,
		dbs: make(map[string]*sql.DB),
		tlsConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
	}

	log.Printf("TLS enabled with certificate: %s", cfg.TLSCertFile)
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

	s.dbs[username] = db
	log.Printf("Opened DuckDB database for user %q at %s", username, dbPath)
	return db, nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	c := &clientConn{
		server: s,
		conn:   conn,
	}

	if err := c.serve(); err != nil {
		log.Printf("Connection error: %v", err)
	}
}
