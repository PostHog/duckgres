package server

import (
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
}

type Server struct {
	cfg      Config
	listener net.Listener
	dbs      map[string]*sql.DB // username -> db connection
	dbsMu    sync.RWMutex
	wg       sync.WaitGroup
	closed   bool
	closeMu  sync.Mutex
}

func New(cfg Config) (*Server, error) {
	return &Server{
		cfg: cfg,
		dbs: make(map[string]*sql.DB),
	}, nil
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
