package duckdbservice

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
)

// DuckDBService is a standalone Arrow Flight SQL service backed by DuckDB.
type DuckDBService struct {
	cfg       ServiceConfig
	pool      *SessionPool
	flightSrv flight.Server
}

// SessionPool manages multiple DuckDB sessions keyed by session token.
type SessionPool struct {
	mu          sync.RWMutex
	sessions    map[string]*Session
	stopRefresh map[string]func()
	reserved    int // number of session slots reserved but not yet inserted
	duckLakeSem chan struct{}
	cfg         server.Config
	startTime   time.Time
	maxSessions int
	stopCh      chan struct{}
	warmupDB    *sql.DB      // Keep this open to keep shared cache alive
	warmupDone  chan struct{} // Closed when Warmup() completes (success or failure)
	dbInitOnce  sync.Once    // Serializes fallback CreateDBConnection when warmup fails
	fallbackDB  *sql.DB      // Lazily created if warmup fails
	fallbackErr error        // Cached error from fallback creation
}

type trackedTx struct {
	tx       *sql.Tx
	lastUsed atomic.Int64 // unix nano
}

// Session represents a single DuckDB session.
type Session struct {
	ID        string
	DB        *sql.DB
	Conn      *sql.Conn // Dedicated connection for this session
	Username  string
	CreatedAt time.Time
	lastUsed  atomic.Int64 // unix nano

	mu            sync.RWMutex
	queries       map[string]*QueryHandle
	txns          map[string]*trackedTx
	txnOwner      map[string]string
	handleCounter atomic.Uint64
}

// QueryHandle stores a prepared or ad-hoc query for later execution.
type QueryHandle struct {
	Query  string
	Schema *arrow.Schema
	TxnID  string
}

// NewDuckDBService creates a new DuckDB service with the given config.
func NewDuckDBService(cfg ServiceConfig) *DuckDBService {
	pool := &SessionPool{
		sessions:    make(map[string]*Session),
		stopRefresh: make(map[string]func()),
		duckLakeSem: make(chan struct{}, 1),
		cfg:         cfg.ServerConfig,
		startTime:   time.Now(),
		maxSessions: cfg.MaxSessions,
		stopCh:      make(chan struct{}),
		warmupDone:  make(chan struct{}),
	}
	go pool.reapLoop()

	return &DuckDBService{
		cfg:  cfg,
		pool: pool,
	}
}

// Warmup performs one-time initialization of the shared DuckDB instance.
// This loads extensions and attaches catalogs so that subsequent session
// creations are nearly instantaneous.
func (p *SessionPool) Warmup() error {
	defer close(p.warmupDone)

	if os.Getenv("DUCKGRES_MODE") != "duckdb-service" {
		return nil
	}

	slog.Info("Pre-warming worker DuckDB instance...")
	// Use a system-level username for warmup
	db, err := server.CreateDBConnection(p.cfg, p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
	if err != nil {
		return fmt.Errorf("warmup failed: %w", err)
	}

	p.mu.Lock()
	p.warmupDB = db
	p.mu.Unlock()

	slog.Info("Worker pre-warmed successfully.")
	return nil
}

const (
	txnIdleTimeout = 10 * time.Minute
	reapInterval   = 1 * time.Minute
)

func (p *SessionPool) reapLoop() {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.mu.RLock()
			sessions := make([]*Session, 0, len(p.sessions))
			for _, s := range p.sessions {
				sessions = append(sessions, s)
			}
			p.mu.RUnlock()

			now := time.Now()
			for _, s := range sessions {
				s.mu.Lock()
				for id, ttx := range s.txns {
					last := time.Unix(0, ttx.lastUsed.Load())
					if now.Sub(last) > txnIdleTimeout {
						slog.Warn("Rolling back idle transaction.", "user", s.Username, "txn", id)
						_ = ttx.tx.Rollback()
						delete(s.txns, id)
						delete(s.txnOwner, id)
					}
				}
				s.mu.Unlock()
			}
		case <-p.stopCh:
			return
		}
	}
}

// Run starts the DuckDB service, blocking until shutdown.
func Run(cfg ServiceConfig) {
	svc := NewDuckDBService(cfg)

	// Pre-warm the DuckDB instance (load extensions, attach DuckLake)
	// in the background so we don't block the gRPC server from starting.
	// This ensures that waitForWorker doesn't time out during spawn.
	go func() {
		if err := svc.pool.Warmup(); err != nil {
			slog.Warn("Worker pre-warm failed. Sessions may be slow to initialize.", "error", err)
		}
	}()

	var listener net.Listener

	if cfg.ListenFD > 0 {
		// Inherited pre-bound listener from the control plane.
		// The CP creates and binds the Unix socket, then passes the FD via
		// cmd.ExtraFiles to avoid EROFS under systemd ProtectSystem=strict.
		file := os.NewFile(uintptr(cfg.ListenFD), "inherited-listener")
		if file == nil {
			slog.Error("Invalid inherited listener FD", "fd", cfg.ListenFD)
			os.Exit(1)
		}
		var err error
		listener, err = net.FileListener(file)
		_ = file.Close()
		if err != nil {
			slog.Error("Failed to create listener from inherited FD", "fd", cfg.ListenFD, "error", err)
			os.Exit(1)
		}
		slog.Info("Starting DuckDB service (inherited listener)", "addr", listener.Addr().String())
	} else {
		network, addr, err := ParseListenAddr(cfg.ListenAddr)
		if err != nil {
			slog.Error("Invalid listen address", "error", err)
			os.Exit(1)
		}

		// Clean up stale unix socket
		if network == "unix" {
			_ = os.Remove(addr)
		}

		listener, err = net.Listen(network, addr)
		if err != nil {
			slog.Error("Failed to listen", "network", network, "addr", addr, "error", err)
			os.Exit(1)
		}

		// Restrict unix socket permissions to owner only
		if network == "unix" {
			if err := os.Chmod(addr, 0700); err != nil {
				slog.Warn("Failed to set unix socket permissions", "error", err)
			}
		}

		slog.Info("Starting DuckDB service", "network", network, "addr", addr)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Shutting down DuckDB service...")
		svc.Shutdown()
		os.Exit(0)
	}()

	if err := svc.Serve(listener); err != nil {
		slog.Error("DuckDB service error", "error", err)
		os.Exit(1)
	}
}

// Serve starts serving on the given listener.
func (svc *DuckDBService) Serve(listener net.Listener) error {
	handler := NewFlightSQLHandler(svc.pool)

	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.MaxRecvMsgSize(server.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(server.MaxGRPCMessageSize),
	)
	if svc.cfg.BearerToken != "" {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(BearerTokenUnaryInterceptor(svc.cfg.BearerToken)),
			grpc.ChainStreamInterceptor(BearerTokenStreamInterceptor(svc.cfg.BearerToken)),
		)
	}

	// Wrap the flightsql server with custom action handling.
	// flightsql.NewFlightServer routes standard Flight SQL actions but rejects
	// custom action types. Our wrapper intercepts custom actions (CreateSession,
	// DestroySession, HealthCheck) before falling through to the standard router.
	flightSqlSrv := flightsql.NewFlightServer(handler)
	customSrv := &customActionServer{FlightServer: flightSqlSrv, handler: handler}

	svc.flightSrv = flight.NewServerWithMiddleware(nil, opts...)
	svc.flightSrv.RegisterFlightService(customSrv)
	svc.flightSrv.InitListener(listener)
	return svc.flightSrv.Serve()
}

// Shutdown gracefully stops the service.
func (svc *DuckDBService) Shutdown() {
	if svc.flightSrv != nil {
		svc.flightSrv.Shutdown()
	}
	svc.pool.CloseAll()
}

// CreateSession creates a new DuckDB session for the given username.
func (p *SessionPool) CreateSession(username string) (*Session, error) {
	// Reserve a slot under the lock to prevent TOCTOU race on maxSessions.
	p.mu.Lock()
	if p.maxSessions > 0 && len(p.sessions)+p.reserved >= p.maxSessions {
		p.mu.Unlock()
		return nil, fmt.Errorf("max sessions reached (%d)", p.maxSessions)
	}
	p.reserved++
	p.mu.Unlock()

	var (
		db   *sql.DB
		conn *sql.Conn
		err  error
	)

	// Wait for warmup to complete before checking warmupDB.
	// Without this, a CreateSession arriving while warmup is in progress would
	// see warmupDB==nil and call CreateDBConnection concurrently with warmup,
	// causing two goroutines to LOAD native extensions simultaneously in the
	// same process — which corrupts the heap (malloc: unaligned tcache chunk).
	<-p.warmupDone

	// Use shared warmupDB if available (highly preferred for performance)
	p.mu.RLock()
	db = p.warmupDB
	p.mu.RUnlock()

	if db == nil {
		// Fallback: create a shared DB if warmup failed or wasn't run.
		// Uses sync.Once to ensure only one goroutine loads native extensions;
		// concurrent LOAD of the same C++ extension corrupts the heap.
		p.dbInitOnce.Do(func() {
			p.fallbackDB, p.fallbackErr = server.CreateDBConnection(p.cfg, p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
		})
		if p.fallbackErr != nil {
			p.mu.Lock()
			p.reserved--
			p.mu.Unlock()
			return nil, fmt.Errorf("create session db: %w", p.fallbackErr)
		}
		db = p.fallbackDB
	}

	// Obtain a dedicated connection for this session to ensure isolation
	// and consistent session settings (search_path, etc.)
	conn, err = db.Conn(context.Background())
	if err != nil {
		p.mu.Lock()
		p.reserved--
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to obtain connection from pool: %w", err)
	}

	// Initialize the session connection with username-specific state if needed.
	// Since the DB is shared, we must set session-local parameters here.
	initSearchPath(conn, username)

	stop := server.StartCredentialRefresh(db, p.cfg.DuckLake)

	token := generateSessionToken()
	session := &Session{
		ID:        token,
		DB:        db,
		Conn:      conn,
		Username:  username,
		CreatedAt: time.Now(),
		queries:   make(map[string]*QueryHandle),
		txns:      make(map[string]*trackedTx),
		txnOwner:  make(map[string]string),
	}
	session.lastUsed.Store(time.Now().UnixNano())

	p.mu.Lock()
	p.reserved--
	p.sessions[token] = session
	p.stopRefresh[token] = stop
	p.mu.Unlock()

	slog.Debug("Created DuckDB session", "user", username)
	return session, nil
}

// GetSession returns a session by token.
func (p *SessionPool) GetSession(token string) (*Session, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.sessions[token]
	return s, ok
}

// DestroySession closes and removes a session.
func (p *SessionPool) DestroySession(token string) error {
	p.mu.Lock()
	session, ok := p.sessions[token]
	stop := p.stopRefresh[token]
	if ok {
		delete(p.sessions, token)
		delete(p.stopRefresh, token)
	}
	p.mu.Unlock()

	if !ok {
		return fmt.Errorf("session not found")
	}

	// Roll back any open transactions
	session.mu.Lock()
	for id, ttx := range session.txns {
		_ = ttx.tx.Rollback()
		delete(session.txns, id)
		delete(session.txnOwner, id)
	}
	session.mu.Unlock()

	if stop != nil {
		stop()
	}
	if session.Conn != nil {
		// Drop temporary tables before returning the connection to the pool.
		// sql.Conn.Close() returns the underlying driver connection to sql.DB's
		// pool rather than closing it. DuckDB temp tables are connection-scoped,
		// so they'd leak into the next session that gets the same connection.
		cleanupSessionState(session.Conn)
		_ = session.Conn.Close()
	}
	// Do NOT close session.DB if it is a shared DB (warmup or fallback)
	p.mu.RLock()
	isShared := session.DB == p.warmupDB || session.DB == p.fallbackDB
	p.mu.RUnlock()

	if session.DB != nil && !isShared {
		if err := session.DB.Close(); err != nil {
			slog.Warn("Failed to close session database", "error", err)
		}
	}

	slog.Debug("Destroyed DuckDB session", "user", session.Username)
	return nil
}

// ActiveSessions returns the number of active sessions.
func (p *SessionPool) ActiveSessions() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.sessions)
}

// CloseAll closes all sessions.
func (p *SessionPool) CloseAll() {
	close(p.stopCh)
	p.mu.Lock()
	sessions := make(map[string]*Session, len(p.sessions))
	stops := make(map[string]func(), len(p.stopRefresh))
	for k, v := range p.sessions {
		sessions[k] = v
	}
	for k, v := range p.stopRefresh {
		stops[k] = v
	}
	p.sessions = make(map[string]*Session)
	p.stopRefresh = make(map[string]func())
	p.mu.Unlock()

	for _, stop := range stops {
		if stop != nil {
			stop()
		}
	}

	for _, session := range sessions {
		session.mu.Lock()
		for id, ttx := range session.txns {
			_ = ttx.tx.Rollback()
			delete(session.txns, id)
		}
		session.mu.Unlock()

		if session.Conn != nil {
			_ = session.Conn.Close()
		}
		if session.DB != nil && session.DB != p.warmupDB && session.DB != p.fallbackDB {
			_ = session.DB.Close()
		}
	}

	if p.warmupDB != nil {
		_ = p.warmupDB.Close()
	}
	if p.fallbackDB != nil && p.fallbackDB != p.warmupDB {
		_ = p.fallbackDB.Close()
	}
}

// cleanupSessionState drops temporary tables and views on the connection so
// that session-scoped state doesn't leak when the connection is returned to
// the pool.
func cleanupSessionState(conn *sql.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Drop temporary tables
	dropTemporary(ctx, conn,
		"SELECT table_name FROM duckdb_tables() WHERE temporary = true",
		`DROP TABLE IF EXISTS temp."%s"`,
	)

	// Drop temporary views
	dropTemporary(ctx, conn,
		"SELECT view_name FROM duckdb_views() WHERE temporary = true",
		`DROP VIEW IF EXISTS temp."%s"`,
	)
}

func dropTemporary(ctx context.Context, conn *sql.Conn, query, dropFmt string) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		slog.Warn("Failed to query temporary objects for cleanup.", "error", err)
		return
	}
	var names []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		slog.Warn("Error iterating temporary objects for cleanup.", "error", err)
	}
	_ = rows.Close()

	for _, name := range names {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(dropFmt, name)); err != nil {
			slog.Warn("Failed to drop temporary object during cleanup.", "name", name, "error", err)
		}
	}
}

// initSearchPath sets the DuckDB search_path for a session connection.
// It tries to include the user's schema first; if that schema doesn't exist,
// it falls back to just 'main' (DuckDB's default schema).
func initSearchPath(conn *sql.Conn, username string) {
	if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET search_path = '%s,main'", username)); err != nil {
		slog.Debug("User schema not found, using default search_path.", "user", username)
		// Clear the aborted transaction state before retrying.
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		if _, err := conn.ExecContext(context.Background(), "SET search_path = 'main'"); err != nil {
			slog.Warn("Failed to set search_path for session.", "user", username, "error", err)
		}
	}
}

func generateSessionToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("failed to generate session token: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// customActionServer wraps a flight.FlightServer to add custom DoAction
// handlers while preserving the standard flightsql routing for all other
// methods and standard Flight SQL action types.
type customActionServer struct {
	flight.FlightServer
	handler *FlightSQLHandler
}

func (s *customActionServer) DoAction(cmd *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch cmd.Type {
	case "CreateSession":
		return s.handler.doCreateSession(cmd.Body, stream)
	case "DestroySession":
		return s.handler.doDestroySession(cmd.Body, stream)
	case "HealthCheck":
		return s.handler.doHealthCheck(stream)
	default:
		// Fall through to standard flightsql action router (BeginTransaction, etc.)
		return s.FlightServer.DoAction(cmd, stream)
	}
}

// NewFlightSQLHandler creates a new multi-session Flight SQL handler.
func NewFlightSQLHandler(pool *SessionPool) *FlightSQLHandler {
	h := &FlightSQLHandler{
		pool:  pool,
		alloc: memory.DefaultAllocator,
	}

	if err := h.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "duckgres"); err != nil {
		panic(fmt.Sprintf("register sql info server name: %v", err))
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, "1.0.0"); err != nil {
		panic(fmt.Sprintf("register sql info server version: %v", err))
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoTransactionsSupported, true); err != nil {
		panic(fmt.Sprintf("register sql info transactions supported: %v", err))
	}

	return h
}
