package duckdbservice

import (
	"context"
	"crypto/rand"
	"crypto/tls"
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
	"google.golang.org/grpc/credentials"
)

var bootstrapBundledExtensions = server.BootstrapBundledExtensions
var exitProcess = os.Exit

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
	warmupDB    *sql.DB       // Keep this open to keep shared cache alive
	warmupDone  chan struct{} // Closed when Warmup() completes (success or failure)
	dbInitOnce  sync.Once     // Serializes fallback CreateDBConnection when warmup fails
	fallbackDB  *sql.DB       // Lazily created if warmup fails
	fallbackErr error         // Cached error from fallback creation

	sharedWarmMode       bool
	activation           *activatedTenantRuntime
	ownerEpoch           int64
	ownerCPInstanceID    string
	workerID             int
	activateTenantFunc   func(ActivationPayload) error
	createDBConnection   func(server.Config, chan struct{}, string, time.Time, string) (*sql.DB, error)
	activateDBConnection func(*sql.DB, server.Config, chan struct{}, string) error
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

	// sqlTxActive tracks whether a SQL-level transaction is in progress on this
	// session's Conn (i.e., BEGIN was sent as raw SQL without a corresponding
	// COMMIT/ROLLBACK). This is distinct from Flight SQL protocol-level
	// transactions tracked in txns. Used to prevent retryOnTransient from
	// retrying statements inside a user-managed transaction — a retry after a
	// transient error would run in autocommit mode (the transaction is dead)
	// and mask the failure from the client.
	sqlTxActive atomic.Bool

	duckdbConn duckdbConnHandle // raw handle for progress polling (zero if extraction failed)
	progress   progressState    // stall detection state
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
		sessions:             make(map[string]*Session),
		stopRefresh:          make(map[string]func()),
		duckLakeSem:          make(chan struct{}, 1),
		cfg:                  cfg.ServerConfig,
		startTime:            time.Now(),
		maxSessions:          cfg.MaxSessions,
		stopCh:               make(chan struct{}),
		warmupDone:           make(chan struct{}),
		sharedWarmMode:       cfg.RequireActivation || sharedWarmWorkerEnabled(),
		createDBConnection:   server.CreateDBConnection,
		activateDBConnection: server.ActivateDBConnection,
	}
	pool.activateTenantFunc = pool.activateTenant
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

	start := time.Now()
	slog.Info("Pre-warming worker DuckDB instance...")

	// Wait for the local cache proxy to be ready before serving queries.
	// When DUCKGRES_CACHE_PROXY_ADDR is set, DuckDB will route S3 traffic
	// through it — worker startup must block until the proxy is healthy.
	// Included in the pre-warm duration so slow proxy startup is visible.
	waitForCacheProxy()
	// Use a system-level username for warmup
	db, err := p.createDBConnection(p.sharedWarmupConfig(), p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
	if err != nil {
		return fmt.Errorf("warmup failed after %v: %w", time.Since(start), err)
	}

	p.mu.Lock()
	p.warmupDB = db
	p.mu.Unlock()

	slog.Info("Worker pre-warmed successfully.", "duration", time.Since(start))
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

	if err := bootstrapBundledExtensions(cfg.ServerConfig.DataDir); err != nil {
		slog.Error("Failed to bootstrap bundled DuckDB extensions.",
			"source", "/app/extensions",
			"extension_directory", cfg.ServerConfig.DataDir+"/extensions",
			"error", err)
		exitProcess(1)
	}

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
	if listener.Addr() != nil && listener.Addr().Network() == "tcp" &&
		svc.cfg.ServerConfig.TLSCertFile != "" && svc.cfg.ServerConfig.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(svc.cfg.ServerConfig.TLSCertFile, svc.cfg.ServerConfig.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("load worker RPC TLS certificates: %w", err)
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})))
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
func (p *SessionPool) CreateSession(username, memoryLimit string, threads int) (*Session, error) {
	start := time.Now()
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
	waitStart := time.Now()
	<-p.warmupDone
	if waitDur := time.Since(waitStart); waitDur > 100*time.Millisecond {
		slog.Info("Waited for warmup to complete.", "duration", waitDur)
	}

	// Use shared warmupDB if available (highly preferred for performance)
	db = p.activeSharedDB()
	if db == nil {
		p.mu.RLock()
		db = p.warmupDB
		p.mu.RUnlock()
	}

	if db == nil {
		cfg, cfgErr := p.currentSessionConfig()
		if cfgErr != nil {
			p.mu.Lock()
			p.reserved--
			p.mu.Unlock()
			return nil, cfgErr
		}

		// Fallback: create a shared DB if warmup failed or wasn't run.
		// Uses sync.Once to ensure only one goroutine loads native extensions;
		// concurrent LOAD of the same C++ extension corrupts the heap.
		fallbackStart := time.Now()
		p.dbInitOnce.Do(func() {
			p.fallbackDB, p.fallbackErr = p.createDBConnection(cfg, p.duckLakeSem, "duckgres", p.startTime, server.ProcessVersion())
		})
		if p.fallbackErr != nil {
			p.mu.Lock()
			p.reserved--
			p.mu.Unlock()
			return nil, fmt.Errorf("create session db: %w", p.fallbackErr)
		}
		db = p.fallbackDB
		slog.Info("Using fallback DB (warmup failed).", "duration", time.Since(fallbackStart))
	}

	// Obtain a dedicated connection for this session to ensure isolation
	// and consistent session settings (search_path, etc.)
	// Use a timeout to prevent indefinite blocking when a previous session's
	// cleanup hasn't returned its connection to the pool yet.
	slog.Debug("Acquiring DB connection from pool.", "user", username)
	connCtx, connCancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err = db.Conn(connCtx)
	connCancel()
	if err != nil {
		p.mu.Lock()
		p.reserved--
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to obtain connection from pool (timeout after 30s): %w", err)
	}
	slog.Debug("Acquired DB connection from pool.", "user", username, "duration", time.Since(start))

	// Initialize the session connection with username-specific state if needed.
	// Since the DB is shared, we must set session-local parameters here.
	initSearchPath(conn, username)

	// Apply initial resource limits if provided (optimizes handshake by avoiding
	// roundtrips from control plane).
	if memoryLimit != "" {
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET memory_limit = '%s'", memoryLimit)); err != nil {
			slog.Warn("Failed to set initial memory_limit.", "user", username, "error", err)
		}
	}
	if threads > 0 {
		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET threads = %d", threads)); err != nil {
			slog.Warn("Failed to set initial threads.", "user", username, "error", err)
		}
	}

	// Extract the raw DuckDB connection handle for progress polling.
	// Non-fatal if extraction fails — progress monitoring will be unavailable
	// for this session but everything else works normally.
	var duckConn duckdbConnHandle
	if dc, err := extractDuckDBConnection(conn); err != nil {
		slog.Debug("Could not extract DuckDB connection for progress polling.", "user", username, "error", err)
	} else {
		duckConn = dc
	}

	token := generateSessionToken()
	session := &Session{
		ID:         token,
		DB:         db,
		Conn:       conn,
		Username:   username,
		CreatedAt:  time.Now(),
		queries:    make(map[string]*QueryHandle),
		txns:       make(map[string]*trackedTx),
		txnOwner:   make(map[string]string),
		duckdbConn: duckConn,
	}
	session.lastUsed.Store(time.Now().UnixNano())

	cfg, cfgErr := p.currentSessionConfig()
	if cfgErr != nil {
		_ = conn.Close()
		p.mu.Lock()
		p.reserved--
		p.mu.Unlock()
		return nil, cfgErr
	}

	stop := server.StartCredentialRefresh(conn, cfg.DuckLake, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		return len(session.txns) > 0
	})

	p.mu.Lock()
	p.reserved--
	p.sessions[token] = session
	p.stopRefresh[token] = stop
	p.mu.Unlock()

	slog.Debug("Created DuckDB session", "user", username, "duration", time.Since(start))
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
		cleanupStart := time.Now()
		slog.Debug("Cleaning up session state.", "user", session.Username)
		cleanupSessionState(session.Conn)
		slog.Debug("Session state cleaned up.", "user", session.Username, "duration", time.Since(cleanupStart))
		connCloseStart := time.Now()
		_ = session.Conn.Close()
		slog.Debug("Session connection closed (returned to pool).", "user", session.Username, "duration", time.Since(connCloseStart))
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
		cleanupWorkerCatalogs(p.warmupDB)
		_ = p.warmupDB.Close()
	}
	if p.fallbackDB != nil && p.fallbackDB != p.warmupDB {
		cleanupWorkerCatalogs(p.fallbackDB)
		_ = p.fallbackDB.Close()
	}
	if p.activation != nil && p.activation.db != nil && p.activation.db != p.warmupDB && p.activation.db != p.fallbackDB {
		cleanupWorkerCatalogs(p.activation.db)
		_ = p.activation.db.Close()
	}
}

// cleanupWorkerCatalogs detaches the lake catalogs that are actually attached
// on db before shutdown. Probing duckdb_databases() avoids spurious "failed to
// detach" warnings on the warmup/fallback DBs that never had a tenant catalog
// attached, while still cleaning up the activation DB that does.
func cleanupWorkerCatalogs(db *sql.DB) {
	if db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT database_name FROM duckdb_databases() WHERE database_name IN ('delta', 'ducklake')")
	if err != nil {
		slog.Debug("Failed to list attached catalogs during shutdown.", "error", err)
		return
	}
	attached := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			slog.Debug("Failed to scan attached catalog name during shutdown.", "error", err)
			return
		}
		attached[name] = true
	}
	_ = rows.Close()

	if !attached["delta"] && !attached["ducklake"] {
		return
	}
	if _, err := db.ExecContext(ctx, "USE memory"); err != nil {
		slog.Warn("Failed to switch worker DB to memory during shutdown.", "error", err)
		return
	}
	if attached["delta"] {
		if _, err := db.ExecContext(ctx, "DETACH delta"); err != nil {
			slog.Warn("Failed to detach worker Delta catalog during shutdown.", "error", err)
		}
	}
	if attached["ducklake"] {
		if _, err := db.ExecContext(ctx, "DETACH ducklake"); err != nil {
			slog.Warn("Failed to detach worker DuckLake catalog during shutdown.", "error", err)
		}
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
// it falls back to just 'main'.
//
// memory.main is always included so that pg_catalog macros (pg_get_userbyid,
// format_type, etc.) remain resolvable when the default catalog is ducklake.
// Without it, DuckDB restricts function resolution to the ducklake catalog
// and psql commands like \dt fail.
func initSearchPath(conn *sql.Conn, username string) {
	if _, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET search_path = '%s,main,memory.main'", username)); err != nil {
		slog.Debug("User schema not found, using default search_path.", "user", username)
		// Clear the aborted transaction state before retrying.
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		if _, err := conn.ExecContext(context.Background(), "SET search_path = 'main,memory.main'"); err != nil {
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
	case "ActivateTenant":
		return s.handler.doActivateTenant(cmd.Body, stream)
	case "DestroySession":
		return s.handler.doDestroySession(cmd.Body, stream)
	case "HealthCheck":
		return s.handler.doHealthCheck(cmd.Body, stream)
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
