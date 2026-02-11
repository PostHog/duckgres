package duckdbservice

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DuckDBService is a standalone Arrow Flight SQL service backed by DuckDB.
type DuckDBService struct {
	cfg       ServiceConfig
	pool      *SessionPool
	startTime time.Time
	flightSrv flight.Server
}

// SessionPool manages multiple DuckDB sessions keyed by session token.
type SessionPool struct {
	mu          sync.RWMutex
	sessions    map[string]*Session
	stopRefresh map[string]func()
	duckLakeSem chan struct{}
	cfg         server.Config
	startTime   time.Time
	maxSessions int
}

// Session represents a single DuckDB session.
type Session struct {
	ID        string
	DB        *sql.DB
	Username  string
	CreatedAt time.Time

	mu            sync.RWMutex
	queries       map[string]*QueryHandle
	txns          map[string]*sql.Tx
	txnOwner      map[string]string
	handleCounter atomic.Uint64
}

// QueryHandle stores a prepared or ad-hoc query for later execution.
type QueryHandle struct {
	Query string
	TxnID string
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
	}

	return &DuckDBService{
		cfg:       cfg,
		pool:      pool,
		startTime: time.Now(),
	}
}

// Run starts the DuckDB service, blocking until shutdown.
func Run(cfg ServiceConfig) {
	svc := NewDuckDBService(cfg)

	network, addr, err := ParseListenAddr(cfg.ListenAddr)
	if err != nil {
		slog.Error("Invalid listen address", "error", err)
		os.Exit(1)
	}

	// Clean up stale unix socket
	if network == "unix" {
		_ = os.Remove(addr)
	}

	listener, err := net.Listen(network, addr)
	if err != nil {
		slog.Error("Failed to listen", "network", network, "addr", addr, "error", err)
		os.Exit(1)
	}

	slog.Info("Starting DuckDB service", "network", network, "addr", addr)

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
	if svc.cfg.BearerToken != "" {
		opts = append(opts,
			grpc.ChainUnaryInterceptor(BearerTokenUnaryInterceptor(svc.cfg.BearerToken)),
			grpc.ChainStreamInterceptor(BearerTokenStreamInterceptor(svc.cfg.BearerToken)),
		)
	}

	svc.flightSrv = flight.NewServerWithMiddleware(nil, opts...)
	svc.flightSrv.RegisterFlightService(flightsql.NewFlightServer(handler))
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
	p.mu.Lock()
	if p.maxSessions > 0 && len(p.sessions) >= p.maxSessions {
		p.mu.Unlock()
		return nil, fmt.Errorf("max sessions reached (%d)", p.maxSessions)
	}
	p.mu.Unlock()

	db, err := server.CreateDBConnection(p.cfg, p.duckLakeSem, username, p.startTime)
	if err != nil {
		return nil, fmt.Errorf("create session db: %w", err)
	}

	stop := server.StartCredentialRefresh(db, p.cfg.DuckLake)

	token := generateSessionToken()
	session := &Session{
		ID:        token,
		DB:        db,
		Username:  username,
		CreatedAt: time.Now(),
		queries:   make(map[string]*QueryHandle),
		txns:      make(map[string]*sql.Tx),
		txnOwner:  make(map[string]string),
	}

	p.mu.Lock()
	p.sessions[token] = session
	p.stopRefresh[token] = stop
	p.mu.Unlock()

	slog.Debug("Created DuckDB session", "session", token, "user", username)
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
		return fmt.Errorf("session not found: %s", token)
	}

	// Roll back any open transactions
	session.mu.Lock()
	for id, tx := range session.txns {
		_ = tx.Rollback()
		delete(session.txns, id)
		delete(session.txnOwner, id)
	}
	session.mu.Unlock()

	if stop != nil {
		stop()
	}
	if session.DB != nil {
		if err := session.DB.Close(); err != nil {
			slog.Warn("Failed to close session database", "session", token, "error", err)
		}
	}

	slog.Debug("Destroyed DuckDB session", "session", token)
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

	for token, session := range sessions {
		session.mu.Lock()
		for id, tx := range session.txns {
			_ = tx.Rollback()
			delete(session.txns, id)
		}
		session.mu.Unlock()

		if session.DB != nil {
			if err := session.DB.Close(); err != nil {
				slog.Warn("Failed to close session database during shutdown", "session", token, "error", err)
			}
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

// DoAction handles custom Flight actions for session management.
func (h *FlightSQLHandler) DoAction(cmd *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch cmd.Type {
	case "CreateSession":
		return h.doCreateSession(cmd.Body, stream)
	case "DestroySession":
		return h.doDestroySession(cmd.Body, stream)
	case "HealthCheck":
		return h.doHealthCheck(stream)
	default:
		return status.Errorf(codes.Unimplemented, "unknown action type: %s", cmd.Type)
	}
}

func (h *FlightSQLHandler) doCreateSession(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		Username string `json:"username"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("invalid CreateSession request: %w", err)
	}
	if req.Username == "" {
		return fmt.Errorf("username is required")
	}

	session, err := h.pool.CreateSession(req.Username)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	resp, _ := json.Marshal(map[string]string{
		"session_token": session.ID,
	})
	return stream.Send(&flight.Result{Body: resp})
}

func (h *FlightSQLHandler) doDestroySession(body []byte, stream flight.FlightService_DoActionServer) error {
	var req struct {
		SessionToken string `json:"session_token"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("invalid DestroySession request: %w", err)
	}
	if req.SessionToken == "" {
		return fmt.Errorf("session_token is required")
	}

	if err := h.pool.DestroySession(req.SessionToken); err != nil {
		return err
	}

	resp, _ := json.Marshal(map[string]bool{"ok": true})
	return stream.Send(&flight.Result{Body: resp})
}

func (h *FlightSQLHandler) doHealthCheck(stream flight.FlightService_DoActionServer) error {
	resp, _ := json.Marshal(map[string]interface{}{
		"healthy":   true,
		"sessions":  h.pool.ActiveSessions(),
		"uptime_ns": time.Since(h.pool.startTime).Nanoseconds(),
	})
	return stream.Send(&flight.Result{Body: resp})
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

// sessionFromContext extracts the session from gRPC metadata.
// The session token is expected in the "x-duckgres-session" header.
func (h *FlightSQLHandler) sessionFromContext(ctx context.Context) (*Session, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing metadata")
	}

	tokens := md.Get("x-duckgres-session")
	if len(tokens) == 0 {
		return nil, fmt.Errorf("missing x-duckgres-session header")
	}

	session, ok := h.pool.GetSession(tokens[0])
	if !ok {
		return nil, fmt.Errorf("session not found")
	}

	return session, nil
}
