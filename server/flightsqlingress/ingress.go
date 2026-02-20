package flightsqlingress

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/duckdbservice"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	flightBatchSize               = 1024
	defaultFlightSessionIdleTTL   = 10 * time.Minute
	defaultFlightSessionReapTick  = 1 * time.Minute
	defaultFlightHandleIdleTTL    = 15 * time.Minute
	defaultFlightSessionTokenTTL  = 1 * time.Hour
	defaultFlightSessionHeaderKey = "x-duckgres-session"
)

const (
	ReapTriggerPeriodic = "periodic"
	ReapTriggerForced   = "forced"
)

type Config struct {
	SessionIdleTTL     time.Duration
	SessionReapTick    time.Duration
	HandleIdleTTL      time.Duration
	SessionTokenTTL    time.Duration
	WorkerQueueTimeout time.Duration // applied to CreateSession calls; 0 = use request context as-is
}

type SessionProvider interface {
	CreateSession(context.Context, string) (int32, *server.FlightExecutor, error)
	DestroySession(int32)
}

type Hooks struct {
	OnSessionCountChanged func(int)
	OnSessionsReaped      func(trigger string, count int)
}

type Options struct {
	Hooks       Hooks
	RateLimiter *server.RateLimiter
}

// FlightIngress serves Arrow Flight SQL on the control plane with Basic auth.
// It reuses worker sessions via SessionProvider.
type FlightIngress struct {
	flightSrv     flight.Server
	listener      net.Listener
	sessionStore  *flightAuthSessionStore
	listenerAddr  string
	shutdownOnce  sync.Once
	shutdownState atomic.Bool
	wg            sync.WaitGroup
}

func NewFlightIngress(host string, port int, tlsConfig *tls.Config, users map[string]string, provider SessionProvider, cfg Config, opts Options) (*FlightIngress, error) {
	if port <= 0 {
		return nil, fmt.Errorf("invalid flight port: %d", port)
	}
	if tlsConfig == nil {
		return nil, fmt.Errorf("TLS config is required for Flight ingress")
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	// gRPC requires ALPN "h2" for TLS transport.
	flightTLSConfig := tlsConfig.Clone()
	if !containsString(flightTLSConfig.NextProtos, "h2") {
		flightTLSConfig.NextProtos = append(flightTLSConfig.NextProtos, "h2")
	}

	ln, err := tls.Listen("tcp", addr, flightTLSConfig)
	if err != nil {
		return nil, fmt.Errorf("flight listen %s: %w", addr, err)
	}

	if cfg.SessionIdleTTL <= 0 {
		cfg.SessionIdleTTL = defaultFlightSessionIdleTTL
	}
	if cfg.SessionReapTick <= 0 {
		cfg.SessionReapTick = defaultFlightSessionReapTick
	}
	if cfg.HandleIdleTTL <= 0 {
		cfg.HandleIdleTTL = defaultFlightHandleIdleTTL
	}
	if cfg.SessionTokenTTL <= 0 {
		cfg.SessionTokenTTL = defaultFlightSessionTokenTTL
	}

	store := newFlightAuthSessionStore(provider, cfg.SessionIdleTTL, cfg.SessionReapTick, cfg.HandleIdleTTL, cfg.SessionTokenTTL, cfg.WorkerQueueTimeout, opts)
	handler, err := NewControlPlaneFlightSQLHandler(store, users)
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	handler.rateLimiter = opts.RateLimiter

	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(server.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(server.MaxGRPCMessageSize),
	}

	srv := flight.NewServerWithMiddleware(nil, grpcOpts...)
	srv.RegisterFlightService(flightsql.NewFlightServer(handler))
	srv.InitListener(ln)

	return &FlightIngress{
		flightSrv:    srv,
		listener:     ln,
		sessionStore: store,
		listenerAddr: ln.Addr().String(),
	}, nil
}

// Addr returns the bound listener address.
func (fi *FlightIngress) Addr() string {
	return fi.listenerAddr
}

// Start begins serving in the background.
func (fi *FlightIngress) Start() {
	fi.wg.Add(1)
	go func() {
		defer fi.wg.Done()
		if err := fi.flightSrv.Serve(); err != nil && !fi.shutdownState.Load() {
			slog.Error("Flight ingress server exited.", "error", err)
		}
	}()
}

// Shutdown stops accepting new Flight connections and cleans up sessions.
func (fi *FlightIngress) Shutdown() {
	if fi == nil {
		return
	}
	fi.shutdownOnce.Do(func() {
		fi.shutdownState.Store(true)
		if fi.listener != nil {
			_ = fi.listener.Close()
		}
		if fi.flightSrv != nil {
			fi.flightSrv.Shutdown()
		}
		if fi.sessionStore != nil {
			fi.sessionStore.Close()
		}
		fi.wg.Wait()
	})
}

// ControlPlaneFlightSQLHandler implements Flight SQL over control-plane sessions.
type ControlPlaneFlightSQLHandler struct {
	flightsql.BaseServer
	users       map[string]string
	sessions    *flightAuthSessionStore
	rateLimiter *server.RateLimiter
	alloc       memory.Allocator
}

func NewControlPlaneFlightSQLHandler(sessions *flightAuthSessionStore, users map[string]string) (*ControlPlaneFlightSQLHandler, error) {
	h := &ControlPlaneFlightSQLHandler{
		users:    users,
		sessions: sessions,
		alloc:    memory.DefaultAllocator,
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "duckgres-control-plane"); err != nil {
		return nil, fmt.Errorf("register sql info server name: %w", err)
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerVersion, "1.0.0"); err != nil {
		return nil, fmt.Errorf("register sql info server version: %w", err)
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoTransactionsSupported, true); err != nil {
		return nil, fmt.Errorf("register sql info transactions supported: %w", err)
	}
	return h, nil
}

func (h *ControlPlaneFlightSQLHandler) sessionFromContext(ctx context.Context) (*flightClientSession, error) {
	return h.sessionFromContextWithTokenMetadata(ctx, true)
}

func (h *ControlPlaneFlightSQLHandler) sessionFromContextWithTokenMetadata(ctx context.Context, emitTokenMetadata bool) (*flightClientSession, error) {
	var remoteAddr net.Addr
	if p, ok := peer.FromContext(ctx); ok && p != nil {
		remoteAddr = p.Addr
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		server.RecordFailedAuthAttempt(h.rateLimiter, remoteAddr)
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	if h.sessions == nil {
		return nil, status.Error(codes.Unavailable, "session store is not configured")
	}

	if sessionToken := incomingSessionToken(md); sessionToken != "" {
		s, ok := h.sessions.GetByToken(sessionToken)
		if !ok {
			server.RecordFailedAuthAttempt(h.rateLimiter, remoteAddr)
			return nil, status.Error(codes.Unauthenticated, "session not found")
		}

		// When Basic auth is included alongside a bearer session token, enforce
		// principal consistency. Token-only auth is allowed after bootstrap.
		if hasAuthorizationHeader(md) {
			username, err := h.authenticateBasicCredentials(md, remoteAddr)
			if err != nil {
				return nil, err
			}
			if username != s.username {
				server.RecordFailedAuthAttempt(h.rateLimiter, remoteAddr)
				return nil, status.Error(codes.PermissionDenied, "session token does not match authenticated user")
			}
		}

		if emitTokenMetadata {
			setSessionTokenMetadata(ctx, sessionToken)
		}
		s.touch()
		return s, nil
	}

	// Bootstrap requires Basic auth and is subject to auth rate limiting.
	releaseRateLimit, rejectReason := server.BeginRateLimitedAuthAttempt(h.rateLimiter, remoteAddr)
	defer releaseRateLimit()
	if rejectReason != "" {
		slog.Warn("Flight auth rejected by rate limit policy.", "remote_addr", remoteAddr, "reason", rejectReason)
		return nil, status.Error(codes.ResourceExhausted, "authentication rate limit exceeded")
	}

	username, err := h.authenticateBasicCredentials(md, remoteAddr)
	if err != nil {
		return nil, err
	}

	s, err := h.sessions.Create(ctx, username)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "create bootstrap session: %v", err)
	}

	setSessionTokenMetadata(ctx, s.token)
	s.touch()
	return s, nil
}

func hasAuthorizationHeader(md metadata.MD) bool {
	return len(md.Get("authorization")) > 0
}

func (h *ControlPlaneFlightSQLHandler) authenticateBasicCredentials(md metadata.MD, remoteAddr net.Addr) (string, error) {
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		server.RecordFailedAuthAttempt(h.rateLimiter, remoteAddr)
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	username, password, err := parseBasicCredentials(authHeaders[0])
	if err != nil {
		server.RecordFailedAuthAttempt(h.rateLimiter, remoteAddr)
		return "", status.Error(codes.Unauthenticated, err.Error())
	}

	if !server.ValidateUserPassword(h.users, username, password) {
		banned := server.RecordFailedAuthAttempt(h.rateLimiter, remoteAddr)
		if banned {
			slog.Warn("Flight client IP banned after auth failures.", "remote_addr", remoteAddr)
		}
		return "", status.Error(codes.Unauthenticated, "invalid credentials")
	}
	server.RecordSuccessfulAuthAttempt(h.rateLimiter, remoteAddr)
	return username, nil
}

func incomingSessionToken(md metadata.MD) string {
	values := md.Get(defaultFlightSessionHeaderKey)
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func setSessionTokenMetadata(ctx context.Context, sessionToken string) {
	if sessionToken == "" {
		return
	}
	md := metadata.Pairs(defaultFlightSessionHeaderKey, sessionToken)
	_ = grpc.SetHeader(ctx, md)
	_ = grpc.SetTrailer(ctx, md)
}

func (h *ControlPlaneFlightSQLHandler) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	txnKey, err := s.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return nil, err
	}

	query := cmd.GetQuery()
	schema, err := getQuerySchema(ctx, s, query)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to prepare query: %v", err)
	}

	handleID := s.nextHandle("query")
	s.addQuery(handleID, &flightQueryHandle{
		Query:  query,
		Schema: schema,
		TxnID:  txnKey,
	})

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handleID))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create ticket: %v", err)
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticketBytes},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *ControlPlaneFlightSQLHandler) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	handleID := string(ticket.GetStatementHandle())
	handle, ok := s.getQuery(handleID)
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "query handle not found")
	}
	if handle.TxnID != "" && !s.hasTxn(handle.TxnID) {
		return nil, nil, status.Error(codes.NotFound, "transaction not found")
	}

	rows, err := s.query(ctx, handle.Query)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "failed to execute query: %v", err)
	}

	ch := make(chan flight.StreamChunk, 10)
	s.beginStream()
	go func() {
		defer close(ch)
		defer func() {
			s.endStream()
			_ = rows.Close()
			s.deleteQuery(handleID)
		}()

		for {
			record, recErr := rowSetToRecord(h.alloc, rows, handle.Schema, flightBatchSize)
			if recErr != nil {
				_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: recErr})
				return
			}
			if record == nil {
				return
			}
			if !sendStreamChunk(ctx, ch, flight.StreamChunk{Data: record}) {
				record.Release()
				return
			}
		}
	}()

	return handle.Schema, ch, nil
}

func (h *ControlPlaneFlightSQLHandler) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return 0, err
	}

	if _, err := s.getOpenTxn(cmd.GetTransactionId()); err != nil {
		return 0, err
	}

	res, err := s.exec(ctx, cmd.GetQuery())
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "failed to execute update: %v", err)
	}

	return rowsAffectedOrError(res)
}

func rowsAffectedOrError(res server.ExecResult) (int64, error) {
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, status.Errorf(codes.Internal, "failed to fetch affected row count: %v", err)
	}
	return affected, nil
}

func (h *ControlPlaneFlightSQLHandler) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	_ = req

	if s.txnCount() > 0 {
		return nil, status.Error(codes.FailedPrecondition, "transaction already active")
	}

	if _, err := s.exec(ctx, "BEGIN TRANSACTION"); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	txnKey := s.nextHandle("txn")
	s.addTxn(txnKey)
	return []byte(txnKey), nil
}

func (h *ControlPlaneFlightSQLHandler) EndTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}

	txnKey := string(req.GetTransactionId())
	if txnKey == "" {
		return status.Error(codes.InvalidArgument, "missing transaction id")
	}
	if !s.hasTxn(txnKey) {
		return status.Error(codes.NotFound, "transaction not found")
	}

	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		if _, err := s.exec(ctx, "COMMIT"); err != nil {
			return status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
		}
	case flightsql.EndTransactionRollback:
		if _, err := s.exec(ctx, "ROLLBACK"); err != nil {
			return status.Errorf(codes.Internal, "failed to rollback transaction: %v", err)
		}
	default:
		_, _ = s.exec(ctx, "ROLLBACK")
		s.deleteTxn(txnKey)
		return status.Error(codes.InvalidArgument, "unsupported end transaction action")
	}

	s.deleteTxn(txnKey)
	return nil
}

func (h *ControlPlaneFlightSQLHandler) CloseSession(ctx context.Context, req *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	_ = req
	if h.sessions == nil {
		return nil, status.Error(codes.Unavailable, "session store is not configured")
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	sessionToken := incomingSessionToken(md)
	if sessionToken == "" {
		return nil, status.Error(codes.Unauthenticated, "missing x-duckgres-session header")
	}

	// Validate token ownership (and optional Basic-auth principal consistency)
	// before revoking.
	if _, err := h.sessionFromContextWithTokenMetadata(ctx, false); err != nil {
		return nil, err
	}

	if !h.sessions.CloseByToken(sessionToken) {
		return nil, status.Error(codes.Unauthenticated, "session not found")
	}

	return &flight.CloseSessionResult{Status: flight.CloseSessionResultClosed}, nil
}

func (h *ControlPlaneFlightSQLHandler) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	txnKey, err := s.getOpenTxn(req.GetTransactionId())
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	schema, err := getQuerySchema(ctx, s, req.GetQuery())
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.InvalidArgument, "failed to prepare query: %v", err)
	}

	handleID := s.nextHandle("prep")
	s.addQuery(handleID, &flightQueryHandle{
		Query:  req.GetQuery(),
		Schema: schema,
		TxnID:  txnKey,
	})

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        []byte(handleID),
		DatasetSchema: schema,
	}, nil
}

func (h *ControlPlaneFlightSQLHandler) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}
	s.deleteQuery(string(req.GetPreparedStatementHandle()))
	return nil
}

func (h *ControlPlaneFlightSQLHandler) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	handleID := string(cmd.GetPreparedStatementHandle())
	handle, ok := s.getQuery(handleID)
	if !ok {
		return nil, status.Error(codes.NotFound, "prepared statement not found")
	}
	if handle.TxnID != "" && !s.hasTxn(handle.TxnID) {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(handle.Schema, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(handleID)},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *ControlPlaneFlightSQLHandler) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	handleID := string(cmd.GetPreparedStatementHandle())
	handle, ok := s.getQuery(handleID)
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "prepared statement not found")
	}
	if handle.TxnID != "" && !s.hasTxn(handle.TxnID) {
		return nil, nil, status.Error(codes.NotFound, "transaction not found")
	}

	rows, err := s.query(ctx, handle.Query)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "failed to execute prepared statement: %v", err)
	}

	ch := make(chan flight.StreamChunk, 10)
	s.beginStream()
	go func() {
		defer close(ch)
		defer func() {
			s.endStream()
			_ = rows.Close()
		}()

		for {
			record, recErr := rowSetToRecord(h.alloc, rows, handle.Schema, flightBatchSize)
			if recErr != nil {
				_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: recErr})
				return
			}
			if record == nil {
				return
			}
			if !sendStreamChunk(ctx, ch, flight.StreamChunk{Data: record}) {
				record.Release()
				return
			}
		}
	}()

	return handle.Schema, ch, nil
}

func (h *ControlPlaneFlightSQLHandler) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if _, err := h.sessionFromContext(ctx); err != nil {
		return nil, err
	}
	_ = cmd

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema_ref.DBSchemas, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *ControlPlaneFlightSQLHandler) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	schema := schema_ref.DBSchemas
	query := "SELECT catalog_name, schema_name AS db_schema_name FROM information_schema.schemata WHERE 1=1"
	args := make([]any, 0, 2)
	if catalog := cmd.GetCatalog(); catalog != nil && *catalog != "" {
		args = append(args, *catalog)
		query += fmt.Sprintf(" AND catalog_name = $%d", len(args))
	}
	if pattern := cmd.GetDBSchemaFilterPattern(); pattern != nil && *pattern != "" {
		args = append(args, *pattern)
		query += fmt.Sprintf(" AND schema_name LIKE $%d", len(args))
	}
	query += " ORDER BY catalog_name, db_schema_name"

	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "list schemas query failed: %v", err)
	}

	ch := make(chan flight.StreamChunk, 1)
	s.beginStream()
	go func() {
		defer close(ch)
		defer func() {
			s.endStream()
			_ = rows.Close()
		}()

		for {
			record, recErr := rowSetToRecord(h.alloc, rows, schema, flightBatchSize)
			if recErr != nil {
				_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: recErr})
				return
			}
			if record == nil {
				return
			}
			if !sendStreamChunk(ctx, ch, flight.StreamChunk{Data: record}) {
				record.Release()
				return
			}
		}
	}()

	return schema, ch, nil
}

func (h *ControlPlaneFlightSQLHandler) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if _, err := h.sessionFromContext(ctx); err != nil {
		return nil, err
	}

	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, h.alloc),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

func (h *ControlPlaneFlightSQLHandler) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	s, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	schema := schema_ref.Tables
	includeSchema := cmd.GetIncludeSchema()
	if includeSchema {
		schema = schema_ref.TablesWithIncludedSchema
	}

	query := "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE 1=1"
	args := make([]any, 0, 6)
	if catalog := cmd.GetCatalog(); catalog != nil && *catalog != "" {
		args = append(args, *catalog)
		query += fmt.Sprintf(" AND table_catalog = $%d", len(args))
	}
	if pattern := cmd.GetDBSchemaFilterPattern(); pattern != nil && *pattern != "" {
		args = append(args, *pattern)
		query += fmt.Sprintf(" AND table_schema LIKE $%d", len(args))
	}
	if pattern := cmd.GetTableNameFilterPattern(); pattern != nil && *pattern != "" {
		args = append(args, *pattern)
		query += fmt.Sprintf(" AND table_name LIKE $%d", len(args))
	}
	if tableTypes := cmd.GetTableTypes(); len(tableTypes) > 0 {
		placeholders := make([]string, 0, len(tableTypes))
		for _, t := range tableTypes {
			args = append(args, t)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		query += " AND table_type IN (" + strings.Join(placeholders, ", ") + ")"
	}
	query += " ORDER BY table_catalog, table_schema, table_name"

	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "list tables query failed: %v", err)
	}

	ch := make(chan flight.StreamChunk, 1)
	s.beginStream()
	go func() {
		defer close(ch)
		defer s.endStream()
		rowsOpen := true
		closeRows := func() error {
			if !rowsOpen {
				return nil
			}
			rowsOpen = false
			return rows.Close()
		}
		defer func() {
			_ = closeRows()
		}()

		if !includeSchema {
			for {
				record, recErr := rowSetToRecord(h.alloc, rows, schema, flightBatchSize)
				if recErr != nil {
					_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: recErr})
					return
				}
				if record == nil {
					return
				}
				if !sendStreamChunk(ctx, ch, flight.StreamChunk{Data: record}) {
					return
				}
			}
		}

		type tableInfo struct {
			catalog   sql.NullString
			schema    sql.NullString
			name      string
			tableType string
		}
		tables := make([]tableInfo, 0)
		for rows.Next() {
			values := make([]any, 4)
			ptrs := make([]any, 4)
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: err})
				return
			}

			tables = append(tables, tableInfo{
				catalog:   toNullString(values[0]),
				schema:    toNullString(values[1]),
				name:      toString(values[2]),
				tableType: toString(values[3]),
			})
		}
		if err := rows.Err(); err != nil {
			_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: err})
			return
		}
		// Release the session query lock before schema lookups, which execute
		// nested queries through the same session.
		if err := closeRows(); err != nil {
			_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: err})
			return
		}

		tableSchemas, schemaLoadErr := loadTableSchemas(ctx, s, cmd)
		if schemaLoadErr != nil {
			_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: schemaLoadErr})
			return
		}

		builder := array.NewRecordBuilder(h.alloc, schema)
		defer func() {
			if builder != nil {
				builder.Release()
			}
		}()
		schemaBuilder := builder.Field(4).(*array.BinaryBuilder)
		batchCount := 0
		flush := func() bool {
			if batchCount == 0 {
				return true
			}
			record := builder.NewRecordBatch()
			if !sendStreamChunk(ctx, ch, flight.StreamChunk{Data: record}) {
				record.Release()
				return false
			}
			builder.Release()
			builder = array.NewRecordBuilder(h.alloc, schema)
			schemaBuilder = builder.Field(4).(*array.BinaryBuilder)
			batchCount = 0
			return true
		}

		for _, t := range tables {
			if t.catalog.Valid {
				builder.Field(0).(*array.StringBuilder).Append(t.catalog.String)
			} else {
				builder.Field(0).(*array.StringBuilder).AppendNull()
			}
			if t.schema.Valid {
				builder.Field(1).(*array.StringBuilder).Append(t.schema.String)
			} else {
				builder.Field(1).(*array.StringBuilder).AppendNull()
			}
			builder.Field(2).(*array.StringBuilder).Append(t.name)
			builder.Field(3).(*array.StringBuilder).Append(t.tableType)

			key := tableSchemaKey{
				catalog:    t.catalog.String,
				hasCatalog: t.catalog.Valid,
				schema:     t.schema.String,
				hasSchema:  t.schema.Valid,
				name:       t.name,
			}
			tableSchema := tableSchemas[key]
			if tableSchema == nil {
				qualified := duckdbservice.QualifyTableName(t.catalog, t.schema, t.name)
				var schemaErr error
				tableSchema, schemaErr = getQuerySchema(ctx, s, "SELECT * FROM "+qualified)
				if schemaErr != nil {
					_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: schemaErr})
					return
				}
			}
			schemaBuilder.Append(flight.SerializeSchema(tableSchema, h.alloc))
			batchCount++
			if batchCount >= flightBatchSize && !flush() {
				return
			}
		}

		_ = flush()
	}()

	return schema, ch, nil
}

type flightQueryHandle struct {
	Query    string
	Schema   *arrow.Schema
	TxnID    string
	LastUsed time.Time
}

type flightClientSession struct {
	pid      int32
	token    string
	username string
	executor *server.FlightExecutor

	lastUsed atomic.Int64
	// tokenIssuedAt stores when this token was issued; used for absolute token TTL.
	tokenIssuedAt atomic.Int64
	counter       atomic.Uint64
	streams       atomic.Int32

	opMu sync.Mutex

	mu      sync.RWMutex
	txns    map[string]struct{}
	queries map[string]*flightQueryHandle
}

func newFlightClientSession(pid int32, username string, executor *server.FlightExecutor) *flightClientSession {
	s := &flightClientSession{
		pid:      pid,
		username: username,
		executor: executor,
		txns:     make(map[string]struct{}),
		queries:  make(map[string]*flightQueryHandle),
	}
	s.touch()
	return s
}

func (s *flightClientSession) touch() {
	s.lastUsed.Store(time.Now().UnixNano())
}

func (s *flightClientSession) query(ctx context.Context, query string, args ...any) (server.RowSet, error) {
	s.touch()
	s.opMu.Lock()
	rows, err := s.executor.QueryContext(ctx, query, args...)
	if err != nil {
		s.opMu.Unlock()
		return nil, err
	}
	return &lockedRowSet{RowSet: rows, unlock: s.opMu.Unlock}, nil
}

func (s *flightClientSession) exec(ctx context.Context, query string, args ...any) (server.ExecResult, error) {
	s.touch()
	s.opMu.Lock()
	defer s.opMu.Unlock()
	return s.executor.ExecContext(ctx, query, args...)
}

func (s *flightClientSession) nextHandle(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, s.counter.Add(1))
}

func (s *flightClientSession) getOpenTxn(transactionID []byte) (string, error) {
	if len(transactionID) == 0 {
		return "", nil
	}
	txnKey := string(transactionID)
	if !s.hasTxn(txnKey) {
		return "", status.Error(codes.NotFound, "transaction not found")
	}
	return txnKey, nil
}

func (s *flightClientSession) txnCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.txns)
}

func (s *flightClientSession) hasTxn(txnKey string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.txns[txnKey]
	return ok
}

func (s *flightClientSession) addTxn(txnKey string) {
	s.mu.Lock()
	s.txns[txnKey] = struct{}{}
	s.mu.Unlock()
}

func (s *flightClientSession) deleteTxn(txnKey string) {
	s.mu.Lock()
	delete(s.txns, txnKey)
	s.mu.Unlock()
}

func (s *flightClientSession) addQuery(handleID string, q *flightQueryHandle) {
	s.mu.Lock()
	q.LastUsed = time.Now()
	s.queries[handleID] = q
	s.mu.Unlock()
}

func (s *flightClientSession) getQuery(handleID string) (*flightQueryHandle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queries[handleID]
	if ok {
		q.LastUsed = time.Now()
	}
	return q, ok
}

func (s *flightClientSession) deleteQuery(handleID string) {
	s.mu.Lock()
	delete(s.queries, handleID)
	s.mu.Unlock()
}

func (s *flightClientSession) beginStream() {
	s.streams.Add(1)
}

func (s *flightClientSession) endStream() {
	s.streams.Add(-1)
}

func (s *flightClientSession) activeStreamCount() int {
	return int(s.streams.Load())
}

func (s *flightClientSession) queryCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queries)
}

func (s *flightClientSession) reapStaleHandles(now time.Time, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, q := range s.queries {
		if !q.LastUsed.IsZero() && now.Sub(q.LastUsed) < ttl {
			continue
		}
		if q.TxnID != "" {
			if _, ok := s.txns[q.TxnID]; ok {
				continue
			}
		}
		delete(s.queries, id)
	}
}

type flightAuthSessionStore struct {
	provider           SessionProvider
	idleTTL            time.Duration
	reapInterval       time.Duration
	handleIdleTTL      time.Duration
	tokenTTL           time.Duration
	workerQueueTimeout time.Duration
	hooks              Hooks

	createSessionFn  func(context.Context, string) (int32, *server.FlightExecutor, error)
	destroySessionFn func(int32)

	mu       sync.RWMutex
	sessions map[string]*flightClientSession // session token -> session
	byKey    map[string]string               // auth bootstrap key -> session token

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

type lockedRowSet struct {
	server.RowSet
	unlock func()
	once   sync.Once
}

func (r *lockedRowSet) Close() error {
	err := r.RowSet.Close()
	r.once.Do(r.unlock)
	return err
}

func newFlightAuthSessionStore(provider SessionProvider, idleTTL, reapInterval, handleIdleTTL, tokenTTL, workerQueueTimeout time.Duration, opts Options) *flightAuthSessionStore {
	createFn := func(context.Context, string) (int32, *server.FlightExecutor, error) {
		return 0, nil, fmt.Errorf("session provider is not configured")
	}
	destroyFn := func(int32) {}
	if provider != nil {
		createFn = provider.CreateSession
		destroyFn = provider.DestroySession
	}

	s := &flightAuthSessionStore{
		provider:           provider,
		idleTTL:            idleTTL,
		reapInterval:       reapInterval,
		handleIdleTTL:      handleIdleTTL,
		tokenTTL:           tokenTTL,
		workerQueueTimeout: workerQueueTimeout,
		hooks:              opts.Hooks,
		createSessionFn:    createFn,
		destroySessionFn:   destroyFn,
		sessions:           make(map[string]*flightClientSession),
		byKey:              make(map[string]string),
		stopCh:             make(chan struct{}),
		doneCh:             make(chan struct{}),
	}
	go s.reapLoop()
	return s
}

func (s *flightAuthSessionStore) Create(ctx context.Context, username string) (*flightClientSession, error) {
	bootstrapNonce, err := generateSessionIdentityToken()
	if err != nil {
		return nil, fmt.Errorf("generate bootstrap nonce: %w", err)
	}
	key := "bootstrap|" + username + "|" + bootstrapNonce

	// 1. Try a fast acquisition first. If slots are available (busy or idle), this succeeds immediately.
	fastCtx, fastCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	sess, err := s.GetOrCreate(fastCtx, key, username)
	fastCancel()
	if err == nil {
		return sess, nil
	}

	// 2. Acquisition failed or is queuing. Trigger a forced reap of IDLE sessions
	// to free up slots for this and other queued requests.
	if ctx.Err() == nil {
		reaped := s.reapIdle(time.Now(), ReapTriggerForced)
		if reaped > 0 {
			slog.Info("Flight auth session store forced idle reap due to worker exhaustion.", "reaped_sessions", reaped)
		}
	}

	// 3. Apply worker queue timeout for the final (potentially blocking) attempt.
	if s.workerQueueTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.workerQueueTimeout)
		defer cancel()
	}

	return s.GetOrCreate(ctx, key, username)
}

func (s *flightAuthSessionStore) notifySessionCountChanged(count int) {
	if s.hooks.OnSessionCountChanged != nil {
		s.hooks.OnSessionCountChanged(count)
	}
}

func (s *flightAuthSessionStore) notifySessionsReaped(trigger string, count int) {
	if count <= 0 {
		return
	}
	if s.hooks.OnSessionsReaped != nil {
		s.hooks.OnSessionsReaped(trigger, count)
	}
}

func (s *flightAuthSessionStore) GetOrCreate(ctx context.Context, key, username string) (*flightClientSession, error) {
	existing, ok := s.getExistingByKey(key)
	if ok {
		existing.touch()
		return existing, nil
	}

	pid, executor, err := s.createSessionFn(ctx, username)
	if err != nil {
		return nil, err
	}

	token, tokenErr := generateSessionIdentityToken()
	if tokenErr != nil {
		s.destroySessionFn(pid)
		return nil, fmt.Errorf("generate session identity token: %w", tokenErr)
	}
	created := newFlightClientSession(pid, username, executor)
	created.token = token

	s.mu.Lock()
	s.ensureMapsLocked()
	if existing, ok := s.getExistingByKeyLocked(key); ok {
		s.mu.Unlock()
		s.destroySessionFn(pid)
		existing.touch()
		return existing, nil
	}
	for {
		if _, exists := s.sessions[created.token]; !exists {
			break
		}
		token, tokenErr = generateSessionIdentityToken()
		if tokenErr != nil {
			s.mu.Unlock()
			s.destroySessionFn(pid)
			return nil, fmt.Errorf("generate session identity token: %w", tokenErr)
		}
		created.token = token
	}
	s.sessions[created.token] = created
	created.tokenIssuedAt.Store(time.Now().UnixNano())
	s.byKey[key] = created.token
	sessionCount := len(s.sessions)
	s.mu.Unlock()
	s.notifySessionCountChanged(sessionCount)

	return created, nil
}

func (s *flightAuthSessionStore) GetByToken(token string) (*flightClientSession, bool) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, false
	}

	var (
		session          *flightClientSession
		ok               bool
		expiredSession   *flightClientSession
		postExpireCount  int
		tokenIssuedAtRaw int64
	)

	s.mu.Lock()
	s.ensureMapsLocked()
	session, ok = s.sessions[token]
	if !ok {
		s.mu.Unlock()
		return nil, false
	}

	tokenIssuedAtRaw = session.tokenIssuedAt.Load()
	if s.tokenTTL > 0 && tokenIssuedAtRaw > 0 {
		tokenAge := time.Since(time.Unix(0, tokenIssuedAtRaw))
		if tokenAge >= s.tokenTTL {
			delete(s.sessions, token)
			s.removeByKeyForTokenLocked(token)
			expiredSession = session
			postExpireCount = len(s.sessions)
			destroyFn := s.destroySessionFn
			s.mu.Unlock()

			if destroyFn != nil {
				destroyFn(expiredSession.pid)
			}
			s.notifySessionCountChanged(postExpireCount)
			return nil, false
		}
	}
	s.mu.Unlock()
	return session, true
}

func (s *flightAuthSessionStore) CloseByToken(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}

	var (
		session      *flightClientSession
		sessionCount int
		destroyFn    func(int32)
		ok           bool
	)

	s.mu.Lock()
	s.ensureMapsLocked()
	session, ok = s.sessions[token]
	if !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.sessions, token)
	s.removeByKeyForTokenLocked(token)
	sessionCount = len(s.sessions)
	destroyFn = s.destroySessionFn
	s.mu.Unlock()

	if destroyFn != nil {
		destroyFn(session.pid)
	}
	s.notifySessionCountChanged(sessionCount)
	return true
}

func (s *flightAuthSessionStore) getExistingByKey(key string) (*flightClientSession, bool) {
	s.mu.RLock()
	existing, ok := s.getExistingByKeyLocked(key)
	s.mu.RUnlock()
	if !ok {
		s.pruneStaleByKey(key)
	}
	return existing, ok
}

func (s *flightAuthSessionStore) getExistingByKeyLocked(key string) (*flightClientSession, bool) {
	token, ok := s.byKey[key]
	if !ok {
		return nil, false
	}
	existing, ok := s.sessions[token]
	return existing, ok
}

// pruneStaleByKey removes a bootstrap key only if it still points to a missing
// session token. This method must hold an exclusive lock because it mutates maps.
func (s *flightAuthSessionStore) pruneStaleByKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureMapsLocked()

	token, ok := s.byKey[key]
	if !ok {
		return
	}
	if _, exists := s.sessions[token]; exists {
		return
	}
	delete(s.byKey, key)
}

func (s *flightAuthSessionStore) ensureMapsLocked() {
	if s.sessions == nil {
		s.sessions = make(map[string]*flightClientSession)
	}
	if s.byKey == nil {
		s.byKey = make(map[string]string)
	}
}

func (s *flightAuthSessionStore) Close() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		<-s.doneCh

		s.mu.Lock()
		s.ensureMapsLocked()
		sessions := make([]*flightClientSession, 0, len(s.sessions))
		for _, cs := range s.sessions {
			sessions = append(sessions, cs)
		}
		s.sessions = make(map[string]*flightClientSession)
		s.byKey = make(map[string]string)
		s.mu.Unlock()
		s.notifySessionCountChanged(0)

		for _, cs := range sessions {
			s.destroySessionFn(cs.pid)
		}
	})
}

func (s *flightAuthSessionStore) ReapIdleNow() int {
	return s.reapIdle(time.Now(), ReapTriggerForced)
}

func (s *flightAuthSessionStore) reapIdle(now time.Time, trigger string) int {
	stale := make([]*flightClientSession, 0)
	sessionCount := 0

	s.mu.Lock()
	s.ensureMapsLocked()
	for token, cs := range s.sessions {
		cs.reapStaleHandles(now, s.handleIdleTTL)

		last := time.Unix(0, cs.lastUsed.Load())
		if now.Sub(last) < s.idleTTL {
			continue
		}
		if cs.txnCount() > 0 {
			continue
		}
		if cs.activeStreamCount() > 0 {
			continue
		}
		if cs.queryCount() > 0 {
			continue
		}
		delete(s.sessions, token)
		s.removeByKeyForTokenLocked(token)
		stale = append(stale, cs)
	}
	sessionCount = len(s.sessions)
	s.mu.Unlock()

	for _, cs := range stale {
		s.destroySessionFn(cs.pid)
	}
	reaped := len(stale)
	if reaped > 0 {
		s.notifySessionCountChanged(sessionCount)
		s.notifySessionsReaped(trigger, reaped)
	}
	return reaped
}

func (s *flightAuthSessionStore) removeByKeyForTokenLocked(token string) {
	for key, mappedToken := range s.byKey {
		if mappedToken == token {
			delete(s.byKey, key)
		}
	}
}

func (s *flightAuthSessionStore) reapLoop() {
	ticker := time.NewTicker(s.reapInterval)
	defer ticker.Stop()
	defer close(s.doneCh)

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			reaped := s.reapIdle(time.Now(), ReapTriggerPeriodic)
			if reaped > 0 {
				slog.Info("Flight auth session store periodic idle reap completed.", "reaped_sessions", reaped)
			}
		}
	}
}

func parseBasicCredentials(authHeader string) (username, password string, err error) {
	parts := strings.SplitN(strings.TrimSpace(authHeader), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Basic") {
		return "", "", fmt.Errorf("expected Basic authorization")
	}

	decoded, decodeErr := base64.StdEncoding.DecodeString(parts[1])
	if decodeErr != nil {
		decoded, decodeErr = base64.RawStdEncoding.DecodeString(parts[1])
		if decodeErr != nil {
			return "", "", fmt.Errorf("invalid basic auth encoding")
		}
	}

	creds := string(decoded)
	sep := strings.IndexByte(creds, ':')
	if sep < 0 {
		return "", "", fmt.Errorf("invalid basic auth payload")
	}
	username = creds[:sep]
	password = creds[sep+1:]
	if username == "" {
		return "", "", fmt.Errorf("username is required")
	}
	return username, password, nil
}

func flightAuthSessionKey(ctx context.Context, username string) string {
	clientID := "unknown"
	if p, ok := peer.FromContext(ctx); ok && p != nil && p.Addr != nil {
		host, _, err := net.SplitHostPort(p.Addr.String())
		if err == nil && host != "" {
			clientID = host
		} else {
			clientID = p.Addr.String()
		}
	}
	return username + "|" + clientID
}

func generateSessionIdentityToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func getQuerySchema(ctx context.Context, session *flightClientSession, query string) (*arrow.Schema, error) {
	q := strings.TrimRight(strings.TrimSpace(query), ";")
	upper := strings.ToUpper(q)
	if !supportsReadOnlySchemaInference(upper) {
		return nil, fmt.Errorf("schema inference only supports read-only query statements")
	}
	queryWithLimit := q
	if !strings.Contains(upper, "LIMIT") && supportsLimit(upper) {
		queryWithLimit = q + " LIMIT 0"
	}

	rows, err := session.query(ctx, queryWithLimit)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		dbType := "VARCHAR"
		if i < len(colTypes) && colTypes[i] != nil {
			dbType = colTypes[i].DatabaseTypeName()
		}
		fields[i] = arrow.Field{
			Name:     col,
			Type:     duckdbservice.DuckDBTypeToArrow(dbType),
			Nullable: true,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func supportsReadOnlySchemaInference(upper string) bool {
	return supportsLimit(upper)
}

func sendStreamChunk(ctx context.Context, ch chan<- flight.StreamChunk, chunk flight.StreamChunk) bool {
	select {
	case <-ctx.Done():
		return false
	case ch <- chunk:
		return true
	}
}

type tableSchemaKey struct {
	catalog    string
	hasCatalog bool
	schema     string
	hasSchema  bool
	name       string
}

func loadTableSchemas(ctx context.Context, s *flightClientSession, cmd flightsql.GetTables) (map[tableSchemaKey]*arrow.Schema, error) {
	query := "SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable " +
		"FROM information_schema.columns WHERE 1=1"
	args := make([]any, 0, 3)
	if catalog := cmd.GetCatalog(); catalog != nil && *catalog != "" {
		args = append(args, *catalog)
		query += fmt.Sprintf(" AND table_catalog = $%d", len(args))
	}
	if pattern := cmd.GetDBSchemaFilterPattern(); pattern != nil && *pattern != "" {
		args = append(args, *pattern)
		query += fmt.Sprintf(" AND table_schema LIKE $%d", len(args))
	}
	if pattern := cmd.GetTableNameFilterPattern(); pattern != nil && *pattern != "" {
		args = append(args, *pattern)
		query += fmt.Sprintf(" AND table_name LIKE $%d", len(args))
	}
	query += " ORDER BY table_catalog, table_schema, table_name, ordinal_position"

	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	fieldsByTable := make(map[tableSchemaKey][]arrow.Field)
	for rows.Next() {
		values := make([]any, 6)
		ptrs := make([]any, 6)
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		catalog := toNullString(values[0])
		schema := toNullString(values[1])
		key := tableSchemaKey{
			catalog:    catalog.String,
			hasCatalog: catalog.Valid,
			schema:     schema.String,
			hasSchema:  schema.Valid,
			name:       toString(values[2]),
		}

		nullable := strings.EqualFold(toString(values[5]), "YES")
		fieldsByTable[key] = append(fieldsByTable[key], arrow.Field{
			Name:     toString(values[3]),
			Type:     duckdbservice.DuckDBTypeToArrow(toString(values[4])),
			Nullable: nullable,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	schemas := make(map[tableSchemaKey]*arrow.Schema, len(fieldsByTable))
	for key, fields := range fieldsByTable {
		schemas[key] = arrow.NewSchema(fields, nil)
	}
	return schemas, nil
}

func rowSetToRecord(alloc memory.Allocator, rows server.RowSet, schema *arrow.Schema, batchSize int) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	count := 0
	numFields := schema.NumFields()
	for rows.Next() && count < batchSize {
		values := make([]any, numFields)
		ptrs := make([]any, numFields)
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		for i, val := range values {
			duckdbservice.AppendValue(builder.Field(i), val)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return builder.NewRecordBatch(), nil
}

func supportsLimit(upper string) bool {
	s := strings.TrimSpace(upper)
	return strings.HasPrefix(s, "SELECT") ||
		strings.HasPrefix(s, "WITH") ||
		strings.HasPrefix(s, "VALUES") ||
		strings.HasPrefix(s, "TABLE") ||
		strings.HasPrefix(s, "FROM")
}

func toNullString(v any) sql.NullString {
	switch t := v.(type) {
	case nil:
		return sql.NullString{}
	case string:
		return sql.NullString{String: t, Valid: true}
	case []byte:
		return sql.NullString{String: string(t), Valid: true}
	default:
		return sql.NullString{String: fmt.Sprintf("%v", t), Valid: true}
	}
}

func toString(v any) string {
	ns := toNullString(v)
	return ns.String
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
