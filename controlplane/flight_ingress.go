package controlplane

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
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
	flightBatchSize       = 1024
	flightSessionIdleTTL  = 10 * time.Minute
	flightSessionReapTick = 1 * time.Minute
)

// FlightIngress serves Arrow Flight SQL on the control plane with Basic auth.
// It reuses control-plane worker sessions via SessionManager.
type FlightIngress struct {
	flightSrv     flight.Server
	listener      net.Listener
	sessionStore  *flightAuthSessionStore
	listenerAddr  string
	shutdownOnce  sync.Once
	shutdownState atomic.Bool
	wg            sync.WaitGroup
}

// NewFlightIngress creates a control-plane Flight SQL ingress listener.
func NewFlightIngress(host string, port int, tlsConfig *tls.Config, users map[string]string, sm *SessionManager) (*FlightIngress, error) {
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

	store := newFlightAuthSessionStore(sm, flightSessionIdleTTL, flightSessionReapTick)
	handler := NewControlPlaneFlightSQLHandler(store, users)

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(server.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(server.MaxGRPCMessageSize),
	}

	srv := flight.NewServerWithMiddleware(nil, opts...)
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
	users    map[string]string
	sessions *flightAuthSessionStore
	alloc    memory.Allocator
}

func NewControlPlaneFlightSQLHandler(sessions *flightAuthSessionStore, users map[string]string) *ControlPlaneFlightSQLHandler {
	h := &ControlPlaneFlightSQLHandler{
		users:    users,
		sessions: sessions,
		alloc:    memory.DefaultAllocator,
	}
	if err := h.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerName, "duckgres-control-plane"); err != nil {
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

func (h *ControlPlaneFlightSQLHandler) sessionFromContext(ctx context.Context) (*flightClientSession, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}

	username, password, err := parseBasicCredentials(authHeaders[0])
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	expected, userFound := h.users[username]
	if !userFound {
		expected = "__invalid__"
	}
	passMatch := subtle.ConstantTimeCompare([]byte(password), []byte(expected)) == 1
	if !userFound || !passMatch {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	sessionKey := flightAuthSessionKey(ctx, username)
	s, err := h.sessions.GetOrCreate(ctx, sessionKey, username)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "create session: %v", err)
	}
	s.touch()
	return s, nil
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
	go func() {
		defer close(ch)
		defer func() {
			_ = rows.Close()
			s.deleteQuery(handleID)
		}()

		for {
			record, recErr := rowSetToRecord(h.alloc, rows, handle.Schema, flightBatchSize)
			if recErr != nil {
				ch <- flight.StreamChunk{Err: recErr}
				return
			}
			if record == nil {
				return
			}
			ch <- flight.StreamChunk{Data: record}
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

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, nil
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
	go func() {
		defer close(ch)
		defer func() {
			_ = rows.Close()
		}()

		for {
			record, recErr := rowSetToRecord(h.alloc, rows, handle.Schema, flightBatchSize)
			if recErr != nil {
				ch <- flight.StreamChunk{Err: recErr}
				return
			}
			if record == nil {
				return
			}
			ch <- flight.StreamChunk{Data: record}
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
	go func() {
		defer close(ch)
		defer func() {
			_ = rows.Close()
		}()

		for {
			record, recErr := rowSetToRecord(h.alloc, rows, schema, flightBatchSize)
			if recErr != nil {
				ch <- flight.StreamChunk{Err: recErr}
				return
			}
			if record == nil {
				return
			}
			ch <- flight.StreamChunk{Data: record}
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
	go func() {
		defer close(ch)
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
					ch <- flight.StreamChunk{Err: recErr}
					return
				}
				if record == nil {
					return
				}
				ch <- flight.StreamChunk{Data: record}
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
				ch <- flight.StreamChunk{Err: err}
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
			ch <- flight.StreamChunk{Err: err}
			return
		}
		// Release the session query lock before schema lookups, which execute
		// nested queries through the same session.
		if err := closeRows(); err != nil {
			ch <- flight.StreamChunk{Err: err}
			return
		}

		builder := array.NewRecordBuilder(h.alloc, schema)
		defer builder.Release()

		schemaBuilder := builder.Field(4).(*array.BinaryBuilder)
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

			qualified := duckdbservice.QualifyTableName(t.catalog, t.schema, t.name)
			tableSchema, schemaErr := getQuerySchema(ctx, s, "SELECT * FROM "+qualified)
			if schemaErr != nil {
				ch <- flight.StreamChunk{Err: schemaErr}
				return
			}
			schemaBuilder.Append(flight.SerializeSchema(tableSchema, h.alloc))
		}

		ch <- flight.StreamChunk{Data: builder.NewRecordBatch()}
	}()

	return schema, ch, nil
}

type flightQueryHandle struct {
	Query  string
	Schema *arrow.Schema
	TxnID  string
}

type flightClientSession struct {
	pid      int32
	username string
	executor *server.FlightExecutor

	lastUsed atomic.Int64
	counter  atomic.Uint64

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

func (s *flightClientSession) queryCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queries)
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
	s.queries[handleID] = q
	s.mu.Unlock()
}

func (s *flightClientSession) getQuery(handleID string) (*flightQueryHandle, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.queries[handleID]
	return q, ok
}

func (s *flightClientSession) deleteQuery(handleID string) {
	s.mu.Lock()
	delete(s.queries, handleID)
	s.mu.Unlock()
}

type flightAuthSessionStore struct {
	sm           *SessionManager
	idleTTL      time.Duration
	reapInterval time.Duration

	mu       sync.RWMutex
	sessions map[string]*flightClientSession

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

func newFlightAuthSessionStore(sm *SessionManager, idleTTL, reapInterval time.Duration) *flightAuthSessionStore {
	s := &flightAuthSessionStore{
		sm:           sm,
		idleTTL:      idleTTL,
		reapInterval: reapInterval,
		sessions:     make(map[string]*flightClientSession),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
	go s.reapLoop()
	return s
}

func (s *flightAuthSessionStore) GetOrCreate(ctx context.Context, key, username string) (*flightClientSession, error) {
	s.mu.RLock()
	existing, ok := s.sessions[key]
	s.mu.RUnlock()
	if ok {
		existing.touch()
		return existing, nil
	}

	pid, executor, err := s.sm.CreateSession(ctx, username)
	if err != nil {
		return nil, err
	}
	created := newFlightClientSession(pid, username, executor)

	s.mu.Lock()
	if existing, ok := s.sessions[key]; ok {
		s.mu.Unlock()
		s.sm.DestroySession(pid)
		existing.touch()
		return existing, nil
	}
	s.sessions[key] = created
	s.mu.Unlock()

	return created, nil
}

func (s *flightAuthSessionStore) Close() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		<-s.doneCh

		s.mu.Lock()
		sessions := make([]*flightClientSession, 0, len(s.sessions))
		for _, cs := range s.sessions {
			sessions = append(sessions, cs)
		}
		s.sessions = make(map[string]*flightClientSession)
		s.mu.Unlock()

		for _, cs := range sessions {
			s.sm.DestroySession(cs.pid)
		}
	})
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
			now := time.Now()
			stale := make([]*flightClientSession, 0)

			s.mu.Lock()
			for key, cs := range s.sessions {
				last := time.Unix(0, cs.lastUsed.Load())
				if now.Sub(last) < s.idleTTL {
					continue
				}
				if cs.txnCount() > 0 {
					continue
				}
				if cs.queryCount() > 0 {
					continue
				}
				delete(s.sessions, key)
				stale = append(stale, cs)
			}
			s.mu.Unlock()

			for _, cs := range stale {
				s.sm.DestroySession(cs.pid)
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
		clientID = p.Addr.String()
	}
	return clientID + "|" + username
}

func getQuerySchema(ctx context.Context, session *flightClientSession, query string) (*arrow.Schema, error) {
	q := strings.TrimRight(strings.TrimSpace(query), ";")
	upper := strings.ToUpper(q)
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
