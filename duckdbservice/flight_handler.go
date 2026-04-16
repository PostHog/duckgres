package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	bindings "github.com/duckdb/duckdb-go-bindings"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// FlightSQLHandler implements Arrow Flight SQL for multiple sessions.
// Sessions are identified by the "x-duckgres-session" gRPC metadata header.
type FlightSQLHandler struct {
	flightsql.BaseServer
	pool  *SessionPool
	alloc memory.Allocator
}

// sessionFromContext extracts the session from gRPC metadata.
// The session token is expected in the "x-duckgres-session" header.
func (h *FlightSQLHandler) sessionFromContext(ctx context.Context) (*Session, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	tokens := md.Get("x-duckgres-session")
	if len(tokens) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing x-duckgres-session header")
	}

	session, ok := h.pool.GetSession(tokens[0])
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "session not found")
	}
	if h.pool.sharedWarmMode {
		epochs := md.Get("x-duckgres-owner-epoch")
		if len(epochs) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing x-duckgres-owner-epoch header")
		}
		ownerEpoch, err := strconv.ParseInt(epochs[0], 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid x-duckgres-owner-epoch header")
		}
		workerIDs := md.Get("x-duckgres-worker-id")
		if len(workerIDs) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing x-duckgres-worker-id header")
		}
		workerID, err := strconv.Atoi(workerIDs[0])
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid x-duckgres-worker-id header")
		}
		cpInstanceIDs := md.Get("x-duckgres-cp-instance-id")
		if len(cpInstanceIDs) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing x-duckgres-cp-instance-id header")
		}
		if err := h.pool.validateControlMetadata(server.WorkerControlMetadata{
			WorkerID:     workerID,
			OwnerEpoch:   ownerEpoch,
			CPInstanceID: cpInstanceIDs[0],
		}); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "stale worker owner: %v", err)
		}
	}
	session.lastUsed.Store(time.Now().UnixNano())

	return session, nil
}

// Custom action handlers (called via customActionServer.DoAction)

func (h *FlightSQLHandler) doCreateSession(body []byte, stream flight.FlightService_DoActionServer) error {
	var req server.WorkerCreateSessionPayload
	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid CreateSession request: %v", err)
	}
	if req.Username == "" {
		return status.Error(codes.InvalidArgument, "username is required")
	}
	if err := h.pool.validateControlMetadata(req.WorkerControlMetadata); err != nil {
		return status.Errorf(codes.FailedPrecondition, "stale worker owner: %v", err)
	}

	if h.pool.sharedWarmMode {
		if _, err := h.pool.currentSessionConfig(); err != nil {
			return status.Error(codes.FailedPrecondition, "worker is not activated")
		}
	}

	// Validate username against configured users
	if !h.pool.sharedWarmMode {
		if _, ok := h.pool.cfg.Users[req.Username]; !ok {
			return status.Error(codes.PermissionDenied, "unknown username")
		}
	}

	session, err := h.pool.CreateSession(req.Username, req.MemoryLimit, req.Threads)
	if err != nil {
		return status.Errorf(codes.ResourceExhausted, "create session: %v", err)
	}

	resp, _ := json.Marshal(map[string]string{
		"session_token": session.ID,
	})
	return stream.Send(&flight.Result{Body: resp})
}

func (h *FlightSQLHandler) doActivateTenant(body []byte, stream flight.FlightService_DoActionServer) error {
	var payload ActivationPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid ActivateTenant request: %v", err)
	}
	if strings.TrimSpace(payload.OrgID) == "" {
		return status.Error(codes.InvalidArgument, "org_id is required")
	}
	if current := h.pool.currentActivation(); current != nil && current.payload.OrgID != payload.OrgID {
		return status.Errorf(codes.FailedPrecondition, "activate tenant: worker already activated for org %q", current.payload.OrgID)
	}

	if err := h.pool.activateTenantFunc(payload); err != nil {
		return status.Errorf(codes.FailedPrecondition, "activate tenant: %v", err)
	}

	resp, _ := json.Marshal(map[string]bool{"ok": true})
	return stream.Send(&flight.Result{Body: resp})
}

func (h *FlightSQLHandler) doDestroySession(body []byte, stream flight.FlightService_DoActionServer) error {
	var req server.WorkerDestroySessionPayload
	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid DestroySession request: %v", err)
	}
	if req.SessionToken == "" {
		return status.Error(codes.InvalidArgument, "session_token is required")
	}
	if err := h.pool.validateControlMetadata(req.WorkerControlMetadata); err != nil {
		return status.Errorf(codes.FailedPrecondition, "stale worker owner: %v", err)
	}

	if err := h.pool.DestroySession(req.SessionToken); err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}

	resp, _ := json.Marshal(map[string]bool{"ok": true})
	return stream.Send(&flight.Result{Body: resp})
}

func (h *FlightSQLHandler) doHealthCheck(body []byte, stream flight.FlightService_DoActionServer) error {
	var req server.WorkerHealthCheckPayload
	if err := json.Unmarshal(body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid HealthCheck request: %v", err)
	}
	if err := h.pool.validateControlMetadata(req.WorkerControlMetadata); err != nil {
		return status.Errorf(codes.FailedPrecondition, "stale worker owner: %v", err)
	}

	// Block until warmup (extension loading + DuckLake attachment) completes.
	// Without this, the control plane's waitForWorkerTCP health check passes
	// as soon as the gRPC server starts, and clients get routed to a worker
	// that hasn't attached DuckLake yet.
	<-h.pool.warmupDone

	// Poll DuckDB query progress for each active session.
	//
	// QueryProgress is a CGO call into DuckDB that *should* return instantly
	// (it reads atomic progress counters), but can block if DuckDB holds an
	// internal lock — for example, when the httpfs extension is mid-download
	// on a large remote parquet file. If that happens while we hold the pool
	// RLock, both the health check and any session create/close operations
	// stall, the CP's 3-second health check timeout fires, and after 3
	// consecutive failures the CP kills the worker — even though it's alive
	// and making progress on the download.
	//
	// To prevent this:
	// 1. Snapshot the session data we need under RLock, then release it.
	// 2. Call QueryProgress outside the lock, with a per-session timeout.
	//    If the CGO call doesn't return within queryProgressTimeout, we
	//    report the session as "busy" (pct=-1) and skip stall detection
	//    for this cycle. The health check always responds promptly.
	//
	// Real crashes (process death) are detected by the K8s pod informer
	// independently of health checks, so skipping stall detection during
	// I/O-heavy operations doesn't create a blind spot for crash recovery.
	type sessionProgressInfo struct {
		Pct     float64 `json:"pct"`
		Rows    uint64  `json:"rows"`
		Total   uint64  `json:"total"`
		Stalled bool    `json:"stalled,omitempty"`
	}

	type sessionSnapshot struct {
		key      string
		conn     duckdbConnHandle
		progress *progressState
	}

	h.pool.mu.RLock()
	snapshots := make([]sessionSnapshot, 0, len(h.pool.sessions))
	for token, session := range h.pool.sessions {
		if session.duckdbConn.Ptr == nil {
			continue
		}
		key := token
		if len(key) > 16 {
			key = key[:16]
		}
		snapshots = append(snapshots, sessionSnapshot{
			key:      key,
			conn:     session.duckdbConn,
			progress: &session.progress,
		})
	}
	h.pool.mu.RUnlock()

	sessionProgress := make(map[string]sessionProgressInfo, len(snapshots))
	for _, snap := range snapshots {
		if !snap.progress.queryActive.Load() {
			snap.progress.lastRowsProcessed.Store(0)
			snap.progress.stalledChecks.Store(0)
			sessionProgress[snap.key] = sessionProgressInfo{}
			continue
		}

		type qpResult struct {
			pct   float64
			rows  uint64
			total uint64
		}
		ch := make(chan qpResult, 1)
		go func(conn duckdbConnHandle) {
			qp := bindings.QueryProgress(conn)
			pct, rows, total := bindings.QueryProgressTypeMembers(&qp)
			ch <- qpResult{pct, rows, total}
		}(snap.conn)

		select {
		case pr := <-ch:
			stalled := false
			if pr.pct < 0 {
				// pct == -1 means DuckDB can't track this query; skip stall detection.
			} else {
				if pr.rows == snap.progress.lastRowsProcessed.Load() {
					snap.progress.stalledChecks.Add(1)
				} else {
					snap.progress.lastRowsProcessed.Store(pr.rows)
					snap.progress.stalledChecks.Store(0)
				}
				if snap.progress.stalledChecks.Load() >= stallCheckThreshold {
					stalled = true
					if snap.progress.stalledChecks.Load() == stallCheckThreshold {
						slog.Warn("Query appears stuck — no progress detected.",
							"session", snap.key, "rows_processed", pr.rows, "total_rows", pr.total,
							"stalled_checks", stallCheckThreshold)
					}
				}
			}
			sessionProgress[snap.key] = sessionProgressInfo{Pct: pr.pct, Rows: pr.rows, Total: pr.total, Stalled: stalled}

		case <-time.After(queryProgressTimeout):
			// CGO call blocked — DuckDB is busy with I/O (e.g., httpfs
			// download). Report as untrackable and don't advance stall
			// counters so a legitimately slow operation doesn't get
			// flagged as stuck.
			sessionProgress[snap.key] = sessionProgressInfo{Pct: -1}
		}
	}

	resp, _ := json.Marshal(map[string]interface{}{
		"healthy":          true,
		"sessions":         h.pool.ActiveSessions(),
		"uptime_ns":        time.Since(h.pool.startTime).Nanoseconds(),
		"session_progress": sessionProgress,
	})
	return stream.Send(&flight.Result{Body: resp})
}

// Flight SQL method implementations

func (h *FlightSQLHandler) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	query := cmd.GetQuery()

	// Handle empty queries (e.g., ";" from PostgreSQL client pings).
	// Return an empty schema instead of sending to DuckDB which rejects empty queries.
	if isEmptyFlightQuery(query) {
		emptySchema := arrow.NewSchema(nil, nil)
		handleID := fmt.Sprintf("query-%d", session.handleCounter.Add(1))
		session.mu.Lock()
		session.queries[handleID] = &QueryHandle{Query: query, Schema: emptySchema}
		session.mu.Unlock()

		ticketBytes, ticketErr := flightsql.CreateStatementQueryTicket([]byte(handleID))
		if ticketErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to create ticket: %v", ticketErr)
		}
		return &flight.FlightInfo{
			Schema:           flight.SerializeSchema(emptySchema, h.alloc),
			FlightDescriptor: desc,
			Endpoint: []*flight.FlightEndpoint{{
				Ticket: &flight.Ticket{Ticket: ticketBytes},
			}},
			TotalRecords: 0,
			TotalBytes:   0,
		}, nil
	}

	tx, txnKey, err := session.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return nil, err
	}

	session.progress.queryActive.Store(true)
	defer session.progress.queryActive.Store(false)

	// Only retry on transient errors for autocommit queries. Inside a
	// transaction, a transient error invalidates the transaction — retrying
	// would run in autocommit mode and mask the failure.
	inTransaction := tx != nil || session.sqlTxActive.Load()
	var schema *arrow.Schema
	if inTransaction {
		schema, err = GetQuerySchema(ctx, session.Conn, query, tx)
	} else {
		schema, err = retryOnTransient(func() (*arrow.Schema, error) {
			return GetQuerySchema(ctx, session.Conn, query, tx)
		})
	}
	// Conflict retry for autocommit only. Note: if retryOnTransient exhausted on a
	// transient error that also matches "Transaction conflict", this chains into
	// conflict retry — acceptable since the error patterns are distinct in practice.
	if err != nil && tx == nil && isDuckLakeTransactionConflict(err) {
		ducklakeConflictTotal.Inc()
		schema, err = retryOnConflict(func() (*arrow.Schema, error) {
			return GetQuerySchema(ctx, session.Conn, query, tx)
		})
	}
	if err != nil {
		schema, err, _ = recoverAbortedTransaction(
			err,
			!inTransaction,
			func() error {
				_, rollbackErr := session.Conn.ExecContext(context.Background(), "ROLLBACK")
				return rollbackErr
			},
			func() (*arrow.Schema, error) {
				return GetQuerySchema(ctx, session.Conn, query, tx)
			},
		)
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to prepare query: %v", err)
	}

	handleID := fmt.Sprintf("query-%d", session.handleCounter.Add(1))
	session.mu.Lock()
	session.queries[handleID] = &QueryHandle{Query: query, Schema: schema, TxnID: txnKey}
	session.mu.Unlock()

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

func (h *FlightSQLHandler) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	handleID := string(ticket.GetStatementHandle())

	session.mu.RLock()
	handle, ok := session.queries[handleID]
	session.mu.RUnlock()
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "query handle not found")
	}

	var tx *sql.Tx
	if handle.TxnID != "" {
		session.mu.RLock()
		ttx := session.txns[handle.TxnID]
		session.mu.RUnlock()
		if ttx == nil || ttx.tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
		ttx.lastUsed.Store(time.Now().UnixNano())
		tx = ttx.tx
	}

	schema := handle.Schema

	ch := make(chan flight.StreamChunk, 10)

	// Empty queries have no rows to fetch — return an empty stream immediately.
	if isEmptyFlightQuery(handle.Query) {
		close(ch)
		return schema, ch, nil
	}

	go func() {
		defer close(ch)
		defer func() {
			session.mu.Lock()
			delete(session.queries, handleID)
			session.mu.Unlock()
		}()

		session.progress.queryActive.Store(true)
		defer session.progress.queryActive.Store(false)

		inTxn := tx != nil || session.sqlTxActive.Load()
		queryFn := func() (*sql.Rows, error) {
			if tx != nil {
				return tx.QueryContext(ctx, handle.Query)
			}
			return session.Conn.QueryContext(ctx, handle.Query)
		}

		var rows *sql.Rows
		var qerr error
		if inTxn {
			rows, qerr = queryFn()
		} else {
			rows, qerr = retryOnTransient(queryFn)
		}
		// Conflict retry for autocommit only (see GetFlightInfoStatement comment).
		if qerr != nil && tx == nil && isDuckLakeTransactionConflict(qerr) {
			ducklakeConflictTotal.Inc()
			rows, qerr = retryOnConflict(func() (*sql.Rows, error) {
				return session.Conn.QueryContext(ctx, handle.Query)
			})
		}
		if qerr != nil {
			rows, qerr, _ = recoverAbortedTransaction(
				qerr,
				!inTxn,
				func() error {
					_, rollbackErr := session.Conn.ExecContext(context.Background(), "ROLLBACK")
					return rollbackErr
				},
				func() (*sql.Rows, error) {
					return session.Conn.QueryContext(ctx, handle.Query)
				},
			)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		for {
			record, recErr := RowsToRecord(h.alloc, rows, schema, 1024)
			if recErr != nil {
				ch <- flight.StreamChunk{Err: recErr}
				return
			}
			if record == nil {
				break
			}
			ch <- flight.StreamChunk{Data: record}
		}
	}()

	return schema, ch, nil
}

func (h *FlightSQLHandler) DoPutCommandStatementUpdate(ctx context.Context,
	cmd flightsql.StatementUpdate) (int64, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return 0, err
	}

	tx, _, err := session.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return 0, err
	}

	query := cmd.GetQuery()

	// Handle empty queries (e.g., ";" from PostgreSQL client pings).
	if isEmptyFlightQuery(query) {
		return 0, nil
	}
	session.progress.queryActive.Store(true)
	defer session.progress.queryActive.Store(false)

	execFn := func() (sql.Result, error) {
		if tx != nil {
			return tx.ExecContext(ctx, query)
		}
		return session.Conn.ExecContext(ctx, query)
	}

	// Determine whether this statement is safe to retry on transient errors.
	//
	// Never retry when inside a transaction (Flight SQL or SQL-level):
	// - Transaction control stmts (COMMIT/ROLLBACK/END): retrying after DuckDB
	//   internally rolls back produces "no transaction is active".
	// - Any other stmt inside a transaction: a transient error causes DuckDB to
	//   roll back the transaction internally. Retrying would succeed in autocommit
	//   mode, masking the rollback. The client would later COMMIT and get
	//   "no transaction is active".
	//
	// Only retry for autocommit statements (no Flight SQL txn, no SQL-level txn).
	inTransaction := tx != nil || session.sqlTxActive.Load()
	isTxControl := isTransactionControlStmt(query)

	var result sql.Result
	var execErr error
	if inTransaction || isTxControl {
		result, execErr = execFn()
	} else {
		result, execErr = retryOnTransient(execFn)
	}

	// Track SQL-level transaction state for BEGIN/COMMIT/ROLLBACK sent as raw SQL.
	trackSQLTransactionState(query, execErr, &session.sqlTxActive)
	// Conflict retry for autocommit only (see GetFlightInfoStatement comment).
	if execErr != nil && tx == nil && isDuckLakeTransactionConflict(execErr) {
		ducklakeConflictTotal.Inc()
		result, execErr = retryOnConflict(func() (sql.Result, error) {
			return session.Conn.ExecContext(ctx, query)
		})
	}
	if execErr != nil {
		result, execErr, _ = recoverAbortedTransaction(
			execErr,
			!inTransaction,
			func() error {
				_, rollbackErr := session.Conn.ExecContext(context.Background(), "ROLLBACK")
				return rollbackErr
			},
			func() (sql.Result, error) {
				return execFn()
			},
		)
	}
	if execErr != nil {
		return 0, status.Errorf(codes.InvalidArgument, "failed to execute update: %v", execErr)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return affected, nil
}

func (h *FlightSQLHandler) BeginTransaction(ctx context.Context,
	req flightsql.ActionBeginTransactionRequest) ([]byte, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	_ = req

	tx, err := session.Conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	txnKey := fmt.Sprintf("txn-%d", session.handleCounter.Add(1))
	ttx := &trackedTx{tx: tx}
	ttx.lastUsed.Store(time.Now().UnixNano())

	session.mu.Lock()
	session.txns[txnKey] = ttx
	session.txnOwner[txnKey] = session.Username
	session.mu.Unlock()

	return []byte(txnKey), nil
}

func (h *FlightSQLHandler) EndTransaction(ctx context.Context,
	req flightsql.ActionEndTransactionRequest) error {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}

	txnKey := string(req.GetTransactionId())
	if txnKey == "" {
		return status.Error(codes.InvalidArgument, "missing transaction id")
	}

	session.mu.Lock()
	ttx, ok := session.txns[txnKey]
	if ok {
		delete(session.txns, txnKey)
		delete(session.txnOwner, txnKey)
	}
	session.mu.Unlock()

	if !ok || ttx == nil || ttx.tx == nil {
		return status.Error(codes.NotFound, "transaction not found")
	}

	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		if err := ttx.tx.Commit(); err != nil {
			return status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
		}
		return nil
	case flightsql.EndTransactionRollback:
		if err := ttx.tx.Rollback(); err != nil {
			return status.Errorf(codes.Internal, "failed to rollback transaction: %v", err)
		}
		return nil
	default:
		_ = ttx.tx.Rollback()
		return status.Error(codes.InvalidArgument, "unsupported end transaction action")
	}
}

func (h *FlightSQLHandler) CreatePreparedStatement(ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	tx, txnKey, err := session.getOpenTxn(req.GetTransactionId())
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	query := req.GetQuery()

	// Handle empty queries (e.g., ";" from PostgreSQL client pings).
	if isEmptyFlightQuery(query) {
		emptySchema := arrow.NewSchema(nil, nil)
		handleID := fmt.Sprintf("prep-%d", session.handleCounter.Add(1))
		session.mu.Lock()
		session.queries[handleID] = &QueryHandle{Query: query, Schema: emptySchema, TxnID: txnKey}
		session.mu.Unlock()
		return flightsql.ActionCreatePreparedStatementResult{
			Handle:        []byte(handleID),
			DatasetSchema: emptySchema,
		}, nil
	}

	session.progress.queryActive.Store(true)
	defer session.progress.queryActive.Store(false)
	schema, err := GetQuerySchema(ctx, session.Conn, query, tx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.InvalidArgument, "failed to prepare: %v", err)
	}

	handleID := fmt.Sprintf("prep-%d", session.handleCounter.Add(1))
	session.mu.Lock()
	session.queries[handleID] = &QueryHandle{Query: query, Schema: schema, TxnID: txnKey}
	session.mu.Unlock()

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        []byte(handleID),
		DatasetSchema: schema,
	}, nil
}

func (h *FlightSQLHandler) ClosePreparedStatement(ctx context.Context,
	req flightsql.ActionClosePreparedStatementRequest) error {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return err
	}

	handleID := string(req.GetPreparedStatementHandle())
	session.mu.Lock()
	delete(session.queries, handleID)
	session.mu.Unlock()
	return nil
}

func (h *FlightSQLHandler) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	handleID := string(cmd.GetPreparedStatementHandle())
	session.mu.RLock()
	handle, ok := session.queries[handleID]
	session.mu.RUnlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "prepared statement not found")
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

func (h *FlightSQLHandler) DoGetPreparedStatement(ctx context.Context,
	cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	session.mu.RLock()
	handle, ok := session.queries[string(cmd.GetPreparedStatementHandle())]
	session.mu.RUnlock()
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "prepared statement not found")
	}

	var tx *sql.Tx
	if handle.TxnID != "" {
		session.mu.RLock()
		ttx := session.txns[handle.TxnID]
		session.mu.RUnlock()
		if ttx == nil || ttx.tx == nil {
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
		ttx.lastUsed.Store(time.Now().UnixNano())
		tx = ttx.tx
	}

	schema := handle.Schema

	ch := make(chan flight.StreamChunk, 10)

	// Empty queries have no rows to fetch — return an empty stream immediately.
	if isEmptyFlightQuery(handle.Query) {
		close(ch)
		return schema, ch, nil
	}

	go func() {
		defer close(ch)
		session.progress.queryActive.Store(true)
		defer session.progress.queryActive.Store(false)
		var rows *sql.Rows
		var qerr error
		if tx != nil {
			rows, qerr = tx.QueryContext(ctx, handle.Query)
		} else {
			rows, qerr = session.Conn.QueryContext(ctx, handle.Query)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		for {
			record, recErr := RowsToRecord(h.alloc, rows, schema, 1024)
			if recErr != nil {
				ch <- flight.StreamChunk{Err: recErr}
				return
			}
			if record == nil {
				break
			}
			ch <- flight.StreamChunk{Data: record}
		}
	}()

	return schema, ch, nil
}

func (h *FlightSQLHandler) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	_, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

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

func (h *FlightSQLHandler) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	schema := schema_ref.DBSchemas
	query := "SELECT catalog_name, schema_name AS db_schema_name FROM information_schema.schemata WHERE 1=1"
	args := make([]any, 0, 2)

	if catalog := cmd.GetCatalog(); catalog != nil && *catalog != "" {
		query += " AND catalog_name = ?"
		args = append(args, *catalog)
	}
	if pattern := cmd.GetDBSchemaFilterPattern(); pattern != nil && *pattern != "" {
		query += " AND schema_name LIKE ?"
		args = append(args, *pattern)
	}
	query += " ORDER BY catalog_name, db_schema_name"

	activeTx := session.getActiveTxn()

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		var qerr error
		if activeTx != nil {
			rows, qerr = activeTx.QueryContext(ctx, query, args...)
		} else {
			rows, qerr = session.Conn.QueryContext(ctx, query, args...)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		builder := array.NewRecordBuilder(h.alloc, schema)
		defer builder.Release()
		catalogBuilder := builder.Field(0).(*array.StringBuilder)
		schemaBuilder := builder.Field(1).(*array.StringBuilder)

		for rows.Next() {
			var catalog sql.NullString
			var schemaName sql.NullString
			if scanErr := rows.Scan(&catalog, &schemaName); scanErr != nil {
				ch <- flight.StreamChunk{Err: scanErr}
				return
			}
			if catalog.Valid {
				catalogBuilder.Append(catalog.String)
			} else {
				catalogBuilder.AppendNull()
			}
			if schemaName.Valid {
				schemaBuilder.Append(schemaName.String)
			} else {
				schemaBuilder.AppendNull()
			}
		}
		if rowErr := rows.Err(); rowErr != nil {
			ch <- flight.StreamChunk{Err: rowErr}
			return
		}
		ch <- flight.StreamChunk{Data: builder.NewRecordBatch()}
	}()

	return schema, ch, nil
}

func (h *FlightSQLHandler) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	_, err := h.sessionFromContext(ctx)
	if err != nil {
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

func (h *FlightSQLHandler) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema,
	<-chan flight.StreamChunk, error) {

	session, err := h.sessionFromContext(ctx)
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
		query += " AND table_catalog = ?"
		args = append(args, *catalog)
	}
	if pattern := cmd.GetDBSchemaFilterPattern(); pattern != nil && *pattern != "" {
		query += " AND table_schema LIKE ?"
		args = append(args, *pattern)
	}
	if pattern := cmd.GetTableNameFilterPattern(); pattern != nil && *pattern != "" {
		query += " AND table_name LIKE ?"
		args = append(args, *pattern)
	}
	if tableTypes := cmd.GetTableTypes(); len(tableTypes) > 0 {
		placeholders := make([]string, 0, len(tableTypes))
		for range tableTypes {
			placeholders = append(placeholders, "?")
		}
		query += " AND table_type IN (" + strings.Join(placeholders, ", ") + ")"
		for _, t := range tableTypes {
			args = append(args, t)
		}
	}
	query += " ORDER BY table_catalog, table_schema, table_name"

	activeTx := session.getActiveTxn()

	ch := make(chan flight.StreamChunk, 1)
	go func() {
		defer close(ch)
		var rows *sql.Rows
		var qerr error
		if activeTx != nil {
			rows, qerr = activeTx.QueryContext(ctx, query, args...)
		} else {
			rows, qerr = session.Conn.QueryContext(ctx, query, args...)
		}
		if qerr != nil {
			ch <- flight.StreamChunk{Err: qerr}
			return
		}
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

		type tableInfo struct {
			catalog   sql.NullString
			schema    sql.NullString
			name      string
			tableType string
		}
		tables := make([]tableInfo, 0)
		for rows.Next() {
			var t tableInfo
			if scanErr := rows.Scan(&t.catalog, &t.schema, &t.name, &t.tableType); scanErr != nil {
				ch <- flight.StreamChunk{Err: scanErr}
				return
			}
			tables = append(tables, t)
		}
		if rowErr := rows.Err(); rowErr != nil {
			ch <- flight.StreamChunk{Err: rowErr}
			return
		}
		// Close the metadata cursor before issuing schema probe queries on the
		// same DB/transaction.
		if closeErr := closeRows(); closeErr != nil {
			ch <- flight.StreamChunk{Err: closeErr}
			return
		}

		builder := array.NewRecordBuilder(h.alloc, schema)
		defer builder.Release()

		var schemaBuilder *array.BinaryBuilder
		if includeSchema {
			schemaBuilder = builder.Field(4).(*array.BinaryBuilder)
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

			if includeSchema {
				qualified := QualifyTableName(t.catalog, t.schema, t.name)
				tableSchema, schemaErr := GetQuerySchema(ctx, session.Conn, "SELECT * FROM "+qualified, activeTx)
				if schemaErr != nil {
					ch <- flight.StreamChunk{Err: schemaErr}
					return
				}
				schemaBuilder.Append(flight.SerializeSchema(tableSchema, h.alloc))
			}
		}

		ch <- flight.StreamChunk{Data: builder.NewRecordBatch()}
	}()

	return schema, ch, nil
}

// isEmptyFlightQuery checks if a query is empty or contains only semicolons, whitespace, and/or comments.
// DuckDB rejects empty queries with SQLSTATE 42000, but PostgreSQL clients use these for pings
// (e.g., pgx sends "-- ping").
func isEmptyFlightQuery(query string) bool {
	stripped := stripFlightComments(query)
	for _, r := range stripped {
		if r != ';' && r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}

// stripFlightComments removes leading SQL comments (block and line) from a query.
func stripFlightComments(query string) string {
	for {
		query = strings.TrimSpace(query)
		if strings.HasPrefix(query, "/*") {
			end := strings.Index(query, "*/")
			if end == -1 {
				return query
			}
			query = query[end+2:]
		} else if strings.HasPrefix(query, "--") {
			end := strings.Index(query, "\n")
			if end == -1 {
				return ""
			}
			query = query[end+1:]
		} else {
			return query
		}
	}
}

// Session helpers

func (s *Session) getOpenTxn(transactionID []byte) (*sql.Tx, string, error) {
	if len(transactionID) == 0 {
		return nil, "", nil
	}
	txnKey := string(transactionID)
	s.mu.RLock()
	ttx, ok := s.txns[txnKey]
	s.mu.RUnlock()
	if !ok || ttx == nil || ttx.tx == nil {
		return nil, "", status.Error(codes.NotFound, "transaction not found")
	}
	ttx.lastUsed.Store(time.Now().UnixNano())
	return ttx.tx, txnKey, nil
}

func (s *Session) getActiveTxn() *sql.Tx {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ttx := range s.txns {
		if ttx != nil && ttx.tx != nil {
			ttx.lastUsed.Store(time.Now().UnixNano())
			return ttx.tx
		}
	}
	return nil
}
