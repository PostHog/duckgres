package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
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

func workerDrainingStatus(err error) error {
	if errors.Is(err, ErrWorkerDraining) {
		return status.Error(codes.Unavailable, "worker is draining")
	}
	return nil
}

func sessionOpenStatus(ok bool) error {
	if ok {
		return nil
	}
	return status.Error(codes.NotFound, "session closed")
}

func sessionClosedStatus(err error) error {
	if errors.Is(err, errSessionClosed) {
		return status.Error(codes.NotFound, "session closed")
	}
	return nil
}

func sendStreamChunk(ctx context.Context, ch chan<- flight.StreamChunk, chunk flight.StreamChunk) bool {
	select {
	case ch <- chunk:
		return true
	case <-ctx.Done():
		return false
	}
}

func sendActionResult(stream flight.FlightService_DoActionServer, result *flight.Result) error {
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	default:
		return stream.Send(result)
	}
}

func releaseQueryHandleValue(handle *QueryHandle) {
	if handle == nil {
		return
	}
	var releaseDrains []func()
	if handle.finishDrain != nil {
		releaseDrains = append(releaseDrains, handle.finishDrain)
		handle.finishDrain = nil
	}
	if handle.finishOperation != nil {
		releaseDrains = append(releaseDrains, handle.finishOperation)
		handle.finishOperation = nil
	}
	for _, release := range releaseDrains {
		releaseDrainFunc(release)
	}
}

func appendQueryHandleReleaseFuncs(handle *QueryHandle, drainReleases, operationReleases []func()) ([]func(), []func()) {
	if handle == nil {
		return drainReleases, operationReleases
	}
	if handle.finishDrain != nil {
		drainReleases = append(drainReleases, handle.finishDrain)
		handle.finishDrain = nil
	}
	if handle.finishOperation != nil {
		operationReleases = append(operationReleases, handle.finishOperation)
		handle.finishOperation = nil
	}
	return drainReleases, operationReleases
}

func popAbandonedTransactionContinuations(session *Session, txnKey string) ([]func(), []func()) {
	var drainReleases []func()
	var operationReleases []func()
	if txnKey == "" {
		return drainReleases, operationReleases
	}
	session.mu.Lock()
	for id, handle := range session.queries {
		if handle.TxnID != txnKey {
			continue
		}
		if handle.finishOperation == nil {
			continue
		}
		delete(session.queries, id)
		drainReleases, operationReleases = appendQueryHandleReleaseFuncs(handle, drainReleases, operationReleases)
	}
	session.mu.Unlock()
	return drainReleases, operationReleases
}

func popQueryHandle(session *Session, handleID string) (*QueryHandle, bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	handle, ok := session.queries[handleID]
	if !ok {
		return nil, false
	}
	delete(session.queries, handleID)
	return handle, true
}

func addQueryHandle(session *Session, handleID string, handle *QueryHandle) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return false
	}
	session.queries[handleID] = handle
	return true
}

func addTrackedTransaction(session *Session, txnKey string, ttx *trackedTx) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return false
	}
	session.txns[txnKey] = ttx
	session.txnOwner[txnKey] = session.Username
	return true
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
	if h.pool.IsDraining() {
		return status.Error(codes.Unavailable, "worker is draining")
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
	if drainErr := workerDrainingStatus(err); drainErr != nil {
		return drainErr
	}
	if err != nil {
		return status.Errorf(codes.ResourceExhausted, "create session: %v", err)
	}

	resp, _ := json.Marshal(map[string]string{
		"session_token": session.ID,
	})
	if err := sendActionResult(stream, &flight.Result{Body: resp}); err != nil {
		_ = h.pool.DestroySession(session.ID)
		return err
	}
	return nil
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
	finishDrain, err := h.pool.beginDrainWork(false)
	if drainErr := workerDrainingStatus(err); drainErr != nil {
		return drainErr
	}
	if err != nil {
		return status.Errorf(codes.Internal, "start activation drain tracking: %v", err)
	}
	defer finishDrain()

	if err := h.pool.activateTenantFunc(payload); err != nil {
		return status.Errorf(codes.FailedPrecondition, "activate tenant: %v", err)
	}

	resp, _ := json.Marshal(map[string]bool{"ok": true})
	return sendActionResult(stream, &flight.Result{Body: resp})
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
	return sendActionResult(stream, &flight.Result{Body: resp})
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
		"draining":         h.pool.IsDraining(),
		"sessions":         h.pool.ActiveSessions(),
		"active_queries":   h.pool.ActiveDrainWork(),
		"uptime_ns":        time.Since(h.pool.startTime).Nanoseconds(),
		"session_progress": sessionProgress,
	})
	return sendActionResult(stream, &flight.Result{Body: resp})
}

// Flight SQL method implementations

func (h *FlightSQLHandler) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {

	session, err := h.sessionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	finishOperation, ok := session.beginOperation()
	if openErr := sessionOpenStatus(ok); openErr != nil {
		return nil, openErr
	}
	releaseOperationOnReturn := true
	defer func() {
		if releaseOperationOnReturn {
			finishOperation()
		}
	}()

	query := cmd.GetQuery()
	var tx *sql.Tx
	var txnKey string
	var ttx *trackedTx
	if !isEmptyFlightQuery(query) {
		tx, txnKey, ttx, err = session.getOpenTxn(cmd.GetTransactionId())
		if err != nil {
			return nil, err
		}
	}
	var endConnWork func()
	if tx == nil && !isEmptyFlightQuery(query) {
		var ok bool
		endConnWork, ok = session.beginConnWork()
		if !ok {
			return nil, status.Error(codes.NotFound, "session closed")
		}
		defer endConnWork()
	}
	finishDrain, err := h.pool.beginDrainWork(session.allowsDrainContinuation(txnKey))
	if drainErr := workerDrainingStatus(err); drainErr != nil {
		return nil, drainErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start query drain tracking: %v", err)
	}
	releaseOnReturn := true
	defer func() {
		if releaseOnReturn {
			finishDrain()
		}
	}()

	// Handle empty queries (e.g., ";" from PostgreSQL client pings).
	// Return an empty schema instead of sending to DuckDB which rejects empty queries.
	if isEmptyFlightQuery(query) {
		emptySchema := arrow.NewSchema(nil, nil)
		handleID := fmt.Sprintf("query-%d", session.handleCounter.Add(1))
		ticketBytes, ticketErr := flightsql.CreateStatementQueryTicket([]byte(handleID))
		if ticketErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to create ticket: %v", ticketErr)
		}
		if !addQueryHandle(session, handleID, &QueryHandle{Query: query, Schema: emptySchema, finishOperation: finishOperation, createdAt: time.Now()}) {
			return nil, status.Error(codes.NotFound, "session closed")
		}
		releaseOperationOnReturn = false
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

	session.progress.queryActive.Store(true)
	defer session.progress.queryActive.Store(false)
	endTxnWork := ttx.beginWork()
	defer endTxnWork()

	// Only retry on transient errors for autocommit queries. Inside a
	// transaction, a transient error invalidates the transaction — retrying
	// would run in autocommit mode and mask the failure.
	inTransaction := tx != nil || session.sqlTxActive.Load()
	var schema *arrow.Schema
	if inTransaction {
		schema, err = session.getQuerySchema(ctx, query, tx)
	} else {
		schema, err = retryOnTransient(func() (*arrow.Schema, error) {
			return session.getQuerySchema(ctx, query, tx)
		})
	}
	// Conflict retry for autocommit only. Note: if retryOnTransient exhausted on a
	// transient error that also matches "Transaction conflict", this chains into
	// conflict retry — acceptable since the error patterns are distinct in practice.
	if shouldRetryDuckLakeConflict(err, inTransaction) {
		ducklakeConflictTotal.Inc()
		schema, err = retryOnConflict(func() (*arrow.Schema, error) {
			return session.getQuerySchema(ctx, query, tx)
		})
	}
	if err != nil {
		schema, err, _ = recoverAbortedTransaction(
			err,
			!inTransaction,
			func() error { return session.rollbackConn(context.Background()) },
			func() (*arrow.Schema, error) {
				return session.getQuerySchema(ctx, query, tx)
			},
		)
	}
	if err != nil {
		if closedErr := sessionClosedStatus(err); closedErr != nil {
			return nil, closedErr
		}
		return nil, status.Errorf(codes.InvalidArgument, "failed to prepare query: %v", err)
	}

	// Send DuckDB profiling output as gRPC trailing metadata so the
	// control plane can attach it to the trace span.
	sendProfilingMetadata(ctx, session)

	handleID := fmt.Sprintf("query-%d", session.handleCounter.Add(1))
	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(handleID))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create ticket: %v", err)
	}
	if !addQueryHandle(session, handleID, &QueryHandle{Query: query, Schema: schema, TxnID: txnKey, finishDrain: finishDrain, finishOperation: finishOperation, createdAt: time.Now()}) {
		return nil, status.Error(codes.NotFound, "session closed")
	}
	releaseOnReturn = false
	releaseOperationOnReturn = false

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

	handle, ok := popQueryHandle(session, handleID)
	if !ok {
		return nil, nil, status.Error(codes.NotFound, "query handle not found")
	}

	var tx *sql.Tx
	var ttx *trackedTx
	if handle.TxnID != "" {
		session.mu.RLock()
		ttx = session.txns[handle.TxnID]
		session.mu.RUnlock()
		if ttx == nil || ttx.tx == nil {
			releaseQueryHandleValue(handle)
			return nil, nil, status.Error(codes.NotFound, "transaction not found")
		}
		ttx.lastUsed.Store(time.Now().UnixNano())
		tx = ttx.tx
	}

	schema := handle.Schema

	ch := make(chan flight.StreamChunk, 10)

	// Empty queries have no rows to fetch — return an empty stream immediately.
	if isEmptyFlightQuery(handle.Query) {
		releaseQueryHandleValue(handle)
		close(ch)
		return schema, ch, nil
	}
	var endConnWork func()
	if tx == nil {
		var ok bool
		endConnWork, ok = session.beginConnWork()
		if !ok {
			releaseQueryHandleValue(handle)
			return nil, nil, status.Error(codes.NotFound, "session closed")
		}
	} else {
		endConnWork = func() {}
	}
	go func() {
		defer close(ch)
		defer func() {
			releaseQueryHandleValue(handle)
		}()
		defer endConnWork()

		session.progress.queryActive.Store(true)
		defer session.progress.queryActive.Store(false)
		endTxnWork := ttx.beginWork()
		defer endTxnWork()

		inTxn := tx != nil || session.sqlTxActive.Load()
		var closeRows func() error
		queryFn := func() (*sql.Rows, error) {
			rows, closer, err := session.queryRows(ctx, tx, handle.Query)
			if err != nil {
				return nil, err
			}
			closeRows = closer
			return rows, nil
		}

		var rows *sql.Rows
		var qerr error
		if inTxn {
			rows, qerr = queryFn()
		} else {
			rows, qerr = retryOnTransient(queryFn)
		}
		// Conflict retry for autocommit only (see GetFlightInfoStatement comment).
		if shouldRetryDuckLakeConflict(qerr, inTxn) {
			ducklakeConflictTotal.Inc()
			rows, qerr = retryOnConflict(func() (*sql.Rows, error) {
				return queryFn()
			})
		}
		if qerr != nil {
			rows, qerr, _ = recoverAbortedTransaction(
				qerr,
				!inTxn,
				func() error { return session.rollbackConn(context.Background()) },
				func() (*sql.Rows, error) { return queryFn() },
			)
		}
		if qerr != nil {
			_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: qerr})
			return
		}
		defer func() {
			_ = closeRows()
		}()

		for {
			record, recErr := RowsToRecord(h.alloc, rows, schema, 1024)
			if recErr != nil {
				_ = sendStreamChunk(ctx, ch, flight.StreamChunk{Err: recErr})
				return
			}
			if record == nil {
				break
			}
			if !sendStreamChunk(ctx, ch, flight.StreamChunk{Data: record}) {
				record.Release()
				return
			}
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
	finishOperation, ok := session.beginOperation()
	if openErr := sessionOpenStatus(ok); openErr != nil {
		return 0, openErr
	}
	defer finishOperation()

	tx, _, ttx, err := session.getOpenTxn(cmd.GetTransactionId())
	if err != nil {
		return 0, err
	}

	query := cmd.GetQuery()
	var endConnWork func()
	if tx == nil && !isEmptyFlightQuery(query) {
		var ok bool
		endConnWork, ok = session.beginConnWork()
		if !ok {
			return 0, status.Error(codes.NotFound, "session closed")
		}
		defer endConnWork()
	}
	finishDrain, err := h.pool.beginDrainWork(session.allowsDrainContinuation(string(cmd.GetTransactionId())))
	if drainErr := workerDrainingStatus(err); drainErr != nil {
		return 0, drainErr
	}
	if err != nil {
		return 0, status.Errorf(codes.Internal, "start update drain tracking: %v", err)
	}
	releaseOnReturn := true
	defer func() {
		if releaseOnReturn {
			finishDrain()
		}
	}()

	// Handle empty queries (e.g., ";" from PostgreSQL client pings).
	if isEmptyFlightQuery(query) {
		return 0, nil
	}
	session.progress.queryActive.Store(true)
	defer session.progress.queryActive.Store(false)
	endTxnWork := ttx.beginWork()
	defer endTxnWork()

	execFn := func() (sql.Result, error) {
		return session.exec(ctx, tx, query)
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

	// Conflict retry for autocommit only (see GetFlightInfoStatement comment).
	if shouldRetryDuckLakeConflict(execErr, inTransaction) {
		ducklakeConflictTotal.Inc()
		result, execErr = retryOnConflict(func() (sql.Result, error) {
			return session.execConn(ctx, query)
		})
	}
	if execErr != nil {
		result, execErr, _ = recoverAbortedTransaction(
			execErr,
			!inTransaction,
			func() error { return session.rollbackConn(context.Background()) },
			func() (sql.Result, error) {
				return execFn()
			},
		)
	}
	// Track SQL-level transaction state for BEGIN/COMMIT/ROLLBACK sent as raw SQL.
	trackSQLTransactionState(query, execErr, &session.sqlTxActive)
	if tx == nil && isTransactionStartStmt(query) && execErr == nil {
		if !session.setSQLTransactionDrain(finishDrain) {
			return 0, status.Error(codes.NotFound, "session closed")
		}
		releaseOnReturn = false
	}
	if tx == nil && isTxControl && execErr == nil {
		session.releaseSQLTransactionDrain()
	}
	if execErr != nil {
		if closedErr := sessionClosedStatus(execErr); closedErr != nil {
			return 0, closedErr
		}
		return 0, status.Errorf(codes.InvalidArgument, "failed to execute update: %v", execErr)
	}

	sendProfilingMetadata(ctx, session)

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
	finishOperation, ok := session.beginOperation()
	if openErr := sessionOpenStatus(ok); openErr != nil {
		return nil, openErr
	}
	defer finishOperation()
	finishDrain, err := h.pool.beginDrainWork(false)
	if drainErr := workerDrainingStatus(err); drainErr != nil {
		return nil, drainErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start transaction drain tracking: %v", err)
	}
	_ = req

	tx, err := session.beginTx(context.Background())
	if err != nil {
		finishDrain()
		if closedErr := sessionClosedStatus(err); closedErr != nil {
			return nil, closedErr
		}
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	txnKey := fmt.Sprintf("txn-%d", session.handleCounter.Add(1))
	ttx := &trackedTx{tx: tx, finishDrain: finishDrain}
	ttx.lastUsed.Store(time.Now().UnixNano())

	if !addTrackedTransaction(session, txnKey, ttx) {
		_ = session.rollbackTx(tx)
		finishDrain()
		return nil, status.Error(codes.NotFound, "session closed")
	}

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

	finishOperation, txnExists, ok := session.beginOperationForTransaction(txnKey)
	if !txnExists {
		return status.Error(codes.NotFound, "transaction not found")
	}
	var handleDrainReleases []func()
	var handleOperationReleases []func()
	if !ok {
		handleDrainReleases, handleOperationReleases = popAbandonedTransactionContinuations(session, txnKey)
		if len(handleOperationReleases) == 0 {
			if openErr := sessionOpenStatus(false); openErr != nil {
				return openErr
			}
		}
	} else {
		handleDrainReleases, handleOperationReleases = popAbandonedTransactionContinuations(session, txnKey)
		handleOperationReleases = append(handleOperationReleases, finishOperation)
	}
	defer func() {
		for _, release := range handleOperationReleases {
			releaseDrainFunc(release)
		}
	}()
	for _, release := range handleDrainReleases {
		releaseDrainFunc(release)
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
	defer func() {
		if ttx.finishDrain != nil {
			ttx.finishDrain()
			ttx.finishDrain = nil
		}
	}()

	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		if err := session.commitTx(ttx.tx); err != nil {
			return status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
		}
		return nil
	case flightsql.EndTransactionRollback:
		if err := session.rollbackTx(ttx.tx); err != nil {
			return status.Errorf(codes.Internal, "failed to rollback transaction: %v", err)
		}
		return nil
	default:
		_ = session.rollbackTx(ttx.tx)
		return status.Error(codes.InvalidArgument, "unsupported end transaction action")
	}
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

func (s *Session) getOpenTxn(transactionID []byte) (*sql.Tx, string, *trackedTx, error) {
	if len(transactionID) == 0 {
		return nil, "", nil, nil
	}
	txnKey := string(transactionID)
	s.mu.RLock()
	ttx, ok := s.txns[txnKey]
	s.mu.RUnlock()
	if !ok || ttx == nil || ttx.tx == nil {
		return nil, "", nil, status.Error(codes.NotFound, "transaction not found")
	}
	ttx.lastUsed.Store(time.Now().UnixNano())
	return ttx.tx, txnKey, ttx, nil
}
