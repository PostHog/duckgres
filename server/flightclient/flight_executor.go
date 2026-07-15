package flightclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/duckdbservice/arrowmap"
	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/sqlcore"
	"github.com/posthog/duckgres/server/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MaxGRPCMessageSize is the max gRPC message size for Flight SQL communication.
// DuckDB query results can easily exceed the default 4MB limit.
const MaxGRPCMessageSize = 1 << 30 // 1GB

const (
	waitSessionIdleAction    = "WaitSessionIdle"
	releaseQueryHandleAction = "ReleaseQueryHandle"
	logQueryAction           = "LogQuery"
	queryCloseWaitTimeout    = 30 * time.Second
	queryLogForwardTimeout   = 5 * time.Second
	queryLogForwardBatchSize = 100
	queryLogForwardInterval  = 1 * time.Second
	queryLogForwardQueueSize = 10000
)

// ErrWorkerDead is returned when the backing worker process has crashed.
var ErrWorkerDead = errors.New("flight worker is dead")

// FlightExecutor implements QueryExecutor backed by an Arrow Flight SQL client.
// It routes queries to a duckdb-service worker process over a Unix socket.
type FlightExecutor struct {
	client       *flightsql.Client
	sessionToken string
	workerID     int
	cpInstanceID string
	ownerEpoch   int64
	alloc        memory.Allocator
	ownsClient   bool // if true, Close() closes the client

	// dead is set to true when the backing worker crashes. Once set, all
	// RPC methods return ErrWorkerDead without touching the gRPC client.
	dead atomic.Bool

	ctx    context.Context    // base context for all requests
	cancel context.CancelFunc // cancels the base context

	// lastProfiling stores the most recent DuckDB profiling output received
	// from the worker via gRPC trailing metadata.
	lastProfiling atomic.Value // stores string

	queryLogCh       chan wire.QueryLogEntry
	queryLogDone     chan struct{}
	queryLogStopOnce sync.Once
}

// NewFlightExecutor creates a FlightExecutor connected to the given address.
// addr should be "unix:///path/to/socket" for Unix sockets or "host:port" for TCP.
// bearerToken is the authentication token for the duckdb-service.
// sessionToken is the session identifier for the x-duckgres-session header.
func NewFlightExecutor(addr, bearerToken, sessionToken string) (*FlightExecutor, error) {
	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize),
		grpc.MaxCallSendMsgSize(MaxGRPCMessageSize),
	))

	// Propagate OTEL trace context across gRPC to worker pods.
	// Filtered to query RPCs only (GetFlightInfo, DoGet).
	dialOpts = append(dialOpts, sqlcore.OTELGRPCClientHandler())

	if bearerToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&bearerCreds{token: bearerToken}))
	}

	client, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("flight sql client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e := &FlightExecutor{
		client:       client,
		sessionToken: sessionToken,
		ownerEpoch:   0,
		alloc:        memory.DefaultAllocator,
		ownsClient:   true,
		ctx:          ctx,
		cancel:       cancel,
	}
	e.startQueryLogForwarder()
	return e, nil
}

// NewFlightExecutorFromClient creates a FlightExecutor that shares an existing
// Flight SQL client. The client is NOT closed when this executor is closed.
// This avoids creating a new gRPC connection per session.
func NewFlightExecutorFromClient(client *flightsql.Client, sessionToken string) *FlightExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	e := &FlightExecutor{
		client:       client,
		sessionToken: sessionToken,
		ownerEpoch:   0,
		alloc:        memory.DefaultAllocator,
		ownsClient:   false,
		ctx:          ctx,
		cancel:       cancel,
	}
	e.startQueryLogForwarder()
	return e
}

// MarkDead marks this executor's backing worker as dead. All subsequent RPC
// calls will return ErrWorkerDead without touching the (possibly closed) gRPC client.
func (e *FlightExecutor) MarkDead() {
	e.dead.Store(true)
}

// IsDead reports whether this executor has been marked dead.
func (e *FlightExecutor) IsDead() bool {
	return e.dead.Load()
}

// withSession adds the session token to the gRPC context.
func (e *FlightExecutor) withSession(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(
		ctx,
		"x-duckgres-session", e.sessionToken,
		"x-duckgres-worker-id", strconv.Itoa(e.workerID),
		"x-duckgres-cp-instance-id", e.cpInstanceID,
		"x-duckgres-owner-epoch", strconv.FormatInt(e.ownerEpoch, 10),
	)
}

func (e *FlightExecutor) SetOwnerEpoch(ownerEpoch int64) {
	e.ownerEpoch = ownerEpoch
}

func (e *FlightExecutor) SetControlMetadata(workerID int, cpInstanceID string, ownerEpoch int64) {
	e.workerID = workerID
	e.cpInstanceID = cpInstanceID
	e.ownerEpoch = ownerEpoch
}

// recoverClientPanic converts a nil-pointer panic from a closed Flight SQL
// client into an error. The arrow-go Close() method nils out the embedded
// FlightServiceClient, so any concurrent RPC on the shared client panics.
// Only nil-pointer dereferences are recovered; other panics are re-raised
// to preserve stack traces for unrelated programmer errors.
func recoverClientPanic(err *error) {
	if r := recover(); r != nil {
		if re, ok := r.(runtime.Error); ok && strings.Contains(re.Error(), "nil pointer") {
			*err = fmt.Errorf("flight client panic (worker likely crashed): %v", r)
			return
		}
		panic(r)
	}
}

func (e *FlightExecutor) QueryContext(ctx context.Context, query string, args ...any) (rs sqlcore.RowSet, err error) {
	if e.dead.Load() {
		return nil, ErrWorkerDead
	}

	// Return empty results for queries that are only semicolons, whitespace,
	// and/or comments. These represent PostgreSQL client pings (e.g., pgx sends "-- ping").
	if sqlcore.IsEmptyQuery(query) {
		return &emptyRowSet{}, nil
	}

	defer recoverClientPanic(&err)

	if len(args) > 0 {
		query = interpolateArgs(query, args)
	}

	reqCtx, cancel := e.mergedContext(ctx)
	success := false
	defer func() {
		if !success {
			cancel()
		}
	}()

	reqCtx = e.withSession(reqCtx)

	var trailer metadata.MD
	info, err := e.client.Execute(reqCtx, query, grpc.Trailer(&trailer))
	e.storeProfilingFromTrailer(trailer)
	if err != nil {
		return nil, fmt.Errorf("flight execute: %w", err)
	}

	if len(info.Endpoint) == 0 {
		// No data, but the schema may still be available (e.g., LIMIT 0 queries).
		// Preserve it so callers can inspect column types.
		if len(info.Schema) > 0 {
			schema, schemaErr := flight.DeserializeSchema(info.Schema, e.alloc)
			if schemaErr == nil {
				return &emptySchemaRowSet{schema: schema}, nil
			}
		}
		return &emptyRowSet{}, nil
	}

	ticket := info.Endpoint[0].Ticket
	if err := reqCtx.Err(); err != nil {
		_ = e.releaseQueryHandle(ticket)
		_ = e.waitForSessionIdle()
		return nil, err
	}

	reader, err := e.client.DoGet(reqCtx, ticket)
	if err != nil {
		// If cancellation lands after Execute has registered a worker-side
		// handle but before DoGet returns a RowSet, there is no Close call to
		// acknowledge the abandoned split-phase operation. Release the handle
		// explicitly, then wait in case DoGet consumed it and is still unwinding.
		_ = e.releaseQueryHandle(ticket)
		_ = e.waitForSessionIdle()
		return nil, fmt.Errorf("flight doget: %w", err)
	}

	schema, err := flight.DeserializeSchema(info.Schema, e.alloc)
	if err != nil {
		cancel()
		reader.Release()
		_ = e.waitForSessionIdle()
		return nil, fmt.Errorf("flight deserialize schema: %w", err)
	}

	success = true
	return &FlightRowSet{
		reader:        reader,
		schema:        schema,
		cancel:        cancel,
		waitForClosed: e.waitForSessionIdle,
	}, nil
}

func (e *FlightExecutor) ExecContext(ctx context.Context, query string, args ...any) (result sqlcore.ExecResult, err error) {
	if e.dead.Load() {
		return nil, ErrWorkerDead
	}

	// Return zero rows affected for empty/comment-only queries.
	if sqlcore.IsEmptyQuery(query) {
		return &flightExecResult{rowsAffected: 0}, nil
	}

	defer recoverClientPanic(&err)

	if len(args) > 0 {
		query = interpolateArgs(query, args)
	}

	reqCtx, cancel := e.mergedContext(ctx)
	defer cancel()

	reqCtx = e.withSession(reqCtx)

	// Note: we hand-roll the DoPut here instead of calling
	// flightsql.Client.ExecuteUpdate so we can capture the gRPC trailer.
	// ExecuteUpdate is a bidi-streaming RPC and grpc.Trailer(&md) as a
	// CallOption only works for unary RPCs — for streams the trailer is
	// only reachable via stream.Trailer() after Recv returns io.EOF.
	// Without that capture the worker's per-query profiling JSON is
	// silently dropped on this side. See executeUpdateWithTrailer.
	affected, trailer, err := executeUpdateWithTrailer(reqCtx, e.client, query)
	e.storeProfilingFromTrailer(trailer)
	if err != nil {
		return nil, fmt.Errorf("flight execute update: %w", err)
	}

	return &flightExecResult{rowsAffected: affected}, nil
}

func (e *FlightExecutor) Query(query string, args ...any) (sqlcore.RowSet, error) {
	return e.QueryContext(context.Background(), query, args...)
}

func (e *FlightExecutor) Exec(query string, args ...any) (sqlcore.ExecResult, error) {
	return e.ExecContext(context.Background(), query, args...)
}

// QueryWithBoundParams executes a query using compact Bind parameters. Text
// values are appended straight into the final Flight SQL request buffer by the
// portal-owned appender, avoiding an intermediate string/interface for every
// value in a large extended-protocol Bind.
func (e *FlightExecutor) QueryWithBoundParams(query string, params sqlcore.SQLLiteralAppender) (sqlcore.RowSet, error) {
	interpolated, err := interpolateBoundArgs(query, params)
	if err != nil {
		return nil, err
	}
	return e.Query(interpolated)
}

// ExecWithBoundParams is the Exec counterpart of QueryWithBoundParams.
func (e *FlightExecutor) ExecWithBoundParams(query string, params sqlcore.SQLLiteralAppender) (sqlcore.ExecResult, error) {
	interpolated, err := interpolateBoundArgs(query, params)
	if err != nil {
		return nil, err
	}
	return e.Exec(interpolated)
}

func (e *FlightExecutor) ConnContext(ctx context.Context) (sqlcore.RawConn, error) {
	return nil, fmt.Errorf("ConnContext not supported in Flight mode (use batched INSERT for COPY FROM)")
}

func (e *FlightExecutor) PingContext(ctx context.Context) error {
	// Use a simple query to verify connectivity
	rows, err := e.QueryContext(ctx, "SELECT 1")
	if err != nil {
		return fmt.Errorf("flight ping: %w", err)
	}
	return rows.Close()
}

// mergedContext returns a context that is cancelled when either the caller's
// context or the executor's base context is done. This ensures gRPC calls are
// cancelled both when the client disconnects (caller ctx) and when the
// executor is closed (e.g. worker crash).
func (e *FlightExecutor) mergedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	merged, cancel := context.WithCancel(ctx)
	if e.ctx != nil {
		go func() {
			select {
			case <-e.ctx.Done():
				cancel()
			case <-merged.Done():
			}
		}()
	}
	return merged, cancel
}

func (e *FlightExecutor) Close() error {
	if !e.stopQueryLogForwarder() && e.cancel != nil {
		e.cancel()
		e.waitQueryLogForwarder()
	}
	if e.cancel != nil {
		e.cancel()
	}
	if e.ownsClient {
		return e.client.Close()
	}
	return nil
}

// Log implements the server query-log forwarding hook without making query
// completion wait on worker RPC or DuckLake writes.
func (e *FlightExecutor) Log(entry wire.QueryLogEntry) {
	if e == nil {
		return
	}
	if e.dead.Load() {
		observe.AddQueryLogDroppedEntries("forward_worker_dead", 1)
		return
	}
	if e.queryLogCh == nil {
		observe.AddQueryLogDroppedEntries("forward_unavailable", 1)
		return
	}
	defer func() {
		if recover() != nil {
			observe.AddQueryLogDroppedEntries("forward_closed", 1)
		}
	}()
	select {
	case e.queryLogCh <- entry:
	default:
		observe.AddQueryLogDroppedEntries("forward_buffer_full", 1)
	}
}

func (e *FlightExecutor) startQueryLogForwarder() {
	e.queryLogCh = make(chan wire.QueryLogEntry, queryLogForwardQueueSize)
	e.queryLogDone = make(chan struct{})
	go e.queryLogForwardLoop()
}

func (e *FlightExecutor) stopQueryLogForwarder() bool {
	stopped := true
	e.queryLogStopOnce.Do(func() {
		if e.queryLogCh != nil {
			close(e.queryLogCh)
		}
		stopped = e.waitQueryLogForwarder()
	})
	return stopped
}

func (e *FlightExecutor) waitQueryLogForwarder() bool {
	if e.queryLogDone == nil {
		return true
	}
	select {
	case <-e.queryLogDone:
		return true
	case <-time.After(queryLogForwardTimeout):
		return false
	}
}

func (e *FlightExecutor) queryLogForwardLoop() {
	defer close(e.queryLogDone)
	ticker := time.NewTicker(queryLogForwardInterval)
	defer ticker.Stop()

	batch := make([]wire.QueryLogEntry, 0, queryLogForwardBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		// forwardQueryLogBatch records dropped-entry metrics for failed batches.
		_ = e.forwardQueryLogBatch(batch)
		batch = batch[:0]
	}

	for {
		select {
		case entry, ok := <-e.queryLogCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, entry)
			if len(batch) >= queryLogForwardBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-e.ctx.Done():
			observe.AddQueryLogDroppedEntries("forward_context_done", len(batch)+len(e.queryLogCh))
			return
		}
	}
}

func (e *FlightExecutor) forwardQueryLogBatch(entries []wire.QueryLogEntry) (err error) {
	if len(entries) == 0 {
		return nil
	}
	if e.dead.Load() {
		observe.AddQueryLogDroppedEntries("forward_worker_dead", len(entries))
		return nil
	}
	if e.client == nil || e.client.Client == nil {
		observe.AddQueryLogDroppedEntries("forward_unavailable", len(entries))
		return nil
	}
	defer func() {
		recoverClientPanic(&err)
		if err != nil {
			observe.AddQueryLogDroppedEntries("forward_error", len(entries))
		}
	}()

	payload, err := json.Marshal(wire.WorkerQueryLogPayload{
		WorkerControlMetadata: wire.WorkerControlMetadata{
			WorkerID:     e.workerID,
			OwnerEpoch:   e.ownerEpoch,
			CPInstanceID: e.cpInstanceID,
		},
		Entries: entries,
	})
	if err != nil {
		return err
	}

	baseCtx := context.Background()
	if e.ctx != nil {
		baseCtx = e.ctx
	}
	ctx, cancel := context.WithTimeout(baseCtx, queryLogForwardTimeout)
	defer cancel()
	stream, err := e.client.Client.DoAction(
		e.withSession(ctx),
		&flight.Action{Type: logQueryAction, Body: payload},
	)
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func isTerminalSessionIdleWaitError(err error) bool {
	if strings.Contains(err.Error(), "flight client panic") {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.Unavailable, codes.FailedPrecondition, codes.NotFound:
		return true
	default:
		return false
	}
}

func (e *FlightExecutor) waitForSessionIdle() (err error) {
	if e.dead.Load() {
		return nil
	}
	if e.client == nil || e.client.Client == nil {
		return nil
	}
	defer func() {
		recoverClientPanic(&err)
		if err != nil && isTerminalSessionIdleWaitError(err) {
			err = nil
		}
	}()

	payload, err := json.Marshal(wire.WorkerWaitSessionIdlePayload{
		WorkerControlMetadata: wire.WorkerControlMetadata{
			WorkerID:     e.workerID,
			OwnerEpoch:   e.ownerEpoch,
			CPInstanceID: e.cpInstanceID,
		},
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryCloseWaitTimeout)
	defer cancel()
	if e.ctx != nil {
		go func() {
			select {
			case <-e.ctx.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	stream, err := e.client.Client.DoAction(
		e.withSession(ctx),
		&flight.Action{Type: waitSessionIdleAction, Body: payload},
	)
	if err != nil {
		if isTerminalSessionIdleWaitError(err) {
			return nil
		}
		return err
	}
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if isTerminalSessionIdleWaitError(err) {
				return nil
			}
			return err
		}
	}
}

func (e *FlightExecutor) releaseQueryHandle(ticket *flight.Ticket) (err error) {
	if e.dead.Load() {
		return nil
	}
	if e.client == nil || e.client.Client == nil || ticket == nil || len(ticket.Ticket) == 0 {
		return nil
	}
	defer func() {
		recoverClientPanic(&err)
		if err != nil && isTerminalSessionIdleWaitError(err) {
			err = nil
		}
	}()

	payload, err := json.Marshal(wire.WorkerReleaseQueryHandlePayload{
		WorkerControlMetadata: wire.WorkerControlMetadata{
			WorkerID:     e.workerID,
			OwnerEpoch:   e.ownerEpoch,
			CPInstanceID: e.cpInstanceID,
		},
		Ticket: ticket.Ticket,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryCloseWaitTimeout)
	defer cancel()
	if e.ctx != nil {
		go func() {
			select {
			case <-e.ctx.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	stream, err := e.client.Client.DoAction(
		e.withSession(ctx),
		&flight.Action{Type: releaseQueryHandleAction, Body: payload},
	)
	if err != nil {
		if isTerminalSessionIdleWaitError(err) {
			return nil
		}
		return err
	}
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if isTerminalSessionIdleWaitError(err) {
				return nil
			}
			return err
		}
	}
}

func (e *FlightExecutor) LastProfilingOutput() string {
	v := e.lastProfiling.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

const profilingMetadataKey = "x-duckgres-profiling"

func (e *FlightExecutor) storeProfilingFromTrailer(trailer metadata.MD) {
	if vals := trailer.Get(profilingMetadataKey); len(vals) > 0 {
		e.lastProfiling.Store(vals[0])
	} else {
		e.lastProfiling.Store("")
	}
}

// FlightRowSet wraps an Arrow Flight RecordBatch reader to implement RowSet.
type FlightRowSet struct {
	reader *flight.Reader
	schema *arrow.Schema

	// Current batch state
	currentBatch  arrow.RecordBatch
	batchRow      int // current row index within currentBatch
	done          bool
	err           error
	closeOnce     sync.Once
	closeErr      error
	cancel        context.CancelFunc
	waitForClosed func() error
}

func (r *FlightRowSet) Columns() ([]string, error) {
	names := make([]string, r.schema.NumFields())
	for i := 0; i < r.schema.NumFields(); i++ {
		names[i] = r.schema.Field(i).Name
	}
	return names, nil
}

func (r *FlightRowSet) ColumnTypes() ([]sqlcore.ColumnTyper, error) {
	types := make([]sqlcore.ColumnTyper, r.schema.NumFields())
	for i := 0; i < r.schema.NumFields(); i++ {
		types[i] = &arrowColumnType{dt: r.schema.Field(i).Type}
	}
	return types, nil
}

func (r *FlightRowSet) Next() bool {
	if r.done || r.err != nil {
		return false
	}

	// Advance within current batch
	if r.currentBatch != nil && r.batchRow+1 < int(r.currentBatch.NumRows()) {
		r.batchRow++
		return true
	}

	// Release previous batch
	if r.currentBatch != nil {
		r.currentBatch.Release()
		r.currentBatch = nil
	}

	// Read next batch, skipping empty batches
	for r.reader.Next() {
		r.currentBatch = r.reader.RecordBatch()
		r.currentBatch.Retain()
		r.batchRow = 0
		if r.currentBatch.NumRows() > 0 {
			return true
		}
		r.currentBatch.Release()
		r.currentBatch = nil
	}

	r.done = true
	r.err = r.reader.Err()
	return false
}

func (r *FlightRowSet) Scan(dest ...any) error {
	if r.currentBatch == nil {
		return fmt.Errorf("no current row")
	}

	numCols := int(r.currentBatch.NumCols())
	if len(dest) != numCols {
		return fmt.Errorf("scan: expected %d destinations, got %d", numCols, len(dest))
	}

	for i := 0; i < numCols; i++ {
		col := r.currentBatch.Column(i)
		val := extractArrowValue(col, r.batchRow)
		ptr, ok := dest[i].(*interface{})
		if !ok {
			return fmt.Errorf("scan: destination %d must be *interface{}", i)
		}
		*ptr = val
	}

	return nil
}

func (r *FlightRowSet) Close() error {
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		if r.currentBatch != nil {
			r.currentBatch.Release()
			r.currentBatch = nil
		}
		r.reader.Release()
		if r.waitForClosed != nil && (!r.done || r.err != nil) {
			r.closeErr = r.waitForClosed()
		}
	})
	return r.closeErr
}

func (r *FlightRowSet) Err() error {
	return r.err
}

// emptyRowSet is returned when a query produces no endpoints and no schema.
type emptyRowSet struct{}

func (e *emptyRowSet) Columns() ([]string, error)                  { return nil, nil }
func (e *emptyRowSet) ColumnTypes() ([]sqlcore.ColumnTyper, error) { return nil, nil }
func (e *emptyRowSet) Next() bool                                  { return false }
func (e *emptyRowSet) Scan(dest ...any) error                      { return fmt.Errorf("no rows") }
func (e *emptyRowSet) Close() error                                { return nil }
func (e *emptyRowSet) Err() error                                  { return nil }

// emptySchemaRowSet is returned when a query produces no data rows but does
// have schema information (e.g., SELECT ... LIMIT 0). This preserves column
// names and types for callers like COPY FROM STDIN that need to inspect the
// target table schema.
type emptySchemaRowSet struct {
	schema *arrow.Schema
}

func (e *emptySchemaRowSet) Columns() ([]string, error) {
	cols := make([]string, e.schema.NumFields())
	for i := 0; i < e.schema.NumFields(); i++ {
		cols[i] = e.schema.Field(i).Name
	}
	return cols, nil
}

func (e *emptySchemaRowSet) ColumnTypes() ([]sqlcore.ColumnTyper, error) {
	types := make([]sqlcore.ColumnTyper, e.schema.NumFields())
	for i := 0; i < e.schema.NumFields(); i++ {
		types[i] = &arrowColumnType{dt: e.schema.Field(i).Type}
	}
	return types, nil
}

func (e *emptySchemaRowSet) Next() bool        { return false }
func (e *emptySchemaRowSet) Scan(...any) error { return fmt.Errorf("no rows") }
func (e *emptySchemaRowSet) Close() error      { return nil }
func (e *emptySchemaRowSet) Err() error        { return nil }

// flightExecResult implements ExecResult for Flight SQL updates.
type flightExecResult struct {
	rowsAffected int64
}

func (r *flightExecResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// arrowColumnType implements ColumnTyper by mapping Arrow DataType to DuckDB type names.
type arrowColumnType struct {
	dt arrow.DataType
}

func (c *arrowColumnType) DatabaseTypeName() string {
	return arrowTypeToDuckDB(c.dt)
}

// arrowTypeToDuckDB maps an Arrow DataType back to a DuckDB type name string.
func arrowTypeToDuckDB(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "UTINYINT"
	case arrow.UINT16:
		return "USMALLINT"
	case arrow.UINT32:
		return "UINTEGER"
	case arrow.UINT64:
		return "UBIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING, arrow.LARGE_STRING:
		return "VARCHAR"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "BLOB"
	case arrow.DATE32:
		return "DATE"
	case arrow.TIME64:
		return "TIME"
	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		if ts.TimeZone != "" {
			return "TIMESTAMPTZ"
		}
		return "TIMESTAMP"
	case arrow.DECIMAL128:
		dec := dt.(*arrow.Decimal128Type)
		return fmt.Sprintf("DECIMAL(%d,%d)", dec.Precision, dec.Scale)
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return "INTERVAL"
	case arrow.LIST:
		elem := dt.(*arrow.ListType).Elem()
		return arrowTypeToDuckDB(elem) + "[]"
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		var buf strings.Builder
		buf.WriteString("STRUCT(")
		for i := 0; i < st.NumFields(); i++ {
			if i > 0 {
				buf.WriteString(", ")
			}
			f := st.Field(i)
			fmt.Fprintf(&buf, "%q %s", f.Name, arrowTypeToDuckDB(f.Type))
		}
		buf.WriteByte(')')
		return buf.String()
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		return fmt.Sprintf("MAP(%s, %s)", arrowTypeToDuckDB(mt.KeyType()), arrowTypeToDuckDB(mt.ItemType()))
	default:
		return "VARCHAR"
	}
}

// extractArrowValue extracts a Go value from an Arrow array at the given row index.
func extractArrowValue(col arrow.Array, row int) interface{} {
	if col.IsNull(row) {
		return nil
	}

	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(row)
	case *array.Int8:
		return int8(arr.Value(row))
	case *array.Int16:
		return int16(arr.Value(row))
	case *array.Int32:
		return int32(arr.Value(row))
	case *array.Int64:
		return int64(arr.Value(row))
	case *array.Uint8:
		return uint8(arr.Value(row))
	case *array.Uint16:
		return uint16(arr.Value(row))
	case *array.Uint32:
		return uint32(arr.Value(row))
	case *array.Uint64:
		return uint64(arr.Value(row))
	case *array.Float32:
		return float32(arr.Value(row))
	case *array.Float64:
		return float64(arr.Value(row))
	case *array.String:
		return arr.Value(row)
	case *array.LargeString:
		return arr.Value(row)
	case *array.Binary:
		return arr.Value(row)
	case *array.LargeBinary:
		return arr.Value(row)
	case *array.Date32:
		days := int64(arr.Value(row))
		return time.Unix(days*86400, 0).UTC()
	case *array.Timestamp:
		ts := arr.DataType().(*arrow.TimestampType)
		val := arr.Value(row)
		return timestampToTime(val, ts.Unit)
	case *array.Time64:
		micros := int64(arr.Value(row))
		return time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(micros) * time.Microsecond)
	case *array.Decimal128:
		val := arr.Value(row)
		dt := arr.DataType().(*arrow.Decimal128Type)
		return decimalToBigInt(val, dt)
	case *array.MonthDayNanoInterval:
		val := arr.Value(row)
		return monthDayNanoToInterval(val)
	case *array.List:
		start, end := arr.ValueOffsets(row)
		child := arr.ListValues()
		elems := make([]any, 0, end-start)
		for i := int(start); i < int(end); i++ {
			elems = append(elems, extractArrowValue(child, i))
		}
		return elems
	case *array.Struct:
		st := arr.DataType().(*arrow.StructType)
		m := make(map[string]interface{}, st.NumFields())
		for i := 0; i < st.NumFields(); i++ {
			m[st.Field(i).Name] = extractArrowValue(arr.Field(i), row)
		}
		return m
	case *array.Map:
		keys := arr.Keys()
		items := arr.Items()
		start, end := arr.ValueOffsets(row)
		n := int(end - start)
		ks := make([]any, 0, n)
		vs := make([]any, 0, n)
		for i := int(start); i < int(end); i++ {
			ks = append(ks, extractArrowValue(keys, i))
			vs = append(vs, extractArrowValue(items, i))
		}
		return arrowmap.OrderedMapValue{Keys: ks, Values: vs}
	default:
		// Fallback: use String representation
		return arr.ValueStr(row)
	}
}

func timestampToTime(val arrow.Timestamp, unit arrow.TimeUnit) time.Time {
	v := int64(val)
	switch unit {
	case arrow.Second:
		return time.Unix(v, 0).UTC()
	case arrow.Millisecond:
		return time.Unix(v/1000, (v%1000)*1e6).UTC()
	case arrow.Microsecond:
		return time.Unix(v/1e6, (v%1e6)*1000).UTC()
	case arrow.Nanosecond:
		return time.Unix(v/1e9, v%1e9).UTC()
	default:
		return time.Unix(v/1e6, (v%1e6)*1000).UTC()
	}
}

func decimalToBigInt(val decimal128.Num, dt *arrow.Decimal128Type) interface{} {
	bi := val.BigInt()
	if dt.Scale == 0 {
		return bi
	}
	// Return as a string representation with decimal point for non-zero scale.
	// This matches what DuckDB's Go driver returns for DECIMAL types.
	str := bi.String()
	neg := false
	if len(str) > 0 && str[0] == '-' {
		neg = true
		str = str[1:]
	}
	scale := int(dt.Scale)
	for len(str) <= scale {
		str = "0" + str
	}
	intPart := str[:len(str)-scale]
	fracPart := str[len(str)-scale:]
	result := intPart + "." + fracPart
	if neg {
		result = "-" + result
	}
	return result
}

func monthDayNanoToInterval(val arrow.MonthDayNanoInterval) arrowmap.IntervalValue {
	return arrowmap.IntervalValue{
		Months: val.Months,
		Days:   val.Days,
		Micros: val.Nanoseconds / 1000,
	}
}

// bearerCreds implements grpc.PerRPCCredentials for bearer token auth.
type bearerCreds struct {
	token string
}

func (c *bearerCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.token,
	}, nil
}

func (c *bearerCreds) RequireTransportSecurity() bool {
	return false // Unix sockets don't need TLS
}

// interpolateArgs performs simple positional argument interpolation for Flight SQL.
// Flight SQL's Execute doesn't support $1-style parameters natively,
// so we interpolate them into the query string.
//
// Safety: args come from PostgreSQL wire protocol typed parameter binding,
// not raw user strings. The caller (handleBind) decodes typed values from
// the binary protocol, so the values are trusted internal data.
// interpolateArgs inlines query arguments into the SQL string. The Flight
// executor sends ad-hoc statements without separate bind parameters, so any
// placeholder left in the query reaches the worker unbound and fails to prepare
// ("incorrect argument count for command"). It supports both placeholder styles
// the rest of the stack emits:
//   - `$N` PostgreSQL positional placeholders (selects args[N-1]); and
//   - `?` positional placeholders (each consumes the next arg in order), which
//     the transpiler produces for prepared statements (convertPlaceholders) and
//     a few internal queries.
//
// Placeholders inside single-quoted string literals, double-quoted identifiers,
// and `--` / `/* */` comments are left untouched. Unmatched placeholders (no
// corresponding arg) are passed through unchanged.
func interpolateArgs(query string, args []any) string {
	if len(args) == 0 {
		return query
	}

	var b strings.Builder
	b.Grow(len(query) + 16*len(args))
	nextPositional := 0

	for i := 0; i < len(query); {
		ch := query[i]
		switch {
		case ch == '\'' || ch == '"':
			// String literal ('...') or quoted identifier ("..."); copy verbatim,
			// honoring doubled-quote escapes.
			end := scanQuoted(query, i, ch)
			b.WriteString(query[i:end])
			i = end
		case ch == '-' && i+1 < len(query) && query[i+1] == '-':
			end := indexOrEnd(query, i+2, "\n")
			b.WriteString(query[i:end])
			i = end
		case ch == '/' && i+1 < len(query) && query[i+1] == '*':
			end := blockCommentEnd(query, i+2)
			b.WriteString(query[i:end])
			i = end
		case ch == '?':
			if nextPositional < len(args) {
				b.WriteString(formatArgValue(args[nextPositional]))
				nextPositional++
			} else {
				b.WriteByte(ch)
			}
			i++
		case ch == '$' && i+1 < len(query) && query[i+1] >= '1' && query[i+1] <= '9':
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			if n, err := strconv.Atoi(query[i+1 : j]); err == nil && n >= 1 && n <= len(args) {
				b.WriteString(formatArgValue(args[n-1]))
			} else {
				b.WriteString(query[i:j])
			}
			i = j
		default:
			b.WriteByte(ch)
			i++
		}
	}
	return b.String()
}

// InterpolateBoundArgs is interpolateArgs for a compact portal parameter
// source. It preserves the same placeholder and quoted/comment scanning rules
// while asking the source to append each literal directly to the one final SQL
// builder. That keeps a large text Bind from creating one transient string per
// parameter before the Flight RPC is made.
func InterpolateBoundArgs(query string, params sqlcore.SQLLiteralAppender) (string, error) {
	if params == nil || params.BindParameterCount() == 0 {
		return query, nil
	}

	var b strings.Builder
	b.Grow(len(query) + 16*params.BindParameterCount())
	nextPositional := 0

	for i := 0; i < len(query); {
		ch := query[i]
		switch {
		case ch == '\'' || ch == '"':
			end := scanQuoted(query, i, ch)
			b.WriteString(query[i:end])
			i = end
		case ch == '-' && i+1 < len(query) && query[i+1] == '-':
			end := indexOrEnd(query, i+2, "\n")
			b.WriteString(query[i:end])
			i = end
		case ch == '/' && i+1 < len(query) && query[i+1] == '*':
			end := blockCommentEnd(query, i+2)
			b.WriteString(query[i:end])
			i = end
		case ch == '?':
			if nextPositional < params.BindParameterCount() {
				if err := params.AppendBindParameterLiteral(&b, nextPositional); err != nil {
					return "", err
				}
				nextPositional++
			} else {
				b.WriteByte(ch)
			}
			i++
		case ch == '$' && i+1 < len(query) && query[i+1] >= '1' && query[i+1] <= '9':
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			if n, err := strconv.Atoi(query[i+1 : j]); err == nil && n >= 1 && n <= params.BindParameterCount() {
				if err := params.AppendBindParameterLiteral(&b, n-1); err != nil {
					return "", err
				}
			} else {
				b.WriteString(query[i:j])
			}
			i = j
		default:
			b.WriteByte(ch)
			i++
		}
	}
	return b.String(), nil
}

func interpolateBoundArgs(query string, params sqlcore.SQLLiteralAppender) (string, error) {
	return InterpolateBoundArgs(query, params)
}

// scanQuoted returns the index just past a quoted region starting at start
// (query[start] == quote), treating a doubled quote (” or "") as an escape.
func scanQuoted(query string, start int, quote byte) int {
	for i := start + 1; i < len(query); i++ {
		if query[i] != quote {
			continue
		}
		if i+1 < len(query) && query[i+1] == quote {
			i++ // skip the doubled (escaped) quote
			continue
		}
		return i + 1
	}
	return len(query)
}

func indexOrEnd(query string, start int, sub string) int {
	if idx := strings.Index(query[start:], sub); idx >= 0 {
		return start + idx + len(sub)
	}
	return len(query)
}

func blockCommentEnd(query string, start int) int {
	if idx := strings.Index(query[start:], "*/"); idx >= 0 {
		return start + idx + 2
	}
	return len(query)
}

func formatArgValue(v any) string {
	var b strings.Builder
	sqlcore.AppendSQLLiteral(&b, v)
	return b.String()
}
