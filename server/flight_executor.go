package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"runtime"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// MaxGRPCMessageSize is the max gRPC message size for Flight SQL communication.
// DuckDB query results can easily exceed the default 4MB limit.
const MaxGRPCMessageSize = 1 << 30 // 1GB

// ErrWorkerDead is returned when the backing worker process has crashed.
var ErrWorkerDead = errors.New("flight worker is dead")

// FlightExecutor implements QueryExecutor backed by an Arrow Flight SQL client.
// It routes queries to a duckdb-service worker process over a Unix socket.
type FlightExecutor struct {
	client       *flightsql.Client
	sessionToken string
	alloc        memory.Allocator
	ownsClient   bool // if true, Close() closes the client

	// dead is set to true when the backing worker crashes. Once set, all
	// RPC methods return ErrWorkerDead without touching the gRPC client.
	dead atomic.Bool

	ctx    context.Context    // base context for all requests
	cancel context.CancelFunc // cancels the base context
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

	if bearerToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&bearerCreds{token: bearerToken}))
	}

	client, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("flight sql client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &FlightExecutor{
		client:       client,
		sessionToken: sessionToken,
		alloc:        memory.DefaultAllocator,
		ownsClient:   true,
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// NewFlightExecutorFromClient creates a FlightExecutor that shares an existing
// Flight SQL client. The client is NOT closed when this executor is closed.
// This avoids creating a new gRPC connection per session.
func NewFlightExecutorFromClient(client *flightsql.Client, sessionToken string) *FlightExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	return &FlightExecutor{
		client:       client,
		sessionToken: sessionToken,
		alloc:        memory.DefaultAllocator,
		ownsClient:   false,
		ctx:          ctx,
		cancel:       cancel,
	}
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
	return metadata.AppendToOutgoingContext(ctx, "x-duckgres-session", e.sessionToken)
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

func (e *FlightExecutor) QueryContext(ctx context.Context, query string, args ...any) (rs RowSet, err error) {
	if e.dead.Load() {
		return nil, ErrWorkerDead
	}

	// Return empty results for queries that are only semicolons/whitespace.
	// These represent PostgreSQL client pings (e.g., pgx sends ";").
	trimmed := strings.TrimSpace(query)
	if trimmed == "" || IsEmptyQuery(trimmed) {
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

	info, err := e.client.Execute(reqCtx, query)
	if err != nil {
		return nil, fmt.Errorf("flight execute: %w", err)
	}

	if len(info.Endpoint) == 0 {
		return &emptyRowSet{}, nil
	}

	reader, err := e.client.DoGet(reqCtx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, fmt.Errorf("flight doget: %w", err)
	}

	schema, err := flight.DeserializeSchema(info.Schema, e.alloc)
	if err != nil {
		reader.Release()
		return nil, fmt.Errorf("flight deserialize schema: %w", err)
	}

	success = true
	return &FlightRowSet{
		reader: reader,
		schema: schema,
		cancel: cancel,
	}, nil
}

func (e *FlightExecutor) ExecContext(ctx context.Context, query string, args ...any) (result ExecResult, err error) {
	if e.dead.Load() {
		return nil, ErrWorkerDead
	}

	// Return zero rows affected for queries that are only semicolons/whitespace.
	trimmed := strings.TrimSpace(query)
	if trimmed == "" || IsEmptyQuery(trimmed) {
		return &flightExecResult{rowsAffected: 0}, nil
	}

	defer recoverClientPanic(&err)

	if len(args) > 0 {
		query = interpolateArgs(query, args)
	}

	reqCtx, cancel := e.mergedContext(ctx)
	defer cancel()

	reqCtx = e.withSession(reqCtx)

	affected, err := e.client.ExecuteUpdate(reqCtx, query)
	if err != nil {
		return nil, fmt.Errorf("flight execute update: %w", err)
	}

	return &flightExecResult{rowsAffected: affected}, nil
}

func (e *FlightExecutor) Query(query string, args ...any) (RowSet, error) {
	return e.QueryContext(context.Background(), query, args...)
}

func (e *FlightExecutor) Exec(query string, args ...any) (ExecResult, error) {
	return e.ExecContext(context.Background(), query, args...)
}

func (e *FlightExecutor) ConnContext(ctx context.Context) (RawConn, error) {
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
	if e.cancel != nil {
		e.cancel()
	}
	if e.ownsClient {
		return e.client.Close()
	}
	return nil
}

// FlightRowSet wraps an Arrow Flight RecordBatch reader to implement RowSet.
type FlightRowSet struct {
	reader *flight.Reader
	schema *arrow.Schema

	// Current batch state
	currentBatch arrow.RecordBatch
	batchRow     int // current row index within currentBatch
	done         bool
	err          error
	closeOnce    sync.Once
	cancel       context.CancelFunc
}

func (r *FlightRowSet) Columns() ([]string, error) {
	names := make([]string, r.schema.NumFields())
	for i := 0; i < r.schema.NumFields(); i++ {
		names[i] = r.schema.Field(i).Name
	}
	return names, nil
}

func (r *FlightRowSet) ColumnTypes() ([]ColumnTyper, error) {
	types := make([]ColumnTyper, r.schema.NumFields())
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
	})
	return nil
}

func (r *FlightRowSet) Err() error {
	return r.err
}

// emptyRowSet is returned when a query produces no endpoints.
type emptyRowSet struct{}

func (e *emptyRowSet) Columns() ([]string, error)        { return nil, nil }
func (e *emptyRowSet) ColumnTypes() ([]ColumnTyper, error) { return nil, nil }
func (e *emptyRowSet) Next() bool                         { return false }
func (e *emptyRowSet) Scan(dest ...any) error             { return fmt.Errorf("no rows") }
func (e *emptyRowSet) Close() error                       { return nil }
func (e *emptyRowSet) Err() error                         { return nil }

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

type intervalValue struct {
	Months int32
	Days   int32
	Micros int64
}

func monthDayNanoToInterval(val arrow.MonthDayNanoInterval) intervalValue {
	return intervalValue{
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
func interpolateArgs(query string, args []any) string {
	for i := len(args); i >= 1; i-- {
		placeholder := fmt.Sprintf("$%d", i)
		replacement := formatArgValue(args[i-1])
		query = strings.ReplaceAll(query, placeholder, replacement)
	}
	return query
}

func formatArgValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		escaped := strings.ReplaceAll(val, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, "'", "''")
		return "'" + escaped + "'"
	case []byte:
		return "decode('" + hex.EncodeToString(val) + "', 'hex')"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05.999999") + "'"
	case *big.Int:
		return val.String()
	case int:
		return fmt.Sprintf("%d", val)
	case int8:
		return fmt.Sprintf("%d", val)
	case int16:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case float64:
		return fmt.Sprintf("%g", val)
	default:
		// Treat unknown types as strings to avoid injection via Stringer
		s := fmt.Sprintf("%v", val)
		s = strings.ReplaceAll(s, `\`, `\\`)
		s = strings.ReplaceAll(s, "'", "''")
		return "'" + s + "'"
	}
}
