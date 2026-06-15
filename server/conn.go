package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/posthog/duckgres/server/auth"
	"github.com/posthog/duckgres/server/iceberg"
	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/sessionmeta"
	"github.com/posthog/duckgres/server/sqlcore"
	"github.com/posthog/duckgres/server/usersecrets"
	"github.com/posthog/duckgres/server/wire"
	"github.com/posthog/duckgres/transpiler"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// errCancelHandled is returned by handleStartup when a cancel request was
// processed. This signals serve() to exit without creating a DB connection.
var errCancelHandled = errors.New("cancel request handled")

// nextPID generates a unique per-connection PID for standalone mode.
// In standalone mode, os.Getpid() is the same for all connections, which causes
// the pg_stat_activity registry (keyed by PID) to overwrite entries when connections
// are replaced. Using a counter ensures each connection has a unique identity.
// The counter starts at os.Getpid()-1 so the first Add(1) returns os.Getpid(),
// matching the OS PID for debugging. Subsequent connections get unique incrementing PIDs.
var pidCounter = func() *atomic.Int32 {
	c := &atomic.Int32{}
	c.Store(int32(os.Getpid()) - 1)
	return c
}()

func nextPID() int32 {
	return pidCounter.Add(1)
}

// cursorOp identifies cursor-related statement types detected during Parse.
type cursorOp int

const (
	cursorOpNone           cursorOp = iota
	cursorOpDeclare                 // DECLARE cursor_name CURSOR FOR ...
	cursorOpFetch                   // FETCH [count] FROM cursor_name
	cursorOpClose                   // CLOSE cursor_name / CLOSE ALL
	cursorOpPgCursorsQuery          // SELECT ... FROM pg_cursors WHERE name = ...
	cursorOpPgStatActivity          // SELECT ... FROM pg_stat_activity
)

// cursorState holds the state of an emulated server-side cursor.
type cursorState struct {
	query    string        // Inner SELECT query (transpiled)
	rows     RowSet        // Open result set (nil until first FETCH)
	cols     []string      // Column names (cached after first FETCH)
	colTypes []ColumnTyper // Column types (cached after first FETCH)
	typeOIDs []int32       // PG type OIDs (cached after first FETCH)
	cleanup  func()        // Query context cleanup
}

type preparedStmt struct {
	query             string
	convertedQuery    string
	paramTypes        []int32
	numParams         int
	isIgnoredSet      bool     // True if this is an ignored SET parameter
	isNoOp            bool     // True if this is a no-op command (CREATE INDEX, etc.)
	noOpTag           string   // Command tag for no-op commands
	described         bool     // True if Describe(S) was called on this statement
	statements        []string // Multi-statement rewrite (e.g., writable CTE)
	cleanupStatements []string // Cleanup statements for multi-statement (DROP temp tables, COMMIT)
	cursorOp          cursorOp // Cursor operation type (for Extended Query)
	cursorName        string   // Cursor name
	cursorQuery       string   // Transpiled inner SELECT (for DECLARE)
	fetchCount        int64    // FETCH row count
	cursorIsMove      bool     // FETCH is a MOVE: advance position without returning rows
	warnings          []string // Transpiler warnings to surface as NoticeResponse at Execute
}

type portal struct {
	stmt          *preparedStmt
	paramValues   [][]byte
	paramFormats  []int16 // 0=text, 1=binary for each parameter
	resultFormats []int16
	described     bool // true if Describe was called on this portal
}

// decodeParams converts raw parameter bytes to Go values based on format codes.
// Returns (args, nil) on success, or (nil, error) for malformed binary data.
// On error, caller should send ErrorResponse with SQLSTATE 08P01.
func (p *portal) decodeParams() ([]interface{}, error) {
	args := make([]interface{}, len(p.paramValues))
	for i, v := range p.paramValues {
		if v == nil {
			args[i] = nil
			continue
		}

		// Get type OID for this parameter
		typeOID := int32(0) // Unknown
		if i < len(p.stmt.paramTypes) {
			typeOID = p.stmt.paramTypes[i]
		}

		// Get format code for this parameter
		format := int16(0) // Default to text
		if len(p.paramFormats) == 1 {
			// Single format code applies to all parameters
			format = p.paramFormats[0]
		} else if i < len(p.paramFormats) {
			// Per-parameter format codes
			format = p.paramFormats[i]
		}

		// CRITICAL: Per PostgreSQL spec, when type is unknown (OID 0),
		// IGNORE binary format code and always treat as text.
		// "Anything you have down as UNKNOWN, send as text."
		if typeOID == 0 || format == 0 {
			// Unknown type OR text format: treat as string
			args[i] = string(v)
		} else {
			// Known type AND binary format: decode per type
			val, err := decodeBinary(v, typeOID)
			if err != nil {
				return nil, fmt.Errorf("parameter %d: %w", i+1, err)
			}
			args[i] = val
		}
	}
	return args, nil
}

// Transaction status constants for PostgreSQL wire protocol
const (
	txStatusIdle        = 'I' // Not in a transaction
	txStatusTransaction = 'T' // In a transaction
	txStatusError       = 'E' // In a failed transaction
)

type clientConn struct {
	server                 *Server
	conn                   net.Conn
	reader                 *bufio.Reader
	writer                 *bufio.Writer
	username               string
	orgID                  string
	database               string
	executor               QueryExecutor
	pid                    int32
	secretKey              int32                    // unique key for cancel requests
	stmts                  map[string]*preparedStmt // prepared statements by name
	portals                map[string]*portal       // portals by name
	txStatus               byte                     // current transaction status ('I', 'T', or 'E')
	ignoreTillSync         bool                     // discard extended-query messages until Sync after an error; see runExtendedQueryMessage
	errorResponsesSent     uint64                   // ErrorResponses sent via sendError; observed by runExtendedQueryMessage
	lastErrorCode          string                   // most recent SQLSTATE sent via sendError; observed by query metrics
	activeQueryMetrics     *queryMetricsScope       // active query attempt metrics scope for non-ErrorResponse failures
	passthrough            bool                     // true for passthrough users (skip transpiler + pg_catalog)
	cursors                map[string]*cursorState  // server-side cursor emulation
	catalogUseRewrite      bool                     // true when bare `USE ducklake`/`USE iceberg` should expand to the reliable two-part target
	tenantIcebergConfig    IcebergConfig
	hasTenantIcebergConfig bool
	ctx                    context.Context    // connection context, cancelled when connection is closed
	cancel                 context.CancelFunc // cancels the connection context

	// sharedDB is true when this connection uses a shared file-persistence DB pool.
	// Cleanup differs: we return the pinned conn to the pool instead of closing the DB.
	sharedDB bool

	// pg_stat_activity fields
	backendStart    time.Time    // when this connection started
	applicationName string       // from startup params
	currentQuery    atomic.Value // stores string — current/last query (lock-free)
	queryStart      atomic.Value // stores time.Time — when current query started (lock-free)
	workerID        int          // control plane worker ID, -1 for standalone
	workerPod       string       // K8s pod name of the worker, empty for standalone or in-process workers
	physicalCatalog string       // selected DuckDB catalog for execution, empty for memory/standalone default

	// lastProfilingSummary holds the rollup from the most recent
	// EnrichSpanWithProfiling call on this connection. Consumed by the very
	// next logQuery and then cleared. Per-connection state is safe because
	// each connection processes queries serially on a single goroutine.
	lastProfilingSummary observe.QueryProfilingSummary
}

// newTranspiler creates a transpiler configured for this connection.
//
// The backend profile selects DDL/DML/catalog compatibility policy. The client
// database remains PostgreSQL-visible; LogicalDatabaseName maps three-part
// references using that database to the selected physical catalog.
func (c *clientConn) newTranspiler(convertPlaceholders bool) *transpiler.Transpiler {
	backend := transpiler.BackendMemory
	physicalCatalog := c.physicalCatalog
	switch {
	case physicalCatalog == iceberg.CatalogName:
		backend = transpiler.BackendIceberg
	case physicalCatalog == physicalDuckLakeCatalog:
		backend = transpiler.BackendDuckLake
	case c.server.cfg.Iceberg.Enabled || c.hasTenantIcebergConfig:
		backend = transpiler.BackendIceberg
		physicalCatalog = iceberg.CatalogName
	case c.server.cfg.DuckLake.MetadataStore != "" || c.server.cfg.AlwaysDuckLake:
		backend = transpiler.BackendDuckLake
		physicalCatalog = physicalDuckLakeCatalog
	}

	logicalDatabaseName := ""
	if backend == transpiler.BackendDuckLake || backend == transpiler.BackendIceberg {
		logicalDatabaseName = c.database
	}

	return transpiler.New(transpiler.Config{
		Backend:             backend,
		LogicalDatabaseName: logicalDatabaseName,
		PhysicalCatalogName: physicalCatalog,
		ConvertPlaceholders: convertPlaceholders,
	})
}

// generateSecretKey is a thin alias for wire.GenerateSecretKey kept for the
// internal call sites in this file. New code should call wire.GenerateSecretKey
// directly.
var generateSecretKey = wire.GenerateSecretKey

// backendKey returns the backend key for this connection, used for cancel requests.
func (c *clientConn) backendKey() BackendKey {
	return BackendKey{Pid: c.pid, SecretKey: c.secretKey}
}

func (c *clientConn) ensureConnectionContext() {
	if c.ctx != nil && c.cancel != nil {
		return
	}

	parent := c.ctx
	if parent == nil {
		parent = context.Background()
	}
	c.ctx, c.cancel = context.WithCancel(parent)
}

// queryContext returns a cancellable context for query execution.
// The cancel function is registered with the server so it can be invoked
// via a cancel request from another connection.
// The caller must call the returned cleanup function when the query completes.
//
// A disconnect monitor goroutine runs for the duration of the query. It uses
// bufio.Reader.Peek to detect client disconnects (TCP FIN/RST) while the
// query is in-flight, without consuming any bytes from the stream. When a
// disconnect is detected, the connection context (c.ctx) is cancelled,
// propagating cancellation to gRPC calls on Flight SQL workers.
//
// In child worker processes, the context is also cancelled when the server's
// externalCancelCh is closed (triggered by SIGUSR1 signal).
func (c *clientConn) queryContext() (context.Context, func()) {
	return c.queryContextInner(true)
}

// queryContextForCursor returns a cancellable context without a disconnect
// monitor. Cursors are long-lived and span multiple message loop iterations,
// so the monitor cannot be used — it would race with readMessage on the
// bufio.Reader between FETCH calls. Disconnect detection still works via
// c.ctx cancellation when the connection handler exits.
func (c *clientConn) queryContextForCursor() (context.Context, func()) {
	return c.queryContextInner(false)
}

func (c *clientConn) queryContextInner(monitor bool) (context.Context, func()) {
	c.ensureConnectionContext()
	ctx, cancel := context.WithCancel(c.ctx)
	key := c.backendKey()
	c.server.RegisterQuery(key, cancel)

	// If there's an external cancel channel (child worker mode), set up a
	// goroutine to cancel the context when a cancel token arrives. Each
	// SIGUSR1 sends one token (worker.go notifyQueryCancel); discard a token
	// that arrived while no query was in flight — like Postgres, a cancel
	// request targets only the query running when it is delivered, so a stale
	// one must not kill the next query.
	if c.server.externalCancelCh != nil {
		select {
		case <-c.server.externalCancelCh:
		default:
		}
		go func() {
			select {
			case <-c.server.externalCancelCh:
				cancel()
			case <-ctx.Done():
				// Context already cancelled, nothing to do
			}
		}()
	}

	var stopMonitor func()
	if monitor {
		stopMonitor = c.startDisconnectMonitor(ctx)
	}

	cleanup := func() {
		if stopMonitor != nil {
			stopMonitor()
		}
		c.server.UnregisterQuery(key)
		cancel()
	}

	return ctx, cleanup
}

// startDisconnectMonitor starts a goroutine that polls the client connection
// for disconnects using bufio.Reader.Peek. During query execution the message
// loop is blocked on the executor, so nobody else touches the bufio.Reader,
// making concurrent Peek calls safe. When the client sends a TCP FIN or RST,
// Peek returns a non-timeout error and the connection context is cancelled.
//
// The returned stop function MUST be called when query execution completes.
// It waits for the monitor goroutine to exit before returning, ensuring the
// bufio.Reader is not accessed concurrently with the message loop.
func (c *clientConn) startDisconnectMonitor(ctx context.Context) (stop func()) {
	stopped := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(stopped)
		for {
			_ = c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			_, err := c.reader.Peek(1)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					select {
					case <-done:
						return
					case <-ctx.Done():
						return
					default:
						continue
					}
				}
				// Non-timeout error: client disconnected (EOF, connection reset, etc.)
				c.cancel()
				return
			}
			// Data available (e.g. pipelined Sync message) — safe in the
			// bufio.Reader buffer. Throttle to avoid busy-waiting since
			// Peek returns instantly when data is already buffered.
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
				continue
			}
		}
	}()

	return func() {
		close(done)
		// Interrupt any in-progress Peek by setting a past deadline.
		_ = c.conn.SetReadDeadline(time.Now())
		<-stopped
		// Clear the deadline so the message loop's next read uses
		// the idle timeout (or no deadline).
		_ = c.conn.SetReadDeadline(time.Time{})
	}
}

// logQueryStarted records a query handing off to a worker. Pairs with
// logQueryFinished at every termination point so logs and traces can
// be cross-referenced — the trace_id attribute matches the OTEL span
// ID exported by the same query, so a search like trace_id=abc123 in
// Loki/Grafana lines up directly with the trace view.
//
// Includes worker_id and worker_pod so an operator chasing a specific
// worker incident (e.g. the one in the worker-40761 postmortem) can
// filter to just that worker's queries without joining across logs.
// logger returns the connection-scoped logger: every line carries the session
// identity (user, org, worker, worker_pod) so the full request/query lifecycle
// is filterable by org or worker without joining log streams. Built per call —
// the identity fields settle at different times (username after auth, worker
// after assignment), so caching would freeze a half-built identity.
func (c *clientConn) logger() *slog.Logger {
	attrs := make([]any, 0, 8)
	if c.username != "" {
		attrs = append(attrs, "user", c.username)
	}
	if c.orgID != "" {
		attrs = append(attrs, "org", c.orgID)
	}
	if c.workerID > 0 {
		attrs = append(attrs, "worker", c.workerID)
	}
	if c.workerPod != "" {
		attrs = append(attrs, "worker_pod", c.workerPod)
	}
	return slog.With(attrs...)
}

func (c *clientConn) logQueryStarted(query string) {
	query = usersecrets.RedactForLog(query)
	c.logger().Info("Query started.",
		"query", query,
		"trace_id", observe.TraceIDFromContext(c.ctx))
}

// logQueryFinished records a query terminating on the worker. Counter-
// part to logQueryStarted; emit once per query regardless of outcome
// so the start/finish pair is always balanced.
//
// On error paths logQueryError still fires for severity routing
// (Info vs Error based on SQLSTATE class). logQueryFinished
// deliberately stays at Info even on error so the lifecycle pair stays
// readable as a stream — operators following one trace see both a
// "started" and a "finished" line, and can look at the separate error
// line for severity context.
func (c *clientConn) logQueryFinished(query string, start time.Time, rows int64, err error) {
	attrs := []any{
		"query", usersecrets.RedactForLog(query),
		"duration_ms", time.Since(start).Milliseconds(),
		"rows", rows,
		"trace_id", observe.TraceIDFromContext(c.ctx),
	}
	if err != nil {
		// Engine errors echo the offending SQL, so a failed CREATE SECRET
		// leaks the credential here unless the error is redacted too.
		attrs = append(attrs, "error", usersecrets.RedactErrorForLog(query, err.Error()))
	}
	c.logger().Info("Query finished.", attrs...)
}

// logQueryError logs a query execution failure. DuckLake-specific
// retryable conditions and user-attributable errors get Warn / Info so
// the Error level stays meaningful as an alerting signal — "Query
// execution errored." should mean the system genuinely went wrong
// (worker crash, IO failure, internal panic, infra unreachable), not
// "user typo'd a column name."
func (c *clientConn) logQueryError(query string, err error) {
	// Engine errors echo the offending SQL, so a failed CREATE SECRET leaks the
	// credential via the error attribute unless it is redacted too. Classify
	// against the original query before it is replaced with the redacted form.
	attrs := []any{
		"query", usersecrets.RedactForLog(query),
		"error", usersecrets.RedactErrorForLog(query, err.Error()),
	}
	if isDuckLakeTransactionConflict(err) {
		c.logger().Warn("DuckLake transaction conflict.", attrs...)
		return
	}
	if isDuckLakeMetadataConnectionLost(err) {
		c.logger().Warn("DuckLake metadata connection lost during transaction.", attrs...)
		return
	}
	if isUserQueryError(err) {
		c.logger().Info("Query execution failed.", attrs...)
		return
	}
	c.logger().Error("Query execution errored.", attrs...)
}

// safeCleanupDB safely closes the database connection, handling the case where
// the underlying connection (e.g., DuckLake's SSL connection to RDS) may be broken.
//
// This mitigates crashes from DuckDB throwing C++ exceptions during cleanup by:
// 1. Detecting broken connections early via a health check query
// 2. Explicitly rolling back transactions before Close() to avoid DuckDB's internal ROLLBACK
// 3. Skipping SQL cleanup operations when the connection is known to be broken
//
// Note: If the connection breaks between our health check and Close(), DuckDB may still
// throw a C++ exception. This is a best-effort mitigation, not a complete fix.
func (c *clientConn) safeCleanupDB() {
	// Recover from Go-level panics during database cleanup (e.g., nil pointer
	// dereference if connection state is inconsistent after cancellation).
	// Note: DuckDB C++ crashes (SIGABRT/SIGSEGV) are fatal signals that cannot be
	// caught by recover() — process isolation mode is needed to survive those.
	defer func() {
		if r := recover(); r != nil {
			c.logger().Error("Recovered from panic during database cleanup.", "panic", r)
		}
	}()

	cleanupTimeout := 5 * time.Second

	if c.sharedDB {
		// Shared file-persistence pool: ROLLBACK any open transaction on the
		// pinned connection, then return it to the pool. Skip DuckLake DETACH
		// since the underlying DB is shared across connections.
		if c.txStatus == txStatusTransaction || c.txStatus == txStatusError {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			_, err := c.executor.ExecContext(ctx, "ROLLBACK")
			cancel()
			if err != nil {
				c.logger().Warn("Failed to rollback transaction during cleanup.", "error", err)
			}
		}
		// Close returns the pinned *sql.Conn to the pool (does not close the DB).
		if err := c.executor.Close(); err != nil {
			c.logger().Warn("Failed to return connection to pool.", "error", err)
		}
		c.server.releaseFileDB(c.username)
		return
	}

	connHealthy := true

	// Check connection health. For DuckLake, we need to actually run a query that
	// touches the metadata connection, not just ping the local DuckDB connection.
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	if c.server.cfg.DuckLake.MetadataStore != "" {
		// Probe the attached DuckLake catalog via DuckDB's catalog table function.
		// This stays valid even though DuckLake does not expose an information_schema
		// catalog that can be referenced as ducklake.information_schema.*.
		_, err := c.executor.ExecContext(ctx, "SELECT 1 FROM duckdb_tables() WHERE database_name = 'ducklake' LIMIT 1")
		if err != nil {
			c.logger().Warn("DuckLake connection unhealthy during cleanup, skipping SQL cleanup.", "error", err)
			connHealthy = false
		}
	} else {
		if err := c.executor.PingContext(ctx); err != nil {
			c.logger().Warn("Database connection unhealthy during cleanup, skipping SQL cleanup.", "error", err)
			connHealthy = false
		}
	}
	cancel()

	// If we're in a transaction, explicitly ROLLBACK before closing.
	// This prevents DuckDB from trying to ROLLBACK internally during Close(),
	// which can throw exceptions if the connection is in a bad state.
	if connHealthy && (c.txStatus == txStatusTransaction || c.txStatus == txStatusError) {
		ctx2, cancel2 := context.WithTimeout(context.Background(), cleanupTimeout)
		_, err := c.executor.ExecContext(ctx2, "ROLLBACK")
		cancel2()
		if err != nil {
			c.logger().Warn("Failed to rollback transaction during cleanup.", "error", err)
			if isConnectionBroken(err) {
				connHealthy = false
			}
		}
	}

	// Detach attached lake catalogs to release remote metadata/object-store handles
	// before closing the DuckDB connection.
	if connHealthy && (c.server.cfg.DuckLake.MetadataStore != "" || c.server.cfg.DuckLake.DeltaCatalogEnabled) {
		// Must switch away from ducklake before detaching - DuckDB doesn't allow
		// detaching the default database
		ctx3, cancel3 := context.WithTimeout(context.Background(), cleanupTimeout)
		_, err := c.executor.ExecContext(ctx3, "USE memory")
		cancel3()
		if err != nil {
			c.logger().Warn("Failed to switch to memory.", "error", err)
			if isConnectionBroken(err) {
				connHealthy = false
			}
		}

		if connHealthy {
			if c.server.cfg.Iceberg.Enabled {
				// The iceberg catalog stays attached for the life of the session
				// (attachLakekeeperCatalog no longer detaches empty warehouses),
				// so release it here too — otherwise a pooled connection reused by
				// the next activation hits the count>0 early-return in
				// attachLakekeeperCatalog and keeps a stale catalog secret.
				ctxIce, cancelIce := context.WithTimeout(context.Background(), cleanupTimeout)
				_, err := c.executor.ExecContext(ctxIce, "DETACH "+iceberg.CatalogName)
				cancelIce()
				if err != nil {
					c.logger().Warn("Failed to detach Iceberg catalog.", "error", err)
				}
			}
			if c.server.cfg.DuckLake.DeltaCatalogEnabled {
				ctxDelta, cancelDelta := context.WithTimeout(context.Background(), cleanupTimeout)
				_, err := c.executor.ExecContext(ctxDelta, "DETACH delta")
				cancelDelta()
				if err != nil {
					c.logger().Warn("Failed to detach Delta catalog.", "error", err)
				}
			}

			if c.server.cfg.DuckLake.MetadataStore != "" {
				ctx4, cancel4 := context.WithTimeout(context.Background(), cleanupTimeout)
				_, err := c.executor.ExecContext(ctx4, "DETACH ducklake")
				cancel4()
				if err != nil {
					c.logger().Warn("Failed to detach DuckLake.", "error", err)
				}
			}
		}
	}

	// Always attempt to close the database connection.
	// If the connection is broken, this may still throw, but we've done our best
	// to clean up the transaction state first.
	if err := c.executor.Close(); err != nil {
		c.logger().Warn("Failed to close database.", "error", err)
	}
}

// validateWithDuckDB checks if a query is valid DuckDB syntax.
// This is used when PostgreSQL parsing fails to determine if the query should
// be executed natively by DuckDB.
func (c *clientConn) validateWithDuckDB(query string) error {
	// Check if this is a utility command that doesn't support EXPLAIN
	// For these, we skip validation and let DuckDB handle them directly
	if isDuckDBUtilityCommand(query) {
		return nil
	}

	// Skip validation for queries that already start with EXPLAIN to avoid EXPLAIN EXPLAIN
	upperQuery := strings.ToUpper(strings.TrimSpace(stripLeadingComments(query)))
	if strings.HasPrefix(upperQuery, "EXPLAIN") {
		return nil
	}

	// Skip EXPLAIN validation for queries with parameter placeholders ($1, $2, etc.)
	// EXPLAIN cannot handle unbound parameters - we'll let DuckDB validate at execution time
	if hasParameterPlaceholders(query) {
		return nil
	}

	// Use EXPLAIN to validate the query without executing it
	// DuckDB's EXPLAIN will fail if the query is syntactically invalid
	_, err := c.executor.Exec("EXPLAIN " + query)
	if err != nil {
		// Strip "EXPLAIN " from error messages to avoid confusing users
		errMsg := strings.Replace(err.Error(), "EXPLAIN ", "", 1)
		return fmt.Errorf("%s", errMsg)
	}
	return nil
}

// paramPlaceholderRegex matches PostgreSQL-style $N parameter placeholders
var paramPlaceholderRegex = regexp.MustCompile(`\$\d+`)

// hasParameterPlaceholders returns true if the query contains $N placeholders
func hasParameterPlaceholders(query string) bool {
	return paramPlaceholderRegex.MatchString(query)
}

// isDuckDBUtilityCommand checks if a query is a DuckDB utility command
// that doesn't support EXPLAIN validation. These commands are passed
// through directly to DuckDB without pre-validation.
func isDuckDBUtilityCommand(query string) bool {
	// Strip leading comments and get the first keyword (case-insensitive)
	upper := strings.ToUpper(stripLeadingComments(query))

	// List of DuckDB utility commands that don't support EXPLAIN
	// All prefixes should NOT have trailing spaces - we check word boundaries separately
	utilityPrefixes := []string{
		"ATTACH",
		"DETACH",
		"USE",
		"INSTALL",
		"LOAD",
		"UNLOAD",
		"CREATE SECRET",
		"DROP SECRET",
		// DuckDB's own disambiguation hint for a secret that exists in more
		// than one storage backend is "DROP <PERSISTENT|TEMPORARY> SECRET ...".
		// Those variants don't start with "DROP SECRET" (the next token is
		// PERSISTENT/TEMPORARY), so without these explicit prefixes they fall
		// through to the PG transpiler and die with `syntax error at or near
		// "PERSISTENT"` — i.e. the exact command DuckDB tells the user to run is
		// unrunnable. Pass them straight through. (List order is irrelevant:
		// matching is by string prefix, and "DROP SECRET" is not a prefix of
		// "DROP PERSISTENT SECRET", so the two never collide.)
		"DROP PERSISTENT SECRET",
		"DROP TEMPORARY SECRET",
		"CREATE PERSISTENT SECRET",
		"CREATE TEMPORARY SECRET",
		"CREATE OR REPLACE SECRET",
		"CREATE OR REPLACE PERSISTENT SECRET",
		"CREATE OR REPLACE TEMPORARY SECRET",
		"PRAGMA",
		"CHECKPOINT",
		"FORCE CHECKPOINT",
		"EXPORT DATABASE",
		"IMPORT DATABASE",
		"CALL",
		"SET",
		"RESET",
	}

	for _, prefix := range utilityPrefixes {
		if hasCommandPrefix(upper, prefix) {
			return true
		}
	}

	return false
}

// hasCommandPrefix checks if query starts with the given command prefix,
// ensuring it's followed by a word boundary (space, newline, semicolon, or end of string).
func hasCommandPrefix(query, prefix string) bool {
	if !strings.HasPrefix(query, prefix) {
		return false
	}
	// Check that prefix is followed by a word boundary
	if len(query) == len(prefix) {
		return true // Exact match
	}
	next := query[len(prefix)]
	return next == ' ' || next == '\t' || next == '\n' || next == '\r' || next == ';'
}

func (c *clientConn) serve() error {
	c.ensureConnectionContext()
	defer c.cancel()

	c.reader = bufio.NewReader(c.conn)
	c.writer = bufio.NewWriter(c.conn)
	c.pid = nextPID()
	c.secretKey = generateSecretKey()
	c.stmts = make(map[string]*preparedStmt)
	c.portals = make(map[string]*portal)
	c.cursors = make(map[string]*cursorState)
	c.txStatus = txStatusIdle

	// Handle startup
	if err := c.handleStartup(); err != nil {
		if errors.Is(err, errCancelHandled) {
			return nil // Cancel request was processed, exit cleanly
		}
		return fmt.Errorf("startup failed: %w", err)
	}

	// Track connection for pg_stat_activity
	c.backendStart = time.Now()
	c.workerID = -1
	c.server.registerConn(c)
	defer c.server.unregisterConn(c.pid)

	// Check if this is a passthrough user (skip transpiler + pg_catalog)
	c.passthrough = c.server.cfg.PassthroughUsers[c.username]
	if c.passthrough {
		c.logger().Info("Passthrough mode enabled.")
	}

	// Create a DuckDB connection for this client session (unless pre-created by caller)
	var stopRefresh func()
	if c.executor == nil {
		if c.server.cfg.FilePersistence {
			db, err := c.server.acquireFileDB(c.username, c.passthrough)
			if err != nil {
				c.sendError("FATAL", "28000", fmt.Sprintf("failed to open database: %v", err))
				return err
			}
			conn, err := db.Conn(c.ctx)
			if err != nil {
				c.server.releaseFileDB(c.username)
				c.sendError("FATAL", "28000", fmt.Sprintf("failed to get pooled connection: %v", err))
				return err
			}
			c.executor = NewPinnedExecutor(conn, db)
			c.sharedDB = true
			// Don't start per-connection credential refresh; the pool manages it.
		} else {
			var db *sql.DB
			var err error
			if c.passthrough {
				db, err = CreatePassthroughDBConnection(c.server.cfg, c.server.duckLakeSem, c.username, processStartTime, processVersion)
			} else {
				db, err = c.server.createDBConnection(c.username)
			}
			if err != nil {
				c.sendError("FATAL", "28000", fmt.Sprintf("failed to open database: %v", err))
				return err
			}
			c.executor = NewLocalExecutor(db)

			// Start background credential refresh for long-lived connections.
			// Only needed when we create the DB here; the control plane manages
			// refresh for pre-created connections via DBPool.
			stopRefresh = StartCredentialRefresh(db, c.server.cfg.DuckLake)
		}
	}
	// Defers run LIFO: close cursors first (they hold open RowSets), then stop
	// credential refresh, then clean up the database connection.
	defer func() {
		if c.executor != nil {
			c.safeCleanupDB()
		}
	}()
	defer c.closeAllCursors()
	defer func() {
		if stopRefresh != nil {
			stopRefresh()
		}
	}()

	if !c.passthrough {
		initTimeout := c.server.cfg.SessionInitTimeout
		if initTimeout == 0 {
			initTimeout = DefaultSessionInitTimeout
		}
		initCtx, initCancel := context.WithTimeout(context.Background(), initTimeout)
		duckLakeAttached, err := sessionmeta.HasAttachedCatalog(initCtx, c.executor, physicalDuckLakeCatalog)
		if err != nil {
			initCancel()
			c.sendError("FATAL", "XX000", fmt.Sprintf("failed to detect ducklake catalog attachment: %v", err))
			return err
		}
		icebergAttached, err := sessionmeta.HasAttachedCatalog(initCtx, c.executor, iceberg.CatalogName)
		if err != nil {
			initCancel()
			c.sendError("FATAL", "XX000", fmt.Sprintf("failed to detect iceberg catalog attachment: %v", err))
			return err
		}
		// De-mask: current_database() and the pg_catalog surfaces should reflect
		// the real attached catalog, not the client's connection database name.
		// Standalone has a single backing catalog, so honor whatever is attached.
		catalog := c.database
		switch {
		case duckLakeAttached:
			catalog = physicalDuckLakeCatalog
		case icebergAttached:
			catalog = iceberg.CatalogName
		}
		// Prime the Iceberg REST catalog's schema list on this connection before the
		// compat-view bind in InitSessionDatabaseMetadata enumerates every attached
		// catalog. On the first session of a cold backing instance an unmaterialized
		// Iceberg catalog otherwise surfaces a schema with an empty name, failing the
		// bind with `Schema with name "" not found`. Best-effort: a real failure
		// still surfaces from InitSessionDatabaseMetadata below.
		if icebergAttached {
			if err := sessionmeta.PrimeIcebergCatalog(initCtx, c.executor, iceberg.CatalogName); err != nil {
				c.logger().Warn("Failed to prime Iceberg catalog before session metadata init.", "error", err)
			}
		}
		if err := sessionmeta.InitSessionDatabaseMetadata(initCtx, c.executor, catalog); err != nil {
			initCancel()
			c.sendError("FATAL", "XX000", fmt.Sprintf("failed to initialize session database metadata: %v", err))
			return err
		}
		// InitSessionDatabaseMetadata's defer only restores the catalog for
		// DuckLake sessions; it otherwise leaves the session in `memory`. For an
		// Iceberg session we must issue the USE ourselves, or current_database()
		// reports 'iceberg' while unqualified DDL/DML silently lands in the
		// ephemeral in-memory catalog. Mirror server.setIcebergDefault and the
		// control plane's effectiveSessionDefaultCommand: target
		// iceberg.<DefaultSchema> (not a bare `USE iceberg`) because DuckDB
		// shadows `main` on a REST catalog.
		if catalog == iceberg.CatalogName {
			useStmt := fmt.Sprintf("USE %s.%s", iceberg.CatalogName, iceberg.DefaultSchema)
			if _, err := c.executor.ExecContext(initCtx, useStmt); err != nil {
				initCancel()
				c.sendError("FATAL", "XX000", fmt.Sprintf("failed to set iceberg as default catalog: %v", err))
				return err
			}
		}
		initCancel()
		// Keep c.database aligned with the real catalog so observability surfaces
		// agree with current_database(); record the physical catalog so the
		// transpiler selects the right backend profile (DuckLake/Iceberg).
		c.database = catalog
		c.physicalCatalog = catalog
		c.catalogUseRewrite = duckLakeAttached || icebergAttached
	}

	// Send initial parameters
	c.sendInitialParams()

	// Send ready for query
	if err := c.writeReadyForQuery(c.txStatus); err != nil {
		return err
	}
	if err := c.flushWriter(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	// Main message loop
	return c.messageLoop()
}

func (c *clientConn) handleStartup() error {
	tlsUpgraded := false

	if err := c.conn.SetReadDeadline(time.Now().Add(startupReadTimeout)); err != nil {
		return fmt.Errorf("failed to set startup deadline: %w", err)
	}

	for {
		params, err := wire.ReadStartupMessage(c.reader)
		if err != nil {
			return err
		}

		// Handle GSSENCRequest - decline and let client retry with SSL
		if params["__gssenc_request"] == "true" {
			c.logger().Debug("GSSENCRequest received, declining.", "remote_addr", c.conn.RemoteAddr())
			if _, err := c.conn.Write([]byte("N")); err != nil {
				return err
			}
			continue
		}

		// Handle SSL request - upgrade to TLS
		if params["__ssl_request"] == "true" {
			if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
				return fmt.Errorf("failed to clear startup deadline: %w", err)
			}

			// Send 'S' to indicate we support SSL
			if _, err := c.conn.Write([]byte("S")); err != nil {
				return err
			}

			// Upgrade connection to TLS
			tlsConn := tls.Server(c.conn, c.server.tlsConfig)
			if err := tlsConn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
				return fmt.Errorf("failed to set TLS deadline: %w", err)
			}
			if err := tlsConn.Handshake(); err != nil {
				return fmt.Errorf("TLS handshake failed: %w", err)
			}
			if err := tlsConn.SetDeadline(time.Time{}); err != nil {
				return fmt.Errorf("failed to clear TLS deadline: %w", err)
			}

			// Replace connection with TLS connection
			c.conn = tlsConn
			c.reader = bufio.NewReader(tlsConn)
			c.writer = bufio.NewWriter(tlsConn)
			tlsUpgraded = true

			c.logger().Info("TLS connection established.", "remote_addr", c.conn.RemoteAddr())
			continue
		}

		// Handle cancel request
		if params["__cancel_request"] == "true" {
			// Extract pid and secret key from the cancel request
			if pidStr, ok := params["__cancel_pid"]; ok {
				if secretKeyStr, ok := params["__cancel_secret_key"]; ok {
					pid, _ := strconv.ParseInt(pidStr, 10, 32)
					secretKey, _ := strconv.ParseInt(secretKeyStr, 10, 32)
					key := BackendKey{Pid: int32(pid), SecretKey: int32(secretKey)}
					c.server.CancelQuery(key)
				}
			}
			return errCancelHandled
		}

		// Reject non-TLS connections
		if !tlsUpgraded {
			c.sendError("FATAL", "28000", "SSL/TLS connection required. Connect with sslmode=require or higher.")
			return fmt.Errorf("client did not request SSL")
		}

		c.username = params["user"]
		c.database = params["database"]
		c.applicationName = params["application_name"]

		c.logger().Info("Client startup.", "database", c.database,
			"application_name", c.applicationName, "remote_addr", c.conn.RemoteAddr())

		if c.username == "" {
			c.sendError("FATAL", "28000", "no user specified")
			return fmt.Errorf("no user specified")
		}

		break
	}

	// Request password
	if err := wire.WriteAuthCleartextPassword(c.writer); err != nil {
		return err
	}
	if err := c.flushWriter(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	// Read password response
	msgType, body, err := wire.ReadMessage(c.reader)
	if err != nil {
		return err
	}

	if msgType != wire.MsgPassword {
		c.sendError("FATAL", "28000", "expected password message")
		return fmt.Errorf("expected password message, got %c", msgType)
	}

	// Password is null-terminated
	password := string(bytes.TrimRight(body, "\x00"))

	// Validate password (constant-time; does not leak whether the user exists)
	if !auth.ValidateUserPassword(c.server.cfg.Users, c.username, password) {
		// Record failed authentication attempt
		banned := c.server.rateLimiter.RecordFailedAuth(c.conn.RemoteAddr())
		if banned {
			c.logger().Warn("IP banned after too many failed auth attempts.", "remote_addr", c.conn.RemoteAddr())
		}
		c.sendError("FATAL", "28P01", "password authentication failed")
		return fmt.Errorf("authentication failed for user %q", c.username)
	}

	// Record successful authentication (clears failed attempt counter)
	c.server.rateLimiter.RecordSuccessfulAuth(c.conn.RemoteAddr())

	// Send auth OK
	if err := wire.WriteAuthOK(c.writer); err != nil {
		return err
	}

	c.logger().Info("User authenticated.", "remote_addr", c.conn.RemoteAddr())
	return nil
}

func (c *clientConn) sendInitialParams() {
	params := map[string]string{
		"server_version":              "15.0 (Duckgres)",
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
	}

	for name, value := range params {
		if err := wire.WriteParameterStatus(c.writer, name, value); err != nil {
			c.logger().Warn("Failed to write parameter.", "param", name, "error", err)
		}
	}

	// Send backend key data (pid and secret key for cancel requests)
	if err := wire.WriteBackendKeyData(c.writer, c.pid, c.secretKey); err != nil {
		c.logger().Warn("Failed to write backend key data.", "error", err)
	}
}

func (c *clientConn) messageLoop() error {
	for {
		// Set read deadline if idle timeout is configured
		if c.server.cfg.IdleTimeout > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.server.cfg.IdleTimeout))
		}

		msgType, body, err := wire.ReadMessage(c.reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			// Check if this is a timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.logger().Info("Connection idle timeout, closing.")
				return nil
			}
			return err
		}

		switch msgType {
		case wire.MsgQuery:
			// A simple Query resets extended-protocol state: it is processed
			// (not discarded by skip-until-Sync) — its trailing ReadyForQuery
			// resynchronizes the client.
			c.ignoreTillSync = false
			if err := c.handleQuery(body); err != nil {
				if isConnectionBroken(err) {
					c.logger().Info("Client connection lost during query.", "error", err)
					return nil
				}
				c.logger().Error("Query error.", "error", err)
			}

		case wire.MsgParse:
			// Extended query protocol - Parse
			c.runExtendedQueryMessage(c.handleParse, body)

		case wire.MsgBind:
			// Extended query protocol - Bind
			c.runExtendedQueryMessage(c.handleBind, body)

		case wire.MsgDescribe:
			// Extended query protocol - Describe
			c.runExtendedQueryMessage(c.handleDescribe, body)

		case wire.MsgExecute:
			// Extended query protocol - Execute
			c.runExtendedQueryMessage(c.handleExecute, body)

		case wire.MsgSync:
			// Extended query protocol - Sync: ends any skip-until-Sync error
			// recovery, then reports readiness.
			c.ignoreTillSync = false
			if err := c.writeReadyForQuery(c.txStatus); err != nil {
				return err
			}
			_ = c.flushWriter()

		case wire.MsgClose:
			// Extended query protocol - Close
			c.runExtendedQueryMessage(c.handleClose, body)

		case wire.MsgFlush:
			// Discarded during skip-until-Sync error recovery, like real
			// PostgreSQL (the ErrorResponse was already flushed by sendError).
			if !c.ignoreTillSync {
				_ = c.flushWriter()
			}

		case wire.MsgTerminate:
			return nil

		default:
			c.logger().Warn("Unknown message type.", "type", string(msgType))
		}
	}
}

func (c *clientConn) handleQuery(body []byte) (retErr error) {
	query := string(bytes.TrimRight(body, "\x00"))
	query = strings.TrimSpace(query)

	// Treat empty queries or queries with just semicolons as empty
	// PostgreSQL returns EmptyQueryResponse for queries like "" or ";" or ";;;"
	if query == "" || isEmptyQuery(query) {
		_ = wire.WriteEmptyQueryResponse(c.writer)
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Redacted form for everything observable (pg_stat_activity, spans,
	// logs): CREATE SECRET option lists carry credential material.
	loggableQuery := usersecrets.RedactForLog(query)

	c.currentQuery.Store(loggableQuery)
	c.queryStart.Store(time.Now())
	defer func() {
		c.currentQuery.Store("")
		c.queryStart.Store(time.Time{})
	}()

	start := time.Now()
	queryMetrics := c.beginQueryMetrics(start)
	defer func() {
		if retErr != nil {
			queryMetrics.markError(retErr)
		}
		c.finishQueryMetrics(queryMetrics)
	}()

	ctx, span := observe.Tracer().Start(c.ctx, "duckgres.query",
		trace.WithAttributes(
			attribute.String("duckgres.protocol", "simple"),
			attribute.String("duckgres.org_id", c.orgID),
			attribute.String("db.user", c.username),
			attribute.String("db.statement", observe.TruncateForSpan(loggableQuery)),
		),
	)
	defer span.End()
	// Replace connection context for the duration of this query so child
	// operations (queryContext, etc.) inherit the span.
	prevCtx := c.ctx
	c.ctx = ctx
	defer func() { c.ctx = prevCtx }()

	c.logger().Debug("Query received.", "query", loggableQuery)

	// Check for cursor operations (DECLARE, FETCH, CLOSE) before passthrough
	// or transpilation. DuckDB doesn't support these natively, so cursor
	// emulation is needed for all users including passthrough.
	{
		tree, parseErr := pg_query.Parse(query)
		if parseErr == nil && len(tree.Stmts) == 1 {
			switch s := tree.Stmts[0].Stmt.Node.(type) {
			case *pg_query.Node_DeclareCursorStmt:
				return c.handleDeclareCursor(query, s.DeclareCursorStmt)
			case *pg_query.Node_FetchStmt:
				return c.handleFetchCursor(query, s.FetchStmt)
			case *pg_query.Node_ClosePortalStmt:
				return c.handleCloseCursor(query, s.ClosePortalStmt)
			}
		}
	}

	// Intercept pg_cursors queries (e.g. psycopg's "SELECT 1 FROM pg_cursors WHERE name = ...").
	// DuckDB doesn't have this system view; return synthetic results from cursor emulation state.
	if cursorName, _, ok := matchPgCursorsQuery(query); ok {
		return c.handlePgCursorsQuery(cursorName)
	}

	// Intercept pg_stat_activity queries. Return synthetic results from the connection registry.
	if matchPgStatActivityQuery(query) {
		return c.handlePgStatActivity()
	}

	// Intercept persistent-secret DDL (CREATE PERSISTENT SECRET / DROP
	// SECRET) when a user secret manager is configured (multitenant remote
	// backend), so customer secrets survive across sessions and worker pods.
	// Must run before the passthrough branch: passthrough users get
	// persistence too.
	if c.handleUserSecretDDLSimple(query) {
		return nil
	}

	// Passthrough mode: skip all transpilation, send query directly to DuckDB
	if c.passthrough {
		upperQuery := strings.ToUpper(query)
		cmdType := c.getCommandType(upperQuery)
		if cmdType == "COPY" {
			return c.handleCopy(query, upperQuery)
		}
		return c.executeQueryDirect(query, cmdType)
	}

	// Route COPY directly to handleCopy()
	// before transpilation, because:
	// 1. File COPY options such as FORMAT 'parquet' must reach DuckDB intact;
	//    pg_query deparses them into PostgreSQL's FORMAT parquet form.
	// 2. The inner SELECT may contain DuckDB-specific syntax (QUALIFY, ASOF, struct literals)
	//    that pg_query can't parse
	// 3. handleCopyOut() already extracts and transpiles the inner SELECT separately
	// 4. validateWithDuckDB() can't EXPLAIN COPY with FORMAT "binary"
	//
	// NOTE: This must stay above queryContext(). handleCopyIn reads from
	// c.reader directly, which would race with the disconnect monitor.
	upperQueryEarly := strings.ToUpper(query)
	if shouldHandleCopyBeforeTranspile(query) {
		return c.handleCopy(query, upperQueryEarly)
	}

	// Check for multi-statement query (PostgreSQL simple query protocol supports
	// multiple semicolon-separated statements in a single Q message).
	// Each statement gets its own results, with a single ReadyForQuery at the end.
	tree, parseErr := pg_query.Parse(query)
	if parseErr == nil && len(tree.Stmts) > 1 {
		return c.handleMultiStatementQuery(query)
	}

	// Transpile PostgreSQL SQL to DuckDB-compatible SQL
	_, transpileSpan := observe.Tracer().Start(c.ctx, "duckgres.transpile")
	tr := c.newTranspiler(false)
	result, err := tr.Transpile(query)
	transpileSpan.End()
	if err != nil {
		// Transform error - send error to client
		c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Handle fallback to native DuckDB: PostgreSQL parsing failed, try DuckDB directly
	if result.FallbackToNative {
		if err := c.validateWithDuckDB(query); err != nil {
			// Neither PostgreSQL nor DuckDB can parse this query
			c.sendError("ERROR", "42601", fmt.Sprintf("syntax error: %v", err))
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}
		c.logger().Debug("Fallback to native DuckDB: query not valid PostgreSQL but valid DuckDB.", "query", loggableQuery)
	}

	// Handle transform-detected errors (e.g., unrecognized config parameter)
	if result.Error != nil {
		c.sendError("ERROR", transformErrorSQLState(result.Error), result.Error.Error())
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Surface any transpiler warnings (e.g. an unenforced constraint stripped on a
	// lake catalog) as NoticeResponse before the command result.
	for _, w := range result.Warnings {
		c.sendNotice("WARNING", "01000", w)
	}

	// Handle ignored SET parameters
	if result.IsIgnoredSet {
		c.logger().Debug("Ignoring PostgreSQL-specific SET.", "query", query)
		_ = c.writeCommandComplete("SET")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Handle no-op commands (CREATE INDEX, VACUUM, etc.)
	if result.IsNoOp {
		c.logger().Debug("No-op command (DuckLake limitation).", "query", query)
		_ = c.writeCommandComplete(result.NoOpTag)
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Handle multi-statement results (writable CTE rewrites)
	if len(result.Statements) > 0 {
		c.logger().Debug("Multi-statement query.", "statements", len(result.Statements), "cleanup", len(result.CleanupStatements))
		return c.executeMultiStatement(result.Statements, result.CleanupStatements)
	}

	// Use the transpiled SQL
	originalQuery := query
	query = c.rewriteDirectQuery(result.SQL)

	// Log the transpiled query if it differs from the original
	if query != originalQuery {
		c.logger().Debug("Query transpiled.", "executed", query)
	}

	// Determine command type for proper response
	upperQuery := strings.ToUpper(query)
	cmdType := c.getCommandType(upperQuery)

	// Handle COPY commands specially
	if cmdType == "COPY" {
		return c.handleCopy(query, upperQuery)
	}

	// For queries that don't return result rows, use Exec
	if !queryReturnsResults(query) {
		// Handle nested BEGIN: PostgreSQL issues a warning but continues,
		// while DuckDB throws an error. Match PostgreSQL behavior.
		if cmdType == "BEGIN" && c.txStatus == txStatusTransaction {
			c.sendNotice("WARNING", "25001", "there is already a transaction in progress")
			_ = c.writeCommandComplete("BEGIN")
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}

		// Open cursors pin the session's single DuckDB connection — release
		// them before a transaction-end statement needs it.
		c.closeCursorsAtTxEnd(cmdType)

		ctx, cleanup := c.queryContext()
		defer cleanup()

		execStart := time.Now()
		execCtx, execSpan := observe.Tracer().Start(ctx, "duckgres.execute")
		runExec := func() (ExecResult, error) {
			execResult, err := c.executor.ExecContext(ctx, query)
			if err != nil {
				fallbackResult, handled, fallbackErr := c.execCompatibilityFallback(ctx, query, err, func(fallbackQuery string) (ExecResult, error) {
					return c.executor.ExecContext(ctx, fallbackQuery)
				})
				if handled {
					return fallbackResult, fallbackErr
				}
			}
			return execResult, err
		}

		execResult, err := runExec()
		c.lastProfilingSummary = observe.EnrichSpanWithProfiling(execCtx, execSpan, execStart, c.executor, c.orgID)
		execSpan.End()
		if err != nil {
			if c.txStatus == txStatusIdle && isDuckLakeTransactionConflict(err) {
				ducklakeConflictTotal.Inc()
				execResult, err = retryOnConflict(runExec)
			}
			if err != nil {
				execResult, err, _ = recoverAbortedTransaction(
					err,
					c.txStatus == txStatusIdle,
					func() error {
						_, rollbackErr := c.executor.ExecContext(context.Background(), "ROLLBACK")
						return rollbackErr
					},
					runExec,
				)
			}
			if err != nil {
				errCode := classifyErrorCode(err)
				errMsg := friendlyExecError(err)
				if c.isCallerCancellation(err) {
					errMsg = "canceling statement due to user request"
				} else {
					c.logQueryError(query, err)
				}
				c.sendError("ERROR", errCode, errMsg)
				c.setTxError()
				c.logQuery(start, originalQuery, query, cmdType, 0, 0, errCode, errMsg, "simple")
				_ = c.writeReadyForQuery(c.txStatus)
				_ = c.flushWriter()
				return nil
			}
		}

		var writtenRows int64
		if execResult != nil {
			writtenRows, _ = execResult.RowsAffected()
		}
		c.updateTxStatus(cmdType)
		tag := c.buildCommandTag(cmdType, execResult)
		_ = c.writeCommandComplete(tag)
		c.logQuery(start, originalQuery, query, cmdType, 0, writtenRows, "", "", "simple")
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Execute query that returns results (SELECT, DML RETURNING, etc.)
	rowCount, errCode, errMsg, err := c.executeSelectQuery(query, cmdType)
	if err == nil {
		c.logQuery(start, originalQuery, query, cmdType, rowCount, 0, errCode, errMsg, "simple")
	}
	return err
}

// countDollarParams counts $N-style parameter placeholders in a query string
// using a manual byte scan. This avoids pg_query.Parse which may fail on
// DuckDB-native SQL syntax.
func countDollarParams(query string) int {
	max := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '$' && i+1 < len(query) && query[i+1] >= '1' && query[i+1] <= '9' {
			n := 0
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				n = n*10 + int(query[j]-'0')
				j++
			}
			if n > max {
				max = n
			}
		}
	}
	return max
}

// isEmptyQuery and stripLeadingComments moved to server/sqlcore so the
// Flight client can call them without importing server. Local thin wrappers
// preserve the unexported call-site spellings used throughout this file.
func isEmptyQuery(query string) bool           { return sqlcore.IsEmptyQuery(query) }
func stripLeadingComments(query string) string { return sqlcore.StripLeadingComments(query) }

// stripLeadingNoise strips leading whitespace, comments, and parentheses from
// a query string in a loop until none remain. This handles cases like
// "(/* comment */ SELECT 1)" where comments are interleaved with parens.
func stripLeadingNoise(query string) string {
	for {
		prev := query
		query = stripLeadingComments(query)
		query = strings.TrimLeft(query, "( \t\n\r")
		if query == prev {
			return query
		}
	}
}

// describeSupportsLimit returns true if the (uppercased) query supports a LIMIT clause.
// SHOW, DESCRIBE, EXPLAIN, PRAGMA, CALL etc. do not.
func describeSupportsLimit(upper string) bool {
	s := strings.TrimSpace(upper)
	return strings.HasPrefix(s, "SELECT") ||
		strings.HasPrefix(s, "WITH") ||
		strings.HasPrefix(s, "VALUES") ||
		strings.HasPrefix(s, "TABLE") ||
		strings.HasPrefix(s, "FROM") // DuckDB FROM-first syntax
}

// queryReturnsResults checks if a SQL query returns a result set.
// This is used to determine whether to send RowDescription or NoData.
func queryReturnsResults(query string) bool {
	upper := strings.ToUpper(stripLeadingNoise(query))
	// SELECT is the most common
	if strings.HasPrefix(upper, "SELECT") {
		return true
	}
	// WITH ... SELECT (CTEs)
	if strings.HasPrefix(upper, "WITH") && (len(upper) == 4 || isWSChar(upper[4]) || upper[4] == '/' || upper[4] == '-') {
		return true
	}
	// VALUES clause returns rows
	if strings.HasPrefix(upper, "VALUES") {
		return true
	}
	// SHOW commands return results
	if strings.HasPrefix(upper, "SHOW") {
		return true
	}
	// TABLE is shorthand for SELECT * FROM table
	if strings.HasPrefix(upper, "TABLE") {
		return true
	}
	// EXECUTE can return results if the prepared statement is a SELECT
	if strings.HasPrefix(upper, "EXECUTE") {
		return true
	}
	// EXPLAIN returns results
	if strings.HasPrefix(upper, "EXPLAIN") {
		return true
	}
	// DESCRIBE returns results (DuckDB-specific)
	if strings.HasPrefix(upper, "DESCRIBE") {
		return true
	}
	// SUMMARIZE returns results (DuckDB-specific)
	if strings.HasPrefix(upper, "SUMMARIZE") {
		return true
	}
	// FROM-first syntax returns results (DuckDB-specific)
	if strings.HasPrefix(upper, "FROM") {
		return true
	}
	// DML with RETURNING clause produces result rows.
	// Check for RETURNING preceded by any whitespace (space, newline, tab).
	if (strings.HasPrefix(upper, "INSERT") ||
		strings.HasPrefix(upper, "UPDATE") ||
		strings.HasPrefix(upper, "DELETE")) &&
		containsReturning(upper) {
		return true
	}
	return false
}

// containsReturning checks if an uppercased SQL string contains a top-level
// RETURNING keyword at parenthesis depth 0. It skips content inside
// parentheses, single-quoted strings (including E-string backslash escapes),
// dollar-quoted strings, double-quoted identifiers, and SQL comments to avoid
// false positives.
func containsReturning(upper string) bool {
	return scanForReturning(upper, true)
}

// containsReturningAnyDepth is like containsReturning but matches RETURNING at
// any parenthesis depth. This is needed for WITH-prefixed queries (writable
// CTEs) where the RETURNING clause is structurally inside AS (...) parens.
func containsReturningAnyDepth(upper string) bool {
	return scanForReturning(upper, false)
}

// scanForReturning is the shared SQL-aware lexer for RETURNING detection.
// When topLevelOnly is true, RETURNING is only matched at parenthesis depth 0.
// When false, RETURNING is matched at any depth (for writable CTE detection).
// In both modes, content inside strings, identifiers, and comments is skipped.
func scanForReturning(upper string, topLevelOnly bool) bool {
	depth := 0
	i := 0
	for i < len(upper) {
		switch upper[i] {
		case '(':
			depth++
			i++
		case ')':
			if depth > 0 {
				depth--
			}
			i++

		case '\'':
			// Single-quoted string literal.
			// Check for E-string prefix (E'...' with backslash escaping).
			estring := i > 0 && upper[i-1] == 'E'
			i++
			for i < len(upper) {
				if upper[i] == '\'' {
					i++
					if i < len(upper) && upper[i] == '\'' {
						i++ // '' escape (works in both normal and E-strings)
						continue
					}
					break
				}
				if estring && upper[i] == '\\' {
					i++ // skip escaped character in E-string
					if i < len(upper) {
						i++
					}
					continue
				}
				i++
			}

		case '"':
			// Double-quoted identifier — skip to closing quote.
			i++
			for i < len(upper) {
				if upper[i] == '"' {
					i++
					if i < len(upper) && upper[i] == '"' {
						i++ // "" escape
						continue
					}
					break
				}
				i++
			}

		case '$':
			// Dollar-quoted string: $tag$...$tag$ or $$...$$
			if tag, ok := parseDollarTag(upper, i); ok {
				i += len(tag) // skip opening tag
				for i+len(tag) <= len(upper) {
					if upper[i] == '$' && upper[i:i+len(tag)] == tag {
						i += len(tag) // skip closing tag
						break
					}
					i++
				}
			} else {
				i++
			}

		case '-':
			// Line comment: -- ... \n
			if i+1 < len(upper) && upper[i+1] == '-' {
				i += 2
				for i < len(upper) && upper[i] != '\n' {
					i++
				}
			} else {
				i++
			}

		case '/':
			// Block comment: /* ... */
			if i+1 < len(upper) && upper[i+1] == '*' {
				i += 2
				for i+1 < len(upper) {
					if upper[i] == '*' && upper[i+1] == '/' {
						i += 2
						break
					}
					i++
				}
			} else {
				i++
			}

		case 'R':
			if (!topLevelOnly || depth == 0) && i+9 <= len(upper) && upper[i:i+9] == "RETURNING" {
				// Check preceded by whitespace
				if i > 0 && isWSChar(upper[i-1]) {
					// Check followed by end-of-string or a non-identifier character
					end := i + 9
					if end >= len(upper) || isReturningTrailer(upper[end]) {
						return true
					}
				}
			}
			i++

		default:
			i++
		}
	}
	return false
}

// parseDollarTag extracts a dollar-quote tag starting at position i.
// Returns the full tag (e.g., "$$" or "$tag$") and true, or ("", false).
func parseDollarTag(s string, i int) (string, bool) {
	if i >= len(s) || s[i] != '$' {
		return "", false
	}
	j := i + 1
	// Tag is $[identifier]$ where identifier is [A-Z_0-9] (already uppercased).
	for j < len(s) {
		if s[j] == '$' {
			return s[i : j+1], true
		}
		c := s[j]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			j++
			continue
		}
		return "", false // invalid tag character
	}
	return "", false
}

// isWSChar reports whether c is an ASCII whitespace character.
func isWSChar(c byte) bool {
	return c == ' ' || c == '\n' || c == '\t' || c == '\r'
}

// isReturningTrailer reports whether c is a valid character immediately after
// the RETURNING keyword (i.e., not a continuation of an identifier).
func isReturningTrailer(c byte) bool {
	return c == ' ' || c == '\n' || c == '\t' || c == '\r' ||
		c == ';' || c == '*' || c == '(' || c == ',' ||
		c == '"' || // double-quoted identifier: RETURNING"col"
		c == '-' || // line comment: RETURNING-- comment
		c == '/' || // block comment: RETURNING/* comment */
		c == '$' // dollar-quoted: RETURNING$tag$expr$tag$
}

// isDMLReturning reports whether query is a DML statement (INSERT/UPDATE/DELETE)
// with a RETURNING clause, or a writable CTE (WITH ... DML ... RETURNING).
// Such statements produce result rows but cannot be described without executing
// the mutation.
//
// For WITH-prefixed queries, RETURNING is matched at any parenthesis depth
// because writable CTEs place the RETURNING clause inside AS (...), making
// depth-0-only matching structurally unable to detect them.
func isDMLReturning(query string) bool {
	upper := strings.ToUpper(stripLeadingNoise(query))
	switch {
	case strings.HasPrefix(upper, "INSERT"),
		strings.HasPrefix(upper, "UPDATE"),
		strings.HasPrefix(upper, "DELETE"):
		return containsReturning(upper)
	case strings.HasPrefix(upper, "WITH") && (len(upper) == 4 || isWSChar(upper[4]) || upper[4] == '/' || upper[4] == '-'):
		return containsReturningAnyDepth(upper)
	default:
		return false
	}
}

// isWithDML reports whether a query is a WITH (CTE) whose outer statement is
// DML (INSERT/UPDATE/DELETE). Such queries don't return results (unless they
// have a RETURNING clause, handled separately by isDMLReturning) and must not
// be executed during Describe to avoid unintended mutations.
func isWithDML(query string) bool {
	upper := strings.ToUpper(stripLeadingNoise(query))
	outer := outerStatementOfCTE(upper)
	if outer == "" {
		return false
	}
	return strings.HasPrefix(outer, "INSERT") ||
		strings.HasPrefix(outer, "UPDATE") ||
		strings.HasPrefix(outer, "DELETE")
}

// isExplainStmt reports whether the query is an EXPLAIN statement (the EXPLAIN
// keyword followed by a space or '('). Used to avoid executing EXPLAIN at
// Describe time — EXPLAIN ANALYZE of a write mutates, and a describe-probe
// execution would run it a second time.
func isExplainStmt(query string) bool {
	upper := strings.ToUpper(stripLeadingNoise(query))
	const kw = "EXPLAIN"
	if !strings.HasPrefix(upper, kw) {
		return false
	}
	if len(upper) == len(kw) {
		return true
	}
	switch upper[len(kw)] {
	case ' ', '\t', '\n', '\r', '(':
		return true
	}
	return false
}

// explainPlanColumn returns the single column name DuckDB uses for an EXPLAIN
// result: "analyzed_plan" for EXPLAIN ANALYZE, "physical_plan" otherwise.
func explainPlanColumn(query string) string {
	if strings.Contains(strings.ToUpper(query), "ANALYZE") {
		return "analyzed_plan"
	}
	return "physical_plan"
}

// staticColumnType is a minimal ColumnTyper reporting a fixed DuckDB type name,
// used to synthesize a RowDescription without executing a query.
type staticColumnType string

func (s staticColumnType) DatabaseTypeName() string { return string(s) }

// skipBalancedParens advances past a parenthesized group in an uppercased SQL
// string. i must point to the character immediately after the opening '('.
// It tracks paren depth while correctly skipping SQL constructs that may
// contain literal parentheses: single-quoted strings (including ” escapes and
// E-string backslash escapes), double-quoted identifiers, dollar-quoted strings,
// line comments (--), and block comments (/* */).
// Returns the position immediately after the matching ')'.
func skipBalancedParens(upper string, i int) int {
	depth := 1
	for i < len(upper) && depth > 0 {
		switch upper[i] {
		case '(':
			depth++
			i++
		case ')':
			depth--
			i++

		case '\'':
			// Single-quoted string; handle E-string prefix and '' escapes.
			estring := i > 0 && upper[i-1] == 'E'
			i++
			for i < len(upper) {
				if upper[i] == '\'' {
					i++
					if i < len(upper) && upper[i] == '\'' {
						i++
						continue
					}
					break
				}
				if estring && upper[i] == '\\' {
					i++
					if i < len(upper) {
						i++
					}
					continue
				}
				i++
			}

		case '"':
			// Double-quoted identifier.
			i++
			for i < len(upper) {
				if upper[i] == '"' {
					i++
					if i < len(upper) && upper[i] == '"' {
						i++
						continue
					}
					break
				}
				i++
			}

		case '$':
			// Dollar-quoted string.
			if tag, ok := parseDollarTag(upper, i); ok {
				i += len(tag)
				for i+len(tag) <= len(upper) {
					if upper[i] == '$' && upper[i:i+len(tag)] == tag {
						i += len(tag)
						break
					}
					i++
				}
			} else {
				i++
			}

		case '-':
			// Line comment: -- ... \n
			if i+1 < len(upper) && upper[i+1] == '-' {
				i += 2
				for i < len(upper) && upper[i] != '\n' {
					i++
				}
			} else {
				i++
			}

		case '/':
			// Block comment: /* ... */
			if i+1 < len(upper) && upper[i+1] == '*' {
				i += 2
				for i+1 < len(upper) {
					if upper[i] == '*' && upper[i+1] == '/' {
						i += 2
						break
					}
					i++
				}
			} else {
				i++
			}

		default:
			i++
		}
	}
	return i
}

// skipWhitespaceAndComments advances i past any whitespace, line comments (--),
// and block comments (/* */) in an uppercased SQL string. Returns the new position.
func skipWhitespaceAndComments(upper string, i int) int {
	for i < len(upper) {
		if isWSChar(upper[i]) {
			i++
		} else if i+1 < len(upper) && upper[i] == '-' && upper[i+1] == '-' {
			i += 2
			for i < len(upper) && upper[i] != '\n' {
				i++
			}
		} else if i+1 < len(upper) && upper[i] == '/' && upper[i+1] == '*' {
			i += 2
			for i+1 < len(upper) {
				if upper[i] == '*' && upper[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
		} else {
			break
		}
	}
	return i
}

// outerStatementOfCTE returns the portion of an uppercased query after all CTE
// definitions, or "" if the query doesn't start with WITH.
// For "WITH a AS (...) SELECT ...", it returns "SELECT ...".
func outerStatementOfCTE(upper string) string {
	if !strings.HasPrefix(upper, "WITH") || (len(upper) > 4 && !isWSChar(upper[4]) && upper[4] != '/' && upper[4] != '-') {
		return ""
	}

	// Skip past "WITH" (and optional "RECURSIVE")
	i := 4 // len("WITH")
	i = skipWhitespaceAndComments(upper, i)
	if i+9 <= len(upper) && upper[i:i+9] == "RECURSIVE" && (i+9 >= len(upper) || isWSChar(upper[i+9]) || upper[i+9] == '/' || upper[i+9] == '-') {
		i += 9
		i = skipWhitespaceAndComments(upper, i)
	}

	// Skip CTE definitions: name AS (...) [, name AS (...)]
	for i < len(upper) {
		// Skip CTE name (identifier or quoted identifier)
		if i < len(upper) && upper[i] == '"' {
			i++
			for i < len(upper) {
				if upper[i] == '"' {
					i++
					if i < len(upper) && upper[i] == '"' {
						i++
						continue
					}
					break
				}
				i++
			}
		} else {
			for i < len(upper) && !isWSChar(upper[i]) && upper[i] != '(' && upper[i] != '/' && upper[i] != '-' {
				i++
			}
		}

		i = skipWhitespaceAndComments(upper, i)

		// Skip optional column alias list: cte (col1, col2) AS (...)
		if i < len(upper) && upper[i] == '(' {
			// Only treat as column list if "AS" follows the closing paren.
			// Peek ahead to distinguish column list from CTE body (when AS is missing).
			saved := i
			i = skipBalancedParens(upper, i+1)
			i = skipWhitespaceAndComments(upper, i)
			if i+2 > len(upper) || upper[i:i+2] != "AS" || (i+2 < len(upper) && !isWSChar(upper[i+2]) && upper[i+2] != '(') {
				// No AS after parens — this wasn't a column list, restore position.
				i = saved
			}
		}

		// Expect "AS"
		if i+2 <= len(upper) && upper[i:i+2] == "AS" && (i+2 >= len(upper) || isWSChar(upper[i+2]) || upper[i+2] == '(' || upper[i+2] == '/' || upper[i+2] == '-') {
			i += 2
			i = skipWhitespaceAndComments(upper, i)
		}

		// Skip the CTE body: balanced parentheses with SQL-aware scanning
		// to correctly handle parens inside strings, comments, and identifiers.
		if i < len(upper) && upper[i] == '(' {
			i = skipBalancedParens(upper, i+1)
		}

		i = skipWhitespaceAndComments(upper, i)

		// If there's a comma, another CTE definition follows
		if i < len(upper) && upper[i] == ',' {
			i++
			i = skipWhitespaceAndComments(upper, i)
			continue
		}

		// Otherwise, we've reached the outer statement
		break
	}

	return upper[i:]
}

func (c *clientConn) getCommandType(upperQuery string) string {
	// Strip leading comments like /*Fivetran*/ before checking command type
	upperQuery = stripLeadingComments(upperQuery)

	// For WITH (CTE) queries, determine command type from the outer statement.
	// e.g. "WITH cte AS (...) INSERT INTO t ..." → "INSERT"
	if strings.HasPrefix(upperQuery, "WITH") && (len(upperQuery) == 4 || isWSChar(upperQuery[4]) || upperQuery[4] == '/' || upperQuery[4] == '-') {
		if outer := outerStatementOfCTE(upperQuery); outer != "" {
			return c.getCommandType(outer)
		}
	}

	switch {
	case strings.HasPrefix(upperQuery, "SELECT"):
		return "SELECT"
	case strings.HasPrefix(upperQuery, "INSERT"):
		return "INSERT"
	case strings.HasPrefix(upperQuery, "UPDATE"):
		return "UPDATE"
	case strings.HasPrefix(upperQuery, "DELETE"):
		return "DELETE"
	case strings.HasPrefix(upperQuery, "CREATE TABLE"),
		strings.HasPrefix(upperQuery, "CREATE TEMPORARY TABLE"),
		strings.HasPrefix(upperQuery, "CREATE TEMP TABLE"),
		strings.HasPrefix(upperQuery, "CREATE UNLOGGED TABLE"):
		return "CREATE TABLE"
	case strings.HasPrefix(upperQuery, "CREATE INDEX"),
		strings.HasPrefix(upperQuery, "CREATE UNIQUE INDEX"):
		return "CREATE INDEX"
	case strings.HasPrefix(upperQuery, "CREATE VIEW"),
		strings.HasPrefix(upperQuery, "CREATE OR REPLACE VIEW"):
		return "CREATE VIEW"
	case strings.HasPrefix(upperQuery, "CREATE SCHEMA"):
		return "CREATE SCHEMA"
	case strings.HasPrefix(upperQuery, "CREATE"):
		return "CREATE"
	case strings.HasPrefix(upperQuery, "DROP TABLE"):
		return "DROP TABLE"
	case strings.HasPrefix(upperQuery, "DROP INDEX"):
		return "DROP INDEX"
	case strings.HasPrefix(upperQuery, "DROP VIEW"):
		return "DROP VIEW"
	case strings.HasPrefix(upperQuery, "DROP SCHEMA"):
		return "DROP SCHEMA"
	case strings.HasPrefix(upperQuery, "DROP"):
		return "DROP"
	case strings.Contains(upperQuery, "ADD CONSTRAINT") ||
		strings.Contains(upperQuery, "ADD PRIMARY KEY") ||
		strings.Contains(upperQuery, "ADD UNIQUE") ||
		strings.Contains(upperQuery, "ADD FOREIGN KEY") ||
		strings.Contains(upperQuery, "ADD CHECK"):
		return "ALTER TABLE ADD CONSTRAINT"
	case strings.HasPrefix(upperQuery, "ALTER"):
		return "ALTER TABLE"
	case strings.HasPrefix(upperQuery, "TRUNCATE"):
		return "TRUNCATE TABLE"
	case strings.HasPrefix(upperQuery, "BEGIN"):
		return "BEGIN"
	case strings.HasPrefix(upperQuery, "COMMIT"):
		return "COMMIT"
	case strings.HasPrefix(upperQuery, "ROLLBACK"):
		return "ROLLBACK"
	case strings.HasPrefix(upperQuery, "SET"):
		return "SET"
	case strings.HasPrefix(upperQuery, "COPY"):
		return "COPY"
	default:
		return "SELECT" // fallback to SELECT behavior
	}
}

// updateTxStatus updates the transaction status based on the executed command.
// This is called after a successful command execution.
func (c *clientConn) updateTxStatus(cmdType string) {
	switch cmdType {
	case "BEGIN":
		c.txStatus = txStatusTransaction
	case "COMMIT", "ROLLBACK":
		c.txStatus = txStatusIdle
		c.closeAllCursors()
	}
	// For other commands, keep the current status
}

// setTxError marks the transaction as failed if we're in a transaction.
// This should be called when a query fails within a transaction.
func (c *clientConn) setTxError() {
	if c.txStatus == txStatusTransaction {
		c.txStatus = txStatusError
	}
}

// buildCommandTagFromRowCount builds a command tag from a command type and row count.
// Used when results are streamed via Query (e.g., DML RETURNING) rather than Exec.
func buildCommandTagFromRowCount(cmdType string, rowCount int64) string {
	switch cmdType {
	case "INSERT":
		return fmt.Sprintf("INSERT 0 %d", rowCount)
	case "UPDATE":
		return fmt.Sprintf("UPDATE %d", rowCount)
	case "DELETE":
		return fmt.Sprintf("DELETE %d", rowCount)
	default:
		return fmt.Sprintf("SELECT %d", rowCount)
	}
}

func (c *clientConn) buildCommandTag(cmdType string, result ExecResult) string {
	switch cmdType {
	case "INSERT":
		rowsAffected, _ := result.RowsAffected()
		return fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case "UPDATE":
		rowsAffected, _ := result.RowsAffected()
		return fmt.Sprintf("UPDATE %d", rowsAffected)
	case "DELETE":
		rowsAffected, _ := result.RowsAffected()
		return fmt.Sprintf("DELETE %d", rowsAffected)
	default:
		return cmdType
	}
}
