package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"time"

	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/sessionmeta"
	"github.com/posthog/duckgres/server/wire"
)

// Exported wrappers for protocol functions used by the control plane worker.
// These delegate to the internal (lowercase) implementations.

func ReadStartupMessage(r io.Reader) (map[string]string, error) {
	return wire.ReadStartupMessage(r)
}

func ReadMessage(r io.Reader) (byte, []byte, error) {
	return wire.ReadMessage(r)
}

func WriteAuthOK(w io.Writer) error {
	return wire.WriteAuthOK(w)
}

func WriteAuthCleartextPassword(w io.Writer) error {
	return wire.WriteAuthCleartextPassword(w)
}

func WriteReadyForQuery(w io.Writer, txStatus byte) error {
	return wire.WriteReadyForQuery(w, txStatus)
}

func WriteErrorResponse(w io.Writer, severity, code, message string) error {
	return wire.WriteErrorResponse(w, severity, code, message)
}

func WriteParameterStatus(w io.Writer, name, value string) error {
	return wire.WriteParameterStatus(w, name, value)
}

func WriteBackendKeyData(w io.Writer, pid, secretKey int32) error {
	return wire.WriteBackendKeyData(w, pid, secretKey)
}

// NewClientConn creates a clientConn with pre-initialized fields for use by
// the control plane worker. The returned value is opaque (*clientConn) but
// can be used with SendInitialParams and RunMessageLoop.
func NewClientConn(s *Server, conn net.Conn, reader *bufio.Reader, writer *bufio.Writer,
	username, orgID, database, applicationName string, executor QueryExecutor, pid, secretKey int32, workerID int, workerPod string) *clientConn {

	ctx, cancel := context.WithCancel(context.Background())
	return &clientConn{
		server:          s,
		conn:            conn,
		reader:          reader,
		writer:          writer,
		username:        username,
		orgID:           orgID,
		database:        database,
		applicationName: applicationName,
		executor:        executor,
		pid:             pid,
		secretKey:       secretKey,
		passthrough:     s.cfg.PassthroughUsers[username],
		stmts:           make(map[string]*preparedStmt),
		portals:         make(map[string]*portal),
		cursors:         make(map[string]*cursorState),
		txStatus:        txStatusIdle,
		ctx:             ctx,
		cancel:          cancel,
		backendStart:    time.Now(),
		workerID:        workerID,
		workerPod:       workerPod,
	}
}

// ConnDetail is a redacted snapshot of one live client connection, for the
// admin live-query detail view. Query is the ALREADY-redacted current/last
// query (usersecrets.RedactForLog, same as pg_stat_activity) — callers must
// never expose raw SQL here.
type ConnDetail struct {
	PID             int32
	OrgID           string
	Username        string
	Database        string
	ApplicationName string
	ClientAddr      string
	ClientPort      int32
	WorkerID        int
	WorkerPod       string
	State           string // active | idle | idle in transaction | idle in transaction (aborted)
	Query           string // redacted current/last query ("" when idle)
	BackendStart    time.Time
	QueryStart      time.Time // zero when no query is in flight
}

// ConnDetailByPID returns a redacted snapshot of the live connection for pid,
// or ok=false if no such connection is registered on this server. Used by the
// control-plane admin API to render the live-query detail view.
func (s *Server) ConnDetailByPID(pid int32) (ConnDetail, bool) {
	if s == nil {
		return ConnDetail{}, false
	}
	s.connsMu.RLock()
	c, ok := s.conns[pid]
	s.connsMu.RUnlock()
	if !ok {
		return ConnDetail{}, false
	}
	return c.connDetail(), true
}

// SetConnectionWorkerSize records the provisioned worker pod size (in
// milli-units) on a control-plane connection for compute-usage billing.
// millicores == 0 means the size is unknown (non-remote / standalone) and
// metering is skipped. Constant for the connection's life.
func SetConnectionWorkerSize(cc *clientConn, millicores, mib int64) {
	if cc != nil {
		cc.workerMillicores = millicores
		cc.workerMiB = mib
	}
}

// ConnectionBilling returns the data needed to meter one connection's
// compute-usage at teardown: the org, the provisioned worker size in
// milli-units, and the connection's elapsed lifetime. millicores == 0 means
// metering should be skipped (unknown worker size). Call at the same teardown
// point as CloseConnectionMetrics.
func ConnectionBilling(cc *clientConn) (orgID string, millicores, mib int64, dur time.Duration) {
	if cc == nil {
		return "", 0, 0, 0
	}
	return cc.orgID, cc.workerMillicores, cc.workerMiB, time.Since(cc.backendStart)
}

// CancelClientConn cancels the context of a clientConn.
func CancelClientConn(cc *clientConn) {
	if cc.cancel != nil {
		cc.cancel()
	}
}

// SetCatalogUseRewrite records whether this session should expand a bare
// `USE ducklake`/`USE iceberg` into its reliable two-part target. This is not
// masking — the catalog names are real; it only works around DuckDB's
// bare-catalog `USE` resolution.
func SetCatalogUseRewrite(cc *clientConn, enabled bool) {
	if cc != nil {
		cc.catalogUseRewrite = enabled
	}
}

// SetPassthrough flips this session into passthrough mode (bypasses the SQL
// transpiler + pg_catalog). The control plane resolves the per-org flag from
// the config store after auth and calls this before the message loop starts.
// Single-tenant mode keeps using server.Config.PassthroughUsers and never
// calls this.
func SetPassthrough(cc *clientConn, enabled bool) {
	if cc != nil {
		cc.passthrough = enabled
	}
}

// SetConnectionIcebergConfig records the per-tenant Iceberg config for
// control-plane proxy connections. The proxy server's global config is not
// tenant-specific, so query-time metadata loading must use this override.
func SetConnectionIcebergConfig(cc *clientConn, cfg IcebergConfig) {
	if cc != nil {
		cc.tenantIcebergConfig = cfg
		cc.hasTenantIcebergConfig = true
	}
}

// SetConnectionPhysicalCatalog records the resolved DuckDB catalog for
// control-plane proxy connections. The PostgreSQL-visible database remains on
// clientConn.database; this value is used only for execution/transpiler policy.
func SetConnectionPhysicalCatalog(cc *clientConn, catalog string) {
	if cc != nil {
		cc.physicalCatalog = catalog
	}
}

// HasAttachedCatalog and InitSessionDatabaseMetadata moved to
// server/sessionmeta. Re-exports kept here for the dozen call sites in
// the control plane and elsewhere; new code should import server/sessionmeta
// directly.
var (
	HasAttachedCatalog          = sessionmeta.HasAttachedCatalog
	InitSessionDatabaseMetadata = sessionmeta.InitSessionDatabaseMetadata
)

// SendInitialParams sends the initial parameter status messages and backend key data.
func SendInitialParams(cc *clientConn) {
	cc.sendInitialParams()
}

// RunMessageLoop runs the main message loop for a client connection.
// It cancels the connection context when the loop exits, ensuring in-flight
// query contexts (and any gRPC calls derived from them) are cancelled promptly.
func RunMessageLoop(cc *clientConn) error {
	cc.ensureConnectionContext()
	defer cc.cancel()
	cc.server.registerConn(cc)
	defer cc.server.unregisterConn(cc.pid)
	return cc.messageLoop()
}

// CloseConnectionMetrics records the completed connection's lifetime in the
// duckgres_connection_duration_seconds histogram (per org) and returns the
// elapsed duration so the caller can log it. Call exactly once per connection
// at teardown. backendStart is always set in NewClientConn, so the duration is
// always meaningful for control-plane connections.
func CloseConnectionMetrics(cc *clientConn) time.Duration {
	d := time.Since(cc.backendStart)
	observe.ObserveConnectionDuration(cc.orgID, d.Seconds())
	return d
}

// InitMinimalServer initializes a Server struct with minimal fields for use
// in control plane worker sessions.
func InitMinimalServer(s *Server, cfg Config, queryCancelCh <-chan struct{}) {
	s.cfg = cfg
	s.activeQueries = make(map[BackendKey]context.CancelFunc)
	s.duckLakeSem = make(chan struct{}, 1)
	s.externalCancelCh = queryCancelCh
	s.conns = make(map[int32]*clientConn)
}

// GenerateSecretKey re-exports wire.GenerateSecretKey so existing callers
// that imported it from server keep compiling. New code should use
// server/wire directly.
var GenerateSecretKey = wire.GenerateSecretKey

// SetQueryLogger sets the query logger on a Server. Used by the control plane
// to attach a query logger to the minimal server after creation.
func SetQueryLogger(s *Server, ql *QueryLogger) {
	s.queryLogger = ql
	s.queryLogSink = ql
}

// SetQueryLogSink sets the active query-log sink on a Server.
func SetQueryLogSink(s *Server, sink QueryLogSink) {
	s.queryLogSink = sink
	if ql, ok := sink.(*QueryLogger); ok {
		s.queryLogger = ql
	} else {
		s.queryLogger = nil
	}
}

// StopQueryLogging drains and stops the active query-log sink, if any.
func (s *Server) StopQueryLogging(ctx context.Context) error {
	if s == nil {
		return nil
	}
	ctx, cancel := queryLogStopContext(ctx, 30*time.Second)
	defer cancel()
	if s.queryLogSink != nil {
		err := s.queryLogSink.StopContext(ctx)
		s.queryLogSink = nil
		s.queryLogger = nil
		return err
	}
	if s.queryLogger != nil {
		err := s.queryLogger.StopContext(ctx)
		s.queryLogger = nil
		return err
	}
	return nil
}

// SetUserSecretManager installs the per-user persistent secret manager on a
// Server. Used by the multitenant control plane after the config store is up.
// Must be called before the server starts accepting connections.
func SetUserSecretManager(s *Server, mgr UserSecretManager) {
	s.cfg.UserSecrets = mgr
}

// UserSecretManager returns the installed per-user persistent secret manager
// (nil when the feature is not configured).
func (s *Server) UserSecretManager() UserSecretManager {
	return s.cfg.UserSecrets
}

// QueryLogger returns the server's query logger (may be nil).
func (s *Server) QueryLogger() *QueryLogger {
	return s.queryLogger
}

// SetProgressFn sets the progress lookup function on a Server.
// Used by the control plane to provide cached query progress from worker health checks.
func SetProgressFn(s *Server, fn func(pid int32) (pct float64, rows, totalRows uint64, stalled bool)) {
	s.progressFn = fn
}
