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
	"github.com/posthog/duckgres/transpiler/transform"
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

// DefaultControlPlaneIdleTimeout is the connection idle timeout the control
// plane applies when none is configured. In remote/process control-plane mode
// an idle client connection pins a worker (a scarce k8s pod or local process),
// so an idle connection is closed after this long — its message loop hits the
// read deadline, returns, and the worker is released back to the hot-idle pool.
// Operators override it with --idle-timeout (a negative value disables it).
const DefaultControlPlaneIdleTimeout = 60 * time.Second

// NormalizeIdleTimeout resolves a configured connection idle timeout: zero means
// "unset" → use zeroDefault; a negative value means "explicitly disabled" → 0
// (no timeout); a positive value is used as-is. Shared by standalone (24h
// default) and the control plane (DefaultControlPlaneIdleTimeout).
func NormalizeIdleTimeout(configured, zeroDefault time.Duration) time.Duration {
	switch {
	case configured == 0:
		return zeroDefault
	case configured < 0:
		return 0
	default:
		return configured
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

// ConnDetailByWorkerID returns a redacted snapshot of the live connection bound
// to the given control-plane worker id, or ok=false if none is registered here.
//
// Worker ids are CLUSTER-UNIQUE (config-store issued) and there is exactly one
// session per worker, so this is the collision-free address for the admin
// live-query detail. PID is NOT safe: the CP allocates backend pids per-org
// (every org's SessionManager starts at 1000), so two orgs can share a pid and a
// lookup keyed by pid can return the wrong org's connection. A negative workerID
// (standalone / transient describe conns) never matches.
func (s *Server) ConnDetailByWorkerID(workerID int) (ConnDetail, bool) {
	if s == nil || workerID < 0 {
		return ConnDetail{}, false
	}
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	for _, c := range s.conns {
		if c.workerID == workerID {
			return c.connDetail(), true
		}
	}
	return ConnDetail{}, false
}

// ConnLiveSummary is the per-connection live state the admin Live list needs:
// the pg_stat_activity-style State and the current-query start. State is
// "active" when a query is in flight; anything else means no in-flight query.
type ConnLiveSummary struct {
	State      string    // active | idle | idle in transaction | idle in transaction (aborted)
	QueryStart time.Time // zero when no query is in flight
}

// ConnSummariesByWorkerID returns a one-pass snapshot of every live connection
// keyed by cluster-unique worker id: its state and current-query start. Used to
// attach running-query duration AND the active/idle state to the live list in a
// single lock acquisition (keyed on worker id, not the collision-prone per-org
// pid). Connections with no worker (standalone / transient) are omitted.
func (s *Server) ConnSummariesByWorkerID() map[int]ConnLiveSummary {
	if s == nil {
		return nil
	}
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	out := make(map[int]ConnLiveSummary, len(s.conns))
	for _, c := range s.conns {
		if c.workerID < 0 {
			continue
		}
		sum := ConnLiveSummary{State: c.connState()}
		if qs, ok := c.queryStart.Load().(time.Time); ok && !qs.IsZero() {
			sum.QueryStart = qs
		}
		out[c.workerID] = sum
	}
	return out
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
// compute-usage at teardown: the org, the session's query source (the
// `duckgres.query_source` GUC — "standard" unless the client set it), the
// provisioned worker size in milli-units, and the connection's elapsed
// lifetime. millicores == 0 means metering should be skipped (unknown worker
// size). Call at the same teardown point as CloseConnectionMetrics. A
// mid-connection GUC change is not split: the whole connection is metered
// under the final value (documented in docs/design/billing-pull-api.md).
//
// The query source is clamped to the closed {standard, endpoints} set as
// defense in depth: every set path already validates (22023 at SET / startup
// time), so a non-canonical value here means a validation bypass — degrade it
// to the default rather than writing unbounded-cardinality client input into
// the billing bucket key (and onward into billing exports).
func ConnectionBilling(cc *clientConn) (orgID, querySource string, millicores, mib int64, dur time.Duration) {
	if cc == nil {
		return "", "", 0, 0, 0
	}
	qs := cc.QuerySource()
	if qs != transform.QuerySourceStandard && qs != transform.QuerySourceEndpoints {
		// Should be unreachable. Log the length, not the value — it is
		// arbitrary client input.
		cc.logger().Warn("Non-canonical duckgres.query_source at billing teardown; degrading to default.",
			"value_len", len(qs))
		qs = defaultQuerySource
	}
	return cc.orgID, qs, cc.workerMillicores, cc.workerMiB, time.Since(cc.backendStart)
}

// CancelClientConn cancels the context of a clientConn.
func CancelClientConn(cc *clientConn) {
	if cc.cancel != nil {
		cc.cancel()
	}
}

// SetCatalogUseRewrite records whether this session should expand a bare
// `USE ducklake` into its reliable two-part target. This is not
// masking — the catalog name is real; it only works around DuckDB's
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

// SetQueryAccessPolicy binds a fail-closed project policy before the message
// loop starts. nil keeps root and internal users unrestricted.
func SetQueryAccessPolicy(cc *clientConn, policy *QueryAccessPolicy) {
	if cc != nil {
		cc.queryAccessPolicy = policy
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
	s.recentErrors = newRecentErrorRing(0)
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
