package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"time"

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

// CancelClientConn cancels the context of a clientConn.
func CancelClientConn(cc *clientConn) {
	if cc.cancel != nil {
		cc.cancel()
	}
}

// SetLogicalCatalogMapping records whether this session should rewrite DuckLake
// metadata surfaces to the client-visible logical database name.
func SetLogicalCatalogMapping(cc *clientConn, enabled bool) {
	if cc != nil {
		cc.logicalCatalogMapping = enabled
	}
}

// HasAttachedCatalog and InitSessionDatabaseMetadata moved to
// server/sessionmeta. Re-exports kept here for the dozen call sites in
// the control plane and elsewhere; new code should import server/sessionmeta
// directly.
var (
	HasAttachedCatalog           = sessionmeta.HasAttachedCatalog
	InitSessionDatabaseMetadata  = sessionmeta.InitSessionDatabaseMetadata
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
