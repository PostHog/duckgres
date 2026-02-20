package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"time"
)

// Exported wrappers for protocol functions used by the control plane worker.
// These delegate to the internal (lowercase) implementations.

func ReadStartupMessage(r io.Reader) (map[string]string, error) {
	return readStartupMessage(r)
}

func ReadMessage(r io.Reader) (byte, []byte, error) {
	return readMessage(r)
}

func WriteAuthOK(w io.Writer) error {
	return writeAuthOK(w)
}

func WriteAuthCleartextPassword(w io.Writer) error {
	return writeAuthCleartextPassword(w)
}

func WriteReadyForQuery(w io.Writer, txStatus byte) error {
	return writeReadyForQuery(w, txStatus)
}

func WriteErrorResponse(w io.Writer, severity, code, message string) error {
	return writeErrorResponse(w, severity, code, message)
}

func WriteParameterStatus(w io.Writer, name, value string) error {
	return writeParameterStatus(w, name, value)
}

func WriteBackendKeyData(w io.Writer, pid, secretKey int32) error {
	return writeBackendKeyData(w, pid, secretKey)
}

// NewClientConn creates a clientConn with pre-initialized fields for use by
// the control plane worker. The returned value is opaque (*clientConn) but
// can be used with SendInitialParams and RunMessageLoop.
func NewClientConn(s *Server, conn net.Conn, reader *bufio.Reader, writer *bufio.Writer,
	username, database, applicationName string, executor QueryExecutor, pid, secretKey int32, workerID int) *clientConn {

	ctx, cancel := context.WithCancel(context.Background())
	return &clientConn{
		server:          s,
		conn:            conn,
		reader:          reader,
		writer:          writer,
		username:        username,
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
	}
}

// SendInitialParams sends the initial parameter status messages and backend key data.
func SendInitialParams(cc *clientConn) {
	cc.sendInitialParams()
}

// RunMessageLoop runs the main message loop for a client connection.
// It cancels the connection context when the loop exits, ensuring in-flight
// query contexts (and any gRPC calls derived from them) are cancelled promptly.
func RunMessageLoop(cc *clientConn) error {
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

// GenerateSecretKey generates a cryptographically random secret key for cancel requests.
func GenerateSecretKey() int32 {
	return generateSecretKey()
}

// IsEmptyQuery checks if a query contains only semicolons and whitespace.
// PostgreSQL returns EmptyQueryResponse for queries like ";" or ";;;" or "; ; ;".
func IsEmptyQuery(query string) bool {
	return isEmptyQuery(query)
}
