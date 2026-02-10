package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/posthog/duckgres/controlplane/fdpass"
	pb "github.com/posthog/duckgres/controlplane/proto"
	"github.com/posthog/duckgres/server"
)

// Worker is a long-lived worker process that handles many client connections.
// It exposes a gRPC server for control and a Unix socket for FD passing.
type Worker struct {
	pb.UnimplementedWorkerControlServer

	grpcSocketPath string
	fdSocketPath   string

	mu          sync.RWMutex
	cfg         server.Config
	configured  bool
	draining    bool
	tlsConfig   *tls.Config
	dbPool      *DBPool
	startTime   time.Time
	totalQueries atomic.Int64

	// Session tracking
	sessions   map[int32]*workerSession
	sessionsMu sync.RWMutex
	sessionsWg sync.WaitGroup

	// Cancellation
	activeQueries map[server.BackendKey]context.CancelFunc

	// FD passing - stores the most recently received FD
	pendingFD int
}

type workerSession struct {
	pid        int32
	secretKey  int32
	cancel     context.CancelFunc
	remoteAddr string
	minServer  *server.Server // per-session server for query cancellation
}

// RunWorker is the entry point for a worker process.
// It starts the gRPC server and FD receiver, then waits for signals.
func RunWorker(grpcSocket, fdSocket string) {
	w := &Worker{
		grpcSocketPath: grpcSocket,
		fdSocketPath:   fdSocket,
		startTime:      time.Now(),
		sessions:       make(map[int32]*workerSession),
		activeQueries:  make(map[server.BackendKey]context.CancelFunc),
		pendingFD:      -1,
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := <-sigChan
		slog.Info("Worker received signal, shutting down.", "signal", sig)
		cancel()
	}()

	if err := w.run(ctx); err != nil {
		slog.Error("Worker exited with error.", "error", err)
		os.Exit(1)
	}
}

func (w *Worker) run(ctx context.Context) error {
	// Start gRPC server
	grpcLn, err := net.Listen("unix", w.grpcSocketPath)
	if err != nil {
		return fmt.Errorf("listen gRPC socket: %w", err)
	}
	defer func() { _ = os.Remove(w.grpcSocketPath) }()

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerControlServer(grpcServer, w)

	go func() {
		if err := grpcServer.Serve(grpcLn); err != nil {
			slog.Error("gRPC server error.", "error", err)
		}
	}()

	// Start FD receiver
	fdLn, err := net.Listen("unix", w.fdSocketPath)
	if err != nil {
		return fmt.Errorf("listen FD socket: %w", err)
	}
	defer func() { _ = os.Remove(w.fdSocketPath) }()

	// Accept FD connections in a goroutine
	go w.fdReceiverLoop(fdLn)

	slog.Info("Worker started.",
		"pid", os.Getpid(),
		"grpc_socket", w.grpcSocketPath,
		"fd_socket", w.fdSocketPath,
	)

	// Wait for context cancellation (signal)
	<-ctx.Done()

	slog.Info("Worker shutting down, draining sessions...")
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()

	// Wait for active sessions with timeout
	done := make(chan struct{})
	go func() {
		w.sessionsWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("All sessions drained.")
	case <-time.After(30 * time.Second):
		slog.Warn("Drain timeout, force closing sessions.")
	}

	grpcServer.GracefulStop()
	_ = fdLn.Close()

	if w.dbPool != nil {
		w.dbPool.CloseAll(5 * time.Second)
	}

	return nil
}

// fdReceiverLoop accepts connections on the FD socket and reads file descriptors from them.
// Each connection from the control plane is used for a single FD pass.
func (w *Worker) fdReceiverLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Check if listener was closed
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			slog.Error("FD socket accept error.", "error", err)
			continue
		}

		uc, ok := conn.(*net.UnixConn)
		if !ok {
			slog.Error("FD socket: not a UnixConn")
			_ = conn.Close()
			continue
		}

		// Receive the FD
		fd, err := fdpass.RecvFD(uc)
		_ = uc.Close()
		if err != nil {
			slog.Error("Failed to receive FD.", "error", err)
			continue
		}

		// Store the FD for the next AcceptConnection RPC to pick up
		w.mu.Lock()
		w.pendingFD = fd
		w.mu.Unlock()
	}
}

func (w *Worker) Configure(_ context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.configured {
		return &pb.ConfigureResponse{Ok: false, Error: "already configured"}, nil
	}

	// Build server config from proto
	w.cfg = server.Config{
		DataDir:     req.DataDir,
		Extensions:  req.Extensions,
		IdleTimeout: time.Duration(req.IdleTimeoutNs),
		TLSCertFile: req.TlsCertFile,
		TLSKeyFile:  req.TlsKeyFile,
		Users:       req.Users,
	}

	if req.Ducklake != nil {
		w.cfg.DuckLake = server.DuckLakeConfig{
			MetadataStore: req.Ducklake.MetadataStore,
			ObjectStore:   req.Ducklake.ObjectStore,
			DataPath:      req.Ducklake.DataPath,
			S3Provider:    req.Ducklake.S3Provider,
			S3Endpoint:    req.Ducklake.S3Endpoint,
			S3AccessKey:   req.Ducklake.S3AccessKey,
			S3SecretKey:   req.Ducklake.S3SecretKey,
			S3Region:      req.Ducklake.S3Region,
			S3UseSSL:      req.Ducklake.S3UseSsl,
			S3URLStyle:    req.Ducklake.S3UrlStyle,
			S3Chain:       req.Ducklake.S3Chain,
			S3Profile:     req.Ducklake.S3Profile,
		}
	}

	// Load TLS
	if w.cfg.TLSCertFile != "" && w.cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(w.cfg.TLSCertFile, w.cfg.TLSKeyFile)
		if err != nil {
			return &pb.ConfigureResponse{Ok: false, Error: fmt.Sprintf("load TLS: %v", err)}, nil
		}
		w.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	// Create DB pool with the worker's start time as serverStartTime.
	// In control plane mode, workers are spawned at startup so w.startTime ≈
	// control plane start time. Both uptime() and process_uptime() will show
	// the same value (worker lifetime) since this is a single long-lived process.
	// The two functions diverge only in process isolation mode.
	w.dbPool = NewDBPool(w.cfg, w.startTime)

	w.configured = true
	slog.Info("Worker configured.", "data_dir", w.cfg.DataDir, "ducklake", w.cfg.DuckLake.MetadataStore != "")
	return &pb.ConfigureResponse{Ok: true}, nil
}

func (w *Worker) AcceptConnection(_ context.Context, req *pb.AcceptConnectionRequest) (*pb.AcceptConnectionResponse, error) {
	w.mu.RLock()
	if !w.configured {
		w.mu.RUnlock()
		return &pb.AcceptConnectionResponse{Ok: false, Error: "not configured"}, nil
	}
	if w.draining {
		w.mu.RUnlock()
		return &pb.AcceptConnectionResponse{Ok: false, Error: "draining"}, nil
	}
	w.mu.RUnlock()

	// Get the pending FD
	w.mu.Lock()
	fd := w.pendingFD
	w.pendingFD = -1
	w.mu.Unlock()

	if fd < 0 {
		return &pb.AcceptConnectionResponse{Ok: false, Error: "no pending FD"}, nil
	}

	// Create a net.Conn from the FD
	file := os.NewFile(uintptr(fd), "tcp-conn")
	if file == nil {
		return &pb.AcceptConnectionResponse{Ok: false, Error: "invalid FD"}, nil
	}
	fc, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		return &pb.AcceptConnectionResponse{Ok: false, Error: fmt.Sprintf("FileConn: %v", err)}, nil
	}

	tcpConn, ok := fc.(*net.TCPConn)
	if !ok {
		_ = fc.Close()
		return &pb.AcceptConnectionResponse{Ok: false, Error: "not TCP"}, nil
	}

	// Use a unique session counter within this worker
	sessionPid := w.nextSessionPID()

	w.sessionsWg.Add(1)
	go w.handleSession(tcpConn, req.RemoteAddr, sessionPid, req.BackendSecretKey)

	return &pb.AcceptConnectionResponse{Ok: true, BackendPid: sessionPid}, nil
}

var sessionCounter atomic.Int32

func (w *Worker) nextSessionPID() int32 {
	// Use worker PID * 1000 + counter to create unique pseudo-PIDs
	base := int32(os.Getpid())
	counter := sessionCounter.Add(1)
	return base*1000 + counter%1000
}

func (w *Worker) handleSession(tcpConn *net.TCPConn, remoteAddr string, pid, secretKey int32) {
	defer w.sessionsWg.Done()

	slog.Info("Session starting.", "pid", pid, "remote_addr", remoteAddr)

	// TLS handshake
	tlsConn := tls.Server(tcpConn, w.tlsConfig)
	if err := tlsConn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		slog.Error("Failed to set TLS deadline.", "error", err)
		_ = tcpConn.Close()
		return
	}
	if err := tlsConn.Handshake(); err != nil {
		slog.Error("TLS handshake failed.", "error", err, "remote_addr", remoteAddr)
		_ = tcpConn.Close()
		return
	}
	if err := tlsConn.SetDeadline(time.Time{}); err != nil {
		slog.Error("Failed to clear TLS deadline.", "error", err)
		_ = tlsConn.Close()
		return
	}

	reader := bufio.NewReader(tlsConn)
	writer := bufio.NewWriter(tlsConn)

	// Read startup message
	params, err := server.ReadStartupMessage(reader)
	if err != nil {
		slog.Error("Failed to read startup message.", "error", err, "remote_addr", remoteAddr)
		_ = tlsConn.Close()
		return
	}

	username := params["user"]
	database := params["database"]

	if username == "" {
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "no user specified")
		_ = writer.Flush()
		_ = tlsConn.Close()
		return
	}

	// Authenticate
	expectedPassword, ok := w.cfg.Users[username]
	if !ok {
		_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
		_ = writer.Flush()
		_ = tlsConn.Close()
		return
	}

	if err := server.WriteAuthCleartextPassword(writer); err != nil {
		slog.Error("Failed to request password.", "error", err)
		_ = tlsConn.Close()
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush.", "error", err)
		_ = tlsConn.Close()
		return
	}

	msgType, body, err := server.ReadMessage(reader)
	if err != nil {
		slog.Error("Failed to read password.", "error", err)
		_ = tlsConn.Close()
		return
	}
	if msgType != 'p' {
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "expected password message")
		_ = writer.Flush()
		_ = tlsConn.Close()
		return
	}

	password := string(bytes.TrimRight(body, "\x00"))
	if password != expectedPassword {
		_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
		_ = writer.Flush()
		_ = tlsConn.Close()
		return
	}

	if err := server.WriteAuthOK(writer); err != nil {
		slog.Error("Failed to send auth OK.", "error", err)
		_ = tlsConn.Close()
		return
	}

	slog.Info("Session authenticated.", "user", username, "pid", pid, "remote_addr", remoteAddr)

	// Create per-session DuckDB connection
	db, err := w.dbPool.CreateSession(pid, username)
	if err != nil {
		slog.Error("Failed to create session DB.", "error", err)
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", fmt.Sprintf("failed to open database: %v", err))
		_ = writer.Flush()
		_ = tlsConn.Close()
		return
	}
	defer w.dbPool.CloseSession(pid)

	// Register session for cancel tracking
	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	defer sessionCancel()

	w.sessionsMu.Lock()
	w.sessions[pid] = &workerSession{
		pid:        pid,
		secretKey:  secretKey,
		cancel:     sessionCancel,
		remoteAddr: remoteAddr,
	}
	w.sessionsMu.Unlock()

	defer func() {
		w.sessionsMu.Lock()
		delete(w.sessions, pid)
		w.sessionsMu.Unlock()
	}()

	// Create a minimal Server for the clientConn
	queryCancelCh := make(chan struct{})
	minServer := &server.Server{}
	server.InitMinimalServer(minServer, w.cfg, queryCancelCh)

	// Store minServer in session so CancelQuery can cancel individual queries
	// rather than tearing down the entire session.
	w.sessionsMu.Lock()
	if s, ok := w.sessions[pid]; ok {
		s.minServer = minServer
	}
	w.sessionsMu.Unlock()

	cc := server.NewClientConn(minServer, tlsConn, reader, writer, username, database, db, pid, secretKey)

	// Send initial params and ready for query
	server.SendInitialParams(cc)
	if err := server.WriteReadyForQuery(writer, 'I'); err != nil {
		slog.Error("Failed to send ready for query.", "error", err)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush.", "error", err)
		return
	}

	// Run message loop
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.RunMessageLoop(cc)
	}()

	select {
	case err := <-errChan:
		if err != nil {
			slog.Error("Session error.", "error", err, "pid", pid)
		} else {
			slog.Info("Session disconnected cleanly.", "user", username, "pid", pid)
		}
	case <-sessionCtx.Done():
		slog.Info("Session cancelled.", "user", username, "pid", pid)
		_ = tlsConn.Close()
	}
}

func (w *Worker) CancelQuery(_ context.Context, req *pb.CancelQueryRequest) (*pb.CancelQueryResponse, error) {
	w.sessionsMu.RLock()
	defer w.sessionsMu.RUnlock()

	for _, s := range w.sessions {
		if s.pid == req.BackendPid && s.secretKey == req.SecretKey {
			// Cancel the active query via the per-session server's activeQueries map,
			// matching standalone mode behavior. This cancels only the in-flight query
			// while keeping the connection alive (instead of tearing down the session).
			key := server.BackendKey{Pid: req.BackendPid, SecretKey: req.SecretKey}
			if s.minServer != nil && s.minServer.CancelQuery(key) {
				slog.Info("Query cancelled via gRPC.", "pid", req.BackendPid)
				return &pb.CancelQueryResponse{Cancelled: true}, nil
			}
			// No active query registered yet (session still initializing)
			return &pb.CancelQueryResponse{Cancelled: false}, nil
		}
	}
	return &pb.CancelQueryResponse{Cancelled: false}, nil
}

func (w *Worker) Drain(_ context.Context, req *pb.DrainRequest) (*pb.DrainResponse, error) {
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()

	timeout := time.Duration(req.TimeoutNs)
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	done := make(chan struct{})
	go func() {
		w.sessionsWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return &pb.DrainResponse{Ok: true, RemainingConnections: 0}, nil
	case <-time.After(timeout):
		w.sessionsMu.RLock()
		remaining := int32(len(w.sessions))
		w.sessionsMu.RUnlock()
		return &pb.DrainResponse{Ok: false, RemainingConnections: remaining}, nil
	}
}

func (w *Worker) Health(_ context.Context, _ *pb.HealthRequest) (*pb.HealthResponse, error) {
	w.mu.RLock()
	draining := w.draining
	w.mu.RUnlock()

	w.sessionsMu.RLock()
	active := int32(len(w.sessions))
	w.sessionsMu.RUnlock()

	return &pb.HealthResponse{
		Healthy:           w.configured && !draining,
		ActiveConnections: active,
		UptimeNs:          int64(time.Since(w.startTime)),
		TotalQueries:      w.totalQueries.Load(),
		Draining:          draining,
	}, nil
}

func (w *Worker) Shutdown(_ context.Context, req *pb.ShutdownRequest) (*pb.ShutdownResponse, error) {
	timeout := time.Duration(req.TimeoutNs)
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()

	go func() {
		done := make(chan struct{})
		go func() {
			w.sessionsWg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(timeout):
			slog.Warn("Shutdown timeout, exiting.")
		}

		os.Exit(0)
	}()

	return &pb.ShutdownResponse{Ok: true}, nil
}
