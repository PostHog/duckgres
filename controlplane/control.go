package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/posthog/duckgres/server"
)

// ControlPlaneConfig extends server.Config with control-plane-specific settings.
type ControlPlaneConfig struct {
	server.Config

	SocketDir           string
	ConfigPath          string // Path to config file, passed to workers
	HandoverSocket      string
	HealthCheckInterval time.Duration
}

// ControlPlane manages the TCP listener and routes connections to Flight SQL workers.
// The control plane owns client connections end-to-end: TLS, authentication,
// PostgreSQL wire protocol, and SQL transpilation all happen here. Workers are
// thin DuckDB execution engines reachable via Arrow Flight SQL over Unix sockets.
type ControlPlane struct {
	cfg         ControlPlaneConfig
	pool        *FlightWorkerPool
	sessions    *SessionManager
	srv         *server.Server // Minimal server for cancel request routing
	rateLimiter *server.RateLimiter
	tlsConfig   *tls.Config
	pgListener  net.Listener
	activeConns int64
	closed      bool
	closeMu     sync.Mutex
	wg          sync.WaitGroup
	reloading   atomic.Bool // guards against concurrent selfExec from double SIGUSR1
}

// RunControlPlane is the entry point for the control plane process.
func RunControlPlane(cfg ControlPlaneConfig) {
	// Apply defaults
	if cfg.SocketDir == "" {
		cfg.SocketDir = "/var/run/duckgres"
	}
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 5 * time.Second
	}

	// Enforce secure defaults for control-plane mode.
	if err := validateControlPlaneSecurity(cfg); err != nil {
		slog.Error("Invalid control-plane security configuration.", "error", err)
		os.Exit(1)
	}

	// Create socket directory
	if err := os.MkdirAll(cfg.SocketDir, 0755); err != nil {
		slog.Error("Failed to create socket directory.", "error", err)
		os.Exit(1)
	}

	// Load TLS certificates
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		slog.Error("Failed to load TLS certificates.", "error", err)
		os.Exit(1)
	}

	// Use default rate limit config if not specified
	if cfg.RateLimit.MaxFailedAttempts == 0 {
		cfg.RateLimit = server.DefaultRateLimitConfig()
	}

	pool := NewFlightWorkerPool(cfg.SocketDir, cfg.ConfigPath, cfg.MaxWorkers)

	// Create a minimal server for cancel request routing
	srv := &server.Server{}
	server.InitMinimalServer(srv, cfg.Config, nil)

	// Initialize memory rebalancer
	memBudget := server.ParseMemoryBytes(cfg.MemoryBudget)
	rebalancer := NewMemoryRebalancer(memBudget, 0, nil) // sessions set below

	sessions := NewSessionManager(pool, rebalancer)
	rebalancer.sessions = sessions // wire up the circular dependency
	pool.SetSessionCounter(sessions)

	cp := &ControlPlane{
		cfg:         cfg,
		pool:        pool,
		sessions:    sessions,
		srv:         srv,
		rateLimiter: server.NewRateLimiter(cfg.RateLimit),
		tlsConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)

	// Try handover from existing control plane if handover socket exists
	handoverDone := false
	if cfg.HandoverSocket == "" {
		slog.Info("No handover socket configured, starting fresh.")
	} else {
		if _, err := os.Stat(cfg.HandoverSocket); err != nil {
			slog.Info("Handover socket not found, starting fresh.", "socket", cfg.HandoverSocket, "error", err)
		} else {
			slog.Info("Existing handover socket found, attempting handover.", "socket", cfg.HandoverSocket)
			pgLn, err := receiveHandover(cfg.HandoverSocket)
			if err != nil {
				slog.Warn("Handover failed, starting fresh.", "error", err)
			} else {
				cp.pgListener = pgLn
				handoverDone = true
				slog.Info("Handover complete, took over PG listener.")
			}
		}
	}

	if !handoverDone {
		// Bind TCP listener FIRST, before spawning workers. If the port is
		// already in use (e.g. old CP still running after a failed handover),
		// we exit immediately without touching worker sockets.
		addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Error("Failed to listen.", "addr", addr, "error", err)
			os.Exit(1)
		}
		cp.pgListener = ln
	}

	// Pre-warm workers if min_workers is set
	if cfg.MinWorkers > 0 {
		if err := cp.pool.SpawnMinWorkers(cfg.MinWorkers); err != nil {
			slog.Error("Failed to spawn min workers.", "error", err)
			os.Exit(1)
		}
	}

	// Start health check loop with crash notification
	onCrash := func(workerID int) {
		cp.sessions.OnWorkerCrash(workerID, func(pid int32) {
			// Sessions on the crashed worker will see gRPC errors on
			// their next query. OnWorkerCrash cleans up session state
			// so load balancing stays accurate.
			slog.Warn("Session orphaned by worker crash.", "pid", pid, "worker", workerID)
		})
	}
	go cp.pool.HealthCheckLoop(makeShutdownCtx(), cfg.HealthCheckInterval, onCrash)

	// Start handover listener for future deployments
	cp.startHandoverListener()

	// Notify systemd we're ready (fresh start only).
	// After handover, sd_notify(MAINPID+READY) is sent synchronously in
	// receiveHandover() before handover_complete, so the old CP can't exit
	// before systemd knows our PID.
	if !handoverDone {
		if err := sdNotify("READY=1"); err != nil {
			slog.Warn("sd_notify READY failed.", "error", err)
		}
	}

	slog.Info("Control plane listening.",
		"pg_addr", cp.pgListener.Addr().String(),
		"min_workers", cfg.MinWorkers,
		"max_workers", cfg.MaxWorkers,
		"memory_budget", formatBytes(rebalancer.memoryBudget))

	// Handle signals
	go func() {
		for sig := range sigChan {
			switch sig {
			case syscall.SIGUSR1:
				if !cp.reloading.CompareAndSwap(false, true) {
					slog.Warn("SIGUSR1 ignored, handover already in progress.")
					break
				}
				slog.Info("Received SIGUSR1, starting graceful handover via self-exec.")
				if err := sdNotify("RELOADING=1"); err != nil {
					slog.Warn("sd_notify RELOADING failed.", "error", err)
				}
				go cp.selfExec()
			case syscall.SIGTERM, syscall.SIGINT:
				slog.Info("Received shutdown signal.", "signal", sig)
				cp.shutdown()
				os.Exit(0)
			}
		}
	}()

	// Accept loop (blocks)
	cp.acceptLoop()
}

func makeShutdownCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()
	return ctx
}

func (cp *ControlPlane) acceptLoop() {
	for {
		conn, err := cp.pgListener.Accept()
		if err != nil {
			cp.closeMu.Lock()
			closed := cp.closed
			cp.closeMu.Unlock()
			if closed {
				return
			}
			slog.Error("Accept error.", "error", err)
			continue
		}

		// Enable TCP keepalive
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}

		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.handleConnection(conn)
		}()
	}
}

func (cp *ControlPlane) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()

	// Rate limiting
	if msg := cp.rateLimiter.CheckConnection(remoteAddr); msg != "" {
		slog.Warn("Connection rejected.", "remote_addr", remoteAddr, "reason", msg)
		_ = conn.Close()
		return
	}

	if !cp.rateLimiter.RegisterConnection(remoteAddr) {
		slog.Warn("Connection rejected: rate limit.", "remote_addr", remoteAddr)
		_ = conn.Close()
		return
	}
	defer cp.rateLimiter.UnregisterConnection(remoteAddr)

	// Read startup message to determine SSL vs cancel
	params, err := readStartupFromRaw(conn)
	if err != nil {
		slog.Error("Failed to read startup.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}

	// Handle cancel request
	if params.cancelRequest {
		key := server.BackendKey{Pid: params.cancelPid, SecretKey: params.cancelSecretKey}
		cp.srv.CancelQuery(key)
		_ = conn.Close()
		return
	}

	// Require SSL
	if !params.sslRequest {
		slog.Warn("Connection rejected: SSL required.", "remote_addr", remoteAddr)
		_ = conn.Close()
		return
	}

	// Send 'S' to indicate SSL support
	if _, err := conn.Write([]byte("S")); err != nil {
		slog.Error("Failed to send SSL response.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}

	atomic.AddInt64(&cp.activeConns, 1)
	defer atomic.AddInt64(&cp.activeConns, -1)

	// TLS handshake
	tlsConn := tls.Server(conn, cp.tlsConfig)
	if err := tlsConn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		slog.Error("Failed to set TLS deadline.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}
	if err := tlsConn.Handshake(); err != nil {
		slog.Error("TLS handshake failed.", "remote_addr", remoteAddr, "error", err)
		_ = tlsConn.Close()
		return
	}
	defer func() { _ = tlsConn.Close() }()

	if err := tlsConn.SetDeadline(time.Time{}); err != nil {
		slog.Error("Failed to clear TLS deadline.", "remote_addr", remoteAddr, "error", err)
		return
	}

	reader := bufio.NewReader(tlsConn)
	writer := bufio.NewWriter(tlsConn)

	// Read startup message (user/database)
	startupParams, err := server.ReadStartupMessage(reader)
	if err != nil {
		slog.Error("Failed to read startup message.", "remote_addr", remoteAddr, "error", err)
		return
	}

	username := startupParams["user"]
	database := startupParams["database"]

	if username == "" {
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "no user specified")
		_ = writer.Flush()
		return
	}

	// Look up expected password for this user
	expectedPassword, ok := cp.cfg.Users[username]
	if !ok {
		slog.Warn("Unknown user.", "user", username, "remote_addr", remoteAddr)
		_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
		_ = writer.Flush()
		return
	}

	// Request password
	if err := server.WriteAuthCleartextPassword(writer); err != nil {
		slog.Error("Failed to request password.", "remote_addr", remoteAddr, "error", err)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush writer.", "remote_addr", remoteAddr, "error", err)
		return
	}

	// Read password response
	msgType, body, err := server.ReadMessage(reader)
	if err != nil {
		slog.Error("Failed to read password message.", "remote_addr", remoteAddr, "error", err)
		return
	}

	if msgType != 'p' {
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "expected password message")
		_ = writer.Flush()
		return
	}

	password := string(bytes.TrimRight(body, "\x00"))
	if password != expectedPassword {
		slog.Warn("Authentication failed.", "user", username, "remote_addr", remoteAddr)
		cp.rateLimiter.RecordFailedAuth(remoteAddr)
		_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
		_ = writer.Flush()
		return
	}

	// Send auth OK
	if err := server.WriteAuthOK(writer); err != nil {
		slog.Error("Failed to send auth OK.", "remote_addr", remoteAddr, "error", err)
		return
	}

	cp.rateLimiter.RecordSuccessfulAuth(remoteAddr)
	slog.Info("User authenticated.", "user", username, "remote_addr", remoteAddr)

	// Create session on a worker
	ctx := context.Background()
	pid, executor, err := cp.sessions.CreateSession(ctx, username)
	if err != nil {
		slog.Error("Failed to create session.", "user", username, "remote_addr", remoteAddr, "error", err)
		_ = server.WriteErrorResponse(writer, "FATAL", "53300", "too many connections")
		_ = writer.Flush()
		return
	}
	defer cp.sessions.DestroySession(pid)

	secretKey := server.GenerateSecretKey()

	// Create clientConn with FlightExecutor
	cc := server.NewClientConn(cp.srv, tlsConn, reader, writer, username, database, executor, pid, secretKey)

	// Send initial parameters and ReadyForQuery
	server.SendInitialParams(cc)
	if err := server.WriteReadyForQuery(writer, 'I'); err != nil {
		slog.Error("Failed to send ReadyForQuery.", "remote_addr", remoteAddr, "error", err)
		return
	}
	if err := writer.Flush(); err != nil {
		slog.Error("Failed to flush writer.", "remote_addr", remoteAddr, "error", err)
		return
	}

	// Run message loop
	if err := server.RunMessageLoop(cc); err != nil {
		slog.Error("Message loop error.", "user", username, "remote_addr", remoteAddr, "error", err)
		return
	}

	slog.Info("Client disconnected.", "user", username, "remote_addr", remoteAddr)
}

// startupResult holds the parsed initial startup message.
type startupResult struct {
	sslRequest      bool
	cancelRequest   bool
	cancelPid       int32
	cancelSecretKey int32
}

// readStartupFromRaw reads the startup message from a raw (unbuffered) connection.
func readStartupFromRaw(conn net.Conn) (startupResult, error) {
	// Read length (4 bytes)
	var length int32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return startupResult{}, fmt.Errorf("read length: %w", err)
	}

	if length < 8 || length > 10000 {
		return startupResult{}, fmt.Errorf("invalid startup message length: %d", length)
	}

	remaining := make([]byte, length-4)
	if _, err := fullRead(conn, remaining); err != nil {
		return startupResult{}, fmt.Errorf("read body: %w", err)
	}

	protocolVersion := binary.BigEndian.Uint32(remaining[:4])

	// SSL request
	if protocolVersion == 80877103 {
		return startupResult{sslRequest: true}, nil
	}

	// Cancel request
	if protocolVersion == 80877102 && len(remaining) >= 12 {
		pid := int32(binary.BigEndian.Uint32(remaining[4:8]))
		key := int32(binary.BigEndian.Uint32(remaining[8:12]))
		return startupResult{cancelRequest: true, cancelPid: pid, cancelSecretKey: key}, nil
	}

	return startupResult{}, fmt.Errorf("unexpected protocol version: %d", protocolVersion)
}

func fullRead(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (cp *ControlPlane) shutdown() {
	cp.closeMu.Lock()
	cp.closed = true
	cp.closeMu.Unlock()

	if cp.pgListener != nil {
		_ = cp.pgListener.Close()
	}

	// Wait for in-flight connections to finish
	slog.Info("Waiting for connections to drain...")
	cp.wg.Wait()

	slog.Info("Shutting down workers...")
	cp.pool.ShutdownAll()

	slog.Info("Control plane shutdown complete.")
}

// selfExec spawns a new control plane process from the binary on disk.
// The new process detects the existing handover socket and initiates the
// handover protocol to receive listener FDs.
func (cp *ControlPlane) selfExec() {
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	if err := cmd.Start(); err != nil {
		slog.Error("Self-exec failed.", "error", err)
		cp.recoverFromFailedReload()
		return
	}

	slog.Info("New control plane spawned.", "pid", cmd.Process.Pid)

	// Reap the child in the background to avoid a zombie. The old CP normally
	// exits via os.Exit(0) in handleHandoverRequest before the child finishes,
	// but if handover fails the child may exit first and needs to be reaped.
	go func() {
		if err := cmd.Wait(); err != nil {
			slog.Warn("Self-exec'd process exited with error.", "error", err)
		}
	}()
}

// recoverFromFailedReload cancels the RELOADING state, resets the guard flag,
// and restarts the handover listener so a future SIGUSR1 can retry.
func (cp *ControlPlane) recoverFromFailedReload() {
	if err := sdNotify("READY=1"); err != nil {
		slog.Warn("sd_notify READY (recovery) failed.", "error", err)
	}
	cp.reloading.Store(false)
}
