package controlplane

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/posthog/duckgres/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ControlPlaneConfig extends server.Config with control-plane-specific settings.
type ControlPlaneConfig struct {
	server.Config

	SocketDir           string
	ConfigPath          string // Path to config file, passed to workers
	HandoverSocket      string
	HealthCheckInterval time.Duration
	WorkerQueueTimeout   time.Duration // How long to wait for an available worker slot (default: 5m)
	WorkerIdleTimeout    time.Duration // How long to keep an idle worker alive (default: 5m)
	HandoverDrainTimeout time.Duration // How long to wait for connections to drain during handover (default: 24h)
	MetricsServer        *http.Server  // Optional metrics server to shut down during handover
}

// ControlPlane manages the TCP listener and routes connections to Flight SQL workers.
// The control plane owns client connections end-to-end: TLS, authentication,
// PostgreSQL wire protocol, and SQL transpilation all happen here. Workers are
// thin DuckDB execution engines reachable via Arrow Flight SQL over Unix sockets.
type ControlPlane struct {
	cfg         ControlPlaneConfig
	pool        *FlightWorkerPool
	sessions    *SessionManager
	flight      *FlightIngress
	rebalancer  *MemoryRebalancer
	srv         *server.Server // Minimal server for cancel request routing
	rateLimiter *server.RateLimiter
	tlsConfig   *tls.Config
	pgListener  net.Listener
	activeConns int64
	closed      bool
	closeMu     sync.Mutex
	wg          sync.WaitGroup
	reloading        atomic.Bool    // guards against concurrent selfExec from double SIGUSR1
	handoverDraining atomic.Bool    // true after handover succeeded; SIGTERM should exit immediately
	handoverLn  net.Listener        // current handover listener; protected by closeMu
	acmeManager *server.ACMEManager // ACME manager for Let's Encrypt (nil when using static certs)
}

// RunControlPlane is the entry point for the control plane process.
func RunControlPlane(cfg ControlPlaneConfig) {
	// Apply defaults
	if cfg.SocketDir == "" {
		cfg.SocketDir = "/var/run/duckgres"
	}
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 2 * time.Second
	}
	if cfg.WorkerQueueTimeout == 0 {
		cfg.WorkerQueueTimeout = 5 * time.Minute
	}
	if cfg.WorkerIdleTimeout == 0 {
		cfg.WorkerIdleTimeout = 5 * time.Minute
	}
	if cfg.HandoverDrainTimeout == 0 {
		cfg.HandoverDrainTimeout = 24 * time.Hour
	}

	// Enforce secure defaults for control-plane mode.
	if err := validateControlPlaneSecurity(cfg); err != nil {
		slog.Error("Invalid control-plane security configuration.", "error", err)
		os.Exit(1)
	}

	// Create socket directory. Writability is checked below, after
	// attempting handover — the old CP may provide pre-bound socket FDs,
	// making a writable directory unnecessary under EROFS.
	if err := os.MkdirAll(cfg.SocketDir, 0755); err != nil {
		slog.Error("Failed to create socket directory.", "error", err)
		os.Exit(1)
	}

	// Configure TLS: ACME (Let's Encrypt) or static certificate files
	var tlsCfg *tls.Config
	var acmeMgr *server.ACMEManager
	if cfg.ACMEDomain != "" {
		mgr, err := server.NewACMEManager(cfg.ACMEDomain, cfg.ACMEEmail, cfg.ACMECacheDir, ":80")
		if err != nil {
			slog.Error("Failed to start ACME manager.", "error", err)
			os.Exit(1)
		}
		acmeMgr = mgr
		tlsCfg = mgr.TLSConfig()
	} else {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			slog.Error("Failed to load TLS certificates.", "error", err)
			os.Exit(1)
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	// --- Attempt handover from existing control plane ---
	// This must happen BEFORE the writability check because under systemd's
	// ProtectSystem=strict, the RuntimeDirectory can become read-only (EROFS)
	// after startup. The old CP sends pre-bound socket FDs during handover,
	// allowing the new CP to work without creating new sockets.
	var handoverPgLn *net.TCPListener
	var handoverPrebound []*preboundSocket
	handoverDone := false
	if cfg.HandoverSocket == "" {
		slog.Info("No handover socket configured, starting fresh.")
	} else {
		if _, err := os.Stat(cfg.HandoverSocket); err != nil {
			slog.Info("Handover socket not found, starting fresh.", "socket", cfg.HandoverSocket, "error", err)
		} else {
			slog.Info("Existing handover socket found, attempting handover.", "socket", cfg.HandoverSocket)
			pgLn, prebound, err := receiveHandover(cfg.HandoverSocket, cfg.SocketDir)
			if err != nil {
				slog.Warn("Handover failed, starting fresh.", "error", err)
			} else {
				handoverPgLn = pgLn
				handoverPrebound = prebound
				handoverDone = true
				slog.Info("Handover complete, took over PG listener.", "prebound_count", len(prebound))
			}
		}
	}

	// Verify socket directory is writable. Skip when handover provided
	// pre-bound sockets — the directory may be read-only (EROFS under
	// systemd ProtectSystem=strict), which is fine since we use inherited FDs.
	if len(handoverPrebound) == 0 {
		if err := checkSocketDirWritable(cfg.SocketDir); err != nil {
			slog.Error("Socket directory is not writable. Control plane will not be able to create worker sockets.", "dir", cfg.SocketDir, "error", err)
			os.Exit(1)
		}
	}

	// Use default rate limit config if not specified
	if cfg.RateLimit.MaxFailedAttempts == 0 {
		cfg.RateLimit = server.DefaultRateLimitConfig()
	}

	// Initialize memory rebalancer. Every session gets the full memory budget
	// (75% of system RAM by default). DuckDB spills to disk/swap if needed.
	memBudget := server.ParseMemoryBytes(cfg.MemoryBudget)
	maxWorkers := cfg.MaxWorkers

	// Use a temporary rebalancer to auto-detect the budget and derive
	// a default max_workers if not explicitly set.
	tempRebalancer := NewMemoryRebalancer(memBudget, 0, nil, false)
	memBudget = tempRebalancer.memoryBudget // capture auto-detected value

	// If max_workers is not set, derive a reasonable concurrency cap from
	// the memory budget and CPU count.
	if maxWorkers == 0 {
		maxWorkers = tempRebalancer.DefaultMaxWorkers()
		slog.Info("Derived max_workers from memory budget and CPU count.",
			"max_workers", maxWorkers,
			"memory_budget", formatBytes(memBudget),
			"num_cpu", runtime.NumCPU())
	}

	rebalancer := NewMemoryRebalancer(memBudget, 0, nil, cfg.MemoryRebalance)

	// Validate min_workers <= max_workers
	minWorkers := cfg.MinWorkers
	if minWorkers > maxWorkers {
		slog.Warn("min_workers exceeds max_workers; capping to max_workers.",
			"min_workers", minWorkers, "max_workers", maxWorkers)
		minWorkers = maxWorkers
	}

	pool := NewFlightWorkerPool(cfg.SocketDir, cfg.ConfigPath, maxWorkers)
	pool.idleTimeout = cfg.WorkerIdleTimeout

	// Import pre-bound sockets from handover, or pre-bind new ones.
	// During handover, the old CP passes its pre-bound socket FDs so the
	// new CP works even when the socket directory is read-only (EROFS).
	if len(handoverPrebound) > 0 {
		pool.ImportPrebound(handoverPrebound)
	} else if maxWorkers > 0 {
		if err := pool.PreBindSockets(maxWorkers); err != nil {
			slog.Error("Failed to pre-bind worker sockets.", "error", err)
			os.Exit(1)
		}
	}

	// Create a minimal server for cancel request routing
	srv := &server.Server{}
	server.InitMinimalServer(srv, cfg.Config, nil)

	sessions := NewSessionManager(pool, rebalancer)

	// Wire the circular dependency: rebalancer needs sessions to iterate,
	// sessions needs rebalancer to trigger rebalance on create/destroy.
	// This is safe because Rebalance is only called from CreateSession/DestroySession,
	// and no sessions exist yet at this point.
	//
	// Lock ordering invariant: rebalancer.mu → sm.mu(RLock). Never acquire
	// rebalancer.mu while holding sm.mu to avoid deadlock.
	rebalancer.SetSessionLister(sessions)

	cp := &ControlPlane{
		cfg:         cfg,
		pool:        pool,
		sessions:    sessions,
		rebalancer:  rebalancer,
		srv:         srv,
		rateLimiter: server.NewRateLimiter(cfg.RateLimit),
		tlsConfig:   tlsCfg,
		acmeManager: acmeMgr,
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)

	// Set PG listener from handover or bind a fresh TCP listener.
	if handoverDone {
		cp.pgListener = handoverPgLn
	} else {
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
	if minWorkers > 0 {
		if err := cp.pool.SpawnMinWorkers(minWorkers); err != nil {
			slog.Error("Failed to spawn min workers.", "error", err)
			os.Exit(1)
		}
	}

	// Flight ingress is created AFTER handover so the old CP can keep
	// serving Flight SQL clients until it shuts down its listener in
	// handleHandoverRequest. This minimizes the Flight unavailability
	// window to just the brief port rebind, rather than the entire
	// handover + pre-warm duration.
	cp.startFlightIngress()

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
		"flight_addr", cp.flightAddr(),
		"min_workers", minWorkers,
		"max_workers", maxWorkers,
		"worker_queue_timeout", cfg.WorkerQueueTimeout,
		"memory_budget", formatBytes(rebalancer.memoryBudget),
		"memory_rebalance", cfg.MemoryRebalance)

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
				// Metrics server must be stopped before spawning the replacement
				// so it can bind to the same port.
				if cp.cfg.MetricsServer != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err := cp.cfg.MetricsServer.Shutdown(ctx); err != nil {
						slog.Warn("Metrics server shutdown failed.", "error", err)
					}
					cancel()
				}
				// ACME HTTP challenge listener (port 80) must be stopped before
				// spawning the replacement so the new CP can bind it.
				if cp.acmeManager != nil {
					if err := cp.acmeManager.Close(); err != nil {
						slog.Warn("ACME manager shutdown failed.", "error", err)
					}
				}
				// Flight ingress is NOT stopped here. The old CP keeps serving
				// Flight SQL clients during the handover. It is shut down in
				// handleHandoverRequest after the new CP confirms handover_complete,
				// minimizing the unavailability window.
				if err := sdNotify("RELOADING=1"); err != nil {
					slog.Warn("sd_notify RELOADING failed.", "error", err)
				}
				go cp.selfExec()
			case syscall.SIGTERM, syscall.SIGINT:
				if cp.handoverDraining.Load() {
					slog.Info("Received shutdown signal during handover drain, exiting immediately.", "signal", sig)
					cp.pool.ShutdownAll()
					os.Exit(0)
				}
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
				// Block here instead of returning. Returning would cause
				// RunControlPlane() → main() to exit, killing all in-flight
				// connection goroutines before the drain completes.
				// The handover handler or shutdown handler will call os.Exit(0)
				// after draining connections.
				select {}
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

	releaseRateLimit, msg := server.BeginRateLimitedAuthAttempt(cp.rateLimiter, remoteAddr)
	if msg != "" {
		slog.Warn("Connection rejected.", "remote_addr", remoteAddr, "reason", msg)
		_ = conn.Close()
		return
	}
	defer releaseRateLimit()

	// Read startup message to determine SSL vs cancel
	params, err := readStartupFromRaw(conn)
	if err != nil {
		if err == io.EOF || errors.Is(err, io.EOF) {
			slog.Debug("Client closed connection before sending startup message.", "remote_addr", remoteAddr)
		} else {
			slog.Error("Failed to read startup.", "remote_addr", remoteAddr, "error", err)
		}
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
		server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
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
	applicationName := startupParams["application_name"]

	if username == "" {
		server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "no user specified")
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
		server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "expected password message")
		_ = writer.Flush()
		return
	}

	password := string(bytes.TrimRight(body, "\x00"))
	if !server.ValidateUserPassword(cp.cfg.Users, username, password) {
		slog.Warn("Authentication failed.", "user", username, "remote_addr", remoteAddr)
		banned := server.RecordFailedAuthAttempt(cp.rateLimiter, remoteAddr)
		if banned {
			slog.Warn("IP banned after too many failed auth attempts.", "remote_addr", remoteAddr)
		}
		_ = server.WriteErrorResponse(writer, "FATAL", "28P01", "password authentication failed")
		_ = writer.Flush()
		return
	}

	// Send auth OK
	if err := server.WriteAuthOK(writer); err != nil {
		slog.Error("Failed to send auth OK.", "remote_addr", remoteAddr, "error", err)
		return
	}

	server.RecordSuccessfulAuthAttempt(cp.rateLimiter, remoteAddr)
	slog.Info("User authenticated.", "user", username, "remote_addr", remoteAddr)

	// Create session on a worker. The timeout controls how long we wait in the
	// worker queue when all slots are occupied.
	ctx, cancel := context.WithTimeout(context.Background(), cp.cfg.WorkerQueueTimeout)
	pid, executor, err := cp.sessions.CreateSession(ctx, username)
	cancel()
	if err != nil {
		slog.Error("Failed to create session.", "user", username, "remote_addr", remoteAddr, "error", err)
		_ = server.WriteErrorResponse(writer, "FATAL", "53300", "too many connections")
		_ = writer.Flush()
		return
	}
	defer cp.sessions.DestroySession(pid)

	// Register the TCP connection so OnWorkerCrash can close it to unblock
	// the message loop if the backing worker dies.
	cp.sessions.SetConnCloser(pid, tlsConn)

	secretKey := server.GenerateSecretKey()

	// Create clientConn with FlightExecutor
	workerID := cp.sessions.WorkerIDForPID(pid)
	cc := server.NewClientConn(cp.srv, tlsConn, reader, writer, username, database, applicationName, executor, pid, secretKey, workerID)

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
	if cp.flight != nil {
		cp.flight.Shutdown()
		cp.flight = nil
	}

	// Wait for in-flight connections to finish
	slog.Info("Waiting for connections to drain...")
	cp.wg.Wait()

	slog.Info("Shutting down workers...")
	cp.pool.ShutdownAll()

	if cp.rebalancer != nil {
		cp.rebalancer.Stop()
	}

	// Shut down ACME HTTP challenge listener if active
	if cp.acmeManager != nil {
		if err := cp.acmeManager.Close(); err != nil {
			slog.Warn("ACME manager shutdown error.", "error", err)
		}
	}

	slog.Info("Control plane shutdown complete.")
}

// selfExec spawns a new control plane process from the binary on disk.
// The new process detects the existing handover socket and initiates the
// handover protocol to receive listener FDs.
//
// Under systemd (Type=notify), the child must be reparented to PID 1 for
// systemd to properly track it via waitpid(). We achieve this via
// double-fork using setsid --fork: the intermediate exits immediately and
// the grandchild (new CP) is reparented to PID 1. Without this, systemd
// logs "Supervising process X which is not our child" and may not detect
// crashes for Restart=always.
//
// Outside systemd (tests, manual runs), we use direct spawn so the old CP
// can track the child's exit via cmd.Wait() for faster crash recovery.
func (cp *ControlPlane) selfExec() {
	if os.Getenv("NOTIFY_SOCKET") != "" {
		cp.selfExecDetached()
		return
	}

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	if err := cmd.Start(); err != nil {
		slog.Error("Self-exec failed.", "error", err)
		cp.recoverFromFailedReload()
		cp.recoverAfterFailedReload()
		return
	}

	slog.Info("New control plane spawned.", "pid", cmd.Process.Pid)

	// Reap the child in the background to avoid a zombie. If the child exits
	// before completing the handover (e.g. config error, crash, killed by
	// systemd reload timeout), we must recover so the old CP exits the
	// RELOADING state and the handover listener doesn't block forever.
	go func() {
		err := cmd.Wait()
		if err != nil {
			slog.Warn("Self-exec'd process exited with error.", "error", err)
		}

		// If we're still in reloading state, the handover never completed.
		// handleHandoverRequest may also be trying to recover (child connected
		// but died mid-protocol), so recoverFromFailedReload uses CAS to
		// ensure only one goroutine runs the full recovery path.
		//
		// cancelHandoverListener must be inside the CAS winner branch —
		// otherwise it could close a listener that handleHandoverRequest's
		// recovery already created.
		if cp.recoverFromFailedReload() {
			slog.Warn("Child process exited before completing handover, recovering.", "error", err)
			cp.cancelHandoverListener()
			cp.startHandoverListener()
			cp.recoverAfterFailedReload()
		}
	}()
}

// selfExecDetached spawns the new CP via setsid --fork so it is reparented
// to PID 1 (systemd). Because we cannot track the detached grandchild's
// exit directly, we use a timeout for crash recovery instead of cmd.Wait().
func (cp *ControlPlane) selfExecDetached() {
	args := append([]string{"--fork", os.Args[0]}, os.Args[1:]...)
	cmd := exec.Command("setsid", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	// cmd.Run() returns almost immediately: setsid --fork double-forks and
	// the intermediate exits right away. The grandchild (new CP) continues.
	if err := cmd.Run(); err != nil {
		slog.Error("Self-exec (detached) failed.", "error", err)
		cp.recoverFromFailedReload()
		cp.recoverAfterFailedReload()
		return
	}

	slog.Info("New control plane spawned (detached).")

	// Recovery timeout: if no handover connection is received within 30s,
	// the new CP likely crashed during startup. The handover protocol
	// already has a 30s per-connection deadline (handleHandoverRequest),
	// but that only fires after Accept — this timeout covers the case
	// where the new CP never connects at all.
	go func() {
		time.Sleep(30 * time.Second)
		if cp.recoverFromFailedReload() {
			slog.Warn("Handover timeout: new CP did not connect within 30s, recovering.")
			cp.cancelHandoverListener()
			cp.startHandoverListener()
			cp.recoverAfterFailedReload()
		}
	}()
}

// cancelHandoverListener closes the current handover listener, unblocking
// any goroutine stuck in Accept().
func (cp *ControlPlane) cancelHandoverListener() {
	cp.closeMu.Lock()
	defer cp.closeMu.Unlock()
	if cp.handoverLn != nil {
		_ = cp.handoverLn.Close()
		cp.handoverLn = nil
	}
}

// recoverFromFailedReload atomically exits the RELOADING state and notifies
// systemd that the service is ready again. Returns true if this call won the
// race (i.e. actually recovered). Callers that need to restart the handover
// listener should only do so when this returns true, to avoid duplicate
// listeners.
func (cp *ControlPlane) recoverFromFailedReload() bool {
	if !cp.reloading.CompareAndSwap(true, false) {
		return false // another goroutine already recovered
	}
	if err := sdNotify("READY=1"); err != nil {
		slog.Warn("sd_notify READY (recovery) failed.", "error", err)
	}
	return true
}

// startFlightIngress creates and starts the Flight SQL ingress listener.
// During handover, the old CP's Flight listener may still be shutting down,
// so we retry port binding briefly before giving up.
func (cp *ControlPlane) startFlightIngress() {
	if cp.cfg.FlightPort <= 0 {
		return
	}

	flightCfg := FlightIngressConfig{
		SessionIdleTTL:     cp.cfg.FlightSessionIdleTTL,
		SessionReapTick:    cp.cfg.FlightSessionReapInterval,
		HandleIdleTTL:      cp.cfg.FlightHandleIdleTTL,
		SessionTokenTTL:    cp.cfg.FlightSessionTokenTTL,
		WorkerQueueTimeout: cp.cfg.WorkerQueueTimeout,
	}

	var flightIngress *FlightIngress
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		flightIngress, err = NewFlightIngress(cp.cfg.Host, cp.cfg.FlightPort, cp.tlsConfig, cp.cfg.Users, cp.sessions, cp.rateLimiter, flightCfg)
		if err == nil {
			break
		}
		if attempt < 9 {
			slog.Warn("Flight ingress port not yet available, retrying...", "attempt", attempt+1, "error", err)
			time.Sleep(500 * time.Millisecond)
		}
	}
	if err != nil {
		slog.Error("Failed to initialize Flight ingress, continuing without Flight SQL.", "error", err)
		return
	}

	cp.flight = flightIngress
	cp.flight.Start()
}

// recoverAfterFailedReload restores all subsystems that were shut down in the
// SIGUSR1 handler when the new CP fails to complete the handover.
func (cp *ControlPlane) recoverAfterFailedReload() {
	cp.recoverMetricsAfterFailedReload()
	cp.recoverACMEAfterFailedReload()
	// Flight ingress is NOT shut down in the SIGUSR1 handler (it keeps
	// serving during handover), so no recovery is needed here.
}

func (cp *ControlPlane) recoverMetricsAfterFailedReload() {
	if cp.cfg.MetricsServer == nil {
		return
	}

	// The old http.Server is in a "closed" state after Shutdown() and cannot
	// be restarted. Create a fresh one on the same address.
	addr := cp.cfg.MetricsServer.Addr
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	newSrv := &http.Server{Addr: addr, Handler: mux}
	cp.cfg.MetricsServer = newSrv
	go func() {
		slog.Info("Recovered metrics server after reload failure.", "addr", addr)
		if err := newSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("Recovered metrics server error.", "error", err)
		}
	}()
}

func (cp *ControlPlane) recoverACMEAfterFailedReload() {
	if cp.cfg.ACMEDomain == "" {
		return
	}
	// ACME manager was shut down in SIGUSR1 handler. Restart it.
	mgr, err := server.NewACMEManager(cp.cfg.ACMEDomain, cp.cfg.ACMEEmail, cp.cfg.ACMECacheDir, ":80")
	if err != nil {
		slog.Error("Failed to recover ACME manager after reload failure.", "error", err)
		return
	}
	cp.acmeManager = mgr
	cp.tlsConfig = mgr.TLSConfig()
	slog.Info("Recovered ACME manager after reload failure.")
}

func (cp *ControlPlane) flightAddr() string {
	if cp.flight == nil {
		return "disabled"
	}
	return cp.flight.Addr()
}
