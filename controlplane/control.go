package controlplane

import (
	"context"
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

	WorkerCount         int
	SocketDir           string
	HandoverSocket      string
	HealthCheckInterval time.Duration
	MaxConnsPerWorker   int
	FlightPort          int
}

// ControlPlane manages the TCP listener and worker pool.
type ControlPlane struct {
	cfg         ControlPlaneConfig
	pool        *WorkerPool
	rateLimiter *server.RateLimiter
	pgListener  net.Listener
	flightLn    net.Listener
	activeConns int64
	closed      bool
	closeMu     sync.Mutex
	wg          sync.WaitGroup
	reloading   atomic.Bool // guards against concurrent selfExec from double SIGUSR1
}

// RunControlPlane is the entry point for the control plane process.
func RunControlPlane(cfg ControlPlaneConfig) {
	// Apply defaults
	if cfg.WorkerCount == 0 {
		cfg.WorkerCount = 4
	}
	if cfg.SocketDir == "" {
		cfg.SocketDir = "/var/run/duckgres"
	}
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 5 * time.Second
	}
	if cfg.FlightPort == 0 {
		cfg.FlightPort = 8815
	}
	if cfg.FlightPort < 1 || cfg.FlightPort > 65535 {
		slog.Error("Invalid flight port.", "flight_port", cfg.FlightPort)
		os.Exit(1)
	}
	if cfg.FlightPort == cfg.Port {
		slog.Error("Flight listener cannot share port with PG listener on the same host.",
			"host", cfg.Host, "port", cfg.Port, "flight_port", cfg.FlightPort)
		os.Exit(1)
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

	// Use default rate limit config if not specified
	if cfg.RateLimit.MaxFailedAttempts == 0 {
		cfg.RateLimit = server.DefaultRateLimitConfig()
	}

	cp := &ControlPlane{
		cfg:         cfg,
		pool:        NewWorkerPool(cfg.SocketDir, cfg.Config),
		rateLimiter: server.NewRateLimiter(cfg.RateLimit),
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2)

	// Try handover from existing control plane if handover socket exists
	handoverDone := false
	if cfg.HandoverSocket == "" {
		slog.Info("No handover socket configured, starting fresh.")
	} else {
		if _, err := os.Stat(cfg.HandoverSocket); err != nil {
			slog.Info("Handover socket not found, starting fresh.", "socket", cfg.HandoverSocket, "error", err)
		} else {
			slog.Info("Existing handover socket found, attempting handover.", "socket", cfg.HandoverSocket)
			pgLn, flightLn, existingWorkers, err := receiveHandover(cfg.HandoverSocket)
			if err != nil {
				slog.Warn("Handover failed, starting fresh.", "error", err)
			} else {
				cp.pgListener = pgLn
				cp.flightLn = flightLn
				handoverDone = true

				// Connect to existing workers instead of spawning new ones
				for _, w := range existingWorkers {
					if err := cp.pool.ConnectExistingWorker(w.ID, w.GRPCSocket, w.FDSocket); err != nil {
						slog.Error("Failed to connect to handed-over worker.", "id", w.ID, "error", err)
					}
				}
				slog.Info("Handover complete, took over listener and workers.",
					"workers", len(existingWorkers))
			}
		}
	}

	if !handoverDone {
		// Bind TCP listeners FIRST, before spawning workers. If the port is
		// already in use (e.g. old CP still running after a failed handover),
		// we exit immediately without touching worker sockets.
		addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			slog.Error("Failed to listen.", "addr", addr, "error", err)
			os.Exit(1)
		}
		cp.pgListener = ln

		flightAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.FlightPort)
		flightLn, err := net.Listen("tcp", flightAddr)
		if err != nil {
			_ = cp.pgListener.Close()
			slog.Error("Failed to listen for Flight.", "addr", flightAddr, "error", err)
			os.Exit(1)
		}
		cp.flightLn = flightLn

		// Spawn workers only after listeners are bound.
		for i := 0; i < cfg.WorkerCount; i++ {
			if err := cp.pool.SpawnWorker(i); err != nil {
				slog.Error("Failed to spawn worker.", "id", i, "error", err)
				os.Exit(1)
			}
		}
	}

	// Start health check loop
	go cp.pool.HealthCheckLoop(makeShutdownCtx(sigChan), cfg.HealthCheckInterval, cfg.WorkerCount)

	// Start handover listener for future deployments
	cp.startHandoverListener()

	// Notify systemd we're ready.
	if handoverDone {
		// New process after handover: claim PID and signal ready.
		if err := sdNotify(fmt.Sprintf("MAINPID=%d\nREADY=1", os.Getpid())); err != nil {
			slog.Warn("sd_notify MAINPID+READY failed.", "error", err)
		}
	} else {
		if err := sdNotify("READY=1"); err != nil {
			slog.Warn("sd_notify READY failed.", "error", err)
		}
	}

	slog.Info("Control plane listening.",
		"pg_addr", cp.pgListener.Addr().String(),
		"flight_addr", cp.flightLn.Addr().String(),
		"workers", cfg.WorkerCount)

	// After handover, replace adopted workers with freshly spawned ones.
	if handoverDone {
		go func() {
			slog.Info("Post-handover: starting rolling update to replace adopted workers.")
			if err := cp.pool.RollingUpdate(makeShutdownCtx(sigChan)); err != nil {
				slog.Error("Post-handover rolling update failed.", "error", err)
			}
		}()
	}

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
			case syscall.SIGUSR2:
				slog.Info("Received SIGUSR2, starting rolling update.")
				go func() {
					if err := cp.pool.RollingUpdate(makeShutdownCtx(sigChan)); err != nil {
						slog.Error("Rolling update failed.", "error", err)
					}
				}()
			case syscall.SIGTERM, syscall.SIGINT:
				slog.Info("Received shutdown signal.", "signal", sig)
				cp.shutdown()
				os.Exit(0)
			}
		}
	}()

	// Accept loops
	go cp.acceptFlightLoop()
	cp.acceptLoop()
}

func makeShutdownCtx(_ <-chan os.Signal) context.Context {
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
		cp.pool.CancelQuery(params.cancelPid, params.cancelSecretKey)
		_ = conn.Close()
		return
	}

	// Handle SSL request
	if params.sslRequest {
		// Send 'S' to indicate SSL support
		if _, err := conn.Write([]byte("S")); err != nil {
			slog.Error("Failed to send SSL response.", "remote_addr", remoteAddr, "error", err)
			_ = conn.Close()
			return
		}

		// Get TCP file descriptor to pass to worker
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			slog.Error("Not a TCP connection.", "remote_addr", remoteAddr)
			_ = conn.Close()
			return
		}

		file, err := tcpConn.File()
		if err != nil {
			slog.Error("Failed to get FD.", "remote_addr", remoteAddr, "error", err)
			_ = conn.Close()
			return
		}

		// Close the original connection (file has a dup'd FD)
		_ = conn.Close()

		// Generate a secret key for this connection
		secretKey := server.GenerateSecretKey()

		atomic.AddInt64(&cp.activeConns, 1)
		defer atomic.AddInt64(&cp.activeConns, -1)

		// Route to a worker
		backendPid, err := cp.pool.RouteConnection(file, remoteAddr.String(), secretKey)
		_ = file.Close()
		if err != nil {
			slog.Error("Failed to route connection.", "remote_addr", remoteAddr, "error", err)
			return
		}

		slog.Debug("Connection routed.", "remote_addr", remoteAddr, "backend_pid", backendPid)
	} else {
		// No SSL - reject
		slog.Warn("Connection rejected: SSL required.", "remote_addr", remoteAddr)
		_ = conn.Close()
	}
}

func (cp *ControlPlane) acceptFlightLoop() {
	for {
		conn, err := cp.flightLn.Accept()
		if err != nil {
			cp.closeMu.Lock()
			closed := cp.closed
			cp.closeMu.Unlock()
			if closed {
				return
			}
			slog.Error("Flight accept error.", "error", err)
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetKeepAlive(true)
			_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}

		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.handleFlightConnection(conn)
		}()
	}
}

func (cp *ControlPlane) handleFlightConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()

	if msg := cp.rateLimiter.CheckConnection(remoteAddr); msg != "" {
		slog.Warn("Flight connection rejected.", "remote_addr", remoteAddr, "reason", msg)
		_ = conn.Close()
		return
	}
	if !cp.rateLimiter.RegisterConnection(remoteAddr) {
		slog.Warn("Flight connection rejected: rate limit.", "remote_addr", remoteAddr)
		_ = conn.Close()
		return
	}
	defer cp.rateLimiter.UnregisterConnection(remoteAddr)

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		slog.Error("Flight connection is not TCP.", "remote_addr", remoteAddr)
		_ = conn.Close()
		return
	}

	file, err := tcpConn.File()
	if err != nil {
		slog.Error("Failed to get Flight connection FD.", "remote_addr", remoteAddr, "error", err)
		_ = conn.Close()
		return
	}

	_ = conn.Close()
	defer func() { _ = file.Close() }()

	atomic.AddInt64(&cp.activeConns, 1)
	defer atomic.AddInt64(&cp.activeConns, -1)

	backendPid, err := cp.pool.RouteFlightConnection(file, remoteAddr.String())
	if err != nil {
		slog.Error("Failed to route Flight connection.", "remote_addr", remoteAddr, "error", err)
		return
	}
	slog.Debug("Flight connection routed.", "remote_addr", remoteAddr, "backend_pid", backendPid)
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
	if cp.flightLn != nil {
		_ = cp.flightLn.Close()
	}

	slog.Info("Draining workers...")
	_ = cp.pool.DrainAll(30 * time.Second)

	slog.Info("Shutting down workers...")
	cp.pool.ShutdownAll(30 * time.Second)

	// Wait for in-flight accept loop goroutines
	cp.wg.Wait()

	slog.Info("Control plane shutdown complete.")
}

// selfExec spawns a new control plane process from the binary on disk.
// The new process detects the existing handover socket and initiates the
// handover protocol to receive listener FDs and worker info.
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
	// Old CP exits via os.Exit(0) in handleHandoverRequest() after handover completes.
	// If new process crashes before connecting, handleHandoverRequest times out
	// and calls recoverFromFailedReload.
}

// recoverFromFailedReload cancels the RELOADING state, resets the guard flag,
// and restarts the handover listener so a future SIGUSR1 can retry.
func (cp *ControlPlane) recoverFromFailedReload() {
	if err := sdNotify("READY=1"); err != nil {
		slog.Warn("sd_notify READY (recovery) failed.", "error", err)
	}
	cp.reloading.Store(false)
}
