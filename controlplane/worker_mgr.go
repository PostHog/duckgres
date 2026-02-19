package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ManagedWorker represents a duckdb-service worker process.
type ManagedWorker struct {
	ID             int
	cmd            *exec.Cmd
	socketPath     string
	bearerToken    string
	client         *flightsql.Client
	parentListener net.Listener     // CP-side listener; lifecycle managed by releaseSocket
	prebound       *preboundSocket  // non-nil if using a pre-bound socket slot
	done           chan struct{}    // closed when process exits
	exitErr        error
	activeSessions int       // Number of sessions currently assigned to this worker
	lastUsed       time.Time // Last time a session was destroyed on this worker
}

// preboundSocket is a Unix socket pre-bound at startup while the socket
// directory is verified writable. This avoids EROFS errors that can occur
// later under systemd's ProtectSystem=strict when the RuntimeDirectory
// bind mount goes read-only.
type preboundSocket struct {
	socketPath string
	listener   net.Listener
}

type FlightWorkerPool struct {
	mu           sync.RWMutex
	workers      map[int]*ManagedWorker
	nextWorkerID int // auto-incrementing worker ID
	spawning     int // number of workers currently being spawned (not yet in workers map)
	socketDir    string
	configPath   string
	binaryPath   string
	maxWorkers   int           // 0 = unlimited; caps the number of live worker processes
	idleTimeout  time.Duration // how long to keep an idle worker alive
	shuttingDown bool
	shutdownCh   chan struct{} // closed by ShutdownAll to stop idle reaper

	preboundMu sync.Mutex
	prebound   []*preboundSocket // available pre-bound socket slots
}

// NewFlightWorkerPool creates a new worker pool.
func NewFlightWorkerPool(socketDir, configPath string, maxWorkers int) *FlightWorkerPool {
	binaryPath, _ := os.Executable()
	pool := &FlightWorkerPool{
		workers:    make(map[int]*ManagedWorker),
		socketDir:  socketDir,
		configPath: configPath,
		binaryPath: binaryPath,
		maxWorkers: maxWorkers,
		shutdownCh: make(chan struct{}),
	}
	observeControlPlaneWorkers(0)
	go pool.idleReaper()
	return pool
}

// PreBindSockets eagerly binds count Unix sockets at startup while the socket
// directory is verified writable. Under systemd's ProtectSystem=strict, the
// RuntimeDirectory bind mount can go read-only after the service finishes
// starting (e.g., after a handover or namespace event). Pre-binding ensures
// sockets are available regardless of later filesystem state.
func (p *FlightWorkerPool) PreBindSockets(count int) error {
	p.preboundMu.Lock()
	defer p.preboundMu.Unlock()

	for i := 0; i < count; i++ {
		socketPath := fmt.Sprintf("%s/worker-%d.sock", p.socketDir, i)
		_ = os.Remove(socketPath)
		ln, err := net.Listen("unix", socketPath)
		if err != nil {
			// Clean up already-bound sockets on failure.
			// SetUnlinkOnClose(false) means Close() won't remove the files,
			// so we must remove them explicitly.
			for _, ps := range p.prebound {
				_ = ps.listener.Close()
				_ = os.Remove(ps.socketPath)
			}
			p.prebound = nil
			return fmt.Errorf("pre-bind worker socket %s: %w", socketPath, err)
		}
		// Prevent Close() from removing the socket file. During handover,
		// the old CP's Close() would otherwise delete socket files that the
		// new CP has already replaced with its own pre-bound sockets.
		// Socket files are cleaned up by os.Remove in PreBindSockets at next startup.
		ln.(*net.UnixListener).SetUnlinkOnClose(false)
		if err := os.Chmod(socketPath, 0700); err != nil {
			slog.Warn("Failed to set pre-bound socket permissions.", "error", err)
		}
		p.prebound = append(p.prebound, &preboundSocket{socketPath: socketPath, listener: ln})
	}
	slog.Info("Pre-bound worker sockets.", "count", count)
	return nil
}

func (p *FlightWorkerPool) takePrebound() *preboundSocket {
	p.preboundMu.Lock()
	defer p.preboundMu.Unlock()
	if len(p.prebound) == 0 {
		return nil
	}
	n := len(p.prebound) - 1
	ps := p.prebound[n]
	p.prebound = p.prebound[:n]
	return ps
}

func (p *FlightWorkerPool) returnPrebound(ps *preboundSocket) {
	p.preboundMu.Lock()
	defer p.preboundMu.Unlock()
	p.prebound = append(p.prebound, ps)
}

// releaseWorkerSocket returns a pre-bound socket to the pool for reuse, or
// closes the listener and removes the socket file for non-pre-bound sockets.
func (p *FlightWorkerPool) releaseWorkerSocket(w *ManagedWorker) {
	if w.prebound != nil {
		p.returnPrebound(w.prebound)
		w.prebound = nil
	} else {
		if w.parentListener != nil {
			_ = w.parentListener.Close()
		}
		_ = os.Remove(w.socketPath)
	}
}

// closeAllPrebound permanently closes all remaining pre-bound sockets.
// Called during ShutdownAll. Does NOT remove socket files — during handover,
// the new CP may have already replaced them. Stale files are cleaned up by
// the next startup's PreBindSockets.
func (p *FlightWorkerPool) closeAllPrebound() {
	p.preboundMu.Lock()
	defer p.preboundMu.Unlock()
	for _, ps := range p.prebound {
		_ = ps.listener.Close()
	}
	p.prebound = nil
}

// SpawnWorker starts a new duckdb-service worker process.
// It uses a pre-bound socket from the pool if available, falling back to
// binding a new socket (which may fail with EROFS under systemd's
// ProtectSystem=strict after startup).
func (p *FlightWorkerPool) SpawnWorker(id int) error {
	token := generateToken()

	// Try to use a pre-bound socket first. These are bound eagerly at startup
	// while the socket directory is verified writable, avoiding EROFS errors
	// that can occur later under systemd ProtectSystem=strict.
	ps := p.takePrebound()

	var ln net.Listener
	var socketPath string
	if ps != nil {
		ln = ps.listener
		socketPath = ps.socketPath
	} else {
		// Fallback: bind a new socket. This can happen when workers are being
		// retired asynchronously and their pre-bound sockets haven't been
		// returned to the pool yet. May fail with EROFS if the dir went
		// read-only, but that's the best we can do.
		socketPath = fmt.Sprintf("%s/worker-dyn-%d.sock", p.socketDir, id)
		_ = os.Remove(socketPath)
		var err error
		ln, err = net.Listen("unix", socketPath)
		if err != nil {
			return fmt.Errorf("bind worker socket %s: %w", socketPath, err)
		}
		if err := os.Chmod(socketPath, 0700); err != nil {
			slog.Warn("Failed to set worker socket permissions.", "error", err)
		}
	}

	// Get a dup'd FD to pass to the child. ExtraFiles[0] becomes FD 3.
	file, err := ln.(*net.UnixListener).File()
	if err != nil {
		if ps != nil {
			p.returnPrebound(ps)
		} else {
			_ = ln.Close()
		}
		return fmt.Errorf("get listener fd for worker %d: %w", id, err)
	}

	args := []string{
		"--mode", "duckdb-service",
		"--duckdb-listen-fd", "3",
		"--duckdb-token", token,
	}
	if p.configPath != "" {
		args = append(args, "--config", p.configPath)
	}

	cmd := exec.Command(p.binaryPath, args...)
	cmd.ExtraFiles = []*os.File{file}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// Build env with DUCKGRES_MODE=duckdb-service, filtering any existing
	// value to avoid duplicates (glibc getenv returns the first match).
	cmd.Env = appendOrReplaceEnv(os.Environ(), "DUCKGRES_MODE=duckdb-service")

	if err := cmd.Start(); err != nil {
		_ = file.Close()
		if ps != nil {
			p.returnPrebound(ps)
		} else {
			_ = ln.Close()
		}
		return fmt.Errorf("spawn worker %d: %w", id, err)
	}

	// Close our copy of the dup'd FD; the child has its own.
	_ = file.Close()

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:             id,
		cmd:            cmd,
		socketPath:     socketPath,
		bearerToken:    token,
		parentListener: ln,
		prebound:       ps,
		done:           done,
	}

	// Wait for process exit in background
	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()

	// Wait for the worker's gRPC server to become healthy.
	// The socket file already exists (we created it above), so waitForWorker
	// will immediately try connecting and rely on the health check.
	client, err := waitForWorker(socketPath, token, 10*time.Second)
	if err != nil {
		// Kill the process if we can't connect
		_ = cmd.Process.Kill()
		<-done
		if ps != nil {
			p.returnPrebound(ps)
		} else {
			_ = ln.Close()
		}
		return fmt.Errorf("worker %d failed to start: %w", id, err)
	}
	w.client = client

	p.mu.Lock()
	p.workers[id] = w
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	slog.Info("Worker spawned.", "id", id, "pid", cmd.Process.Pid, "socket", socketPath)
	return nil
}

// waitForWorker polls for the worker socket and creates a Flight SQL client.
func waitForWorker(socketPath, bearerToken string, timeout time.Duration) (*flightsql.Client, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			// Socket exists, try to connect
			addr := "unix://" + socketPath
			var dialOpts []grpc.DialOption
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
			dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(server.MaxGRPCMessageSize),
				grpc.MaxCallSendMsgSize(server.MaxGRPCMessageSize),
			))

			if bearerToken != "" {
				dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&workerBearerCreds{token: bearerToken}))
			}

			client, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
			if err == nil {
				// Verify with a health check
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				err = doHealthCheck(ctx, client)
				cancel()
				if err == nil {
					return client, nil
				}
				_ = client.Close()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("timeout waiting for worker socket %s", socketPath)
}

// doHealthCheck performs a HealthCheck action on the worker.
// The server sends exactly one Result message with {"healthy": true, ...}.
func doHealthCheck(ctx context.Context, client *flightsql.Client) error {
	// Use the underlying flight client for custom actions.
	// flightsql.Client.Client is a flight.Client interface which embeds
	// FlightServiceClient, giving us access to DoAction directly.
	stream, err := client.Client.DoAction(ctx, &flight.Action{Type: "HealthCheck"})
	if err != nil {
		return fmt.Errorf("health check action: %w", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("health check recv: %w", err)
	}

	var body struct {
		Healthy bool `json:"healthy"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("health check unmarshal: %w", err)
	}
	if !body.Healthy {
		return fmt.Errorf("worker reported unhealthy")
	}
	return nil
}

// Worker returns a worker by ID.
func (p *FlightWorkerPool) Worker(id int) (*ManagedWorker, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	w, ok := p.workers[id]
	return w, ok
}

// SpawnAll spawns the specified number of workers in parallel.
func (p *FlightWorkerPool) SpawnAll(count int) error {
	var wg sync.WaitGroup
	errs := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := p.SpawnWorker(id); err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err // Return first error encountered
		}
	}

	// Update nextWorkerID past the pre-spawned range
	p.mu.Lock()
	if count > p.nextWorkerID {
		p.nextWorkerID = count
	}
	p.mu.Unlock()
	return nil
}

// SpawnMinWorkers pre-warms the pool with the given number of workers.
// This is used at startup for the elastic 1:1 model.
func (p *FlightWorkerPool) SpawnMinWorkers(count int) error {
	return p.SpawnAll(count)
}

// AcquireWorker returns a worker for a new session.
//
// Strategy:
//  1. Reuse an idle worker (0 active sessions) if available.
//  2. If the pool has fewer live workers than maxWorkers (or maxWorkers is 0),
//     spawn a new worker process.
//  3. If the pool is at capacity, assign to the least-loaded live worker.
//
// This ensures the number of worker processes never exceeds maxWorkers while
// allowing unlimited concurrent sessions across the fixed pool.
func (p *FlightWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is shutting down")
	}

	// Remove dead worker entries so they don't inflate the count.
	p.cleanDeadWorkersLocked()

	// 1. Try to claim an idle worker before spawning a new one.
	idle := p.findIdleWorkerLocked()
	if idle != nil {
		idle.activeSessions++
		p.mu.Unlock()
		return idle, nil
	}

	// 2. If below the process cap (or unlimited), spawn a new worker.
	liveCount := p.liveWorkerCountLocked()
	if p.maxWorkers == 0 || liveCount < p.maxWorkers {
		id := p.nextWorkerID
		p.nextWorkerID++
		p.spawning++
		p.mu.Unlock()

		err := p.SpawnWorker(id)

		p.mu.Lock()
		p.spawning--
		p.mu.Unlock()

		if err != nil {
			return nil, err
		}

		w, ok := p.Worker(id)
		if !ok {
			return nil, fmt.Errorf("worker %d not found after spawn", id)
		}

		p.mu.Lock()
		w.activeSessions++
		p.mu.Unlock()
		return w, nil
	}

	// 3. At capacity — assign to the least-loaded live worker.
	w := p.leastLoadedWorkerLocked()
	if w != nil {
		w.activeSessions++
		p.mu.Unlock()
		return w, nil
	}

	// All workers are dead (already cleaned above). Spawn a replacement.
	// Still respect maxWorkers — another goroutine may already be spawning.
	liveCount = p.liveWorkerCountLocked()
	if p.maxWorkers > 0 && liveCount >= p.maxWorkers {
		// A spawn is already in progress; wait for it to finish and use that worker.
		p.mu.Unlock()
		// Brief backoff then retry — the in-progress spawn will add a worker shortly.
		time.Sleep(100 * time.Millisecond)
		return p.AcquireWorker(ctx)
	}
	id := p.nextWorkerID
	p.nextWorkerID++
	p.spawning++
	p.mu.Unlock()

	err := p.SpawnWorker(id)

	p.mu.Lock()
	p.spawning--
	p.mu.Unlock()

	if err != nil {
		return nil, err
	}

	w, ok := p.Worker(id)
	if !ok {
		return nil, fmt.Errorf("worker %d not found after spawn", id)
	}

	p.mu.Lock()
	w.activeSessions++
	p.mu.Unlock()
	return w, nil
}

// ReleaseWorker decrements the active session count for a worker and updates its lastUsed time.
func (p *FlightWorkerPool) ReleaseWorker(id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.workers[id]
	if ok {
		if w.activeSessions > 0 {
			w.activeSessions--
		}
		w.lastUsed = time.Now()
	}
}

// idleReaper periodically retires workers that have been idle for longer than idleTimeout.
func (p *FlightWorkerPool) idleReaper() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			p.reapIdleWorkers()
		}
	}
}

func (p *FlightWorkerPool) reapIdleWorkers() {
	p.mu.Lock()
	var toRetire []*ManagedWorker
	now := time.Now()
	for id, w := range p.workers {
		if w.activeSessions == 0 && !w.lastUsed.IsZero() && now.Sub(w.lastUsed) > p.idleTimeout {
			toRetire = append(toRetire, w)
			delete(p.workers, id)
		}
	}
	workerCount := len(p.workers)
	p.mu.Unlock()

	if len(toRetire) > 0 {
		slog.Info("Reaping idle workers.", "count", len(toRetire))
		observeControlPlaneWorkers(workerCount)
		for _, w := range toRetire {
			go p.retireWorkerProcess(w)
		}
	}
}

// findIdleWorkerLocked returns a live worker with no active sessions, or nil.
// Caller must hold p.mu (read or write lock).
func (p *FlightWorkerPool) findIdleWorkerLocked() *ManagedWorker {
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue // dead
		default:
		}
		if w.activeSessions == 0 {
			return w
		}
	}
	return nil
}

// leastLoadedWorkerLocked returns the live worker with the fewest active
// sessions, or nil if all workers are dead. Caller must hold p.mu.
func (p *FlightWorkerPool) leastLoadedWorkerLocked() *ManagedWorker {
	var best *ManagedWorker
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue // dead
		default:
		}
		if best == nil || w.activeSessions < best.activeSessions {
			best = w
		}
	}
	return best
}

// liveWorkerCountLocked returns the number of workers whose process is still
// running (done channel not closed) plus workers currently being spawned.
// Including spawning prevents a TOCTOU race where concurrent AcquireWorker
// calls all see the same stale count and exceed maxWorkers.
// Caller must hold p.mu.
func (p *FlightWorkerPool) liveWorkerCountLocked() int {
	count := p.spawning
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
			count++
		}
	}
	return count
}

// cleanDeadWorkersLocked removes all dead worker entries from the map and
// schedules resource cleanup (client, parent listener, socket file) in the
// background. Caller must hold p.mu for writing.
func (p *FlightWorkerPool) cleanDeadWorkersLocked() {
	for id, w := range p.workers {
		select {
		case <-w.done:
			delete(p.workers, id)
			go p.cleanupDeadWorker(w)
		default:
		}
	}
}

// cleanupDeadWorker releases resources for a worker whose process has already
// exited. Called from cleanDeadWorkersLocked when a dead worker is discovered
// before the HealthCheckLoop gets to it.
func (p *FlightWorkerPool) cleanupDeadWorker(w *ManagedWorker) {
	if w.client != nil {
		_ = w.client.Close()
	}
	p.releaseWorkerSocket(w)
}

// RetireWorker stops a worker process and cleans up its resources.
// Sends SIGINT, waits up to 3s, then SIGKILL. Runs asynchronously
// to avoid blocking the calling goroutine (e.g., connection handler).
func (p *FlightWorkerPool) RetireWorker(id int) {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.workers, id)
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	// Run the actual process cleanup asynchronously so DestroySession
	// doesn't block the connection handler goroutine for up to 3s+.
	go p.retireWorkerProcess(w)
}

// RetireWorkerIfNoSessions retires a worker only if it has no active sessions
// after releasing our claim. Used to clean up on session creation failure
// without retiring shared workers that have other active sessions.
func (p *FlightWorkerPool) RetireWorkerIfNoSessions(id int) bool {
	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return false
	}

	// Decrement the acquisition claim we just made.
	if w.activeSessions > 0 {
		w.activeSessions--
	}

	// If it has NO other active sessions, kill it to be safe (it might be broken).
	if w.activeSessions == 0 {
		delete(p.workers, id)
		workerCount := len(p.workers)
		p.mu.Unlock()
		observeControlPlaneWorkers(workerCount)
		go p.retireWorkerProcess(w)
		return true
	}
	p.mu.Unlock()
	return false
}

// retireWorkerProcess handles the actual process shutdown and socket cleanup.
func (p *FlightWorkerPool) retireWorkerProcess(w *ManagedWorker) {
	// Check if the process already exited before we try to retire it.
	// This happens when a worker crashes and the client disconnect triggers
	// RetireWorker before the health check loop detects the crash.
	alreadyDead := false
	select {
	case <-w.done:
		alreadyDead = true
	default:
	}

	if alreadyDead {
		exitCode := -1
		if w.cmd.ProcessState != nil {
			exitCode = w.cmd.ProcessState.ExitCode()
		}
		slog.Warn("Retiring worker that already exited unexpectedly.", "id", w.ID, "exit_code", exitCode, "error", w.exitErr)
	} else {
		slog.Info("Retiring worker.", "id", w.ID)

		// Send SIGINT first so the worker can drain in-flight requests
		if w.cmd.Process != nil {
			_ = w.cmd.Process.Signal(os.Interrupt)
		}

		// Wait up to 3s for graceful exit. The worker just had its session
		// destroyed and should exit almost immediately.
		select {
		case <-w.done:
		case <-time.After(3 * time.Second):
			slog.Warn("Worker did not exit in time, killing.", "id", w.ID)
			if w.cmd.Process != nil {
				_ = w.cmd.Process.Kill()
			}
			<-w.done
		}
	}

	// Close gRPC client after the process has exited
	if w.client != nil {
		_ = w.client.Close()
	}

	// Return pre-bound socket to pool for reuse, or close non-pre-bound listener.
	p.releaseWorkerSocket(w)
}

// ShutdownAll stops all workers gracefully.
func (p *FlightWorkerPool) ShutdownAll() {
	p.mu.Lock()
	p.shuttingDown = true
	workers := make([]*ManagedWorker, 0, len(p.workers))
	for _, w := range p.workers {
		workers = append(workers, w)
	}
	p.mu.Unlock()

	close(p.shutdownCh)

	for _, w := range workers {
		if w.cmd.Process != nil {
			slog.Info("Shutting down worker.", "id", w.ID, "pid", w.cmd.Process.Pid)
			_ = w.cmd.Process.Signal(os.Interrupt)
		}
	}

	// Wait up to 10s for workers to exit
	for _, w := range workers {
		select {
		case <-w.done:
		case <-time.After(10 * time.Second):
			slog.Warn("Worker did not exit in time, killing.", "id", w.ID)
			if w.cmd.Process != nil {
				_ = w.cmd.Process.Kill()
			}
			<-w.done
		}
		if w.client != nil {
			_ = w.client.Close()
		}
		// Return pre-bound sockets to the pool (consistent with retire/crash paths).
		// Non-pre-bound sockets are closed and removed directly.
		p.releaseWorkerSocket(w)
	}

	p.mu.Lock()
	p.workers = make(map[int]*ManagedWorker)
	p.mu.Unlock()
	observeControlPlaneWorkers(0)

	// Close all pre-bound sockets: both those returned above and any that
	// were never assigned to workers. Socket files are not removed — during
	// handover the new CP may have already replaced them via PreBindSockets.
	// Stale files are cleaned up by the next startup's PreBindSockets.
	p.closeAllPrebound()
}

// WorkerCrashHandler is called when a worker crash is detected, before respawning.
type WorkerCrashHandler func(workerID int)

// maxConsecutiveHealthFailures is the number of consecutive health check failures
// before a worker is force-killed. With a typical 2s health check interval,
// this means ~6s of unresponsiveness triggers retirement.
const maxConsecutiveHealthFailures = 3

// HealthCheckLoop periodically checks worker health and handles crashed workers.
// In the elastic 1:1 model, crashed workers with active sessions trigger crash
// notification (so sessions see errors), and the dead worker is cleaned up.
// Workers without sessions are simply retired.
// Workers that fail maxConsecutiveHealthFailures health checks in a row are
// force-killed and their sessions notified.
func (p *FlightWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash ...WorkerCrashHandler) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// consecutive failures per workerID.
	// mu protects access to the failures map across concurrent health check goroutines.
	var mu sync.Mutex
	failures := make(map[int]int)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.RLock()
			workers := make([]*ManagedWorker, 0, len(p.workers))
			for _, w := range p.workers {
				workers = append(workers, w)
			}
			p.mu.RUnlock()

			var wg sync.WaitGroup
			for _, w := range workers {
				wg.Add(1)
				go func(w *ManagedWorker) {
					defer wg.Done()

					select {
					case <-ctx.Done():
						return
					case <-w.done:
						mu.Lock()
						delete(failures, w.ID)
						mu.Unlock()

						// Check if already cleaned up by RetireWorker (intentional shutdown).
						// If so, skip — this is not a crash.
						p.mu.Lock()
						_, stillInPool := p.workers[w.ID]
						if stillInPool {
							delete(p.workers, w.ID)
						}
						workerCount := len(p.workers)
						p.mu.Unlock()
						observeControlPlaneWorkers(workerCount)
						if !stillInPool {
							return
						}
						// Worker crashed — notify sessions, clean up
						slog.Warn("Worker crashed.", "id", w.ID, "error", w.exitErr)
						for _, h := range onCrash {
							h(w.ID)
						}
						if w.client != nil {
							_ = w.client.Close()
						}
						p.releaseWorkerSocket(w)
					default:
						// Worker is alive, do a health check.
						// Recover nil-pointer panics: w.client.Close() (from a
						// concurrent crash/retire) nils out FlightServiceClient,
						// racing with the DoAction call inside doHealthCheck.
						var healthErr error
						func() {
							defer recoverWorkerPanic(&healthErr)
							hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
							healthErr = doHealthCheck(hctx, w.client)
							cancel()
						}()
						err := healthErr

						if err != nil {
							mu.Lock()
							failures[w.ID]++
							count := failures[w.ID]
							mu.Unlock()

							slog.Warn("Worker health check failed.", "id", w.ID, "error", err, "consecutive_failures", count)

							if count >= maxConsecutiveHealthFailures {
								slog.Error("Worker unresponsive, force-killing.", "id", w.ID, "consecutive_failures", count)
								mu.Lock()
								delete(failures, w.ID)
								mu.Unlock()

								p.mu.Lock()
								_, stillInPool := p.workers[w.ID]
								if stillInPool {
									delete(p.workers, w.ID)
								}
								workerCount := len(p.workers)
								p.mu.Unlock()
								observeControlPlaneWorkers(workerCount)

								if stillInPool {
									for _, h := range onCrash {
										h(w.ID)
									}
									// Skip SIGINT (unlike retireWorkerProcess) since the worker
									// has already proven unresponsive. Go straight to SIGKILL.
									if w.cmd.Process != nil {
										_ = w.cmd.Process.Kill()
									}
									<-w.done
									slog.Warn("Force-killed worker exited.", "id", w.ID, "error", w.exitErr)
									if w.client != nil {
										_ = w.client.Close()
									}
									p.releaseWorkerSocket(w)
								}
							}
						} else {
							mu.Lock()
							delete(failures, w.ID)
							mu.Unlock()
						}
					}
				}(w)
			}
			wg.Wait()
		}
	}
}

// recoverWorkerPanic converts a nil-pointer panic from a closed Flight SQL
// client into an error. Same race as FlightExecutor: arrow-go Close() nils out
// FlightServiceClient, and concurrent DoAction calls on the shared client panic.
func recoverWorkerPanic(err *error) {
	if r := recover(); r != nil {
		if re, ok := r.(runtime.Error); ok && strings.Contains(re.Error(), "nil pointer") {
			*err = fmt.Errorf("worker client panic (worker likely crashed): %v", r)
			return
		}
		panic(r)
	}
}

// CreateSession creates a new session on the given worker.
func (w *ManagedWorker) CreateSession(ctx context.Context, username string) (token string, err error) {
	defer recoverWorkerPanic(&err)

	body, _ := json.Marshal(map[string]string{"username": username})

	stream, err := w.client.Client.DoAction(ctx, &flight.Action{
		Type: "CreateSession",
		Body: body,
	})
	if err != nil {
		return "", fmt.Errorf("create session: %w", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		return "", fmt.Errorf("create session recv: %w", err)
	}

	var resp struct {
		SessionToken string `json:"session_token"`
	}
	if err := json.Unmarshal(msg.Body, &resp); err != nil {
		return "", fmt.Errorf("create session unmarshal: %w", err)
	}

	return resp.SessionToken, nil
}

// DestroySession destroys a session on the worker.
func (w *ManagedWorker) DestroySession(ctx context.Context, sessionToken string) (err error) {
	defer recoverWorkerPanic(&err)

	body, _ := json.Marshal(map[string]string{"session_token": sessionToken})

	stream, err := w.client.Client.DoAction(ctx, &flight.Action{
		Type: "DestroySession",
		Body: body,
	})
	if err != nil {
		return fmt.Errorf("destroy session: %w", err)
	}

	// Drain the stream
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	return nil
}

// workerBearerCreds implements grpc.PerRPCCredentials for worker auth.
type workerBearerCreds struct {
	token string
}

func (c *workerBearerCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.token,
	}, nil
}

func (c *workerBearerCreds) RequireTransportSecurity() bool {
	return false
}

// appendOrReplaceEnv appends entry (in KEY=VALUE form) to env, removing any
// existing entry with the same key. This prevents duplicate environment
// variables which can confuse glibc's getenv (returns first match).
func appendOrReplaceEnv(env []string, entry string) []string {
	idx := strings.Index(entry, "=")
	if idx < 0 {
		return append(env, entry)
	}
	key := entry[:idx+1] // e.g. "DUCKGRES_MODE="
	filtered := make([]string, 0, len(env)+1)
	for _, e := range env {
		if !strings.HasPrefix(e, key) {
			filtered = append(filtered, e)
		}
	}
	return append(filtered, entry)
}

func generateToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("failed to generate token: " + err.Error())
	}
	return hex.EncodeToString(b)
}
