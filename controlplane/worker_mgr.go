package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
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
	done           chan struct{} // closed when process exits
	exitErr        error
	activeSessions int       // Number of sessions currently assigned to this worker
	idleSince      time.Time // When this worker became idle (activeSessions dropped to 0)
}

type FlightWorkerPool struct {
	mu             sync.RWMutex
	workers        map[int]*ManagedWorker
	nextWorkerID   int // auto-incrementing worker ID
	socketDir      string
	configPath     string
	binaryPath     string
	maxWorkers     int           // 0 = unlimited
	minIdleWorkers int           // Keep at least this many idle workers alive (from min_workers)
	workerIdleTTL  time.Duration // How long to keep idle workers alive for reuse
	shuttingDown   bool
	workerSem      chan struct{} // buffered to maxWorkers; limits concurrent acquisitions
	shutdownCh     chan struct{} // closed by ShutdownAll to unblock queued waiters
}

// NewFlightWorkerPool creates a new worker pool.
func NewFlightWorkerPool(socketDir, configPath string, maxWorkers int, workerIdleTTL time.Duration) *FlightWorkerPool {
	binaryPath, _ := os.Executable()
	pool := &FlightWorkerPool{
		workers:       make(map[int]*ManagedWorker),
		socketDir:     socketDir,
		configPath:    configPath,
		binaryPath:    binaryPath,
		maxWorkers:    maxWorkers,
		workerIdleTTL: workerIdleTTL,
		shutdownCh:    make(chan struct{}),
	}
	if maxWorkers > 0 {
		pool.workerSem = make(chan struct{}, maxWorkers)
	}
	observeControlPlaneWorkers(0)
	return pool
}

// SpawnWorker starts a new duckdb-service worker process.
func (p *FlightWorkerPool) SpawnWorker(id int) error {
	token := generateToken()
	socketPath := fmt.Sprintf("%s/worker-%d.sock", p.socketDir, id)

	// Clean up stale socket
	_ = os.Remove(socketPath)

	listenAddr := "unix://" + socketPath

	args := []string{
		"--mode", "duckdb-service",
		"--duckdb-listen", listenAddr,
		"--duckdb-token", token,
	}
	if p.configPath != "" {
		args = append(args, "--config", p.configPath)
	}

	cmd := exec.Command(p.binaryPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn worker %d: %w", id, err)
	}

	done := make(chan struct{})
	w := &ManagedWorker{
		ID:          id,
		cmd:         cmd,
		socketPath:  socketPath,
		bearerToken: token,
		done:        done,
	}

	// Wait for process exit in background
	go func() {
		w.exitErr = cmd.Wait()
		close(done)
	}()

	// Wait for the socket to appear and connect
	client, err := waitForWorker(socketPath, token, 10*time.Second)
	if err != nil {
		// Kill the process if we can't connect
		_ = cmd.Process.Kill()
		<-done
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

// SpawnAll spawns the specified number of workers.
func (p *FlightWorkerPool) SpawnAll(count int) error {
	for i := 0; i < count; i++ {
		if err := p.SpawnWorker(i); err != nil {
			return err
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
// This is used at startup for the elastic 1:1 model. The idle reaper
// will keep at least this many idle workers alive.
func (p *FlightWorkerPool) SpawnMinWorkers(count int) error {
	p.mu.Lock()
	p.minIdleWorkers = count
	p.mu.Unlock()
	return p.SpawnAll(count)
}

// AcquireWorker returns a worker for a new session. When maxWorkers is set,
// callers block in FIFO order on the semaphore until a slot is available,
// the context is cancelled, or the pool shuts down.
// Once a slot is acquired, it first tries to claim an idle pre-warmed worker
// (one with no active sessions). If none are available, it spawns a new one.
func (p *FlightWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	// Block until a semaphore slot is available (FIFO via Go's sudog queue).
	if p.workerSem != nil {
		select {
		case p.workerSem <- struct{}{}:
			// Got a slot
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for available worker (max_workers=%d): %w", p.maxWorkers, ctx.Err())
		case <-p.shutdownCh:
			return nil, fmt.Errorf("pool is shutting down")
		}
	}

	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		p.releaseWorkerSem()
		return nil, fmt.Errorf("pool is shutting down")
	}

	// Try to claim an idle worker (pre-warmed or released back to the pool)
	// before spawning a new one. Atomic claim: increment activeSessions
	// while holding the lock.
	idle := p.findIdleWorkerLocked()
	if idle != nil {
		idle.activeSessions++
		idle.idleSince = time.Time{} // clear idle marker
		p.mu.Unlock()
		slog.Debug("Reusing idle worker.", "id", idle.ID)
		return idle, nil
	}

	id := p.nextWorkerID
	p.nextWorkerID++
	p.mu.Unlock()

	if err := p.SpawnWorker(id); err != nil {
		p.releaseWorkerSem()
		return nil, err
	}

	w, ok := p.Worker(id)
	if !ok {
		p.releaseWorkerSem()
		return nil, fmt.Errorf("worker %d not found after spawn", id)
	}

	p.mu.Lock()
	w.activeSessions++
	p.mu.Unlock()

	return w, nil
}

// releaseWorkerSem drains one token from the semaphore (non-blocking).
func (p *FlightWorkerPool) releaseWorkerSem() {
	if p.workerSem != nil {
		select {
		case <-p.workerSem:
		default:
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
	sessions := w.activeSessions
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	// Release semaphore slots so queued waiters can proceed.
	for i := 0; i < sessions; i++ {
		p.releaseWorkerSem()
	}

	// Run the actual process cleanup asynchronously so DestroySession
	// doesn't block the connection handler goroutine for up to 3s+.
	go retireWorkerProcess(w)
}

// RetireWorkerIfNoSessions retires a worker only if it has no active sessions.
// Used to clean up on session creation failure without retiring pre-warmed workers.
// Returns true if the worker was retired (and its semaphore slot released).
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
		p.releaseWorkerSem()
	}

	// If it has NO other active sessions, kill it to be safe (it might be broken).
	if w.activeSessions == 0 {
		delete(p.workers, id)
		workerCount := len(p.workers)
		p.mu.Unlock()
		observeControlPlaneWorkers(workerCount)
		go retireWorkerProcess(w)
		return true
	}
	p.mu.Unlock()
	return false
}

// ReleaseWorker returns a worker to the idle pool instead of retiring it.
// The worker process stays alive so the next connection can reuse it without
// paying the startup cost (extension loading, DuckLake catalog attach, etc.).
// If workerIdleTTL is 0, falls back to RetireWorker for backwards compatibility.
func (p *FlightWorkerPool) ReleaseWorker(id int) {
	if p.workerIdleTTL <= 0 {
		p.RetireWorker(id)
		return
	}

	p.mu.Lock()
	w, ok := p.workers[id]
	if !ok {
		p.mu.Unlock()
		return
	}

	if w.activeSessions > 0 {
		w.activeSessions--
	}
	if w.activeSessions == 0 {
		w.idleSince = time.Now()
	}
	p.mu.Unlock()

	p.releaseWorkerSem()

	slog.Debug("Worker released to idle pool.", "id", id)
}

// retireIdleWorkers retires workers that have been idle longer than workerIdleTTL.
// Called from HealthCheckLoop. Uses a write lock and re-checks activeSessions
// atomically to avoid racing with AcquireWorker claiming the worker.
// Keeps at least minIdleWorkers idle workers alive (from min_workers config).
func (p *FlightWorkerPool) retireIdleWorkers() {
	if p.workerIdleTTL <= 0 {
		return
	}

	p.mu.Lock()

	// Count current idle workers to respect minIdleWorkers.
	idleCount := 0
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue
		default:
		}
		if w.activeSessions == 0 {
			idleCount++
		}
	}

	var toRetire []*ManagedWorker
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue // dead, handled by health check
		default:
		}
		if w.activeSessions == 0 && !w.idleSince.IsZero() && time.Since(w.idleSince) > p.workerIdleTTL {
			if idleCount <= p.minIdleWorkers {
				break // keep enough idle workers alive
			}
			toRetire = append(toRetire, w)
			delete(p.workers, w.ID)
			idleCount--
		}
	}
	workerCount := len(p.workers)
	p.mu.Unlock()

	if len(toRetire) > 0 {
		observeControlPlaneWorkers(workerCount)
	}

	for _, w := range toRetire {
		slog.Info("Retiring idle worker (TTL expired).", "id", w.ID)
		// No semaphore to release — idle workers already released their slot in ReleaseWorker.
		go retireWorkerProcess(w)
	}
}

// retireWorkerProcess handles the actual process shutdown and socket cleanup.
func retireWorkerProcess(w *ManagedWorker) {
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

	// Clean up socket
	_ = os.Remove(w.socketPath)
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

	// Unblock all goroutines waiting in AcquireWorker's semaphore select.
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
		}
		if w.client != nil {
			_ = w.client.Close()
		}
	}

	p.mu.Lock()
	p.workers = make(map[int]*ManagedWorker)
	p.mu.Unlock()
	observeControlPlaneWorkers(0)
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
						_ = os.Remove(w.socketPath)

						// Release as many semaphore slots as this worker was using.
						// In the 1:1 model, this is usually 1.
						for i := 0; i < w.activeSessions; i++ {
							p.releaseWorkerSem()
						}
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
									_ = os.Remove(w.socketPath)

									for i := 0; i < w.activeSessions; i++ {
										p.releaseWorkerSem()
									}
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

			// Retire workers that have been idle longer than the TTL
			p.retireIdleWorkers()
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

func generateToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("failed to generate token: " + err.Error())
	}
	return hex.EncodeToString(b)
}
