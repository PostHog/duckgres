package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrMaxWorkersReached = errors.New("max workers reached")

// ManagedWorker represents a duckdb-service worker process.
type ManagedWorker struct {
	ID          int
	cmd         *exec.Cmd
	socketPath  string
	bearerToken string
	client      *flightsql.Client
	done        chan struct{} // closed when process exits
	exitErr     error
}

// SessionCounter provides session counts per worker for load balancing.
type SessionCounter interface {
	SessionCountForWorker(workerID int) int
}

// FlightWorkerPool manages a pool of duckdb-service worker processes.
//
// Lock ordering invariant: pool.mu → SessionManager.mu(RLock).
// findIdleWorkerLocked calls SessionCountForWorker while holding pool.mu.
// Never acquire pool.mu while holding SessionManager.mu to avoid deadlock.
type FlightWorkerPool struct {
	mu             sync.RWMutex
	workers        map[int]*ManagedWorker
	nextWorkerID   int // auto-incrementing worker ID
	socketDir      string
	configPath     string
	binaryPath     string
	sessionCounter SessionCounter // set after SessionManager is created
	maxWorkers     int            // 0 = unlimited
	shuttingDown   bool
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

// SetSessionCounter sets the session counter for load balancing.
// Must be called before accepting connections.
func (p *FlightWorkerPool) SetSessionCounter(sc SessionCounter) {
	p.sessionCounter = sc
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
// This is used at startup for the elastic 1:1 model.
func (p *FlightWorkerPool) SpawnMinWorkers(count int) error {
	return p.SpawnAll(count)
}

// AcquireWorker returns a worker for a new session. It first tries to claim an
// idle pre-warmed worker (one with no active sessions). If none are available,
// it spawns a new one. The max-workers check is performed atomically under the
// write lock to prevent TOCTOU races from concurrent connections.
func (p *FlightWorkerPool) AcquireWorker() (*ManagedWorker, error) {
	p.mu.Lock()
	if p.shuttingDown {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is shutting down")
	}

	// Check max-workers cap atomically under the write lock
	if p.maxWorkers > 0 && len(p.workers) >= p.maxWorkers {
		// Even at the cap, we may have idle pre-warmed workers to reuse.
		// Only fail if all existing workers are busy.
		idle := p.findIdleWorkerLocked()
		if idle != nil {
			p.mu.Unlock()
			return idle, nil
		}
		p.mu.Unlock()
		return nil, fmt.Errorf("%w (%d)", ErrMaxWorkersReached, p.maxWorkers)
	}

	// Try to claim an idle pre-warmed worker before spawning a new one
	idle := p.findIdleWorkerLocked()
	if idle != nil {
		p.mu.Unlock()
		return idle, nil
	}

	id := p.nextWorkerID
	p.nextWorkerID++
	p.mu.Unlock()

	if err := p.SpawnWorker(id); err != nil {
		return nil, err
	}

	w, ok := p.Worker(id)
	if !ok {
		return nil, fmt.Errorf("worker %d not found after spawn", id)
	}
	return w, nil
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
		if p.sessionCounter != nil && p.sessionCounter.SessionCountForWorker(w.ID) == 0 {
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
	workerCount := len(p.workers)
	p.mu.Unlock()
	observeControlPlaneWorkers(workerCount)

	// Run the actual process cleanup asynchronously so DestroySession
	// doesn't block the connection handler goroutine for up to 3s+.
	go retireWorkerProcess(w)
}

// RetireWorkerIfNoSessions retires a worker only if it has no active sessions.
// Used to clean up on session creation failure without retiring pre-warmed workers.
func (p *FlightWorkerPool) RetireWorkerIfNoSessions(id int) {
	if p.sessionCounter != nil && p.sessionCounter.SessionCountForWorker(id) > 0 {
		return
	}
	p.RetireWorker(id)
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
					default:
						// Worker is alive, do a health check
						hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
						err := doHealthCheck(hctx, w.client)
						cancel()

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

// CreateSession creates a new session on the given worker.
func (w *ManagedWorker) CreateSession(ctx context.Context, username string) (string, error) {
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
func (w *ManagedWorker) DestroySession(ctx context.Context, sessionToken string) error {
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
