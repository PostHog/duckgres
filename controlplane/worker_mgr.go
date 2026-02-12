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
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ManagedWorker represents a duckdb-service worker process.
type ManagedWorker struct {
	ID         int
	cmd        *exec.Cmd
	socketPath string
	bearerToken string
	client     *flightsql.Client
	done       chan struct{} // closed when process exits
	exitErr    error
}

// FlightWorkerPool manages a pool of duckdb-service worker processes.
type FlightWorkerPool struct {
	mu        sync.RWMutex
	workers   map[int]*ManagedWorker
	socketDir string
	configPath string
	binaryPath string
}

// NewFlightWorkerPool creates a new worker pool.
func NewFlightWorkerPool(socketDir, configPath string) *FlightWorkerPool {
	binaryPath, _ := os.Executable()
	return &FlightWorkerPool{
		workers:    make(map[int]*ManagedWorker),
		socketDir:  socketDir,
		configPath: configPath,
		binaryPath: binaryPath,
	}
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
	p.mu.Unlock()

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
func doHealthCheck(ctx context.Context, client *flightsql.Client) error {
	// Use the underlying flight client for custom actions.
	// flightsql.Client.Client is a flight.Client interface which embeds
	// FlightServiceClient, giving us access to DoAction directly.
	stream, err := client.Client.DoAction(ctx, &flight.Action{Type: "HealthCheck"})
	if err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			break
		}
		var body struct {
			Healthy bool `json:"healthy"`
		}
		if err := json.Unmarshal(msg.Body, &body); err == nil && body.Healthy {
			return nil
		}
	}
	return fmt.Errorf("worker not healthy")
}

// SelectWorker returns the worker with the fewest active sessions.
func (p *FlightWorkerPool) SelectWorker() (*ManagedWorker, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.workers) == 0 {
		return nil, fmt.Errorf("no workers available")
	}

	// For now, simple round-robin selection based on worker ID.
	// Session counting will be added when session manager tracks per-worker counts.
	var best *ManagedWorker
	for _, w := range p.workers {
		select {
		case <-w.done:
			continue // skip dead workers
		default:
		}
		if best == nil || w.ID < best.ID {
			best = w
		}
	}

	if best == nil {
		return nil, fmt.Errorf("all workers dead")
	}
	return best, nil
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
	return nil
}

// ShutdownAll stops all workers gracefully.
func (p *FlightWorkerPool) ShutdownAll() {
	p.mu.Lock()
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
}

// ReplaceWorker replaces a dead worker with a new one.
func (p *FlightWorkerPool) ReplaceWorker(id int) error {
	p.mu.Lock()
	old, exists := p.workers[id]
	if exists {
		if old.client != nil {
			_ = old.client.Close()
		}
		delete(p.workers, id)
	}
	p.mu.Unlock()

	return p.SpawnWorker(id)
}

// HealthCheckLoop periodically checks worker health and respawns dead workers.
func (p *FlightWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, expectedCount int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

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

			for _, w := range workers {
				select {
				case <-w.done:
					slog.Warn("Worker crashed, respawning.", "id", w.ID, "error", w.exitErr)
					if err := p.ReplaceWorker(w.ID); err != nil {
						slog.Error("Failed to respawn worker.", "id", w.ID, "error", err)
					}
				default:
					// Worker is alive, do a health check
					hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					if err := doHealthCheck(hctx, w.client); err != nil {
						slog.Warn("Worker health check failed.", "id", w.ID, "error", err)
					}
					cancel()
				}
			}

			// Ensure we have the expected number of workers
			p.mu.RLock()
			currentCount := len(p.workers)
			p.mu.RUnlock()
			for i := currentCount; i < expectedCount; i++ {
				slog.Info("Spawning missing worker.", "id", i)
				if err := p.SpawnWorker(i); err != nil {
					slog.Error("Failed to spawn missing worker.", "id", i, "error", err)
				}
			}
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
