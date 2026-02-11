package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/posthog/duckgres/controlplane/fdpass"
	pb "github.com/posthog/duckgres/controlplane/proto"
	"github.com/posthog/duckgres/server"
)

// ManagedWorker represents a spawned worker process and its connections.
type ManagedWorker struct {
	ID         int
	Cmd        *exec.Cmd
	PID        int
	GRPCSocket string
	FDSocket   string
	GRPCConn   *grpc.ClientConn
	Client     pb.WorkerControlClient
	StartTime  time.Time
	done       chan struct{} // closed when process exits
	routeMu    sync.Mutex   // serializes FD send + AcceptConnection to prevent FD swaps
}

// WorkerPool manages a pool of long-lived worker processes.
type WorkerPool struct {
	mu            sync.RWMutex
	workers       map[int]*ManagedWorker
	socketDir     string
	cfg           server.Config
	rollingUpdate atomic.Bool // suppresses health check spawning during rolling update
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(socketDir string, cfg server.Config) *WorkerPool {
	return &WorkerPool{
		workers:   make(map[int]*ManagedWorker),
		socketDir: socketDir,
		cfg:       cfg,
	}
}

// SpawnWorker spawns a new worker process and establishes gRPC + FD socket connections.
func (p *WorkerPool) SpawnWorker(id int) error {
	grpcSocket := filepath.Join(p.socketDir, fmt.Sprintf("worker-%d-grpc.sock", id))
	fdSocket := filepath.Join(p.socketDir, fmt.Sprintf("worker-%d-fd.sock", id))

	// Clean up old sockets
	_ = os.Remove(grpcSocket)
	_ = os.Remove(fdSocket)

	// Spawn child process
	cmd := exec.Command(os.Args[0],
		"--mode", "worker",
		"--grpc-socket", grpcSocket,
		"--fd-socket", fdSocket,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start worker %d: %w", id, err)
	}

	slog.Info("Spawned worker process.", "id", id, "pid", cmd.Process.Pid,
		"grpc_socket", grpcSocket, "fd_socket", fdSocket)

	// Wait for the gRPC socket to become available
	if err := waitForSocket(grpcSocket, 10*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("worker %d gRPC socket not ready: %w", id, err)
	}

	// Connect gRPC
	conn, err := grpc.NewClient(
		"unix://"+grpcSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("connect gRPC to worker %d: %w", id, err)
	}

	client := pb.NewWorkerControlClient(conn)

	// Send Configure
	configReq := buildConfigureRequest(p.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	resp, err := client.Configure(ctx, configReq)
	cancel()
	if err != nil {
		_ = conn.Close()
		_ = cmd.Process.Kill()
		return fmt.Errorf("configure worker %d: %w", id, err)
	}
	if !resp.Ok {
		_ = conn.Close()
		_ = cmd.Process.Kill()
		return fmt.Errorf("configure worker %d: %s", id, resp.Error)
	}

	worker := &ManagedWorker{
		ID:         id,
		Cmd:        cmd,
		PID:        cmd.Process.Pid,
		GRPCSocket: grpcSocket,
		FDSocket:   fdSocket,
		GRPCConn:   conn,
		Client:     client,
		StartTime:  time.Now(),
		done:       make(chan struct{}),
	}

	// Monitor the process in the background
	go func() {
		err := cmd.Wait()
		if err != nil {
			slog.Error("Worker process exited.", "id", id, "pid", worker.PID, "error", err)
		} else {
			slog.Info("Worker process exited cleanly.", "id", id, "pid", worker.PID)
		}
		close(worker.done)

		// Remove from pool
		p.mu.Lock()
		delete(p.workers, id)
		p.mu.Unlock()
	}()

	p.mu.Lock()
	p.workers[id] = worker
	p.mu.Unlock()

	slog.Info("Worker configured and ready.", "id", id, "pid", worker.PID)
	return nil
}

// ConnectExistingWorker connects to a worker that was handed over from a previous control plane.
// The worker process is already running - we just need to establish gRPC connection.
func (p *WorkerPool) ConnectExistingWorker(id int, grpcSocket, fdSocket string) error {
	// Connect gRPC
	conn, err := grpc.NewClient(
		"unix://"+grpcSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connect gRPC to worker %d: %w", id, err)
	}

	client := pb.NewWorkerControlClient(conn)

	// Verify the worker is healthy
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	health, err := client.Health(ctx, &pb.HealthRequest{})
	cancel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("health check worker %d: %w", id, err)
	}
	if !health.Healthy {
		_ = conn.Close()
		return fmt.Errorf("worker %d is not healthy", id)
	}

	worker := &ManagedWorker{
		ID:         id,
		GRPCSocket: grpcSocket,
		FDSocket:   fdSocket,
		GRPCConn:   conn,
		Client:     client,
		StartTime:  time.Now(),
		done:       make(chan struct{}),
	}

	// Monitor the handed-over worker via gRPC health checks.
	// Unlike SpawnWorker where we can cmd.Wait(), we don't own this process,
	// so we detect exit by polling health. This ensures done is closed for
	// ShutdownAll and RollingUpdate which wait on <-w.done.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := client.Health(ctx, &pb.HealthRequest{})
			cancel()
			if err != nil {
				slog.Info("Handed-over worker unreachable, marking as done.", "id", id, "error", err)
				close(worker.done)
				p.mu.Lock()
				delete(p.workers, id)
				p.mu.Unlock()
				return
			}
		}
	}()

	p.mu.Lock()
	p.workers[id] = worker
	p.mu.Unlock()

	slog.Info("Connected to existing worker.", "id", id,
		"active_connections", health.ActiveConnections)
	return nil
}

// RouteConnection sends a TCP file descriptor to a worker for handling.
// It selects the least-loaded worker based on health checks.
func (p *WorkerPool) RouteConnection(tcpFile *os.File, remoteAddr string, secretKey int32) (int32, error) {
	return p.routeConnection(tcpFile, remoteAddr, secretKey, false)
}

// RouteFlightConnection sends a Flight connection FD to a worker.
func (p *WorkerPool) RouteFlightConnection(tcpFile *os.File, remoteAddr string) (int32, error) {
	return p.routeConnection(tcpFile, remoteAddr, 0, true)
}

func (p *WorkerPool) routeConnection(tcpFile *os.File, remoteAddr string, secretKey int32, isFlight bool) (int32, error) {
	worker, err := p.selectWorker()
	if err != nil {
		return 0, err
	}

	// Serialize FD send + AcceptConnection per worker. Without this, concurrent
	// routeConnection calls to the same worker can interleave: goroutine A sends
	// FD-A, goroutine B sends FD-B, then B's AcceptConnection dequeues FD-A
	// (wrong client). The lock ensures each FD is paired with its gRPC call.
	worker.routeMu.Lock()
	defer worker.routeMu.Unlock()

	// Connect to worker's FD socket and send the FD.
	// Note: we do NOT call waitForSocket here — it probe-connects then closes,
	// which triggers an EOF in the worker's fdReceiverLoop. The FD socket is
	// guaranteed to be ready after SpawnWorker/ConnectExistingWorker completes.
	fdConn, err := net.Dial("unix", worker.FDSocket)
	if err != nil {
		return 0, fmt.Errorf("connect FD socket for worker %d: %w", worker.ID, err)
	}

	uc, ok := fdConn.(*net.UnixConn)
	if !ok {
		_ = fdConn.Close()
		return 0, fmt.Errorf("FD conn not UnixConn")
	}

	if err := fdpass.SendFile(uc, tcpFile); err != nil {
		_ = uc.Close()
		return 0, fmt.Errorf("send FD to worker %d: %w", worker.ID, err)
	}
	_ = uc.Close()

	controlRemoteAddr := remoteAddr
	if isFlight {
		controlRemoteAddr = "flight://" + remoteAddr
	}

	// Tell worker to accept the connection via gRPC
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := worker.Client.AcceptConnection(ctx, &pb.AcceptConnectionRequest{
		RemoteAddr:       controlRemoteAddr,
		BackendSecretKey: secretKey,
	})
	cancel()

	if err != nil {
		return 0, fmt.Errorf("AcceptConnection on worker %d: %w", worker.ID, err)
	}
	if !resp.Ok {
		return 0, fmt.Errorf("AcceptConnection on worker %d: %s", worker.ID, resp.Error)
	}

	return resp.BackendPid, nil
}

// selectWorker picks the least-loaded worker using health information.
func (p *WorkerPool) selectWorker() (*ManagedWorker, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.workers) == 0 {
		return nil, fmt.Errorf("no workers available")
	}

	var best *ManagedWorker
	bestConns := int32(1<<31 - 1)

	for _, w := range p.workers {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		health, err := w.Client.Health(ctx, &pb.HealthRequest{})
		cancel()

		if err != nil || !health.Healthy {
			continue
		}

		if health.ActiveConnections < bestConns {
			bestConns = health.ActiveConnections
			best = w
		}
	}

	if best == nil {
		// Fallback to round-robin if health checks fail
		for _, w := range p.workers {
			best = w
			break
		}
	}

	if best == nil {
		return nil, fmt.Errorf("no healthy workers")
	}

	return best, nil
}

// CancelQuery forwards a cancel request to the appropriate worker.
func (p *WorkerPool) CancelQuery(backendPid, secretKey int32) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, w := range p.workers {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := w.Client.CancelQuery(ctx, &pb.CancelQueryRequest{
			BackendPid: backendPid,
			SecretKey:  secretKey,
		})
		cancel()

		if err == nil && resp.Cancelled {
			return true
		}
	}
	return false
}

// DrainAll sends Drain to all workers.
func (p *WorkerPool) DrainAll(timeout time.Duration) error {
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
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			_, err := w.Client.Drain(ctx, &pb.DrainRequest{TimeoutNs: int64(timeout)})
			cancel()
			if err != nil {
				slog.Warn("Failed to drain worker.", "id", w.ID, "error", err)
			}
		}(w)
	}

	wg.Wait()
	return nil
}

// ShutdownAll sends Shutdown to all workers and waits for them to exit.
func (p *WorkerPool) ShutdownAll(timeout time.Duration) {
	p.mu.RLock()
	workers := make([]*ManagedWorker, 0, len(p.workers))
	for _, w := range p.workers {
		workers = append(workers, w)
	}
	p.mu.RUnlock()

	for _, w := range workers {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, _ = w.Client.Shutdown(ctx, &pb.ShutdownRequest{TimeoutNs: int64(timeout)})
		cancel()
	}

	// Wait for processes to exit
	deadline := time.After(timeout)
	for _, w := range workers {
		select {
		case <-w.done:
		case <-deadline:
			slog.Warn("Worker shutdown timeout, killing.", "id", w.ID, "pid", w.PID)
			if w.Cmd != nil && w.Cmd.Process != nil {
				_ = w.Cmd.Process.Kill()
			}
		}
	}
}

// HealthCheckLoop periodically checks worker health and restarts dead workers.
func (p *WorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, desiredCount int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.RLock()
			currentCount := len(p.workers)
			p.mu.RUnlock()

			if currentCount < desiredCount && !p.rollingUpdate.Load() {
				slog.Warn("Worker count below desired, spawning replacements.",
					"current", currentCount, "desired", desiredCount)
				for i := 0; i < desiredCount; i++ {
					p.mu.RLock()
					_, exists := p.workers[i]
					p.mu.RUnlock()
					if !exists {
						if err := p.SpawnWorker(i); err != nil {
							slog.Error("Failed to respawn worker.", "id", i, "error", err)
						}
					}
				}
			}
		}
	}
}

// Workers returns a snapshot of all managed workers.
func (p *WorkerPool) Workers() []*ManagedWorker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*ManagedWorker, 0, len(p.workers))
	for _, w := range p.workers {
		result = append(result, w)
	}
	return result
}

// RollingUpdate replaces workers one at a time with a new binary.
// Only one rolling update can run at a time; concurrent calls return immediately.
func (p *WorkerPool) RollingUpdate(ctx context.Context) error {
	if !p.rollingUpdate.CompareAndSwap(false, true) {
		slog.Warn("Rolling update already in progress, skipping.")
		return nil
	}
	defer p.rollingUpdate.Store(false)

	p.mu.RLock()
	ids := make([]int, 0, len(p.workers))
	for id := range p.workers {
		ids = append(ids, id)
	}
	p.mu.RUnlock()

	for _, id := range ids {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		slog.Info("Rolling update: replacing worker.", "id", id)

		// Drain old worker
		p.mu.RLock()
		old, exists := p.workers[id]
		p.mu.RUnlock()
		if !exists {
			continue
		}

		drainCtx, drainCancel := context.WithTimeout(ctx, 60*time.Second)
		_, _ = old.Client.Drain(drainCtx, &pb.DrainRequest{TimeoutNs: int64(60 * time.Second)})
		drainCancel()

		// Shutdown old worker
		shutCtx, shutCancel := context.WithTimeout(ctx, 30*time.Second)
		_, _ = old.Client.Shutdown(shutCtx, &pb.ShutdownRequest{TimeoutNs: int64(30 * time.Second)})
		shutCancel()

		// Wait for old worker to exit
		select {
		case <-old.done:
		case <-time.After(30 * time.Second):
			if old.Cmd != nil && old.Cmd.Process != nil {
				_ = old.Cmd.Process.Kill()
			}
		}

		_ = old.GRPCConn.Close()

		// Spawn replacement
		if err := p.SpawnWorker(id); err != nil {
			return fmt.Errorf("failed to spawn replacement worker %d: %w", id, err)
		}

		slog.Info("Rolling update: worker replaced.", "id", id)
	}

	return nil
}

func buildConfigureRequest(cfg server.Config) *pb.ConfigureRequest {
	req := &pb.ConfigureRequest{
		DataDir:       cfg.DataDir,
		Extensions:    cfg.Extensions,
		IdleTimeoutNs: int64(cfg.IdleTimeout),
		TlsCertFile:   cfg.TLSCertFile,
		TlsKeyFile:    cfg.TLSKeyFile,
		Users:         cfg.Users,
	}

	if cfg.DuckLake.MetadataStore != "" {
		req.Ducklake = &pb.DuckLakeConfig{
			MetadataStore: cfg.DuckLake.MetadataStore,
			ObjectStore:   cfg.DuckLake.ObjectStore,
			DataPath:      cfg.DuckLake.DataPath,
			S3Provider:    cfg.DuckLake.S3Provider,
			S3Endpoint:    cfg.DuckLake.S3Endpoint,
			S3AccessKey:   cfg.DuckLake.S3AccessKey,
			S3SecretKey:   cfg.DuckLake.S3SecretKey,
			S3Region:      cfg.DuckLake.S3Region,
			S3UseSsl:      cfg.DuckLake.S3UseSSL,
			S3UrlStyle:    cfg.DuckLake.S3URLStyle,
			S3Chain:       cfg.DuckLake.S3Chain,
			S3Profile:     cfg.DuckLake.S3Profile,
		}
	}

	return req
}

func waitForSocket(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			// Try to connect
			conn, err := net.DialTimeout("unix", path, time.Second)
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("socket %s not ready after %v", path, timeout)
}
