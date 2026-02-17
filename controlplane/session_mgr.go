package controlplane

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/posthog/duckgres/server"
)

// ManagedSession tracks a client session bound to a worker.
type ManagedSession struct {
	PID          int32
	WorkerID     int
	SessionToken string
	Executor     *server.FlightExecutor
	connCloser   io.Closer // TCP connection, closed on worker crash to unblock the message loop
}

// SessionManager tracks all active sessions and their worker assignments.
type SessionManager struct {
	mu         sync.RWMutex
	sessions   map[int32]*ManagedSession // PID → session
	byWorker   map[int][]int32           // workerID → PIDs
	pool       *FlightWorkerPool
	rebalancer *MemoryRebalancer

	nextPID atomic.Int32
}

// NewSessionManager creates a new session manager.
func NewSessionManager(pool *FlightWorkerPool, rebalancer *MemoryRebalancer) *SessionManager {
	sm := &SessionManager{
		sessions:   make(map[int32]*ManagedSession),
		byWorker:   make(map[int][]int32),
		pool:       pool,
		rebalancer: rebalancer,
	}
	sm.nextPID.Store(1000) // Start PIDs above typical OS PIDs
	return sm
}

// CreateSession acquires a worker (reusing an idle one or spawning a new one),
// creates a session on it, and rebalances memory/thread limits across all active sessions.
func (sm *SessionManager) CreateSession(ctx context.Context, username string) (int32, *server.FlightExecutor, error) {
	// Acquire a worker: reuses idle pre-warmed workers or spawns a new one.
	// When max-workers is set, this blocks until a slot is available.
	worker, err := sm.pool.AcquireWorker(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("acquire worker: %w", err)
	}

	sessionToken, err := worker.CreateSession(ctx, username)
	if err != nil {
		// Clean up the worker we just spawned (but not if it was a pre-warmed idle worker
		// that has sessions from other concurrent requests).
		if !sm.pool.RetireWorkerIfNoSessions(worker.ID) {
			// Worker wasn't retired (it has other sessions), but we still hold
			// a semaphore slot for our failed request. Release it.
			sm.pool.releaseWorkerSem()
		}
		return 0, nil, fmt.Errorf("create session on worker %d: %w", worker.ID, err)
	}

	// Create FlightExecutor sharing the worker's existing gRPC connection
	executor := server.NewFlightExecutorFromClient(worker.client, sessionToken)

	pid := sm.nextPID.Add(1)

	session := &ManagedSession{
		PID:          pid,
		WorkerID:     worker.ID,
		SessionToken: sessionToken,
		Executor:     executor,
	}

	sm.mu.Lock()
	sm.sessions[pid] = session
	sm.byWorker[worker.ID] = append(sm.byWorker[worker.ID], pid)
	sm.mu.Unlock()

	slog.Debug("Session created.", "pid", pid, "worker", worker.ID, "user", username)

	// Set memory/thread limits on this session synchronously so it never
	// runs with unlimited resources.
	if sm.rebalancer != nil {
		sm.rebalancer.SetInitialLimits(ctx, session)
		sm.rebalancer.RequestRebalance()
	}

	return pid, executor, nil
}

// DestroySession destroys a session, retires its dedicated worker, and rebalances
// memory/thread limits across remaining sessions.
func (sm *SessionManager) DestroySession(pid int32) {
	sm.mu.Lock()
	session, ok := sm.sessions[pid]
	if !ok {
		sm.mu.Unlock()
		return
	}
	delete(sm.sessions, pid)

	// Remove from byWorker
	pids := sm.byWorker[session.WorkerID]
	for i, p := range pids {
		if p == pid {
			sm.byWorker[session.WorkerID] = append(pids[:i], pids[i+1:]...)
			break
		}
	}
	sm.mu.Unlock()

	// Close the executor
	if session.Executor != nil {
		_ = session.Executor.Close()
	}

	// Destroy session on worker (best effort, skip if worker already dead)
	worker, ok := sm.pool.Worker(session.WorkerID)
	if ok {
		select {
		case <-worker.done:
			// Worker already dead, skip RPC
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = worker.DestroySession(ctx, session.SessionToken)
			cancel()
		}
	}

	// Retire the dedicated worker (1:1 model)
	sm.pool.RetireWorker(session.WorkerID)

	slog.Debug("Session destroyed.", "pid", pid, "worker", session.WorkerID)

	// Rebalance remaining sessions
	if sm.rebalancer != nil {
		sm.rebalancer.RequestRebalance()
	}
}

// OnWorkerCrash handles a worker crash by marking all affected executors as
// dead and notifying sessions. Executors are marked dead BEFORE the shared
// gRPC client is closed to prevent nil-pointer panics from concurrent RPCs.
// errorFn is called for each affected session to send an error to the client.
func (sm *SessionManager) OnWorkerCrash(workerID int, errorFn func(pid int32)) {
	sm.mu.Lock()
	pids := make([]int32, len(sm.byWorker[workerID]))
	copy(pids, sm.byWorker[workerID])

	// Mark all executors as dead first (under lock) so any concurrent RPC
	// sees the dead flag before the gRPC client is closed.
	for _, pid := range pids {
		if s, ok := sm.sessions[pid]; ok && s.Executor != nil {
			s.Executor.MarkDead()
		}
	}
	sm.mu.Unlock()

	slog.Warn("Worker crashed, notifying sessions.", "worker", workerID, "sessions", len(pids))

	for _, pid := range pids {
		errorFn(pid)
		sm.mu.Lock()
		session, ok := sm.sessions[pid]
		if ok {
			delete(sm.sessions, pid)
			if session.Executor != nil {
				_ = session.Executor.Close()
			}
			// Close the TCP connection to unblock the message loop's read.
			// This causes the session goroutine to exit instead of looping
			// with ErrWorkerDead on every query. The deferred close in
			// handleConnection will also call Close() on the same conn;
			// that's harmless (net.Conn.Close on a closed socket returns
			// an error which is discarded).
			if session.connCloser != nil {
				_ = session.connCloser.Close()
			}
			sm.pool.releaseWorkerSem()
		}
		sm.mu.Unlock()
	}

	sm.mu.Lock()
	delete(sm.byWorker, workerID)
	sm.mu.Unlock()

	// Rebalance remaining sessions after crash cleanup
	if sm.rebalancer != nil {
		sm.rebalancer.RequestRebalance()
	}
}

// SetConnCloser registers the client's TCP connection so it can be closed
// when the backing worker crashes. This unblocks the message loop's read,
// causing it to exit cleanly instead of looping on ErrWorkerDead.
func (sm *SessionManager) SetConnCloser(pid int32, closer io.Closer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if s, ok := sm.sessions[pid]; ok {
		s.connCloser = closer
	}
}

// SessionCount returns the number of active sessions.
func (sm *SessionManager) SessionCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}

// SessionCountForWorker returns the number of sessions on a specific worker.
func (sm *SessionManager) SessionCountForWorker(workerID int) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.byWorker[workerID])
}

// AllSessions returns a snapshot of all active sessions.
// The returned slice is safe to iterate without holding the lock.
func (sm *SessionManager) AllSessions() []*ManagedSession {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]*ManagedSession, 0, len(sm.sessions))
	for _, s := range sm.sessions {
		result = append(result, s)
	}
	return result
}

