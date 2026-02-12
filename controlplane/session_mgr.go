package controlplane

import (
	"context"
	"fmt"
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

// CreateSession spawns a dedicated worker, creates a session on it, and rebalances
// memory/thread limits across all active sessions.
func (sm *SessionManager) CreateSession(ctx context.Context, username string) (int32, *server.FlightExecutor, error) {
	// Check max_workers cap
	if err := sm.pool.CheckMaxWorkers(); err != nil {
		return 0, nil, err
	}

	// Spawn a dedicated worker for this connection
	worker, err := sm.pool.SpawnWorkerForSession()
	if err != nil {
		return 0, nil, fmt.Errorf("spawn worker: %w", err)
	}

	sessionToken, err := worker.CreateSession(ctx, username)
	if err != nil {
		// Clean up the worker we just spawned
		sm.pool.RetireWorker(worker.ID)
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

	// Rebalance memory/threads across all sessions (including this new one)
	if sm.rebalancer != nil {
		sm.rebalancer.Rebalance(ctx)
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

	// Destroy session on worker (best effort)
	worker, ok := sm.pool.Worker(session.WorkerID)
	if ok {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = worker.DestroySession(ctx, session.SessionToken)
		cancel()
	}

	// Retire the dedicated worker (1:1 model)
	sm.pool.RetireWorker(session.WorkerID)

	slog.Debug("Session destroyed.", "pid", pid, "worker", session.WorkerID)

	// Rebalance remaining sessions
	if sm.rebalancer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		sm.rebalancer.Rebalance(ctx)
		cancel()
	}
}

// OnWorkerCrash handles a worker crash by sending errors to all affected sessions.
// errorFn is called for each affected session to send an error to the client.
func (sm *SessionManager) OnWorkerCrash(workerID int, errorFn func(pid int32)) {
	sm.mu.Lock()
	pids := make([]int32, len(sm.byWorker[workerID]))
	copy(pids, sm.byWorker[workerID])
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
		}
		sm.mu.Unlock()
	}

	sm.mu.Lock()
	delete(sm.byWorker, workerID)
	sm.mu.Unlock()
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

// HasSessionsForWorker returns true if the given worker has any active sessions.
func (sm *SessionManager) HasSessionsForWorker(workerID int) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.byWorker[workerID]) > 0
}
