package controlplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
)

var ErrTooManyConnections = errors.New("too many connections")
var ErrSessionManagerDraining = errors.New("session manager is draining")

const (
	connectionLeaseReleaseMaxAttempts = 3
	connectionLeaseReleaseRetryDelay  = 50 * time.Millisecond
)

// SessionProgress holds cached query progress from a worker health check.
type SessionProgress struct {
	Percentage float64
	Rows       uint64
	TotalRows  uint64
	Stalled    bool
}

// ManagedSession tracks a client session bound to a worker.
type ManagedSession struct {
	PID          int32
	WorkerID     int
	Protocol     string // "postgres" or "flight"
	SessionToken string
	Executor     *flightclient.FlightExecutor
	connCloser   io.Closer // TCP connection, closed on worker crash to unblock the message loop
	lease        connectionLease

	// Cached query progress from worker health checks.
	queryProgress atomic.Value // stores *SessionProgress (or nil)
}

// SessionManager tracks all active sessions and their worker assignments.
type SessionManager struct {
	mu         sync.RWMutex
	sessions   map[int32]*ManagedSession // PID → session
	byWorker   map[int][]int32           // workerID → PIDs
	pool       WorkerPool
	rebalancer *MemoryRebalancer
	lifecycle  *sessionLifecycle

	nextPID atomic.Int32

	maxConnections int
	activeSlots    int
	waiters        []*connectionWaiter
	limiter        connectionLimiter
}

type flightReconnectPool interface {
	ReconnectFlightWorker(ctx context.Context, workerID int, ownerEpoch int64) (*ManagedWorker, error)
}

type connectionWaiter struct {
	ready   chan struct{}
	granted bool
	err     error
}

// NewSessionManager creates a new session manager.
func NewSessionManager(pool WorkerPool, rebalancer *MemoryRebalancer) *SessionManager {
	sm := &SessionManager{
		sessions:   make(map[int32]*ManagedSession),
		byWorker:   make(map[int][]int32),
		pool:       pool,
		rebalancer: rebalancer,
		lifecycle:  newSessionLifecycle(),
	}
	sm.nextPID.Store(1000) // Start PIDs above typical OS PIDs
	return sm
}

// SetMaxConnections sets the maximum connections for this SessionManager.
func (sm *SessionManager) SetMaxConnections(n int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.maxConnections = n
	sm.grantWaitersLocked()
}

// SetConnectionLimiter replaces the local process limiter with a cluster-wide
// admission limiter. Local session maps still track only this control-plane's
// live sessions.
func (sm *SessionManager) SetConnectionLimiter(limiter connectionLimiter) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.limiter = limiter
}

// ReservePID generates a new unique PID for a session.
func (sm *SessionManager) ReservePID() int32 {
	return sm.nextPID.Add(1)
}

func (sm *SessionManager) acquireSlot(ctx context.Context) error {
	_, err := sm.acquireConnectionSlot(ctx, 0, "postgres")
	return err
}

func (sm *SessionManager) acquireConnectionSlot(ctx context.Context, pid int32, protocol string) (connectionLease, error) {
	sm.mu.Lock()
	if sm.lifecycle.isClosed() {
		sm.mu.Unlock()
		return nil, ErrSessionManagerDraining
	}
	limiter := sm.limiter
	sm.mu.Unlock()
	if limiter != nil {
		maxConnections := func() int {
			sm.mu.RLock()
			defer sm.mu.RUnlock()
			return sm.maxConnections
		}
		lease, err := limiter.Acquire(ctx, pid, protocol, maxConnections)
		if err != nil {
			return nil, err
		}
		if sm.lifecycle.isClosed() {
			sm.releaseConnectionSlot(lease)
			return nil, ErrSessionManagerDraining
		}
		return lease, nil
	}

	sm.mu.Lock()
	if sm.lifecycle.isClosed() {
		sm.mu.Unlock()
		return nil, ErrSessionManagerDraining
	}
	if sm.maxConnections <= 0 || sm.activeSlots < sm.maxConnections {
		sm.activeSlots++
		sm.mu.Unlock()
		return nil, nil
	}
	waiter := &connectionWaiter{ready: make(chan struct{})}
	sm.waiters = append(sm.waiters, waiter)
	sm.mu.Unlock()

	select {
	case <-waiter.ready:
		if waiter.err != nil {
			return nil, waiter.err
		}
		return nil, nil
	case <-ctx.Done():
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if waiter.granted {
			return nil, nil
		}
		sm.removeWaiterLocked(waiter)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrTooManyConnections
		}
		return nil, ctx.Err()
	}
}

func (sm *SessionManager) releaseSlot() {
	sm.releaseConnectionSlot(nil)
}

func (sm *SessionManager) releaseConnectionSlot(lease connectionLease) {
	if lease != nil {
		releaseConnectionLeaseWithRetry(lease)
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.activeSlots > 0 {
		sm.activeSlots--
	}
	sm.grantWaitersLocked()
}

func (sm *SessionManager) grantWaitersLocked() {
	if sm.lifecycle.isClosed() {
		return
	}
	for len(sm.waiters) > 0 && (sm.maxConnections <= 0 || sm.activeSlots < sm.maxConnections) {
		waiter := sm.waiters[0]
		copy(sm.waiters, sm.waiters[1:])
		sm.waiters[len(sm.waiters)-1] = nil
		sm.waiters = sm.waiters[:len(sm.waiters)-1]
		sm.activeSlots++
		waiter.granted = true
		close(waiter.ready)
	}
}

func (sm *SessionManager) failWaitersLocked(err error) {
	for _, waiter := range sm.waiters {
		waiter.err = err
		close(waiter.ready)
	}
	clear(sm.waiters)
	sm.waiters = nil
}

func (sm *SessionManager) removeWaiterLocked(waiter *connectionWaiter) {
	for i, candidate := range sm.waiters {
		if candidate == waiter {
			copy(sm.waiters[i:], sm.waiters[i+1:])
			sm.waiters[len(sm.waiters)-1] = nil
			sm.waiters = sm.waiters[:len(sm.waiters)-1]
			return
		}
	}
}

// CreateSession acquires a worker from the configured pool, creates a session
// on it, and rebalances memory/thread limits across all active sessions.
// If pid is 0, a new one is generated.
func (sm *SessionManager) CreateSession(ctx context.Context, username, searchPath string, pid int32, memoryLimit string, threads int) (int32, *flightclient.FlightExecutor, error) {
	return sm.CreateSessionWithProtocol(ctx, username, searchPath, pid, memoryLimit, threads, "postgres")
}

func (sm *SessionManager) CreateSessionWithProtocol(ctx context.Context, username, searchPath string, pid int32, memoryLimit string, threads int, protocol string) (int32, *flightclient.FlightExecutor, error) {
	ctx, endCreation, err := sm.beginSessionCreation(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer endCreation()

	lease, err := sm.acquireConnectionSlot(ctx, pid, protocol)
	if err != nil {
		return 0, nil, err
	}
	success := false
	defer func() {
		if !success {
			sm.releaseConnectionSlot(lease)
		}
	}()

	memoryLimit, threads = sm.resolveSessionLimits(memoryLimit, threads)

	// Acquire a worker. Backend implementations may reuse warm workers, queue,
	// spawn, or return a typed capacity error when no worker is immediately available.
	observeControlPlaneWorkerQueueDepthDelta(1)
	defer observeControlPlaneWorkerQueueDepthDelta(-1)

	acquireStart := time.Now()
	ctx, acquireSpan := server.Tracer().Start(ctx, "duckgres.worker_acquire")
	slog.Debug("Acquiring worker for session.", "pid", pid, "user", username)
	worker, err := sm.pool.AcquireWorker(ctx)
	acquireSpan.End()
	if err != nil {
		var capacityErr *WarmCapacityExhaustedError
		if errors.As(err, &capacityErr) {
			observeControlPlaneWorkerAcquireFailure("warm_capacity_exhausted")
			slog.Warn("Worker acquisition failed.",
				"pid", pid,
				"user", username,
				"duration", time.Since(acquireStart),
				"reason", "warm_capacity_exhausted",
				"retry_after", capacityErr.RetryAfter,
				"error", err,
			)
		}
		return 0, nil, fmt.Errorf("acquire worker: %w", err)
	}
	slog.Debug("Worker acquired.", "pid", pid, "worker", worker.ID, "user", username, "duration", time.Since(acquireStart))

	pid, exec, err := sm.createSessionOnWorker(ctx, username, searchPath, pid, memoryLimit, threads, worker, protocol, true, lease)
	if err != nil {
		return 0, nil, err
	}
	success = true
	return pid, exec, nil
}

func (sm *SessionManager) resolveSessionLimits(memoryLimit string, threads int) (string, int) {
	if sm.rebalancer == nil {
		return memoryLimit, threads
	}
	if memoryLimit == "" {
		memoryLimit = sm.rebalancer.MemoryLimit()
	}
	if threads <= 0 {
		threads = sm.rebalancer.PerSessionThreads()
	}
	return memoryLimit, threads
}

func (sm *SessionManager) ReconnectFlightSession(ctx context.Context, username string, workerID int, ownerEpoch int64) (int32, *flightclient.FlightExecutor, error) {
	ctx, endCreation, err := sm.beginSessionCreation(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer endCreation()

	lease, err := sm.acquireConnectionSlot(ctx, 0, "flight")
	if err != nil {
		return 0, nil, err
	}
	success := false
	defer func() {
		if !success {
			sm.releaseConnectionSlot(lease)
		}
	}()

	reconnector, ok := sm.pool.(flightReconnectPool)
	if !ok {
		return 0, nil, fmt.Errorf("worker pool does not support flight reconnect")
	}
	worker, err := reconnector.ReconnectFlightWorker(ctx, workerID, ownerEpoch)
	if err != nil {
		return 0, nil, fmt.Errorf("reconnect worker %d: %w", workerID, err)
	}
	pid, exec, err := sm.createSessionOnWorker(ctx, username, "", 0, "", 0, worker, "flight", false, lease)
	if err != nil {
		return 0, nil, err
	}
	success = true
	return pid, exec, nil
}

func (sm *SessionManager) beginSessionCreation(ctx context.Context) (context.Context, func(), error) {
	return sm.lifecycle.begin(ctx)
}

func (sm *SessionManager) createSessionOnWorker(ctx context.Context, username, searchPath string, pid int32, memoryLimit string, threads int, worker *ManagedWorker, protocol string, retireOnFailure bool, lease connectionLease) (int32, *flightclient.FlightExecutor, error) {
	createStart := time.Now()
	slog.Info("Creating session on worker.",
		"pid", pid,
		"worker", worker.ID,
		"user", username,
		"protocol", protocol,
		"memory_limit", memoryLimit,
		"threads", threads,
		"owner_cp_instance_id", worker.OwnerCPInstanceID(),
		"owner_epoch", worker.OwnerEpoch(),
	)
	sessionToken, err := worker.CreateSession(ctx, username, memoryLimit, searchPath, threads)
	if err != nil {
		slog.Warn("Failed to create session on worker.",
			"pid", pid,
			"worker", worker.ID,
			"user", username,
			"protocol", protocol,
			"duration", time.Since(createStart),
			"retire_on_failure", retireOnFailure,
			"error", err,
		)
		if retireOnFailure {
			sm.pool.RetireWorkerIfNoSessions(worker.ID)
		}
		return 0, nil, fmt.Errorf("create session on worker %d: %w", worker.ID, err)
	}

	executor := flightclient.NewFlightExecutorFromClient(worker.client, sessionToken)
	executor.SetControlMetadata(worker.ID, worker.OwnerCPInstanceID(), worker.OwnerEpoch())

	if pid == 0 {
		pid = sm.nextPID.Add(1)
	}

	session := &ManagedSession{
		PID:          pid,
		WorkerID:     worker.ID,
		Protocol:     protocol,
		SessionToken: sessionToken,
		Executor:     executor,
		lease:        lease,
	}

	sm.mu.Lock()
	if sm.lifecycle.isClosed() {
		sm.mu.Unlock()
		sm.cleanupUnregisteredWorkerSession(worker, session.SessionToken)
		return 0, nil, ErrSessionManagerDraining
	}
	sm.sessions[pid] = session
	sm.byWorker[worker.ID] = append(sm.byWorker[worker.ID], pid)
	sessionCount := len(sm.sessions)
	workerSessionCount := len(sm.byWorker[worker.ID])
	sm.mu.Unlock()

	slog.Info("Session created on worker.",
		"pid", pid,
		"worker", worker.ID,
		"user", username,
		"protocol", protocol,
		"create_duration", time.Since(createStart),
		"session_count", sessionCount,
		"worker_session_count", workerSessionCount,
		"owner_cp_instance_id", worker.OwnerCPInstanceID(),
		"owner_epoch", worker.OwnerEpoch(),
	)
	if sm.rebalancer != nil {
		sm.rebalancer.RequestRebalance()
	}
	return pid, executor, nil
}

func (sm *SessionManager) cleanupUnregisteredWorkerSession(worker *ManagedWorker, sessionToken string) {
	if worker != nil {
		select {
		case <-worker.done:
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := worker.DestroySession(ctx, sessionToken); err != nil {
				slog.Warn("Failed to destroy unregistered worker session during drain.", "worker", worker.ID, "error", err)
			}
			cancel()
		}
		sm.pool.ReleaseWorker(worker.ID)
	}
}

// DestroySession destroys a session, retires its dedicated worker, and rebalances
// memory/thread limits across remaining sessions.
func (sm *SessionManager) DestroySession(pid int32) {
	destroyStart := time.Now()
	sm.mu.Lock()
	session, sessionCount, workerSessionCount, ok := sm.detachSessionLocked(pid)
	if !ok {
		sm.mu.Unlock()
		slog.Warn("DestroySession called for unknown session.", "pid", pid)
		return
	}
	finishCleanup := sm.lifecycle.beginCleanup()
	sm.mu.Unlock()
	defer finishCleanup()

	slog.Info("Destroying session.",
		"pid", pid,
		"worker", session.WorkerID,
		"protocol", session.Protocol,
		"remaining_sessions", sessionCount,
		"remaining_worker_sessions", workerSessionCount,
	)

	// Close the executor
	if session.Executor != nil {
		_ = session.Executor.Close()
	}

	// Destroy session on worker (best effort, skip if worker already dead).
	// This must complete BEFORE ReleaseWorker so the next session doesn't
	// overlap with cleanup on the shared DuckDB instance (MaxOpenConns=1
	// enforces single-session isolation; releasing early would let a new
	// session block on db.Conn() or, worse, share catalog state).
	worker, ok := sm.pool.Worker(session.WorkerID)
	workerDestroyAttempted := false
	workerAlreadyDead := false
	var workerDestroyErr error
	if ok {
		select {
		case <-worker.done:
			workerAlreadyDead = true
			// Worker already dead, skip RPC
		default:
			workerDestroyAttempted = true
			workerDestroyStart := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			workerDestroyErr = worker.DestroySession(ctx, session.SessionToken)
			cancel()
			slog.Info("Worker session destroy RPC completed.",
				"pid", pid,
				"worker", session.WorkerID,
				"protocol", session.Protocol,
				"duration", time.Since(workerDestroyStart),
				"error", workerDestroyErr,
			)
		}
	}

	// Release the worker for reuse after cleanup is complete.
	sm.pool.ReleaseWorker(session.WorkerID)
	sm.releaseSessionLease(session, "pid", pid)

	slog.Info("Session destroyed.",
		"pid", pid,
		"worker", session.WorkerID,
		"protocol", session.Protocol,
		"duration", time.Since(destroyStart),
		"worker_found", ok,
		"worker_already_dead", workerAlreadyDead,
		"worker_destroy_attempted", workerDestroyAttempted,
		"worker_destroy_error", workerDestroyErr,
	)

	// Rebalance remaining sessions
	if sm.rebalancer != nil {
		sm.rebalancer.RequestRebalance()
	}
}

// DestroyAllSessions destroys every active session without holding the manager
// lock while running per-session cleanup.
func (sm *SessionManager) DestroyAllSessions() {
	for {
		sm.lifecycle.close()
		sm.mu.Lock()
		sm.failWaitersLocked(ErrSessionManagerDraining)
		pids := sm.sessionPIDsLocked()
		sm.mu.Unlock()
		if len(pids) == 0 {
			sm.lifecycle.closeAndWait()
			return
		}
		for _, pid := range pids {
			sm.DestroySession(pid)
		}
	}
}

func (sm *SessionManager) sessionPIDsLocked() []int32 {
	pids := make([]int32, 0, len(sm.sessions))
	for pid := range sm.sessions {
		pids = append(pids, pid)
	}
	return pids
}

func (sm *SessionManager) detachSessionLocked(pid int32) (*ManagedSession, int, int, bool) {
	session, ok := sm.sessions[pid]
	if !ok {
		return nil, len(sm.sessions), 0, false
	}
	delete(sm.sessions, pid)
	sm.removeSessionFromWorkerLocked(session.WorkerID, pid)
	if session.lease == nil && sm.activeSlots > 0 {
		sm.activeSlots--
	}
	sm.grantWaitersLocked()
	return session, len(sm.sessions), len(sm.byWorker[session.WorkerID]), true
}

func (sm *SessionManager) removeSessionFromWorkerLocked(workerID int, pid int32) {
	pids := sm.byWorker[workerID]
	for i, p := range pids {
		if p == pid {
			pids = append(pids[:i], pids[i+1:]...)
			break
		}
	}
	if len(pids) == 0 {
		delete(sm.byWorker, workerID)
		return
	}
	sm.byWorker[workerID] = pids
}

func (sm *SessionManager) releaseSessionLease(session *ManagedSession, attrs ...any) {
	if session == nil || session.lease == nil {
		return
	}
	releaseConnectionLeaseWithRetry(session.lease, attrs...)
}

func releaseConnectionLeaseWithRetry(lease connectionLease, attrs ...any) {
	var err error
	for attempt := 1; attempt <= connectionLeaseReleaseMaxAttempts; attempt++ {
		err = lease.Release(context.Background())
		if err == nil {
			return
		}
		if attempt < connectionLeaseReleaseMaxAttempts {
			time.Sleep(time.Duration(attempt) * connectionLeaseReleaseRetryDelay)
		}
	}

	args := append([]any{"error", err, "attempts", connectionLeaseReleaseMaxAttempts}, attrs...)
	slog.Warn("Failed to release org connection lease.", args...)
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

	slog.Warn("Worker crashed, notifying sessions.", "worker", workerID, "sessions", len(pids), "pids", pids)

	for _, pid := range pids {
		cleanupStart := time.Now()
		errorFn(pid)
		var executor *flightclient.FlightExecutor
		var connCloser io.Closer
		var session *ManagedSession
		var finishCleanup func()
		sm.mu.Lock()
		detached, remainingSessions, _, ok := sm.detachSessionLocked(pid)
		if ok {
			session = detached
			executor = detached.Executor
			connCloser = detached.connCloser
			finishCleanup = sm.lifecycle.beginCleanup()
		}
		sm.mu.Unlock()
		if executor != nil {
			_ = executor.Close()
		}
		// Close the TCP connection to unblock the message loop's read.
		// This causes the session goroutine to exit instead of looping
		// with ErrWorkerDead on every query. The deferred close in
		// handleConnection will also call Close() on the same conn;
		// that's harmless (net.Conn.Close on a closed socket returns
		// an error which is discarded).
		if connCloser != nil {
			_ = connCloser.Close()
		}
		sm.releaseSessionLease(session, "pid", pid)
		slog.Info("Worker crash session cleanup completed.",
			"pid", pid,
			"worker", workerID,
			"session_found", ok,
			"duration", time.Since(cleanupStart),
			"remaining_sessions", remainingSessions,
		)
		if ok {
			finishCleanup()
		}
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

// WorkerIDForPID returns the worker ID for a session, or -1 if not found.
func (sm *SessionManager) WorkerIDForPID(pid int32) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if s, ok := sm.sessions[pid]; ok {
		return s.WorkerID
	}
	return -1
}

// WorkerPodNameForPID returns the K8s pod name of the worker hosting the
// session, or "" if not found or not running on K8s.
func (sm *SessionManager) WorkerPodNameForPID(pid int32) string {
	sm.mu.RLock()
	s, ok := sm.sessions[pid]
	sm.mu.RUnlock()
	if !ok {
		return ""
	}
	worker, ok := sm.pool.Worker(s.WorkerID)
	if !ok || worker == nil {
		return ""
	}
	return worker.PodName()
}

// GetProgress returns the cached query progress for a session, or nil.
func (sm *SessionManager) GetProgress(pid int32) *SessionProgress {
	sm.mu.RLock()
	s, ok := sm.sessions[pid]
	sm.mu.RUnlock()
	if !ok {
		return nil
	}
	v := s.queryProgress.Load()
	if v == nil {
		return nil
	}
	return v.(*SessionProgress)
}

// UpdateProgress caches query progress data for sessions on the given worker.
// Called from the health check loop after parsing the worker's health check response.
// Progress keys are truncated session tokens (first 16 chars) to avoid leaking
// full bearer tokens in health check JSON.
func (sm *SessionManager) UpdateProgress(workerID int, progress map[string]*SessionProgress) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, pid := range sm.byWorker[workerID] {
		s, ok := sm.sessions[pid]
		if !ok {
			continue
		}
		key := s.SessionToken
		if len(key) > 16 {
			key = key[:16]
		}
		if sp, ok := progress[key]; ok {
			s.queryProgress.Store(sp)
		}
	}
}

// SetProtocol updates the protocol label for an active session.
func (sm *SessionManager) SetProtocol(pid int32, protocol string) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if s, ok := sm.sessions[pid]; ok {
		s.Protocol = protocol
	}
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
