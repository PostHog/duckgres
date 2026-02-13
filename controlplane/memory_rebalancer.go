package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/posthog/duckgres/server"
)

const (
	// minMemoryPerSession is the floor per-session memory limit (256MB).
	minMemoryPerSession = 256 * 1024 * 1024

	// rebalanceTimeout is the per-session timeout for sending SET commands.
	rebalanceTimeout = 5 * time.Second

	// rebalanceDebounce is the minimum interval between rebalance operations.
	// Rapid connect/disconnect bursts are coalesced into a single rebalance.
	rebalanceDebounce = 100 * time.Millisecond
)

// SessionLister provides a snapshot of all active sessions for rebalancing.
type SessionLister interface {
	AllSessions() []*ManagedSession
}

// MemoryRebalancer dynamically distributes memory and thread budgets across
// active DuckDB sessions. On every session create/destroy, it recomputes
// per-session limits and sends SET commands to all active sessions.
//
// Lock ordering invariant: r.mu → SessionManager.mu(RLock). Never acquire
// r.mu while holding SessionManager.mu to avoid deadlock.
type MemoryRebalancer struct {
	mu       sync.Mutex
	sessions SessionLister // mutable: set via SetSessionLister

	// memoryBudget and threadBudget are immutable after construction.
	// They are read without holding mu by PerSessionMemoryLimit,
	// PerSessionThreads, and MaxSessionsForBudget.
	memoryBudget uint64 // total bytes for all sessions
	threadBudget int    // total threads for all sessions

	// Debounce: coalesce rapid rebalance requests
	pendingRebalance chan struct{} // buffered(1), signals rebalance needed
	stopDebounce     chan struct{} // closed to stop the debounce goroutine
}

// NewMemoryRebalancer creates a rebalancer with the given budgets.
// If memoryBudget is 0, it defaults to 75% of system RAM.
// If threadBudget is 0, it defaults to runtime.NumCPU().
func NewMemoryRebalancer(memoryBudget uint64, threadBudget int, sessions SessionLister) *MemoryRebalancer {
	if memoryBudget == 0 {
		totalMem := server.SystemMemoryBytes()
		if totalMem > 0 {
			memoryBudget = totalMem * 3 / 4
		} else {
			memoryBudget = 4 * 1024 * 1024 * 1024 // 4GB fallback
		}
	}
	if threadBudget == 0 {
		threadBudget = runtime.NumCPU()
	}

	r := &MemoryRebalancer{
		memoryBudget:     memoryBudget,
		threadBudget:     threadBudget,
		sessions:         sessions,
		pendingRebalance: make(chan struct{}, 1),
		stopDebounce:     make(chan struct{}),
	}

	// Start background debounce goroutine
	go r.debounceLoop()

	return r
}

// SetSessionLister sets the session lister after construction.
// Must be called before any Rebalance calls (i.e., before accepting connections).
func (r *MemoryRebalancer) SetSessionLister(sl SessionLister) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions = sl
}

// Stop stops the background debounce goroutine. Must be called on shutdown.
func (r *MemoryRebalancer) Stop() {
	close(r.stopDebounce)
}

// RequestRebalance signals that a rebalance is needed. Multiple rapid calls
// are coalesced — the actual rebalance runs at most once per debounce interval.
func (r *MemoryRebalancer) RequestRebalance() {
	// Non-blocking send to buffered(1) channel — if a rebalance is already
	// pending, this is a no-op (the pending rebalance will pick up new state).
	select {
	case r.pendingRebalance <- struct{}{}:
	default:
	}
}

// debounceLoop runs in a background goroutine and coalesces rebalance requests.
func (r *MemoryRebalancer) debounceLoop() {
	for {
		select {
		case <-r.stopDebounce:
			return
		case <-r.pendingRebalance:
			// Wait briefly to coalesce rapid connect/disconnect bursts,
			// but respect shutdown signals during the debounce window.
			select {
			case <-r.stopDebounce:
				return
			case <-time.After(rebalanceDebounce):
			}
			// Drain any additional signals that arrived during the debounce
			select {
			case <-r.pendingRebalance:
			default:
			}
			r.doRebalance()
		}
	}
}

// doRebalance performs the actual rebalance: computes limits, sends SET commands.
// The mutex is held only for snapshotting sessions (not during network I/O).
func (r *MemoryRebalancer) doRebalance() {
	r.mu.Lock()
	if r.sessions == nil {
		r.mu.Unlock()
		return
	}
	sessions := r.sessions.AllSessions()
	budget := r.memoryBudget
	threadBudget := r.threadBudget
	r.mu.Unlock()

	count := len(sessions)
	if count == 0 {
		return
	}

	memLimit := perSessionMemory(budget, count)
	threads := perSessionThreads(threadBudget, count)
	memStr := formatBytes(memLimit)

	slog.Info("Rebalancing session resources.",
		"sessions", count,
		"memory_per_session", memStr,
		"threads_per_session", threads,
		"memory_budget", formatBytes(budget),
		"thread_budget", threadBudget)

	// Send SET commands without holding the rebalancer lock
	ctx := context.Background()
	for _, s := range sessions {
		if s.Executor == nil {
			continue
		}

		sessCtx, cancel := context.WithTimeout(ctx, rebalanceTimeout)

		if _, err := s.Executor.ExecContext(sessCtx, fmt.Sprintf("SET memory_limit = '%s'", memStr)); err != nil {
			slog.Warn("Failed to set memory_limit on session.", "pid", s.PID, "error", err)
		}

		if _, err := s.Executor.ExecContext(sessCtx, fmt.Sprintf("SET threads = %d", threads)); err != nil {
			slog.Warn("Failed to set threads on session.", "pid", s.PID, "error", err)
		}

		cancel()
	}
}

// PerSessionMemoryLimit returns the current per-session memory limit string
// based on the given session count.
func (r *MemoryRebalancer) PerSessionMemoryLimit(sessionCount int) string {
	if sessionCount <= 0 {
		sessionCount = 1
	}
	return formatBytes(perSessionMemory(r.memoryBudget, sessionCount))
}

// PerSessionThreads returns the current per-session thread count
// based on the given session count.
func (r *MemoryRebalancer) PerSessionThreads(sessionCount int) int {
	if sessionCount <= 0 {
		sessionCount = 1
	}
	return perSessionThreads(r.threadBudget, sessionCount)
}

func perSessionMemory(budget uint64, count int) uint64 {
	limit := budget / uint64(count)
	if limit < minMemoryPerSession {
		limit = minMemoryPerSession
	}
	return limit
}

func perSessionThreads(budget int, count int) int {
	threads := budget / count
	if threads < 1 {
		threads = 1
	}
	return threads
}

// MaxSessionsForBudget returns the maximum number of sessions that can be
// supported without exceeding the memory budget, given the per-session floor.
// This can be used to derive a dynamic max-workers cap.
func (r *MemoryRebalancer) MaxSessionsForBudget() int {
	return int(r.memoryBudget / minMemoryPerSession)
}

// SetInitialLimits sets memory_limit and threads on a single session synchronously.
// Called during CreateSession so the new session never runs with unlimited resources.
func (r *MemoryRebalancer) SetInitialLimits(ctx context.Context, session *ManagedSession, totalSessions int) {
	if session == nil || session.Executor == nil {
		return
	}

	memStr := r.PerSessionMemoryLimit(totalSessions)
	threads := r.PerSessionThreads(totalSessions)

	setCtx, cancel := context.WithTimeout(ctx, rebalanceTimeout)
	defer cancel()

	if _, err := session.Executor.ExecContext(setCtx, fmt.Sprintf("SET memory_limit = '%s'", memStr)); err != nil {
		slog.Warn("Failed to set initial memory_limit.", "pid", session.PID, "error", err)
	}
	if _, err := session.Executor.ExecContext(setCtx, fmt.Sprintf("SET threads = %d", threads)); err != nil {
		slog.Warn("Failed to set initial threads.", "pid", session.PID, "error", err)
	}
}

// formatBytes formats a byte count as a human-readable DuckDB size string.
// Always uses MB to avoid precision loss from integer division at GB granularity.
// (e.g., 4.8GB → "4915MB" instead of truncating to "4GB")
func formatBytes(b uint64) string {
	const mb = 1024 * 1024
	return fmt.Sprintf("%dMB", b/mb)
}
