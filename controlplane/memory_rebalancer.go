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

// MemoryRebalancer sets memory_limit and threads on DuckDB sessions.
// Every session gets the full memory budget — DuckDB will spill to disk/swap
// if aggregate usage exceeds physical RAM. Threads are not subdivided.
//
// When enabled is true, SET commands are re-sent on every session
// create/destroy (useful if the budget or threads change at runtime).
// When enabled is false (default), each session gets its limits at creation
// time and is never adjusted afterward.
//
// Lock ordering invariant: r.mu → SessionManager.mu(RLock). Never acquire
// r.mu while holding SessionManager.mu to avoid deadlock.
type MemoryRebalancer struct {
	mu       sync.Mutex
	sessions SessionLister // mutable: set via SetSessionLister

	// memoryBudget, threadBudget, and enabled are immutable after construction.
	memoryBudget uint64 // total bytes — each session gets this full amount
	threadBudget int    // threads per session (not subdivided)
	enabled      bool   // when false, skip dynamic rebalancing

	// Debounce: coalesce rapid rebalance requests
	pendingRebalance chan struct{} // buffered(1), signals rebalance needed
	stopDebounce     chan struct{} // closed to stop the debounce goroutine
}

// NewMemoryRebalancer creates a rebalancer with the given budgets.
// If memoryBudget is 0, it defaults to 75% of system RAM.
// If threadBudget is 0, it defaults to runtime.NumCPU().
// If enabled is false, RequestRebalance is a no-op and sessions only get
// their limits set once at creation time.
func NewMemoryRebalancer(memoryBudget uint64, threadBudget int, sessions SessionLister, enabled bool) *MemoryRebalancer {
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
		enabled:          enabled,
		sessions:         sessions,
		pendingRebalance: make(chan struct{}, 1),
		stopDebounce:     make(chan struct{}),
	}

	// Only start background debounce goroutine when rebalancing is enabled
	if enabled {
		go r.debounceLoop()
	}

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
// When rebalancing is disabled, this is a no-op.
func (r *MemoryRebalancer) RequestRebalance() {
	if !r.enabled {
		return
	}
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

	memStr := formatBytes(memoryLimit(budget))

	slog.Info("Rebalancing session resources.",
		"sessions", count,
		"memory_per_session", memStr,
		"threads", threadBudget,
		"memory_budget", formatBytes(budget))

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

		if _, err := s.Executor.ExecContext(sessCtx, fmt.Sprintf("SET threads = %d", threadBudget)); err != nil {
			slog.Warn("Failed to set threads on session.", "pid", s.PID, "error", err)
		}

		cancel()
	}
}

// MemoryLimit returns the memory limit string applied to every session.
func (r *MemoryRebalancer) MemoryLimit() string {
	return formatBytes(memoryLimit(r.memoryBudget))
}

// PerSessionThreads returns the thread count for each session.
// Threads are not subdivided — every session gets the full budget.
func (r *MemoryRebalancer) PerSessionThreads() int {
	return r.threadBudget
}

// memoryLimit returns the memory limit for a session: the full budget,
// with a floor of minMemoryPerSession (256MB).
func memoryLimit(budget uint64) uint64 {
	if budget < minMemoryPerSession {
		return minMemoryPerSession
	}
	return budget
}

// DefaultMaxWorkers returns a reasonable default for max_workers.
// Derived from the memory budget (budget / 256MB).
func (r *MemoryRebalancer) DefaultMaxWorkers() int {
	return int(r.memoryBudget / minMemoryPerSession)
}

// SetInitialLimits sets memory_limit and threads on a single session synchronously.
// Called during CreateSession so the new session never runs with unlimited resources.
func (r *MemoryRebalancer) SetInitialLimits(ctx context.Context, session *ManagedSession) {
	if session == nil || session.Executor == nil {
		return
	}

	memStr := r.MemoryLimit()
	threads := r.PerSessionThreads()

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
