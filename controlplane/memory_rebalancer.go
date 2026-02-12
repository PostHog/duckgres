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
)

// SessionLister provides a snapshot of all active sessions for rebalancing.
type SessionLister interface {
	AllSessions() []*ManagedSession
}

// MemoryRebalancer dynamically distributes memory and thread budgets across
// active DuckDB sessions. On every session create/destroy, it recomputes
// per-session limits and sends SET commands to all active sessions.
type MemoryRebalancer struct {
	mu           sync.Mutex
	memoryBudget uint64 // total bytes for all sessions
	threadBudget int    // total threads for all sessions
	sessions     SessionLister
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

	return &MemoryRebalancer{
		memoryBudget: memoryBudget,
		threadBudget: threadBudget,
		sessions:     sessions,
	}
}

// Rebalance computes new per-session limits and sends SET commands to all sessions.
// This is best-effort: errors on individual sessions are logged but don't fail the operation.
func (r *MemoryRebalancer) Rebalance(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sessions := r.sessions.AllSessions()
	count := len(sessions)
	if count == 0 {
		return
	}

	memLimit := r.perSessionMemory(count)
	threads := r.perSessionThreads(count)
	memStr := formatBytes(memLimit)

	slog.Info("Rebalancing session resources.",
		"sessions", count,
		"memory_per_session", memStr,
		"threads_per_session", threads,
		"memory_budget", formatBytes(r.memoryBudget),
		"thread_budget", r.threadBudget)

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
	return formatBytes(r.perSessionMemory(sessionCount))
}

// PerSessionThreads returns the current per-session thread count
// based on the given session count.
func (r *MemoryRebalancer) PerSessionThreads(sessionCount int) int {
	if sessionCount <= 0 {
		sessionCount = 1
	}
	return r.perSessionThreads(sessionCount)
}

func (r *MemoryRebalancer) perSessionMemory(count int) uint64 {
	limit := r.memoryBudget / uint64(count)
	if limit < minMemoryPerSession {
		limit = minMemoryPerSession
	}
	return limit
}

func (r *MemoryRebalancer) perSessionThreads(count int) int {
	threads := r.threadBudget / count
	if threads < 1 {
		threads = 1
	}
	return threads
}

// formatBytes formats a byte count as a human-readable DuckDB size string.
func formatBytes(b uint64) string {
	const (
		mb = 1024 * 1024
		gb = 1024 * mb
	)
	if b >= gb {
		return fmt.Sprintf("%dGB", b/gb)
	}
	return fmt.Sprintf("%dMB", b/mb)
}
