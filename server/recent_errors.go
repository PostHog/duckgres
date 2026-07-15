package server

import (
	"sync"
	"time"
)

// RecentError is a redacted, bounded snapshot of one failed query, for the
// admin Errors page. Message and Query are sanitized at the capture site — this
// struct must never carry raw SQL or a CREATE SECRET credential, since the
// Errors page surfaces both fields.
type RecentError struct {
	Time       time.Time `json:"time"`
	OrgID      string    `json:"org"`
	Username   string    `json:"user"`
	PID        int32     `json:"pid"`
	WorkerID   int       `json:"worker_id"`
	WorkerPod  string    `json:"worker_pod"`
	SQLState   string    `json:"sqlstate"` // classifyErrorCode
	Category   string    `json:"category"` // user | system | conflict | metadata_connection_lost
	Message    string    `json:"message"`  // REDACTED
	Query      string    `json:"query"`    // REDACTED
	ClientAddr string    `json:"client_addr"`
	TraceID    string    `json:"trace_id"`
}

// DefaultRecentErrorCap is how many recent errors each server retains in memory
// for the admin Errors page. It's a bounded live-triage buffer, not durable
// history — long-term error history lives in the external query-log pipeline.
const DefaultRecentErrorCap = 500

// recentErrorRing is a fixed-size, mutex-guarded ring of the most recent query
// errors. Adds are O(1); errors are rare relative to successful queries, so the
// single lock on the failure path is negligible.
type recentErrorRing struct {
	mu   sync.Mutex
	buf  []RecentError
	next int // index of the next write
	n    int // number of valid entries (<= cap)
}

func newRecentErrorRing(capacity int) *recentErrorRing {
	if capacity <= 0 {
		capacity = DefaultRecentErrorCap
	}
	return &recentErrorRing{buf: make([]RecentError, capacity)}
}

func (r *recentErrorRing) add(e RecentError) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.buf[r.next] = e
	r.next = (r.next + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
	r.mu.Unlock()
}

// snapshot returns up to limit most-recent errors, newest first (limit <= 0
// returns all retained).
func (r *recentErrorRing) snapshot(limit int) []RecentError {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	count := r.n
	if limit > 0 && limit < count {
		count = limit
	}
	out := make([]RecentError, 0, count)
	idx := (r.next - 1 + len(r.buf)) % len(r.buf)
	for i := 0; i < count; i++ {
		out = append(out, r.buf[idx])
		idx = (idx - 1 + len(r.buf)) % len(r.buf)
	}
	return out
}

// recordRecentError adds a redacted error to the server's ring (nil-safe).
func (s *Server) recordRecentError(e RecentError) {
	if s != nil {
		s.recentErrors.add(e)
	}
}

// RecentErrors returns up to limit of this server's most recent redacted query
// errors, newest first. Used by the control-plane admin Errors page (fanned out
// across replicas). limit <= 0 returns all retained.
func (s *Server) RecentErrors(limit int) []RecentError {
	if s == nil {
		return nil
	}
	return s.recentErrors.snapshot(limit)
}
