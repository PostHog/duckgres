package duckdbservice

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// A DuckDB Internal- or Fatal-class exception does not just fail one statement:
// it poisons the whole database instance. Every subsequent statement on ANY
// connection to that instance — including a brand new session on a reused
// hot-idle worker — fails with the invalidated-database wrapper until the
// process is restarted. DuckLake's commit path is a known source (an
// InternalException raised inside the commit retry loop is rethrown by
// ErrorData::Throw with its original type, so it reaches us as INTERNAL and
// invalidates the instance).
//
// One session per worker means the blast radius SHOULD be one pod, but nothing
// retired the pod: the health check never executed SQL, so a poisoned instance
// passed every check and stayed schedulable. This file is the detection half;
// the control plane retires the worker on the signal (see
// healthCheckResult.InstanceInvalidated).
//
// The classifier deliberately errs toward false positives. A false positive
// costs one worker respawn; a false negative leaves the org's next connection
// landing on a dead instance, which is the failure this exists to prevent.

// instanceInvalidatedMarker is DuckDB core's text for an instance poisoned by an
// earlier Internal/Fatal exception. It comes from ErrorManager::InvalidatedDatabase
// and is duckdb-core, not fork-owned.
const instanceInvalidatedMarker = "database has been invalidated because of a previous fatal error"

// instanceProbeTimeout bounds the liveness probe. The probe runs off the health
// check's critical path (see probeInstanceLivenessAsync), so this only decides
// how long a hung probe pins one goroutine, not how long a health check takes.
const instanceProbeTimeout = 2 * time.Second

// isInstanceFatalError reports whether err indicates the DuckDB INSTANCE (not
// the statement, and not the socket) is dead.
//
// Typed check first: the driver maps DuckDB's error prefixes onto *duckdb.Error,
// and ErrorTypeInternal / ErrorTypeFatal are exactly the two classes that
// invalidate the instance. OutOfMemory and Transaction errors are distinct
// types, so an OOM or a DuckLake commit conflict can never false-positive here —
// those must keep retrying in place, never retire a worker.
//
// The string checks are a fallback for errors that reach us already flattened
// (wrapped by database/sql, or relayed as text). instanceInvalidatedMarker also
// catches the case where the ORIGINAL fatal happened before this process started
// classifying — e.g. inside a code path that swallowed the error — and only the
// downstream wrapper is visible.
func isInstanceFatalError(err error) bool {
	if err == nil {
		return false
	}
	var dbErr *duckdb.Error
	if errors.As(err, &dbErr) {
		switch dbErr.Type {
		case duckdb.ErrorTypeInternal, duckdb.ErrorTypeFatal:
			return true
		}
	}
	msg := err.Error()
	return strings.Contains(msg, instanceInvalidatedMarker) ||
		strings.Contains(msg, "INTERNAL Error") ||
		strings.Contains(msg, "FATAL Error")
}

// instanceHealth tracks whether this process's DuckDB instance has been
// invalidated. The flag is sticky by construction: invalidation is permanent
// until the process restarts, so nothing may ever clear it.
type instanceHealth struct {
	mu          sync.Mutex
	invalidated bool
	reason      string
	probing     bool
}

// note records err as an instance-fatal failure if it classifies as one, and
// reports whether this call was the transition. Safe to call on every error.
func (h *instanceHealth) note(err error) bool {
	if !isInstanceFatalError(err) {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.invalidated {
		return false
	}
	h.invalidated = true
	h.reason = err.Error()
	return true
}

func (h *instanceHealth) invalid() (bool, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.invalidated, h.reason
}

// noteInstanceError flags the pool's DuckDB instance as invalidated when err is
// instance-fatal. Best-effort and off the hot path: it never fails a query, and
// the loud log is emitted exactly once per process.
func (p *SessionPool) noteInstanceError(err error) {
	if p == nil {
		return
	}
	if p.instance.note(err) {
		slog.Error("DuckDB instance invalidated by a fatal engine error; worker must be retired.",
			"error", err, "worker_id", p.workerID)
		instanceInvalidatedTotal.Inc()
		instanceInvalidatedGauge.Set(1)
	}
}

// InstanceInvalidated reports whether this worker's DuckDB instance is dead.
// Once true it never goes back to false.
func (p *SessionPool) InstanceInvalidated() bool {
	if p == nil {
		return false
	}
	invalid, _ := p.instance.invalid()
	return invalid
}

// InstanceInvalidReason returns the error text that invalidated the instance,
// or "" if it is healthy. Surfaced to the control plane so an operator sees the
// originating engine error on the pod that died, not just "unhealthy".
func (p *SessionPool) InstanceInvalidReason() string {
	if p == nil {
		return ""
	}
	_, reason := p.instance.invalid()
	return reason
}

// probeInstanceLivenessAsync runs a trivial SELECT 1 against the DuckDB instance
// to detect invalidation that no in-flight statement reported — for example a
// fatal thrown on a session that has since been destroyed, which would otherwise
// leave the pod hot-idle and schedulable.
//
// It runs ASYNCHRONOUSLY, at most one probe in flight, and the caller reads the
// flag rather than waiting. The control plane gives a health check a 3s budget
// that is already shared with per-session progress polling; a probe that blocks
// (DuckDB holding an internal lock during a large httpfs read) must not be able
// to push the whole check past that deadline and get a HEALTHY worker killed for
// unresponsiveness. Detection is therefore delayed by at most one health-check
// interval, which is the same latency an inline probe would give whenever the
// probe is the slow part.
//
// The probe uses controlDB — the side connection that does not queue behind a
// long-running client query — falling back to the main handle before activation.
func (p *SessionPool) probeInstanceLivenessAsync() {
	if p == nil || p.InstanceInvalidated() {
		return // sticky: nothing to learn from probing a known-dead instance
	}

	p.instance.mu.Lock()
	if p.instance.probing {
		p.instance.mu.Unlock()
		return
	}
	p.instance.probing = true
	p.instance.mu.Unlock()

	p.mu.RLock()
	db := p.controlDB
	if db == nil {
		db = p.warmupDB
	}
	p.mu.RUnlock()

	if db == nil {
		// Not activated yet — there is no instance to poison.
		p.instance.mu.Lock()
		p.instance.probing = false
		p.instance.mu.Unlock()
		return
	}

	go func() {
		defer func() {
			p.instance.mu.Lock()
			p.instance.probing = false
			p.instance.mu.Unlock()
		}()
		p.probeInstanceLiveness(db)
	}()
}

// probeInstanceLiveness executes the probe synchronously. Split out so tests can
// drive it without the single-flight goroutine.
func (p *SessionPool) probeInstanceLiveness(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), instanceProbeTimeout)
	defer cancel()

	var one int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
	// A timeout, cancellation, or ordinary failure is NOT evidence of
	// invalidation — noteInstanceError only flags on the fatal classification,
	// so a busy or slow instance is left alone.
	p.noteInstanceError(err)
}
