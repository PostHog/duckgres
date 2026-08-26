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
	"github.com/posthog/duckgres/server/usersecrets"
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

// instanceInvalidatedMarker is DuckDB core's text for an instance poisoned by an
// earlier Internal/Fatal exception. It comes from ErrorManager::InvalidatedDatabase
// and is duckdb-core, not fork-owned.
const instanceInvalidatedMarker = "database has been invalidated because of a previous fatal error"

// instanceProbeTimeout bounds the liveness probe. The probe runs off the health
// check's critical path (see probeInstanceLivenessAsync), so this only decides
// how long a hung probe pins one goroutine, not how long a health check takes.
const instanceProbeTimeout = 2 * time.Second

// instanceProbeStuckAfter is how long a single in-flight probe may run before we
// treat the prober as wedged and say so. QueryRowContext can block inside CGO
// past its context deadline (DuckDB holding an internal lock), and the probe is
// single-flight, so a permanently blocked probe would otherwise disable probe
// detection for the life of the process with no signal at all.
const instanceProbeStuckAfter = 30 * time.Second

// redactedInstanceReason replaces the engine error text on paths whose error may
// echo secret DDL that we cannot classify. See recordInstanceFatal.
const redactedInstanceReason = "(reason redacted: statement may carry secret DDL)"

// isInstanceFatalError reports whether err indicates the DuckDB INSTANCE (not
// the statement, and not the socket) is dead.
//
// The TYPED check is authoritative. The driver maps DuckDB's error prefixes onto
// *duckdb.Error, and ErrorTypeInternal / ErrorTypeFatal are exactly the two
// classes that invalidate the instance. OutOfMemory and Transaction are distinct
// types, so an OOM or a DuckLake commit conflict can never match here — those
// must keep retrying in place, never retire a worker.
//
// Do NOT add substring matches for "INTERNAL Error" / "FATAL Error". DuckDB
// echoes the offending SQL back in its error text ("LINE 1: <query>"), so those
// matched the USER'S OWN QUERY: `SELECT 'INTERNAL Error' + 1` is an ordinary
// binder error whose message contains the marker, and matching it handed every
// tenant a one-statement worker kill. TestInstanceFatalIgnoresEchoedQueryText is
// the regression.
//
// instanceInvalidatedMarker is kept as the one string fallback, for an error
// that reaches us already flattened (relayed as text, or a wrapping layer that
// dropped the type). It is long and specific enough that echoing it is a
// deliberate act rather than an accident, and it only ever fires AFTER an
// instance is already dead — the failure it guards against is missing a
// genuinely poisoned instance, not creating a new one.
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
	return strings.Contains(err.Error(), instanceInvalidatedMarker)
}

// instanceHealth tracks whether this process's DuckDB instance has been
// invalidated. The flag is sticky by construction: invalidation is permanent
// until the process restarts, so nothing may ever clear it.
type instanceHealth struct {
	mu          sync.Mutex
	invalidated bool
	reason      string
	probing     bool
	probeStart  time.Time
	probeStuck  bool
}

// note records the (already redacted) reason and reports whether this call was
// the transition.
func (h *instanceHealth) note(reason string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.invalidated {
		return false
	}
	h.invalidated = true
	h.reason = reason
	return true
}

func (h *instanceHealth) invalid() (bool, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.invalidated, h.reason
}

// noteInstanceError flags the pool's DuckDB instance as invalidated when err is
// instance-fatal. Best-effort and off the hot path: it never fails a query.
//
// query is the statement whose failure produced err (may be ""). It is used
// ONLY to decide whether the error text may echo secret DDL — pass the
// ORIGINAL, un-redacted statement, which is what usersecrets classification
// needs.
func (p *SessionPool) noteInstanceError(query string, err error) {
	if p == nil || !isInstanceFatalError(err) {
		return
	}
	p.recordInstanceFatal(usersecrets.RedactErrorForLog(query, err.Error()))
}

// noteInstanceErrorOpaque is noteInstanceError for paths whose error may echo
// secret material that is not available to us as a query string — session
// create replays the user's persistent CREATE SECRET statements, so its error
// can carry a credential with no statement to classify. The reason is always
// redacted; over-redaction costs diagnostic detail, never a credential.
func (p *SessionPool) noteInstanceErrorOpaque(err error) {
	if p == nil || !isInstanceFatalError(err) {
		return
	}
	p.recordInstanceFatal(redactedInstanceReason)
}

// recordInstanceFatal stores the reason and logs it exactly once per process.
// reason MUST already be redacted: it is logged here, shipped to the control
// plane as instance_invalid_reason, and logged again there on retire, so an
// un-redacted engine error would leak a credential into three sinks (DuckDB
// echoes the offending SQL, including `CREATE SECRET ... '<credential>'`).
func (p *SessionPool) recordInstanceFatal(reason string) {
	if !p.instance.note(reason) {
		return
	}
	slog.Error("DuckDB instance invalidated by a fatal engine error; worker must be retired.",
		"reason", reason, "worker_id", p.workerID)
	instanceInvalidatedTotal.Inc()
	instanceInvalidatedStateGauge.Set(1)
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

// InstanceInvalidReason returns the REDACTED error text that invalidated the
// instance, or "" if it is healthy. Surfaced to the control plane so an operator
// sees why the pod died, not just "unhealthy".
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
// A probe stuck past instanceProbeStuckAfter raises
// duckgres_worker_instance_probe_stuck rather than silently disabling detection.
// We deliberately do NOT start a replacement probe: QueryRowContext can block in
// CGO past its deadline, so re-probing every health-check tick would leak a
// goroutine per tick against an instance that is already wedged. The statement
// and session-create taps still flag invalidation while this is set.
//
// The probe uses controlDB — the side connection that does not queue behind a
// long-running client query — falling back to the main handle before activation.
func (p *SessionPool) probeInstanceLivenessAsync() {
	if p == nil || p.InstanceInvalidated() {
		return // sticky: nothing to learn from probing a known-dead instance
	}

	p.instance.mu.Lock()
	if p.instance.probing {
		stuck := !p.instance.probeStart.IsZero() &&
			time.Since(p.instance.probeStart) > instanceProbeStuckAfter
		firstReport := stuck && !p.instance.probeStuck
		if stuck {
			p.instance.probeStuck = true
		}
		started := p.instance.probeStart
		p.instance.mu.Unlock()
		if firstReport {
			instanceProbeStuckGauge.Set(1)
			slog.Warn("DuckDB liveness probe has not returned; probe-based invalidation detection is degraded.",
				"probe_started", started, "stuck_after", instanceProbeStuckAfter, "worker_id", p.workerID)
		}
		return
	}
	p.instance.probing = true
	p.instance.probeStart = time.Now()
	p.instance.mu.Unlock()

	p.mu.RLock()
	db := p.controlDB
	if db == nil {
		db = p.warmupDB
	}
	p.mu.RUnlock()

	release := func() {
		p.instance.mu.Lock()
		p.instance.probing = false
		p.instance.probeStart = time.Time{}
		if p.instance.probeStuck {
			p.instance.probeStuck = false
			instanceProbeStuckGauge.Set(0)
		}
		p.instance.mu.Unlock()
	}

	if db == nil {
		// Not activated yet — there is no instance to poison.
		release()
		return
	}

	go func() {
		defer release()
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
	// so a busy or slow instance is left alone. The probe SQL is a constant with
	// no secret material, so it is safe to classify against.
	p.noteInstanceError("SELECT 1", err)
}
