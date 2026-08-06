package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/server/usersecrets"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WorkerSwitcher swaps a connection's backing worker/session: the control
// plane destroys the current (stateless, exploratory) session and creates one
// on a normal-size worker, returning the new executor + worker identity.
type WorkerSwitcher func(ctx context.Context, reason string) (exec QueryExecutor, workerID int, workerPod string, err error)

// SessionActivator lazily acquires the connection's first worker/session.
// Installed by the control plane when the exploratory tier defers acquisition
// past connection startup, so a connection that never issues an
// engine-touching statement never spends a worker pod. Invoked on the
// message-loop goroutine by the first statement that needs an engine — the
// same goroutine that reads c.executor, so an activation can never race
// executor use or another activation.
//
// pinned=true means the first statement is ALREADY a pinning one: the control
// plane then acquires the escalation-target (standard) profile directly and
// marks the connection off-tier (MarkConnectionPinned), instead of acquiring
// the small worker only to escalate off it one statement later.
type SessionActivator func(ctx context.Context, pinned bool) (exec QueryExecutor, workerID int, workerPod string, err error)

const (
	escalateReasonState     = "state"
	escalateReasonOOM       = "oom"
	escalateReasonHeuristic = "heuristic"
)

// needsActivation reports whether this connection defers worker acquisition and
// has not acquired yet. False on every eager path, which is what keeps the
// lazy plumbing inert (and byte-for-byte free) outside the exploratory tier.
func (c *clientConn) needsActivation() bool {
	return c.executor == nil && c.sessionActivator != nil
}

// ensureSessionActive acquires the backing session on first need. No-op when an
// executor is already installed or no activator was configured (eager paths:
// standalone, process backend, GUC-sized, tier-disabled). Never re-acquires:
// moving an already-active connection to a bigger worker is escalateWorker's
// job, so a later pinned=true call is a no-op here.
func (c *clientConn) ensureSessionActive(ctx context.Context, pinned bool) error {
	if c.executor != nil || c.sessionActivator == nil {
		return nil
	}
	exec, workerID, workerPod, err := c.sessionActivator(ctx, pinned)
	if err != nil {
		// Leave the connection inactive: a half-installed executor would be
		// dereferenced by the next statement.
		return err
	}
	c.executor = exec
	c.workerID = workerID
	c.workerPod = workerPod
	// Deferred connect-time `-c duckgres.s3_cache=...`: it must be applied HERE,
	// after the executor is installed, exactly like escalateWorker re-applies the
	// bypass after a worker swap. Applying it inside the activator (before this
	// assignment) would find a nil executor, silently skip the worker swap, and
	// still flip the session flag — leaving SHOW reporting a transport the worker
	// is not using, the divergence applyS3CacheSetting exists to prevent. A
	// failure fails the activation, which is connection-fatal: a session that
	// asked for s3_cache=off must never silently run cached.
	if err := c.applyPendingS3CacheOption(); err != nil {
		// Classified like every other activation failure so the lazy path
		// presents the SAME FATAL shape as the eager connect path, which rejects
		// a bad/unappliable startup option with XX000. Without the wrap this
		// leaked through escalationErrorSQLState's substring fallback as a
		// generic 53400 (configuration_limit_exceeded) — a capacity-shaped
		// SQLSTATE for a transport-swap failure.
		return &SessionAcquireError{Code: "XX000", Message: err.Error(), Err: err}
	}
	return nil
}

// applyPendingS3CacheOption applies (once) a connect-time `duckgres.s3_cache`
// startup option that the control plane could not apply at connect because the
// connection had no worker yet. Cleared before the apply so a failed activation
// cannot re-run it against a second worker.
func (c *clientConn) applyPendingS3CacheOption() error {
	if !c.hasPendingS3Cache {
		return nil
	}
	raw := c.pendingS3Cache
	c.hasPendingS3Cache = false
	c.pendingS3Cache = ""
	return c.applyStartupS3Cache(raw)
}

// activateForS3CacheShow activates a lazily-acquired connection before
// answering `SHOW duckgres.s3_cache` — but ONLY when the answer would otherwise
// be a lie. A connection carrying a not-yet-applied connect-time
// `-c duckgres.s3_cache=off` still reports `on` until ensureSessionActive
// applies it, so that case must acquire first. With no pending option the
// session flag is already truthful (every SET path activates BEFORE flipping
// it, and a worker always starts on the cache-proxy transport), so SHOW stays
// engine-free: a client that only introspects never spends a worker pod, which
// is the whole point of lazy acquisition.
func (c *clientConn) activateForS3CacheShow(query string) error {
	if !c.hasPendingS3Cache {
		return nil
	}
	return c.activateForStatement(query, false)
}

// activateForStatement is ensureSessionActive with the connection-fatal
// failure handling every statement-path call site needs. A no-op (and free —
// no classification, no call) on every connection that is not lazily
// activated, which is what keeps the eager paths unchanged.
func (c *clientConn) activateForStatement(query string, pinned bool) error {
	if !c.needsActivation() {
		return nil
	}
	if err := c.ensureSessionActive(c.ctx, pinned); err != nil {
		return c.failWorkerActivation(query, err)
	}
	return nil
}

// Bounded outcome labels shared by the tier's two acquisition metrics —
// duckgres_exploratory_escalations_total here and
// duckgres_session_activation_total in the control plane. Every label is
// derived from the CLASSIFIED SQLSTATE, never from the error text, so the label
// set stays closed no matter what a worker or the K8s API says.
const (
	AcquisitionOutcomeOK       = "ok"
	AcquisitionOutcomeCanceled = "canceled"
	AcquisitionOutcomeCapacity = "capacity"
	AcquisitionOutcomeDraining = "draining"
	AcquisitionOutcomeDisabled = "disabled"
	AcquisitionOutcomeError    = "error"
)

// AcquisitionFailureOutcome maps a classified SQLSTATE to the bounded failure
// class label above. FAILURES only — each metric supplies its own success label
// (this package's counter uses AcquisitionOutcomeOK, the control plane's
// activation counter keeps its established "success"), because the two have
// been published under different names and dashboards read both.
//
// Draining and disabled are broken out from the generic error bucket
// deliberately: a control plane rolling out, and an operator disabling an
// account, each look identical to a broken cluster otherwise.
func AcquisitionFailureOutcome(code string) string {
	switch code {
	case "57014":
		return AcquisitionOutcomeCanceled
	case "53300":
		return AcquisitionOutcomeCapacity
	case "57P03":
		return AcquisitionOutcomeDraining
	case "28000":
		return AcquisitionOutcomeDisabled
	default:
		return AcquisitionOutcomeError
	}
}

var exploratoryEscalationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_exploratory_escalations_total",
	Help: "Attempts to escalate a connection off the exploratory small worker, by reason (state|oom|heuristic) and outcome (ok|canceled|capacity|draining|disabled|error).",
}, []string{"reason", "outcome"})

// errS3CacheReapplyFailed marks the ONE escalateWorker failure that is NOT
// connection-fatal: the worker swap itself succeeded (there is a healthy
// session on the standard worker and the connection is pinned to it), only the
// post-swap `duckgres.s3_cache` re-apply failed. See failS3CacheReapply.
var errS3CacheReapplyFailed = errors.New("s3_cache re-apply after worker switch failed")

// s3CacheReapplyError tags a re-apply failure with errS3CacheReapplyFailed
// WITHOUT the sentinel's text landing in the client-visible message — the
// wrapped error already names the GUC and the worker switch.
type s3CacheReapplyError struct{ err error }

func (e *s3CacheReapplyError) Error() string { return e.err.Error() }

func (e *s3CacheReapplyError) Unwrap() error { return e.err }

func (e *s3CacheReapplyError) Is(target error) bool { return target == errS3CacheReapplyFailed }

// escalateWorker moves the connection from the exploratory small worker to a
// normal-size worker. Sticky: once pinned, later calls are no-ops.
//
// Two failure shapes, and the difference is load-bearing:
//
//   - the SWITCHER failed: the connection's previous session is already gone
//     (the control-plane switcher destroys it before acquiring the target
//     worker), so there is nothing left to resynchronize to and callers MUST
//     treat it as connection-fatal (failWorkerEscalation);
//   - the post-swap s3_cache re-apply failed (errS3CacheReapplyFailed): the
//     escalation SUCCEEDED — healthy session, pin stands — so only the
//     statement fails (failS3CacheReapply). Callers route both through
//     failEscalation, which picks the right one.
func (c *clientConn) escalateWorker(ctx context.Context, reason string) error {
	if !c.onExploratoryWorker || c.workerSwitcher == nil {
		return nil
	}
	exec, workerID, workerPod, err := c.workerSwitcher(ctx, reason)
	if err != nil {
		exploratoryEscalationsTotal.WithLabelValues(reason, AcquisitionFailureOutcome(escalationErrorSQLState(err))).Inc()
		return err
	}
	c.executor = exec
	c.workerID = workerID
	c.workerPod = workerPod
	c.onExploratoryWorker = false
	// The switcher destroyed the previous session, so any suspended portal's
	// open rowset died with it. Destroy those portals now: a later Execute on
	// one gets an honest 34000 instead of a dangling rowset (or, worse, a
	// silent from-row-0 re-run presented as a continuation).
	c.closeSuspendedPortals()
	exploratoryEscalationsTotal.WithLabelValues(reason, AcquisitionOutcomeOK).Inc()
	c.logger().Info("Escalated connection off exploratory worker.", "reason", reason, "worker", workerID, "worker_pod", workerPod)
	// The `duckgres.s3_cache` bypass is worker-side state, and the new session
	// starts on the cache proxy — re-assert it or the connection silently
	// starts reading cached mid-flight. On failure the session state is reset
	// to match the transport the worker is actually in (SHOW must never lie)
	// and the STATEMENT fails; the escalation stands (outcome is already
	// counted "ok" above — the swap happened), so the pin is deliberately NOT
	// rolled back.
	if err := c.reapplyS3CacheAfterWorkerSwitch(ctx); err != nil {
		c.s3CacheOff = false
		return &s3CacheReapplyError{err: err}
	}
	return nil
}

// errConnectionFatal marks an error that must TERMINATE the connection rather
// than be reported and resumed at the next ReadyForQuery. The message loop
// checks for it and returns instead of continuing to read messages.
var errConnectionFatal = errors.New("connection terminated")

// SessionAcquireError carries an ALREADY-CLASSIFIED session-acquisition
// failure from the control plane to the client. The control plane owns every
// sentinel involved (*WorkerCapacityExhaustedError, the vCPU admission
// rejection, draining, cancellation, catalog init) but reaches this package
// through a plain `error`, so it classifies the failure with the same logic
// the eager connect path uses (sessionCreationErrorResponse) and hands the
// result across in this type. Code is the SQLSTATE and Message is exactly what
// the client should see — NOT the wrapped internal error chain.
type SessionAcquireError struct {
	Code    string
	Message string
	Err     error
}

func (e *SessionAcquireError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *SessionAcquireError) Unwrap() error { return e.Err }

// escalationErrorSQLState maps a failed worker acquisition (lazy activation or
// tier escalation) to the SQLSTATE the client sees.
//
// A *SessionAcquireError is authoritative: it was classified by the control
// plane with the same logic the eager connect path uses, so it never has to be
// guessed at. The substring fallback below stays for the paths that still
// return a plain error (the switcher's own wrapped sentinels, and any future
// caller that forgets to classify): keep it in sync with
// controlplane/worker_profile.go (disabledUserMessage) and
// controlplane/capacity_policy.go (capacityMissPolicy.errorString).
func escalationErrorSQLState(err error) string {
	if err == nil {
		return "53400"
	}
	var acq *SessionAcquireError
	if errors.As(err, &acq) && acq.Code != "" {
		return acq.Code
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "account is disabled"):
		return "28000" // invalid_authorization_specification
	case strings.Contains(msg, "worker capacity"):
		return "53300" // too_many_connections — the org/cluster is at its cap
	default:
		return "53400" // configuration_limit_exceeded
	}
}

// acquisitionClientMessage is the client-visible text for a failed worker
// acquisition: the control plane's own classified message when it supplied one,
// otherwise the caller's contextual fallback. This keeps a capacity/draining/
// admission failure reading exactly as it does when it happens at connect,
// instead of leaking the wrapped internal error chain.
func acquisitionClientMessage(err error, fallback string) string {
	var acq *SessionAcquireError
	if errors.As(err, &acq) && acq.Message != "" {
		return acq.Message
	}
	return fallback
}

// failWorkerEscalation terminates the connection after a failed tier
// escalation. By the time the switcher returns an error the connection's
// previous session is ALREADY destroyed (see escalateWorker), so there is no
// session left to resynchronize to: the client gets a FATAL ErrorResponse and
// NO ReadyForQuery, and the returned error unwinds the message loop.
//
// clientMessage is what the client sees: the escalation failure itself for a
// pinning statement, or the original query error for an OOM re-execute whose
// escalation could not be completed.
//
// The error is BOTH returned and parked on c.fatalErr: the simple-query path
// returns it up through handleQuery, while the extended-query handlers are void
// and rely on runExtendedQueryMessage reading it back for the message loop.
func (c *clientConn) failWorkerEscalation(query string, escErr error, clientMessage string) error {
	return c.failWorkerAcquisition(query, escErr, clientMessage, "exploratory worker escalation failed")
}

// failEscalation is the single entry point for an escalateWorker error: it
// routes the connection-fatal shape (the switcher failed, previous session
// gone) to failWorkerEscalation, and the statement-scoped shape (the swap
// succeeded, only the s3_cache re-apply failed) to failS3CacheReapply. Every
// call site uses it so the two can never be confused at one of them.
func (c *clientConn) failEscalation(query string, escErr error, clientMessage string) error {
	if errors.Is(escErr, errS3CacheReapplyFailed) {
		return c.failS3CacheReapply(query, escErr)
	}
	return c.failWorkerEscalation(query, escErr, clientMessage)
}

// errStatementAborted marks a failure that has ALREADY been reported to the
// client as a normal ERROR and must abort the current statement without
// terminating the connection. Callers propagate it like any other error (they
// must not execute the statement), and the message loop resumes reading — the
// client has its ErrorResponse and, on the simple protocol, its ReadyForQuery.
var errStatementAborted = errors.New("statement aborted")

// failS3CacheReapply reports a post-escalation `duckgres.s3_cache` re-apply
// failure. Unlike a failed escalation this is NOT connection-fatal: the
// escalation succeeded, so the connection has a healthy session on the standard
// worker and the pin stands (deliberately not rolled back — the swap really
// happened). Only the transport could not be re-asserted, so:
//
//   - the statement fails with a normal ERROR (XX000) naming the re-apply,
//     rather than a benchmark quietly continuing through the cache;
//   - the session flag was already reset by escalateWorker to the worker's
//     ACTUAL transport (proxied — a fresh session always starts on the cache
//     proxy), so SHOW stays truthful and the client can retry
//     `SET duckgres.s3_cache = off`;
//   - the connection stays alive.
//
// ReadyForQuery is written only on the simple protocol; inside an
// extended-query message Sync owns it, and writing one here would desync the
// client's response accounting.
func (c *clientConn) failS3CacheReapply(query string, err error) error {
	c.logQueryError(query, err)
	c.sendError("ERROR", "XX000", err.Error())
	c.setTxError()
	if !c.inExtendedMessage {
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
	}
	return fmt.Errorf("%w: %w", errStatementAborted, err)
}

// failWorkerActivation terminates the connection after a failed LAZY session
// activation. Same machinery and same contract as failWorkerEscalation: an
// activation failure also leaves the connection with no usable session — there
// was never one — so the client gets a FATAL ErrorResponse and NO
// ReadyForQuery, and the error unwinds the message loop.
func (c *clientConn) failWorkerActivation(query string, actErr error) error {
	return c.failWorkerAcquisition(query, actErr,
		acquisitionClientMessage(actErr,
			fmt.Sprintf("could not allocate a worker for this connection: %v", actErr)),
		"worker session activation failed")
}

// failWorkerAcquisition is the shared body of failWorkerEscalation and
// failWorkerActivation. Kept single so the two can never drift in SQLSTATE
// mapping, redaction, ReadyForQuery suppression, or fatalErr parking.
func (c *clientConn) failWorkerAcquisition(query string, acqErr error, clientMessage, logPhrase string) error {
	if !c.isCallerCancellation(acqErr) {
		c.logQueryError(query, acqErr)
	}
	c.sendError("FATAL", escalationErrorSQLState(acqErr), clientMessage)
	_ = c.flushWriter()
	err := fmt.Errorf("%w: %s: %w", errConnectionFatal, logPhrase, acqErr)
	c.fatalErr = err
	return err
}

// escalateForPinningStatement escalates the connection off the exploratory
// worker before a statement that writes or creates session state executes, so
// the small worker stays stateless by construction. Returns a
// connection-terminating error when the escalation fails; nil otherwise
// (including for every connection that is not on the exploratory tier).
func (c *clientConn) escalateForPinningStatement(query string) error {
	// The classification is deliberately behind the tier check: an off-tier
	// connection must not pay pg_query.Parse on every statement. A connection
	// that has not acquired yet still needs it — the classification is what
	// picks the profile the single lazy acquire lands on.
	//
	// (The two conditions are not independent today: the control plane only
	// installs an activator on a connection it also marks exploratory, so
	// needsActivation implies onExploratoryWorker. The second check is
	// vestigial defense — it keeps the hook correct if a future caller ever
	// installs an activator without the tier flag.)
	if !c.onExploratoryWorker && !c.needsActivation() {
		return nil
	}
	return c.escalateForPinningTier(query, classifyStatementTier(query) == tierPinning)
}

// escalateForSecretDDL escalates off the exploratory worker BEFORE the
// user-secrets interception (handleUserSecretDDLSimple / …Extended) runs. That
// interception owns its own execution and sits ABOVE the general pin hook on
// both protocols, so without this the statement would touch the small worker:
//
//   - a plain / TEMPORARY CREATE SECRET is session-scoped worker state, so
//     creating it on the exploratory worker and escalating later silently drops
//     the credential;
//   - the managed PERSISTENT path executes on the live session too (DuckDB
//     validates before the store write), and DROP SECRET likewise mutates
//     worker state.
//
// Detection is usersecrets.Classify, the same conservative-lexical classifier
// the interception itself keys on — so anything that CAN be intercepted is
// pinned first. A spelling Classify declines (KindNone) is not intercepted
// either: it falls through to the normal execution path, where pg_query cannot
// parse DuckDB's SECRET syntax and classifyStatementTier's parse-failure
// default pins it at the general hook. Both ends of that chain pin; only the
// timing differs.
func (c *clientConn) escalateForSecretDDL(query string) error {
	// needsActivation implies onExploratoryWorker today (the control plane sets
	// both together); the second check is vestigial defense, as in
	// escalateForPinningStatement.
	if !c.onExploratoryWorker && !c.needsActivation() {
		return nil
	}
	pins := usersecrets.Classify(query).Kind != usersecrets.KindNone
	if c.needsActivation() && !pins {
		// Lazy activation: this hook sits ABOVE the interceptions the control
		// plane answers itself, so it must only acquire for a statement the
		// secret interception will actually execute. Anything else continues to
		// the general hook below, which acquires on the right tier — or returns
		// without acquiring at all if it turns out to be engine-free.
		return nil
	}
	return c.escalateForPinningTier(query, pins)
}

// currentWorkerTier reports which worker tier the connection is executing on
// right now, for the query-log worker_tier column. logQueryStart reads it
// before escalation can happen, so a start event may say "exploratory" for a
// statement that goes on to escalate; logQuery reads it after execution, so
// the terminal event always reflects the tier the statement ULTIMATELY ran
// on. Both are correct for what each event represents.
func (c *clientConn) currentWorkerTier() string {
	if c.onExploratoryWorker {
		return "exploratory"
	}
	return "standard"
}

// escalateForPinningTier is escalateForPinningStatement for callers that
// already know the classification — the extended protocol classifies once, at
// Parse (preparedStmt.pinsWorker), rather than re-parsing at every Describe and
// Execute of the same prepared statement.
func (c *clientConn) escalateForPinningTier(query string, pins bool) error {
	// Lazy activation first, on the tier this statement needs: a pinning first
	// statement acquires the standard profile in ONE acquire (the control-plane
	// activator marks the connection off-tier), so the escalation below is then
	// a no-op. Inert on every eager connection.
	if err := c.activateForStatement(query, pins); err != nil {
		return err
	}
	if !c.onExploratoryWorker || !pins {
		return nil
	}
	if err := c.escalateWorker(c.ctx, escalateReasonState); err != nil {
		return c.failEscalation(query, err,
			acquisitionClientMessage(err,
				fmt.Sprintf("could not allocate a standard worker for this statement: %v", err)))
	}
	return nil
}
