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

const (
	escalateReasonState     = "state"
	escalateReasonOOM       = "oom"
	escalateReasonHeuristic = "heuristic"
)

var exploratoryEscalationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_exploratory_escalations_total",
	Help: "Connections escalated off the exploratory small worker, by reason (state|oom|heuristic).",
}, []string{"reason"})

// escalateWorker moves the connection from the exploratory small worker to a
// normal-size worker. Sticky: once pinned, later calls are no-ops. On failure
// the connection's previous session is already gone (the control-plane switcher
// destroys it before acquiring the target worker), so callers MUST treat a
// failed escalation as connection-fatal: surface the error to the client and
// terminate the connection. The query-path integration implements that
// termination.
func (c *clientConn) escalateWorker(ctx context.Context, reason string) error {
	if !c.onExploratoryWorker || c.workerSwitcher == nil {
		return nil
	}
	exec, workerID, workerPod, err := c.workerSwitcher(ctx, reason)
	if err != nil {
		return err
	}
	c.executor = exec
	c.workerID = workerID
	c.workerPod = workerPod
	c.onExploratoryWorker = false
	exploratoryEscalationsTotal.WithLabelValues(reason).Inc()
	c.logger().Info("Escalated connection off exploratory worker.", "reason", reason, "worker", workerID, "worker_pod", workerPod)
	// The `duckgres.s3_cache` bypass is worker-side state, and the new session
	// starts on the cache proxy — re-assert it or the connection silently
	// starts reading cached mid-flight. On failure the session state is reset
	// to match the transport the worker is actually in (SHOW must never lie)
	// and the statement fails, rather than a benchmark quietly going cached.
	if err := c.reapplyS3CacheAfterWorkerSwitch(ctx); err != nil {
		c.s3CacheOff = false
		return err
	}
	return nil
}

// errConnectionFatal marks an error that must TERMINATE the connection rather
// than be reported and resumed at the next ReadyForQuery. The message loop
// checks for it and returns instead of continuing to read messages.
var errConnectionFatal = errors.New("connection terminated")

// escalationErrorSQLState maps a failed worker escalation to the SQLSTATE the
// client sees. The control plane owns the actual sentinels
// (errEscalationUserDisabled, *WorkerCapacityExhaustedError) but reaches this
// package through a plain `error`, so this matches on the client-visible
// message text — the same idiom as every classifier in conn_errors.go. Keep it
// in sync with controlplane/worker_profile.go (disabledUserMessage) and
// controlplane/capacity_policy.go (capacityMissPolicy.errorString).
func escalationErrorSQLState(err error) string {
	if err == nil {
		return "53400"
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
	if !c.isCallerCancellation(escErr) {
		c.logQueryError(query, escErr)
	}
	c.sendError("FATAL", escalationErrorSQLState(escErr), clientMessage)
	_ = c.flushWriter()
	err := fmt.Errorf("%w: exploratory worker escalation failed: %w", errConnectionFatal, escErr)
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
	// connection must not pay pg_query.Parse on every statement.
	if !c.onExploratoryWorker {
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
	if !c.onExploratoryWorker {
		return nil
	}
	return c.escalateForPinningTier(query, usersecrets.Classify(query).Kind != usersecrets.KindNone)
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
	if !c.onExploratoryWorker || !pins {
		return nil
	}
	if err := c.escalateWorker(c.ctx, escalateReasonState); err != nil {
		return c.failWorkerEscalation(query, err,
			fmt.Sprintf("could not allocate a standard worker for this statement: %v", err))
	}
	return nil
}
