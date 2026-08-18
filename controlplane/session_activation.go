package controlplane

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Lazy session activation (exploratory tier): the connection is handed to the
// message loop with NO worker, and the first statement that needs an engine
// acquires one. This file owns that acquisition — everything between "the
// client asked for an engine" and "the connection has an executor" — so it can
// be unit-tested without a cluster. The caller (handleConnection's activator
// closure) keeps only the clientConn-touching bookkeeping, which cannot live
// here: server.clientConn is unexported.

// sessionActivationOutcome is the bounded label set for the activation
// metrics. Derived from the SQLSTATE the failure was CLASSIFIED with (never
// from free text), so the label set stays closed.
type sessionActivationOutcome string

const (
	sessionActivationSuccess  sessionActivationOutcome = "success"
	sessionActivationCanceled sessionActivationOutcome = server.AcquisitionOutcomeCanceled
	sessionActivationCapacity sessionActivationOutcome = server.AcquisitionOutcomeCapacity
	sessionActivationDraining sessionActivationOutcome = server.AcquisitionOutcomeDraining
	sessionActivationDisabled sessionActivationOutcome = server.AcquisitionOutcomeDisabled
	sessionActivationError    sessionActivationOutcome = server.AcquisitionOutcomeError
)

// Activation metrics. Before lazy acquisition, a capacity/draining/admission
// failure at connect showed up in duckgres_session_start_* (BeginSessionStart
// covers the handshake only). A tiered connection's acquisition moved to the
// first statement, so it no longer lands there — these restore that visibility.
//
// Both carry an `org` label, like the sibling per-org session/acquire metrics
// (duckgres_worker_acquire_*): "which tenant is eating cold-spawn waits / hitting
// its cap" is the first question asked of them, and orgs are bounded
// managed-warehouse tenants so the cardinality is acceptable. Single-tenant
// stacks pass "" and land in one series.
var (
	sessionActivationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_session_activation_total",
		Help: "Lazy first-statement worker acquisitions on the exploratory tier, partitioned by org and outcome (success|canceled|capacity|draining|disabled|error).",
	}, []string{"org", "outcome"})

	sessionActivationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "duckgres_session_activation_duration_seconds",
		Help: "Wall time of a lazy first-statement worker acquisition (create + session init), successful or not, partitioned by org. A cold spawn dominates this.",
		// Cold worker spawns are minutes, not milliseconds — the buckets have to
		// reach far past the usual request-latency range or every cold acquire
		// lands in +Inf.
		Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
	}, []string{"org"})
)

// activationOutcomeForCode maps a classified SQLSTATE to the metric label.
// The failure classes come from server.AcquisitionFailureOutcome, the SAME
// helper the tier's escalation counter uses, so the two acquisition metrics
// can never drift into different (or unbounded) label sets. Only the success
// label differs — this counter has always published "success".
func activationOutcomeForCode(code string) sessionActivationOutcome {
	if code == "" {
		return sessionActivationSuccess
	}
	return sessionActivationOutcome(server.AcquisitionFailureOutcome(code))
}

// newSessionAcquireError classifies a session-acquisition failure with the SAME
// logic the eager connect path uses (sessionCreationErrorResponse) and packages
// the result for the server package, which cannot see the control plane's
// sentinels. Without this the server had to guess the SQLSTATE from the error
// text, which silently degraded a resource-admission rejection or a draining
// control plane into a generic 53400.
func newSessionAcquireError(err error) *server.SessionAcquireError {
	code, message := sessionCreationErrorResponse(err)
	if code == "57014" {
		// sessionCreationErrorResponse words 57014 for the connect path
		// ("canceling AUTHENTICATION due to user request") because that is where
		// it fires there. A lazy activation is cancelled by a CancelRequest
		// against an in-flight STATEMENT — authentication finished long ago — so
		// the connect wording would be actively misleading in a client's error
		// log. The connect path keeps its own wording; only this branch is
		// re-worded.
		message = "canceling statement due to user request"
	}
	return &server.SessionAcquireError{Code: code, Message: message, Err: err}
}

// activationSessions is the slice of *SessionManager a session acquisition
// uses. An interface only so the acquisition's ordering and bookkeeping can be
// unit-tested against a fake; production always passes the real manager.
type activationSessions interface {
	CreateSession(ctx context.Context, username string, pid int32, memoryLimit string, threads int, profile *WorkerProfile) (int32, *flightclient.FlightExecutor, error)
	DestroySession(pid int32)
	SetConnCloser(pid int32, closer io.Closer)
	WorkerIDForPID(pid int32) int
	WorkerPodNameForPID(pid int32) string
	SessionCount() int
}

var _ activationSessions = (*SessionManager)(nil)

// sessionActivationRequest is everything a lazy activation needs that is not
// derived from the acquisition itself. Every field is a connect-time constant
// for the connection except `pinned`, which is the first statement's tier
// classification.
type sessionActivationRequest struct {
	sessions activationSessions
	// srv + backendKey register the acquisition for cancellation, so a
	// CancelRequest aborts a slow first-statement acquire exactly as it aborted
	// the eager connect-time acquire.
	srv        *server.Server
	backendKey server.BackendKey
	pid        int32
	orgID      string
	username   string
	// connCloser is re-registered on the new session so OnWorkerCrash can close
	// the client socket.
	connCloser io.Closer
	// pinned: the first statement already pins, so acquire the escalation
	// target directly instead of the small worker.
	pinned bool
	// exploratoryProfile is the small tier; standardProfile is the escalation
	// target (the org/pool default a non-tiered connection would have started
	// on). nil is a valid profile — it means the pool default shape.
	exploratoryProfile *WorkerProfile
	standardProfile    *WorkerProfile
	meta               sessionMetadataInput
	baseClog           *slog.Logger
	// finish is the post-create wiring (metadata init, conn-closer
	// registration, disabled re-check). A seam: production passes
	// cp.finishSessionAcquisition, tests pass a stub so the surrounding
	// ordering is testable without a live worker.
	finish func(ctx context.Context, in sessionAcquisition) (sessionMetadataResult, *sessionInitError, error)
}

// sessionActivationResult is what the caller needs to finish wiring the
// clientConn once a worker exists.
type sessionActivationResult struct {
	exec      *flightclient.FlightExecutor
	workerID  int
	workerPod string
	// profile is the shape actually acquired — the caller stamps the billing
	// size from it.
	profile *WorkerProfile
	meta    sessionMetadataResult
	clog    *slog.Logger
	// sessionCreated reports that a session exists for this pid and the
	// caller's teardown must destroy it. False on every failure path: a create
	// that errored is cleaned up here (including the rare commit-then-cancel
	// race), and a post-create failure is destroyed by finishSessionAcquisition,
	// so the caller must NOT destroy again — that is the spurious
	// "unknown session" warn this flag exists to avoid.
	sessionCreated bool
}

// activateConnectionSession performs one lazy session acquisition: pick the
// profile from the first statement's tier, create the session (cancellable),
// then run the shared post-create wiring. Every failure it returns is already
// classified as a *server.SessionAcquireError, so the client sees the same
// SQLSTATE and message the eager connect path would have sent.
//
// Runs on the connection's message-loop goroutine, so it is single-threaded
// with statement handling and can never run concurrently with itself or with
// the tier-escalation switcher.
func (cp *ControlPlane) activateConnectionSession(
	ctx context.Context,
	req sessionActivationRequest,
) (res sessionActivationResult, err error) {
	start := time.Now()
	defer func() {
		code := ""
		var acq *server.SessionAcquireError
		if err != nil {
			code = "58000"
			if errors.As(err, &acq) && acq.Code != "" {
				code = acq.Code
			}
		}
		sessionActivationTotal.WithLabelValues(req.orgID, string(activationOutcomeForCode(code))).Inc()
		sessionActivationDuration.WithLabelValues(req.orgID).Observe(time.Since(start).Seconds())
	}()

	// pinned means the connection's FIRST statement already pins: take the
	// escalation target directly rather than acquiring the small worker and
	// escalating off it one statement later — a wasted acquire, a wasted pod,
	// and a wasted destroy.
	profile := req.exploratoryProfile
	if req.pinned {
		profile = req.standardProfile
	}
	res.profile = profile
	memLimit, threads := cp.workerDuckDBLimits(profile)

	// Registered for cancellation for the same reason the eager acquire is: a
	// cold worker spawn takes minutes, and a client that sends a CancelRequest
	// must be able to abort it rather than wait out WorkerQueueTimeout. The
	// cancellation surfaces as context.Canceled → 57014, classified below.
	_, exec, createErr := createSessionWithRegisteredCancel(
		ctx,
		req.srv,
		cp.cfg.WorkerQueueTimeout,
		req.backendKey,
		func(createCtx context.Context) (int32, *flightclient.FlightExecutor, error) {
			return req.sessions.CreateSession(createCtx, req.username, req.pid, memLimit, threads, profile)
		},
	)
	if cp.isDraining() {
		createErr = ErrSessionManagerDraining
	}
	if createErr != nil {
		// The create may have committed at the same instant its context was
		// canceled (CancelRequest / drain). Never retain that raced session
		// without a live client to own it — and clean it up HERE so the caller
		// can keep sessionCreated false and skip a second, spurious destroy.
		if exec != nil {
			req.sessions.DestroySession(req.pid)
			cp.observeActivationSessions(req.orgID, req.sessions)
		}
		return res, newSessionAcquireError(createErr)
	}
	res.sessionCreated = true
	cp.observeActivationSessions(req.orgID, req.sessions)

	res.exec = exec
	res.workerID = req.sessions.WorkerIDForPID(req.pid)
	res.workerPod = req.sessions.WorkerPodNameForPID(req.pid)
	res.clog = req.baseClog.With("worker", res.workerID, "worker_pod", res.workerPod)

	meta := req.meta
	meta.clog = res.clog
	// Deliberately NOT the create context: WorkerQueueTimeout budgets the
	// worker ACQUISITION, so reusing it here would leave init only whatever is
	// left of it. The caller's ctx gives each init step its full
	// SessionInitTimeout, exactly like the eager connect path.
	initMeta, initErr, gateErr := req.finish(ctx, sessionAcquisition{
		sessions: req.sessions,
		pid:      req.pid,
		orgID:    req.orgID,
		username: req.username,
		tlsConn:  req.connCloser,
		exec:     exec,
		meta:     meta,
	})
	switch {
	case initErr != nil:
		// finishSessionAcquisition already destroyed the session.
		res.sessionCreated = false
		// Keep the init failure's own SQLSTATE (e.g. 3D000 for an unavailable
		// catalog) rather than re-deriving one — degraded-mode diagnostics
		// depend on it.
		return res, &server.SessionAcquireError{Code: initErr.code, Message: initErr.message, Err: initErr}
	case gateErr != nil:
		res.sessionCreated = false
		return res, &server.SessionAcquireError{Code: "28000", Message: disabledUserMessage, Err: gateErr}
	}
	res.meta = initMeta
	return res, nil
}

// observeActivationSessions refreshes the per-org active-session gauge, no-op
// for a single-tenant stack (empty org).
func (cp *ControlPlane) observeActivationSessions(orgID string, sessions activationSessions) {
	if orgID != "" {
		observeOrgSessionsActive(orgID, sessions.SessionCount())
	}
}
