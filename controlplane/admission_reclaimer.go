package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultAdmissionReclaimerWorkers           = 4
	defaultAdmissionReclaimerReservations      = 4096
	defaultAdmissionReclaimAttemptTimeout      = 5 * time.Second
	defaultAdmissionReclaimInitialBackoff      = 250 * time.Millisecond
	defaultAdmissionReclaimMaximumBackoff      = 30 * time.Second
	defaultAdmissionReclaimDrainMaximumBackoff = 100 * time.Millisecond
	admissionReclaimAttemptOutcomeSuccess      = "success"
	admissionReclaimAttemptOutcomeError        = "error"
	admissionReclaimReservationRejectFull      = "full"
	admissionReclaimReservationRejectClosed    = "closed"
	admissionReclaimReservationRejectDuplicate = "duplicate"
)

var (
	ErrAdmissionReclaimerClosed               = errors.New("admission reclaimer is closed")
	ErrAdmissionReclaimerFull                 = errors.New("admission reclaimer reservation capacity exhausted")
	errAdmissionReclaimerDuplicateReservation = errors.New("admission reclaimer reference is already reserved")

	orgConnectionReclaimPendingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_session_admission_reclaim_pending",
		Help: "Number of activated session admission cleanup intents retained by this control-plane replica.",
	})
	orgConnectionReclaimAttemptsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_session_admission_reclaim_attempts_total",
		Help: "Session admission cleanup attempts by outcome.",
	}, []string{"outcome"})
	orgConnectionReclaimReservationsInUseGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_session_admission_reclaim_reservations_in_use",
		Help: "Number of admission cleanup reservations held before enqueue, while queued, by live leases, or by pending cleanup in this control-plane process.",
	})
	orgConnectionReclaimReservationCapacityGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_session_admission_reclaim_reservation_capacity",
		Help: "Total admission cleanup reservation capacity of reclaimers in this control-plane process.",
	})
	orgConnectionReclaimReservationRejectionsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_session_admission_reclaim_reservation_rejections_total",
		Help: "Admission cleanup reservation attempts rejected before durable admission work, by reason.",
	}, []string{"reason"})
)

// AdmissionReclaimCause identifies why ownership of an admission cleanup was
// transferred to the reclaimer.
type AdmissionReclaimCause string

const (
	// AdmissionReclaimCauseAcquireAbandoned cleans up an admission whose owner
	// stopped waiting before it received a lease handle.
	AdmissionReclaimCauseAcquireAbandoned AdmissionReclaimCause = "acquire_abandoned"
	// AdmissionReclaimCauseLeaseRelease cleans up a completed session's lease.
	AdmissionReclaimCauseLeaseRelease AdmissionReclaimCause = "lease_release"

	admissionReclaimCauseAcquireAbandoned = AdmissionReclaimCauseAcquireAbandoned
	admissionReclaimCauseLeaseRelease     = AdmissionReclaimCauseLeaseRelease
)

type admissionReclaimCause = AdmissionReclaimCause

// AdmissionReclaimReservation owns one bounded reclaimer slot before any
// durable admission write. Reclaim is infallible and idempotent: it activates
// the already-owned slot, so cleanup cannot be lost to later capacity or close
// races.
type AdmissionReclaimReservation interface {
	Reclaim(AdmissionReclaimCause)
}

type admissionReclaimer interface {
	Reserve(configstore.OrgConnectionAdmissionRef) (AdmissionReclaimReservation, error)
	DrainAndClose(context.Context) error
}

type admissionReclaimStore interface {
	ReclaimOrgConnectionAdmissionContext(context.Context, configstore.OrgConnectionAdmissionRef) error
}

// AdmissionReclaimerConfig controls the bounded work performed by an
// AdmissionReclaimer. Zero values select production defaults. RetryBackoff is
// injectable so tests can advance retries without waiting on production
// delays; its argument is the number of consecutive failed attempts.
type AdmissionReclaimerConfig struct {
	MaxWorkers      int
	MaxReservations int
	AttemptTimeout  time.Duration
	RetryBackoff    func(failures int) time.Duration
	Logger          *slog.Logger
}

// AdmissionReclaimer reserves cleanup ownership before durable admission work.
// Activated intents remain in its bounded, deduplicating map until the config
// store confirms the idempotent cleanup. Store work is detached from
// connection/session contexts, bounded globally, and serialized per org to
// avoid piling up behind the same advisory lock.
type AdmissionReclaimer struct {
	store           admissionReclaimStore
	log             *slog.Logger
	maxWorkers      int
	maxReservations int
	attemptTimeout  time.Duration
	retryBackoff    func(int) time.Duration

	runCtx context.Context
	cancel context.CancelFunc
	wake   chan struct{}

	mu            sync.Mutex
	pending       map[configstore.OrgConnectionAdmissionRef]*admissionReclaimIntent
	orgInFlight   map[string]bool
	inFlight      int
	activePending int
	nextToken     uint64
	nextSequence  uint64
	closed        bool
	drainDone     chan struct{}
	drainOnce     sync.Once

	pendingMetricContribution     int
	reservationMetricContribution int
	capacityMetricContribution    int
	metricContributionsReleased   bool

	wg       sync.WaitGroup
	stopOnce sync.Once
	stopped  chan struct{}

	pendingStopLogOnce sync.Once
}

type admissionReclaimIntent struct {
	ref         configstore.OrgConnectionAdmissionRef
	token       uint64
	active      bool
	cause       admissionReclaimCause
	failures    int
	nextAttempt time.Time
	inFlight    bool
	queuedAt    time.Time
	sequence    uint64
}

type admissionReclaimReservation struct {
	reclaimer *AdmissionReclaimer
	ref       configstore.OrgConnectionAdmissionRef
	token     uint64
}

func (r *admissionReclaimReservation) Reclaim(cause AdmissionReclaimCause) {
	if r == nil || r.reclaimer == nil {
		return
	}
	r.reclaimer.activateReservation(r.ref, r.token, cause)
}

func NewAdmissionReclaimer(store admissionReclaimStore, cfg AdmissionReclaimerConfig) *AdmissionReclaimer {
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = defaultAdmissionReclaimerWorkers
	}
	if cfg.MaxReservations <= 0 {
		cfg.MaxReservations = defaultAdmissionReclaimerReservations
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = defaultAdmissionReclaimAttemptTimeout
	}
	if cfg.RetryBackoff == nil {
		cfg.RetryBackoff = defaultAdmissionReclaimBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	runCtx, cancel := context.WithCancel(context.Background())
	r := &AdmissionReclaimer{
		store:                      store,
		log:                        cfg.Logger,
		maxWorkers:                 cfg.MaxWorkers,
		maxReservations:            cfg.MaxReservations,
		attemptTimeout:             cfg.AttemptTimeout,
		retryBackoff:               cfg.RetryBackoff,
		runCtx:                     runCtx,
		cancel:                     cancel,
		wake:                       make(chan struct{}, 1),
		pending:                    make(map[configstore.OrgConnectionAdmissionRef]*admissionReclaimIntent),
		orgInFlight:                make(map[string]bool),
		drainDone:                  make(chan struct{}),
		stopped:                    make(chan struct{}),
		capacityMetricContribution: cfg.MaxReservations,
	}
	orgConnectionReclaimReservationCapacityGauge.Add(float64(cfg.MaxReservations))
	r.wg.Add(1)
	go r.run()
	return r
}

// Reserve owns bounded cleanup capacity for ref before the caller touches the
// durable admission store. The returned reservation must remain owned through
// the lease lifetime and be activated with Reclaim on every terminal path.
func (r *AdmissionReclaimer) Reserve(ref configstore.OrgConnectionAdmissionRef) (AdmissionReclaimReservation, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("admission reclaimer is unavailable")
	}
	if err := validateAdmissionReclaimRef(ref); err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectClosed).Inc()
		return nil, ErrAdmissionReclaimerClosed
	}
	if _, exists := r.pending[ref]; exists {
		r.mu.Unlock()
		orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectDuplicate).Inc()
		return nil, errAdmissionReclaimerDuplicateReservation
	}
	if len(r.pending) >= r.maxReservations {
		r.mu.Unlock()
		orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectFull).Inc()
		return nil, ErrAdmissionReclaimerFull
	}
	r.nextToken++
	token := r.nextToken
	r.pending[ref] = &admissionReclaimIntent{
		ref:   ref,
		token: token,
	}
	r.observeMetricsLocked()
	r.mu.Unlock()

	return &admissionReclaimReservation{reclaimer: r, ref: ref, token: token}, nil
}

// Submit directly transfers ownership of an already-cleanup-ready ref. Runtime
// admission uses Reserve so it cannot lose cleanup ownership to this method's
// capacity check after an ambiguous database write. Repeated submissions of an
// active, fully-scoped reference are idempotent.
func (r *AdmissionReclaimer) Submit(ref configstore.OrgConnectionAdmissionRef, cause AdmissionReclaimCause) error {
	if r == nil || r.store == nil {
		return fmt.Errorf("admission reclaimer is unavailable")
	}
	if err := validateAdmissionReclaimRef(ref); err != nil {
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrAdmissionReclaimerClosed
	}
	if existing, ok := r.pending[ref]; ok {
		activated := r.activateIntentLocked(existing, cause, time.Now())
		if !activated && existing.cause == "" && cause != "" {
			existing.cause = admissionReclaimCause(cause)
		}
		r.mu.Unlock()
		if activated {
			r.signalWake()
		}
		return nil
	}
	if len(r.pending) >= r.maxReservations {
		r.mu.Unlock()
		orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectFull).Inc()
		return ErrAdmissionReclaimerFull
	}
	now := time.Now()
	r.nextToken++
	r.nextSequence++
	r.pending[ref] = &admissionReclaimIntent{
		ref:         ref,
		token:       r.nextToken,
		active:      true,
		cause:       admissionReclaimCause(cause),
		nextAttempt: now,
		queuedAt:    now,
		sequence:    r.nextSequence,
	}
	r.activePending++
	r.observeMetricsLocked()
	r.mu.Unlock()
	r.signalWake()
	return nil
}

func (r *AdmissionReclaimer) activateReservation(ref configstore.OrgConnectionAdmissionRef, token uint64, cause AdmissionReclaimCause) {
	now := time.Now()
	r.mu.Lock()
	intent, ok := r.pending[ref]
	activated := ok && intent.token == token && r.activateIntentLocked(intent, cause, now)
	r.mu.Unlock()
	if activated {
		r.signalWake()
	}
}

func (r *AdmissionReclaimer) activateIntentLocked(intent *admissionReclaimIntent, cause AdmissionReclaimCause, now time.Time) bool {
	if intent == nil || intent.active {
		return false
	}
	intent.active = true
	intent.cause = admissionReclaimCause(cause)
	intent.failures = 0
	intent.nextAttempt = now
	intent.queuedAt = now
	r.nextSequence++
	intent.sequence = r.nextSequence
	r.activePending++
	r.observeMetricsLocked()
	return true
}

// DrainAndClose stops accepting new reservations and retries retained work
// until it drains or ctx expires. Callers must first join every producer that
// can still use a reservation; any held reservation left at that boundary is
// activated as invariant recovery. On timeout it cancels the workers and
// returns without waiting for a store call that ignores cancellation; Shutdown
// can be used to join those workers. Remaining durable admission rows are left
// to control-plane instance expiry recovery.
func (r *AdmissionReclaimer) DrainAndClose(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.beginClose()

	select {
	case <-r.drainDone:
		return r.finishDrain(ctx)
	default:
	}

	select {
	case <-r.drainDone:
		return r.finishDrain(ctx)
	case <-ctx.Done():
		r.logPendingOnStop("drain_timeout")
		r.startShutdown()
		return ctx.Err()
	case <-r.stopped:
		return ErrAdmissionReclaimerClosed
	}
}

// Shutdown immediately closes submission and cancels in-flight store calls.
// It is idempotent. Callers that want best-effort completion should use
// DrainAndClose with a bounded context instead.
func (r *AdmissionReclaimer) Shutdown() {
	if r == nil {
		return
	}
	r.beginClose()
	r.logPendingOnStop("shutdown")
	r.startShutdown()
	<-r.stopped
}

func (r *AdmissionReclaimer) finishDrain(ctx context.Context) error {
	r.startShutdown()
	select {
	case <-r.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *AdmissionReclaimer) startShutdown() {
	r.stopOnce.Do(func() {
		r.cancel()
		r.releaseMetricContributions()
		go func() {
			r.wg.Wait()
			close(r.stopped)
		}()
	})
}

func (r *AdmissionReclaimer) run() {
	defer r.wg.Done()

	for {
		wait, launched := r.dispatchReady()
		if launched {
			continue
		}

		if wait < 0 {
			select {
			case <-r.runCtx.Done():
				return
			case <-r.wake:
			}
			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-r.runCtx.Done():
			timer.Stop()
			return
		case <-r.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// dispatchReady starts every currently-due intent that fits the global and
// per-org bounds. It returns the delay until the next schedulable intent, or a
// negative duration when only a submission/completion can make progress.
func (r *AdmissionReclaimer) dispatchReady() (time.Duration, bool) {
	now := time.Now()
	var ready []configstore.OrgConnectionAdmissionRef
	var next time.Time
	var intents []*admissionReclaimIntent

	r.mu.Lock()
	if r.runCtx.Err() != nil {
		r.mu.Unlock()
		return -1, false
	}
	intents = make([]*admissionReclaimIntent, 0, len(r.pending))
	for _, intent := range r.pending {
		if intent.active {
			intents = append(intents, intent)
		}
	}
	sort.Slice(intents, func(i, j int) bool {
		if !intents[i].nextAttempt.Equal(intents[j].nextAttempt) {
			return intents[i].nextAttempt.Before(intents[j].nextAttempt)
		}
		return intents[i].sequence < intents[j].sequence
	})
	for _, intent := range intents {
		if r.inFlight >= r.maxWorkers {
			break
		}
		ref := intent.ref
		if intent.inFlight || r.orgInFlight[ref.OrgID] {
			continue
		}
		if intent.nextAttempt.After(now) {
			if next.IsZero() || intent.nextAttempt.Before(next) {
				next = intent.nextAttempt
			}
			continue
		}

		intent.inFlight = true
		r.orgInFlight[ref.OrgID] = true
		r.inFlight++
		ready = append(ready, ref)
	}
	r.mu.Unlock()

	for _, ref := range ready {
		r.wg.Add(1)
		go r.attempt(ref)
	}
	if len(ready) > 0 {
		return 0, true
	}
	if next.IsZero() {
		return -1, false
	}
	if !next.After(now) {
		return 0, false
	}
	return next.Sub(now), false
}

func (r *AdmissionReclaimer) attempt(ref configstore.OrgConnectionAdmissionRef) {
	defer r.wg.Done()

	ctx, cancel := context.WithTimeout(r.runCtx, r.attemptTimeout)
	err := r.store.ReclaimOrgConnectionAdmissionContext(ctx, ref)
	cancel()
	if err == nil {
		orgConnectionReclaimAttemptsCounter.WithLabelValues(admissionReclaimAttemptOutcomeSuccess).Inc()
	} else {
		orgConnectionReclaimAttemptsCounter.WithLabelValues(admissionReclaimAttemptOutcomeError).Inc()
	}
	r.completeAttempt(ref, err)
}

func (r *AdmissionReclaimer) completeAttempt(ref configstore.OrgConnectionAdmissionRef, attemptErr error) {
	now := time.Now()
	var failures int
	var queuedAt time.Time
	var cause admissionReclaimCause
	var retryAfter time.Duration
	var calculateBackoff bool

	r.mu.Lock()
	intent, ok := r.pending[ref]
	if ok {
		failures = intent.failures
		queuedAt = intent.queuedAt
		cause = intent.cause
		if attemptErr == nil {
			r.finishAttemptLocked(intent)
			delete(r.pending, ref)
			if r.activePending > 0 {
				r.activePending--
			}
			r.observeMetricsLocked()
			r.signalDrainedLocked()
		} else if r.runCtx.Err() == nil {
			intent.failures++
			failures = intent.failures
			// Keep the intent and its worker/org slots in flight while computing
			// injected backoff outside mu. This prevents a second dispatch without
			// blocking Reserve, Submit, or DrainAndClose on callback behavior.
			calculateBackoff = true
		} else {
			r.finishAttemptLocked(intent)
		}
	}
	r.mu.Unlock()

	if !ok {
		return
	}
	if calculateBackoff {
		retryAfter = r.retryBackoff(failures)
		if retryAfter < 0 {
			retryAfter = 0
		}

		retryAt := time.Now()
		r.mu.Lock()
		intent, stillPending := r.pending[ref]
		if stillPending && intent.inFlight {
			r.finishAttemptLocked(intent)
			if r.runCtx.Err() == nil {
				// Drain wakes pre-existing backoff once, then caps later retries at
				// a short jittered delay. This preserves shutdown progress without
				// hot-looping a config-store outage.
				if r.closed {
					retryAfter = admissionReclaimDrainBackoff(retryAfter)
				}
				intent.nextAttempt = retryAt.Add(retryAfter)
			}
		}
		r.mu.Unlock()
	}
	if attemptErr == nil {
		if failures > 0 {
			r.log.Info("Recovered org connection admission cleanup.",
				"org", ref.OrgID,
				"request_id", ref.RequestID,
				"cause", cause,
				"failed_attempts", failures,
				"pending_duration", now.Sub(queuedAt),
			)
		}
	} else if r.runCtx.Err() == nil && shouldLogAdmissionReclaimFailure(failures) {
		r.log.Warn("Failed to reclaim org connection admission; retrying.",
			"org", ref.OrgID,
			"request_id", ref.RequestID,
			"cause", cause,
			"attempt", failures,
			"pending_duration", now.Sub(queuedAt),
			"retry_after", retryAfter,
			"error", attemptErr,
		)
	}
	r.signalWake()
}

func (r *AdmissionReclaimer) finishAttemptLocked(intent *admissionReclaimIntent) {
	if intent == nil || !intent.inFlight {
		return
	}
	intent.inFlight = false
	delete(r.orgInFlight, intent.ref.OrgID)
	if r.inFlight > 0 {
		r.inFlight--
	}
}

func (r *AdmissionReclaimer) beginClose() {
	now := time.Now()
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		for _, intent := range r.pending {
			if !intent.active {
				// DrainAndClose is called only after admission producers have
				// joined. Activating a leftover held reservation is then safe and
				// recovers cleanup from an otherwise leaked lease handle.
				r.activateIntentLocked(intent, admissionReclaimCauseAcquireAbandoned, now)
			}
			if !intent.inFlight {
				intent.nextAttempt = now
			}
		}
	}
	r.signalDrainedLocked()
	r.mu.Unlock()
	r.signalWake()
}

func (r *AdmissionReclaimer) signalDrainedLocked() {
	if !r.closed || len(r.pending) != 0 {
		return
	}
	r.drainOnce.Do(func() { close(r.drainDone) })
}

func (r *AdmissionReclaimer) signalWake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *AdmissionReclaimer) observeMetricsLocked() {
	if r.metricContributionsReleased {
		return
	}
	pendingDelta := r.activePending - r.pendingMetricContribution
	if pendingDelta != 0 {
		orgConnectionReclaimPendingGauge.Add(float64(pendingDelta))
		r.pendingMetricContribution = r.activePending
	}
	reservationDelta := len(r.pending) - r.reservationMetricContribution
	if reservationDelta != 0 {
		orgConnectionReclaimReservationsInUseGauge.Add(float64(reservationDelta))
		r.reservationMetricContribution = len(r.pending)
	}
}

func (r *AdmissionReclaimer) releaseMetricContributions() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.metricContributionsReleased {
		return
	}
	if r.pendingMetricContribution != 0 {
		orgConnectionReclaimPendingGauge.Sub(float64(r.pendingMetricContribution))
		r.pendingMetricContribution = 0
	}
	if r.reservationMetricContribution != 0 {
		orgConnectionReclaimReservationsInUseGauge.Sub(float64(r.reservationMetricContribution))
		r.reservationMetricContribution = 0
	}
	if r.capacityMetricContribution != 0 {
		orgConnectionReclaimReservationCapacityGauge.Sub(float64(r.capacityMetricContribution))
		r.capacityMetricContribution = 0
	}
	r.metricContributionsReleased = true
}

func defaultAdmissionReclaimBackoff(failures int) time.Duration {
	base := admissionReclaimBackoffBase(failures)
	half := base / 2
	return half + time.Duration(rand.Int64N(int64(base-half)+1))
}

func admissionReclaimDrainBackoff(normal time.Duration) time.Duration {
	if normal <= 0 {
		return 0
	}
	if normal > defaultAdmissionReclaimDrainMaximumBackoff {
		normal = defaultAdmissionReclaimDrainMaximumBackoff
	}
	half := normal / 2
	return half + time.Duration(rand.Int64N(int64(normal-half)+1))
}

func admissionReclaimBackoffBase(failures int) time.Duration {
	if failures <= 1 {
		return defaultAdmissionReclaimInitialBackoff
	}
	delay := defaultAdmissionReclaimInitialBackoff
	for i := 1; i < failures; i++ {
		if delay >= defaultAdmissionReclaimMaximumBackoff/2 {
			return defaultAdmissionReclaimMaximumBackoff
		}
		delay *= 2
	}
	if delay > defaultAdmissionReclaimMaximumBackoff {
		return defaultAdmissionReclaimMaximumBackoff
	}
	return delay
}

func (r *AdmissionReclaimer) logPendingOnStop(reason string) {
	now := time.Now()
	r.mu.Lock()
	pending := len(r.pending)
	oldest := now
	for _, intent := range r.pending {
		if intent.queuedAt.Before(oldest) {
			oldest = intent.queuedAt
		}
	}
	r.mu.Unlock()
	if pending == 0 {
		return
	}
	oldestAge := now.Sub(oldest)
	if oldestAge < 0 {
		oldestAge = 0
	}
	r.pendingStopLogOnce.Do(func() {
		r.log.Warn("Admission reclaimer stopped with pending cleanup intents.",
			"reason", reason,
			"pending", pending,
			"oldest_age", oldestAge,
		)
	})
}

func validateAdmissionReclaimRef(ref configstore.OrgConnectionAdmissionRef) error {
	switch {
	case strings.TrimSpace(ref.RequestID) == "":
		return fmt.Errorf("org connection admission request id is required")
	case strings.TrimSpace(ref.OrgID) == "":
		return fmt.Errorf("org connection admission org id is required")
	case strings.TrimSpace(ref.CPInstanceID) == "":
		return fmt.Errorf("org connection admission control-plane instance id is required")
	default:
		return nil
	}
}

func shouldLogAdmissionReclaimFailure(failures int) bool {
	return failures > 0 && failures&(failures-1) == 0
}
