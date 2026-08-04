package controlplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type scheduleAndClaimTestStore struct {
	enqueuedEntry *configstore.OrgConnectionQueueEntry
	enqueueErr    error
	scheduleCalls []scheduleAndClaimCall
	scheduleErr   error
	scheduleCtxFn func(context.Context, string, string) (*configstore.OrgConnectionLease, error)
}

type scheduleAndClaimCall struct {
	ref configstore.OrgConnectionAdmissionRef
}

type legacyOnlyRuntimeStore struct {
	legacyTries int
}

func (s *legacyOnlyRuntimeStore) EnqueueOrgConnectionRequest(*configstore.OrgConnectionQueueEntry) error {
	return nil
}

func (s *legacyOnlyRuntimeStore) TryAcquireOrgConnectionLease(string, configstore.OrgResourceLimits, time.Time) (*configstore.OrgConnectionLease, error) {
	s.legacyTries++
	return &configstore.OrgConnectionLease{LeaseID: "legacy-lease", RequestID: "legacy-lease"}, nil
}

type exactLimitLookupRuntimeStore struct {
	ref    configstore.OrgConnectionAdmissionRef
	limits configstore.OrgResourceLimits
	calls  int
}

func (s *exactLimitLookupRuntimeStore) EnqueueOrgConnectionRequest(*configstore.OrgConnectionQueueEntry) error {
	return nil
}

func (s *exactLimitLookupRuntimeStore) TryAcquireOrgConnectionLeaseWithLimitLookupForRef(ref configstore.OrgConnectionAdmissionRef, limits func(string) configstore.OrgResourceLimits, _ time.Time) (*configstore.OrgConnectionLease, error) {
	s.calls++
	s.ref = ref
	s.limits = limits("alice")
	return &configstore.OrgConnectionLease{LeaseID: ref.RequestID, RequestID: ref.RequestID}, nil
}

type admissionReclaimSubmission struct {
	ref   configstore.OrgConnectionAdmissionRef
	cause admissionReclaimCause
}

type recordingAdmissionReclaimer struct {
	mu           sync.Mutex
	submissions  []admissionReclaimSubmission
	reservations []configstore.OrgConnectionAdmissionRef
	reserveErr   error
	drainCalls   int
	onDrain      func()
}

type recordingAdmissionReservation struct {
	reclaimer *recordingAdmissionReclaimer
	ref       configstore.OrgConnectionAdmissionRef
}

func (r *recordingAdmissionReclaimer) Reserve(ref configstore.OrgConnectionAdmissionRef) (AdmissionReclaimReservation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reservations = append(r.reservations, ref)
	if r.reserveErr != nil {
		return nil, r.reserveErr
	}
	return &recordingAdmissionReservation{reclaimer: r, ref: ref}, nil
}

func (r *recordingAdmissionReservation) Reclaim(cause AdmissionReclaimCause) {
	if r == nil || r.reclaimer == nil {
		return
	}
	r.reclaimer.mu.Lock()
	defer r.reclaimer.mu.Unlock()
	for _, submission := range r.reclaimer.submissions {
		if submission.ref == r.ref {
			return
		}
	}
	r.reclaimer.submissions = append(r.reclaimer.submissions, admissionReclaimSubmission{ref: r.ref, cause: cause})
}

func (r *recordingAdmissionReclaimer) Submit(ref configstore.OrgConnectionAdmissionRef, cause admissionReclaimCause) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.submissions = append(r.submissions, admissionReclaimSubmission{ref: ref, cause: cause})
	return nil
}

func (r *recordingAdmissionReclaimer) DrainAndClose(context.Context) error {
	r.mu.Lock()
	r.drainCalls++
	onDrain := r.onDrain
	r.mu.Unlock()
	if onDrain != nil {
		onDrain()
	}
	return nil
}

func (r *recordingAdmissionReclaimer) snapshot() ([]admissionReclaimSubmission, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]admissionReclaimSubmission(nil), r.submissions...), r.drainCalls
}

func (s *scheduleAndClaimTestStore) EnqueueOrgConnectionRequest(entry *configstore.OrgConnectionQueueEntry) error {
	entryCopy := *entry
	s.enqueuedEntry = &entryCopy
	return s.enqueueErr
}

func (s *scheduleAndClaimTestStore) EnqueueOrgConnectionRequestContext(_ context.Context, entry *configstore.OrgConnectionQueueEntry) error {
	return s.EnqueueOrgConnectionRequest(entry)
}

func (s *scheduleAndClaimTestStore) ScheduleAndClaimOrgConnectionLease(requestID, cpInstanceID string) (*configstore.OrgConnectionLease, error) {
	return s.ScheduleAndClaimOrgConnectionLeaseForRef(configstore.OrgConnectionAdmissionRef{
		RequestID: requestID, CPInstanceID: cpInstanceID,
	})
}

func (s *scheduleAndClaimTestStore) ScheduleAndClaimOrgConnectionLeaseForRef(ref configstore.OrgConnectionAdmissionRef) (*configstore.OrgConnectionLease, error) {
	s.scheduleCalls = append(s.scheduleCalls, scheduleAndClaimCall{ref: ref})
	if s.scheduleErr != nil {
		return nil, s.scheduleErr
	}
	if len(s.scheduleCalls) == 1 {
		return nil, nil
	}
	return &configstore.OrgConnectionLease{LeaseID: ref.RequestID, RequestID: ref.RequestID}, nil
}

func (s *scheduleAndClaimTestStore) ScheduleAndClaimOrgConnectionLeaseContext(ctx context.Context, requestID, cpInstanceID string) (*configstore.OrgConnectionLease, error) {
	return s.ScheduleAndClaimOrgConnectionLeaseForRefContext(ctx, configstore.OrgConnectionAdmissionRef{
		RequestID: requestID, CPInstanceID: cpInstanceID,
	})
}

func (s *scheduleAndClaimTestStore) ScheduleAndClaimOrgConnectionLeaseForRefContext(ctx context.Context, ref configstore.OrgConnectionAdmissionRef) (*configstore.OrgConnectionLease, error) {
	if s.scheduleCtxFn != nil {
		s.scheduleCalls = append(s.scheduleCalls, scheduleAndClaimCall{ref: ref})
		return s.scheduleCtxFn(ctx, ref.RequestID, ref.CPInstanceID)
	}
	return s.ScheduleAndClaimOrgConnectionLeaseForRef(ref)
}

func TestRuntimeOrgConnectionLimiterPrefersScheduleAndClaimHandshake(t *testing.T) {
	store := &scheduleAndClaimTestStore{}
	reclaimer := &recordingAdmissionReclaimer{}
	limiter := &runtimeOrgConnectionLimiter{
		store:        store,
		reclaimer:    reclaimer,
		orgID:        "org-1",
		cpInstanceID: "cp-owner",
		queueTTL:     time.Second,
		pollInterval: time.Millisecond,
		now:          time.Now,
		newID: func() (string, error) {
			return "request-1", nil
		},
	}

	limitReads := 0
	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID:            1001,
		Username:       "alice",
		Protocol:       "postgres",
		RequestedVCPUs: 2,
	}, func(string) configstore.OrgResourceLimits {
		limitReads++
		return configstore.OrgResourceLimits{OrgMaxVCPUs: 4, UserMaxVCPUs: 2}
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease == nil {
		t.Fatal("expected schedule-and-claim handshake to return a lease")
	}
	if store.enqueuedEntry == nil || store.enqueuedEntry.RequestID != "request-1" {
		t.Fatalf("expected request-1 to be enqueued, got %#v", store.enqueuedEntry)
	}
	if len(store.scheduleCalls) != 2 {
		t.Fatalf("schedule-and-claim calls = %d, want 2 (waiting then granted)", len(store.scheduleCalls))
	}
	for i, call := range store.scheduleCalls {
		want := (configstore.OrgConnectionAdmissionRef{RequestID: "request-1", OrgID: "org-1", CPInstanceID: "cp-owner"})
		if call.ref != want {
			t.Fatalf("schedule-and-claim call %d = %#v, want %#v", i, call.ref, want)
		}
	}
	if limitReads != 0 {
		t.Fatalf("local limit reads = %d, want 0", limitReads)
	}
	if submissions, _ := reclaimer.snapshot(); len(submissions) != 0 {
		t.Fatalf("successful Acquire submitted cleanup before lease release: %#v", submissions)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second Release: %v", err)
	}
	submissions, _ := reclaimer.snapshot()
	wantSubmission := admissionReclaimSubmission{
		ref:   configstore.OrgConnectionAdmissionRef{RequestID: "request-1", OrgID: "org-1", CPInstanceID: "cp-owner"},
		cause: admissionReclaimCauseLeaseRelease,
	}
	if len(submissions) != 1 || submissions[0] != wantSubmission {
		t.Fatalf("lease cleanup submissions = %#v, want exactly %#v", submissions, wantSubmission)
	}
}

func TestNewRuntimeOrgConnectionLimiterFourArgumentCompatibilityFailsClosed(t *testing.T) {
	store := &scheduleAndClaimTestStore{}
	limiter := NewRuntimeOrgConnectionLimiter(store, "org-1", "cp-owner", time.Second)

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1001, Username: "alice", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || err == nil || err.Error() != "org connection admission reclaimer is required" {
		t.Fatalf("Acquire without explicit reclaimer = (%v, %v), want fail-closed compatibility behavior", lease, err)
	}
	if store.enqueuedEntry != nil {
		t.Fatalf("four-argument compatibility call reached durable enqueue: %#v", store.enqueuedEntry)
	}
}

func TestRuntimeOrgConnectionLimiterDoesNotInvokeLegacyIDOnlyAcquisition(t *testing.T) {
	store := &legacyOnlyRuntimeStore{}
	reclaimer := &recordingAdmissionReclaimer{}
	limiter := &runtimeOrgConnectionLimiter{
		store:        store,
		reclaimer:    reclaimer,
		orgID:        "org-1",
		cpInstanceID: "cp-owner",
		queueTTL:     time.Second,
		pollInterval: time.Millisecond,
		now:          time.Now,
		newID: func() (string, error) {
			return "request-legacy-only", nil
		},
	}

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID:            1001,
		Username:       "alice",
		Protocol:       "postgres",
		RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits {
		return configstore.OrgResourceLimits{OrgMaxVCPUs: 1}
	})
	if !errors.Is(err, errRuntimeOrgConnectionExactAcquisitionUnsupported) {
		t.Fatalf("Acquire = (%v, %v), want %v", lease, err, errRuntimeOrgConnectionExactAcquisitionUnsupported)
	}
	if store.legacyTries != 0 {
		t.Fatalf("legacy ID-only acquisition calls = %d, want 0", store.legacyTries)
	}
	submissions, _ := reclaimer.snapshot()
	want := admissionReclaimSubmission{
		ref:   configstore.OrgConnectionAdmissionRef{RequestID: "request-legacy-only", OrgID: "org-1", CPInstanceID: "cp-owner"},
		cause: admissionReclaimCauseAcquireAbandoned,
	}
	if len(submissions) != 1 || submissions[0] != want {
		t.Fatalf("cleanup submissions = %#v, want exactly %#v", submissions, want)
	}
}

func TestRuntimeOrgConnectionLimiterReservesCleanupCapacityBeforeEnqueue(t *testing.T) {
	store := &scheduleAndClaimTestStore{}
	reclaimer := &recordingAdmissionReclaimer{reserveErr: ErrAdmissionReclaimerFull}
	limiter := &runtimeOrgConnectionLimiter{
		store: store, reclaimer: reclaimer, orgID: "org-1", cpInstanceID: "cp-owner",
		queueTTL: time.Second, pollInterval: time.Millisecond, now: time.Now,
		newID: func() (string, error) { return "request-capacity-full", nil },
	}

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1001, Username: "alice", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || !errors.Is(err, ErrTooManyConnections) {
		t.Fatalf("Acquire = (%v, %v), want cleanup-capacity rejection as ErrTooManyConnections", lease, err)
	}
	if store.enqueuedEntry != nil {
		t.Fatalf("request reached durable enqueue without reserved cleanup capacity: %#v", store.enqueuedEntry)
	}
	reclaimer.mu.Lock()
	defer reclaimer.mu.Unlock()
	wantRef := configstore.OrgConnectionAdmissionRef{RequestID: "request-capacity-full", OrgID: "org-1", CPInstanceID: "cp-owner"}
	if len(reclaimer.reservations) != 1 || reclaimer.reservations[0] != wantRef {
		t.Fatalf("reservation attempts = %#v, want [%#v]", reclaimer.reservations, wantRef)
	}
}

func TestRuntimeOrgConnectionLimiterUsesExactRefLimitLookupFallback(t *testing.T) {
	store := &exactLimitLookupRuntimeStore{}
	reclaimer := &recordingAdmissionReclaimer{}
	limiter := &runtimeOrgConnectionLimiter{
		store:        store,
		reclaimer:    reclaimer,
		orgID:        "org-1",
		cpInstanceID: "cp-owner",
		queueTTL:     time.Second,
		pollInterval: time.Millisecond,
		now:          time.Now,
		newID: func() (string, error) {
			return "request-limit-lookup", nil
		},
	}

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID:            1001,
		Username:       "alice",
		Protocol:       "postgres",
		RequestedVCPUs: 2,
	}, func(string) configstore.OrgResourceLimits {
		return configstore.OrgResourceLimits{OrgMaxVCPUs: 4, UserMaxVCPUs: 2}
	})
	if err != nil || lease == nil {
		t.Fatalf("Acquire = (%v, %v), want an exact-ref fallback lease", lease, err)
	}
	wantRef := configstore.OrgConnectionAdmissionRef{RequestID: "request-limit-lookup", OrgID: "org-1", CPInstanceID: "cp-owner"}
	if store.calls != 1 || store.ref != wantRef {
		t.Fatalf("exact limit-lookup calls/ref = %d/%#v, want 1/%#v", store.calls, store.ref, wantRef)
	}
	if store.limits != (configstore.OrgResourceLimits{OrgMaxVCPUs: 4, UserMaxVCPUs: 2}) {
		t.Fatalf("exact limit-lookup limits = %#v", store.limits)
	}
}

func TestRuntimeOrgConnectionLeaseReclaimsReservedCapacityOnce(t *testing.T) {
	reclaimer := &recordingAdmissionReclaimer{}
	ref := configstore.OrgConnectionAdmissionRef{
		RequestID: "request-retry", OrgID: "org-1", CPInstanceID: "cp-owner",
	}
	reservation, err := reclaimer.Reserve(ref)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	lease := &runtimeOrgConnectionLease{reservation: reservation}

	for attempt := 1; attempt <= 3; attempt++ {
		if err := lease.Release(context.Background()); err != nil {
			t.Fatalf("Release %d: %v", attempt, err)
		}
	}

	submissions, _ := reclaimer.snapshot()
	want := admissionReclaimSubmission{ref: ref, cause: admissionReclaimCauseLeaseRelease}
	if len(submissions) != 1 || submissions[0] != want {
		t.Fatalf("cleanup submissions = %#v, want one activation of %#v", submissions, want)
	}
}

func TestRuntimeOrgConnectionLimiterCancelsAfterScheduleAndClaimError(t *testing.T) {
	boom := errors.New("schedule failed")
	store := &scheduleAndClaimTestStore{scheduleErr: boom}
	reclaimer := &recordingAdmissionReclaimer{}
	limiter := &runtimeOrgConnectionLimiter{
		store:        store,
		reclaimer:    reclaimer,
		orgID:        "org-1",
		cpInstanceID: "cp-owner",
		queueTTL:     time.Second,
		pollInterval: time.Millisecond,
		now:          time.Now,
		newID: func() (string, error) {
			return "request-error", nil
		},
	}

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID:            1001,
		Username:       "alice",
		Protocol:       "postgres",
		RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits {
		return configstore.OrgResourceLimits{OrgMaxVCPUs: 1}
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Acquire = lease %v, error %v; want %v", lease, err, boom)
	}
	if len(store.scheduleCalls) != 1 {
		t.Fatalf("schedule-and-claim calls = %d, want 1", len(store.scheduleCalls))
	}
	submissions, _ := reclaimer.snapshot()
	want := admissionReclaimSubmission{
		ref:   configstore.OrgConnectionAdmissionRef{RequestID: "request-error", OrgID: "org-1", CPInstanceID: "cp-owner"},
		cause: admissionReclaimCauseAcquireAbandoned,
	}
	if len(submissions) != 1 || submissions[0] != want {
		t.Fatalf("cleanup submissions = %#v, want exactly %#v", submissions, want)
	}
}

func TestRuntimeOrgConnectionLimiterArmsExactCleanupBeforeEnqueue(t *testing.T) {
	boom := errors.New("ambiguous enqueue failure")
	store := &scheduleAndClaimTestStore{enqueueErr: boom}
	reclaimer := &recordingAdmissionReclaimer{}
	limiter := &runtimeOrgConnectionLimiter{
		store: store, reclaimer: reclaimer, orgID: "org-1", cpInstanceID: "cp-owner",
		queueTTL: time.Second, pollInterval: time.Millisecond, now: time.Now,
		newID: func() (string, error) { return "request-enqueue-error", nil },
	}

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1001, Username: "alice", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if !errors.Is(err, boom) || lease != nil {
		t.Fatalf("Acquire = (%v, %v), want (nil, %v)", lease, err, boom)
	}
	submissions, _ := reclaimer.snapshot()
	want := admissionReclaimSubmission{
		ref:   configstore.OrgConnectionAdmissionRef{RequestID: "request-enqueue-error", OrgID: "org-1", CPInstanceID: "cp-owner"},
		cause: admissionReclaimCauseAcquireAbandoned,
	}
	if len(submissions) != 1 || submissions[0] != want {
		t.Fatalf("cleanup submissions = %#v, want exactly %#v", submissions, want)
	}
}

func TestRuntimeOrgConnectionLimiterCancellationInterruptsScheduleAndReclaimsRequest(t *testing.T) {
	entered := make(chan struct{})
	store := &scheduleAndClaimTestStore{}
	reclaimer := &recordingAdmissionReclaimer{}
	store.scheduleCtxFn = func(ctx context.Context, _, _ string) (*configstore.OrgConnectionLease, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	limiter := &runtimeOrgConnectionLimiter{
		store: store, reclaimer: reclaimer, orgID: "org-1", cpInstanceID: "cp-owner",
		queueTTL: time.Minute, pollInterval: time.Millisecond, now: time.Now,
		newID: func() (string, error) { return "request-canceled-lock-wait", nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := limiter.Acquire(ctx, connectionAdmissionRequest{
			PID: 1001, Username: "alice", Protocol: "postgres", RequestedVCPUs: 1,
		}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{OrgMaxVCPUs: 1} })
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("schedule call did not start")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not return after context cancellation")
	}
	submissions, _ := reclaimer.snapshot()
	if len(submissions) != 1 || submissions[0].ref != (configstore.OrgConnectionAdmissionRef{RequestID: "request-canceled-lock-wait", OrgID: "org-1", CPInstanceID: "cp-owner"}) {
		t.Fatalf("cleanup submissions = %#v, want exact canceled request ref", submissions)
	}
}

func TestRuntimeOrgConnectionLimiterReclaimsLeaseGrantedWithCancellation(t *testing.T) {
	entered := make(chan struct{})
	store := &scheduleAndClaimTestStore{}
	reclaimer := &recordingAdmissionReclaimer{}
	store.scheduleCtxFn = func(ctx context.Context, requestID, _ string) (*configstore.OrgConnectionLease, error) {
		close(entered)
		<-ctx.Done()
		return &configstore.OrgConnectionLease{LeaseID: requestID, RequestID: requestID}, nil
	}
	limiter := &runtimeOrgConnectionLimiter{
		store: store, reclaimer: reclaimer, orgID: "org-1", cpInstanceID: "cp-owner",
		queueTTL: time.Minute, pollInterval: time.Millisecond, now: time.Now,
		newID: func() (string, error) { return "request-grant-cancel-race", nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		lease, err := limiter.Acquire(ctx, connectionAdmissionRequest{
			PID: 1001, Username: "alice", Protocol: "postgres", RequestedVCPUs: 1,
		}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{OrgMaxVCPUs: 1} })
		if lease != nil {
			errCh <- errors.New("canceled admission returned a lease")
			return
		}
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("schedule call did not start")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not return after grant/cancel race")
	}
	submissions, _ := reclaimer.snapshot()
	if len(submissions) != 1 || submissions[0].ref != (configstore.OrgConnectionAdmissionRef{RequestID: "request-grant-cancel-race", OrgID: "org-1", CPInstanceID: "cp-owner"}) {
		t.Fatalf("cleanup submissions = %#v, want exact grant/cancel request ref", submissions)
	}
}

type admissionMetricsScheduleResult struct {
	evaluation configstore.OrgConnectionAdmissionEvaluation
	err        error
}

type admissionMetricsScheduleStore struct {
	mu           sync.Mutex
	results      []admissionMetricsScheduleResult
	enqueueErr   error
	enqueueCalls int
	onEvaluate   func()
}

func (s *admissionMetricsScheduleStore) EnqueueOrgConnectionRequest(*configstore.OrgConnectionQueueEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enqueueCalls++
	return s.enqueueErr
}

func (s *admissionMetricsScheduleStore) ScheduleAndClaimOrgConnectionLeaseForRefWithEvaluationContext(_ context.Context, ref configstore.OrgConnectionAdmissionRef) (*configstore.OrgConnectionLease, configstore.OrgConnectionAdmissionEvaluation, error) {
	s.mu.Lock()
	if len(s.results) == 0 {
		s.mu.Unlock()
		return nil, configstore.OrgConnectionAdmissionEvaluation{Decision: "waiting", Reason: "fifo"}, nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	onEvaluate := s.onEvaluate
	s.mu.Unlock()
	if onEvaluate != nil {
		onEvaluate()
	}
	if result.evaluation.Decision == "granted_current" {
		return &configstore.OrgConnectionLease{LeaseID: ref.RequestID, RequestID: ref.RequestID}, result.evaluation, result.err
	}
	return nil, result.evaluation, result.err
}

func newAdmissionMetricsLimiter(store runtimeOrgConnectionStore, reclaimer admissionReclaimer, org, requestID string) *runtimeOrgConnectionLimiter {
	return &runtimeOrgConnectionLimiter{
		store:        store,
		reclaimer:    reclaimer,
		orgID:        org,
		cpInstanceID: "cp-metrics",
		queueTTL:     time.Minute,
		pollInterval: time.Millisecond,
		now:          time.Now,
		newID:        func() (string, error) { return requestID, nil },
	}
}

func resetSessionAdmissionMetricGauges(t *testing.T, org string) {
	t.Helper()
	sessionAdmissionQueueDepthGauge.DeleteLabelValues(org)
	sessionAdmissionActiveVCPUsGauge.DeleteLabelValues(org)
	sessionAdmissionLimitVCPUsGauge.DeleteLabelValues(org)
	t.Cleanup(func() {
		sessionAdmissionQueueDepthGauge.DeleteLabelValues(org)
		sessionAdmissionActiveVCPUsGauge.DeleteLabelValues(org)
		sessionAdmissionLimitVCPUsGauge.DeleteLabelValues(org)
	})
}

func TestRuntimeOrgConnectionLimiterObservesGrantedRequestLifecycle(t *testing.T) {
	const org = "org-admission-metrics-granted"
	resetSessionAdmissionMetricGauges(t, org)
	sessionAdmissionLimitVCPUsGauge.WithLabelValues(org).Set(7)
	store := &admissionMetricsScheduleStore{results: []admissionMetricsScheduleResult{
		{evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "org_vcpu"}},
		{evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "granted_current", Reason: "none"}},
	}}
	reclaimer := &recordingAdmissionReclaimer{}
	limiter := newAdmissionMetricsLimiter(store, reclaimer, org, "request-metrics-granted")

	requestsBefore := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "granted", "org_vcpu")
	waitsBefore := histogramVecLabelSampleCount(t, sessionAdmissionWaitHistogram, org, "granted", "org_vcpu")
	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1001, Username: "alice", Protocol: "postgres", RequestedVCPUs: 2,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if err != nil || lease == nil {
		t.Fatalf("Acquire = (%v, %v), want lease", lease, err)
	}
	if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "granted", "org_vcpu") - requestsBefore; got != 1 {
		t.Fatalf("request counter delta = %v, want 1", got)
	}
	if got := histogramVecLabelSampleCount(t, sessionAdmissionWaitHistogram, org, "granted", "org_vcpu") - waitsBefore; got != 1 {
		t.Fatalf("wait histogram count delta = %d, want 1", got)
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionQueueDepthGauge, org); got != 0 {
		t.Fatalf("queue depth = %v, want 0", got)
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionActiveVCPUsGauge, org); got != 2 {
		t.Fatalf("active vCPUs = %v, want 2", got)
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionLimitVCPUsGauge, org); got != 7 {
		t.Fatalf("limit vCPUs = %v, want config-reconciled value 7", got)
	}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second Release: %v", err)
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionActiveVCPUsGauge, org); got != 0 {
		t.Fatalf("active vCPUs after repeated release = %v, want 0", got)
	}
	submissions, _ := reclaimer.snapshot()
	if len(submissions) != 1 || submissions[0].cause != admissionReclaimCauseLeaseRelease {
		t.Fatalf("lease cleanup submissions = %#v, want exactly one release", submissions)
	}
}

func TestRuntimeOrgConnectionLimiterRetainsCombinedVCPULimitReason(t *testing.T) {
	const org = "org-admission-metrics-combined"
	resetSessionAdmissionMetricGauges(t, org)
	store := &admissionMetricsScheduleStore{results: []admissionMetricsScheduleResult{
		{evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "org_vcpu"}},
		{evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "user_vcpu"}},
		{evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "granted_current", Reason: "none"}},
	}}
	limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-combined")
	before := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "granted", "org_user_vcpu")
	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1002, Username: "alice", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if err != nil || lease == nil {
		t.Fatalf("Acquire = (%v, %v), want lease", lease, err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "granted", "org_user_vcpu") - before; got != 1 {
		t.Fatalf("combined-vCPU request counter delta = %v, want 1", got)
	}
}

func TestRuntimeOrgConnectionLimiterObservesCancellationAfterEnqueue(t *testing.T) {
	const org = "org-admission-metrics-canceled"
	resetSessionAdmissionMetricGauges(t, org)
	ctx, cancel := context.WithCancel(context.Background())
	store := &admissionMetricsScheduleStore{results: []admissionMetricsScheduleResult{{
		evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "user_vcpu"},
	}}}
	store.onEvaluate = cancel
	reclaimer := &recordingAdmissionReclaimer{}
	limiter := newAdmissionMetricsLimiter(store, reclaimer, org, "request-metrics-canceled")
	before := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "canceled", "user_vcpu")
	lease, err := limiter.Acquire(ctx, connectionAdmissionRequest{
		PID: 1003, Username: "bob", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire = (%v, %v), want context.Canceled", lease, err)
	}
	if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "canceled", "user_vcpu") - before; got != 1 {
		t.Fatalf("canceled request counter delta = %v, want 1", got)
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionQueueDepthGauge, org); got != 0 {
		t.Fatalf("queue depth = %v, want 0", got)
	}
	submissions, _ := reclaimer.snapshot()
	if len(submissions) != 1 || submissions[0].cause != admissionReclaimCauseAcquireAbandoned {
		t.Fatalf("cleanup submissions = %#v, want one abandoned request", submissions)
	}
}

func TestRuntimeOrgConnectionLimiterTracksQueueDepthWhileEvaluationIsInFlight(t *testing.T) {
	const org = "org-admission-metrics-queue-depth"
	resetSessionAdmissionMetricGauges(t, org)
	evaluationStarted := make(chan struct{})
	releaseEvaluation := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseEvaluation) }) })

	store := &admissionMetricsScheduleStore{results: []admissionMetricsScheduleResult{{
		evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "user_vcpu"},
	}}}
	store.onEvaluate = func() {
		close(evaluationStarted)
		<-releaseEvaluation
	}
	limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-queue-depth")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	type acquireResult struct {
		lease connectionLease
		err   error
	}
	result := make(chan acquireResult, 1)
	go func() {
		lease, err := limiter.Acquire(ctx, connectionAdmissionRequest{
			PID: 1009, Username: "heidi", Protocol: "postgres", RequestedVCPUs: 1,
		}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
		result <- acquireResult{lease: lease, err: err}
	}()

	select {
	case <-evaluationStarted:
	case <-time.After(time.Second):
		t.Fatal("admission evaluation did not start")
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionQueueDepthGauge, org); got != 1 {
		t.Fatalf("queue depth during evaluation = %v, want 1", got)
	}

	cancel()
	releaseOnce.Do(func() { close(releaseEvaluation) })
	select {
	case got := <-result:
		if got.lease != nil || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("Acquire = (%v, %v), want context.Canceled", got.lease, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission acquire did not stop after cancellation")
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionQueueDepthGauge, org); got != 0 {
		t.Fatalf("queue depth after cancellation = %v, want 0", got)
	}
}

func TestRuntimeOrgConnectionLimiterClassifiesInterruptedEvaluationErrors(t *testing.T) {
	tests := []struct {
		name        string
		evaluation  error
		wantOutcome string
		wantError   error
	}{
		{name: "canceled", evaluation: context.Canceled, wantOutcome: "canceled", wantError: context.Canceled},
		{name: "deadline", evaluation: context.DeadlineExceeded, wantOutcome: "timeout", wantError: ErrTooManyConnections},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			org := "org-admission-metrics-interrupted-" + tc.name
			resetSessionAdmissionMetricGauges(t, org)
			store := &admissionMetricsScheduleStore{results: []admissionMetricsScheduleResult{{
				err: tc.evaluation,
			}}}
			limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-interrupted-"+tc.name)
			before := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, tc.wantOutcome, "none")
			storeErrorsBefore := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "error", "store_error")

			lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
				PID: int32(1010 + i), Username: "ivan", Protocol: "postgres", RequestedVCPUs: 1,
			}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
			if lease != nil || !errors.Is(err, tc.wantError) {
				t.Fatalf("Acquire = (%v, %v), want %v", lease, err, tc.wantError)
			}
			if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, tc.wantOutcome, "none") - before; got != 1 {
				t.Fatalf("%s/none request counter delta = %v, want 1", tc.wantOutcome, got)
			}
			if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "error", "store_error") - storeErrorsBefore; got != 0 {
				t.Fatalf("error/store_error request counter delta = %v, want 0", got)
			}
		})
	}
}

func TestRuntimeOrgConnectionLimiterObservesPermanentRejection(t *testing.T) {
	const org = "org-admission-metrics-rejected"
	resetSessionAdmissionMetricGauges(t, org)
	rejection := &configstore.OrgConnectionAdmissionRejectedError{
		Reason: configstore.OrgConnectionAdmissionRejectedOrgVCPU, RequestedVCPUs: 4, MaximumVCPUs: 2,
	}
	store := &admissionMetricsScheduleStore{results: []admissionMetricsScheduleResult{{
		evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "rejected", Reason: "org_vcpu"},
		err:        rejection,
	}}}
	limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-rejected")
	before := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "rejected", "org_vcpu")
	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1004, Username: "carol", Protocol: "postgres", RequestedVCPUs: 4,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || !errors.Is(err, configstore.ErrOrgConnectionAdmissionRejected) {
		t.Fatalf("Acquire = (%v, %v), want permanent rejection", lease, err)
	}
	if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "rejected", "org_vcpu") - before; got != 1 {
		t.Fatalf("rejected request counter delta = %v, want 1", got)
	}
}

func TestRuntimeOrgConnectionLimiterExcludesPreEnqueueFailuresFromRequestMetrics(t *testing.T) {
	const org = "org-admission-metrics-enqueue-error"
	resetSessionAdmissionMetricGauges(t, org)
	store := &admissionMetricsScheduleStore{enqueueErr: errors.New("enqueue unavailable")}
	limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-enqueue-error")
	before := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "error", "store_error")
	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1005, Username: "dana", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || err == nil {
		t.Fatalf("Acquire = (%v, %v), want enqueue error", lease, err)
	}
	if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "error", "store_error") - before; got != 0 {
		t.Fatalf("request counter delta = %v, want 0 before successful enqueue", got)
	}
}

func TestRuntimeOrgConnectionLimiterExcludesPreCanceledRequestFromMetrics(t *testing.T) {
	const org = "org-admission-metrics-pre-canceled"
	resetSessionAdmissionMetricGauges(t, org)
	store := &admissionMetricsScheduleStore{}
	limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-pre-canceled")
	before := metricCounterFamilyTotal(t, "duckgres_session_admission_requests_total")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	lease, err := limiter.Acquire(ctx, connectionAdmissionRequest{
		PID: 1006, Username: "erin", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire = (%v, %v), want pre-enqueue context cancellation", lease, err)
	}
	if got := metricCounterFamilyTotal(t, "duckgres_session_admission_requests_total") - before; got != 0 {
		t.Fatalf("request counter delta = %v, want 0 before successful enqueue", got)
	}
	if store.enqueueCalls != 0 {
		t.Fatalf("enqueue calls = %d, want 0", store.enqueueCalls)
	}
}

func TestRuntimeOrgConnectionLimiterObservesTimeoutAfterEnqueue(t *testing.T) {
	const org = "org-admission-metrics-timeout"
	resetSessionAdmissionMetricGauges(t, org)
	store := &admissionMetricsScheduleStore{}
	limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-timeout")
	start := time.Now()
	nowCalls := 0
	limiter.queueTTL = time.Millisecond
	limiter.now = func() time.Time {
		nowCalls++
		if nowCalls == 1 {
			return start
		}
		return start.Add(2 * time.Millisecond)
	}
	before := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "timeout", "none")

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1007, Username: "frank", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || !errors.Is(err, ErrTooManyConnections) {
		t.Fatalf("Acquire = (%v, %v), want admission timeout", lease, err)
	}
	if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "timeout", "none") - before; got != 1 {
		t.Fatalf("timeout request counter delta = %v, want 1", got)
	}
	if got := gaugeVecLabelValue(t, sessionAdmissionQueueDepthGauge, org); got != 0 {
		t.Fatalf("queue depth = %v, want 0", got)
	}
}

func TestRuntimeOrgConnectionLimiterObservesEvaluationError(t *testing.T) {
	const org = "org-admission-metrics-error"
	resetSessionAdmissionMetricGauges(t, org)
	boom := errors.New("config store unavailable")
	store := &admissionMetricsScheduleStore{results: []admissionMetricsScheduleResult{{
		evaluation: configstore.OrgConnectionAdmissionEvaluation{Decision: "error", Reason: "store_error"},
		err:        boom,
	}}}
	limiter := newAdmissionMetricsLimiter(store, &recordingAdmissionReclaimer{}, org, "request-metrics-error")
	before := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "error", "store_error")

	lease, err := limiter.Acquire(context.Background(), connectionAdmissionRequest{
		PID: 1008, Username: "grace", Protocol: "postgres", RequestedVCPUs: 1,
	}, func(string) configstore.OrgResourceLimits { return configstore.OrgResourceLimits{} })
	if lease != nil || !errors.Is(err, boom) {
		t.Fatalf("Acquire = (%v, %v), want evaluation error", lease, err)
	}
	if got := counterVecLabelValue(t, sessionAdmissionRequestsCounter, org, "error", "store_error") - before; got != 1 {
		t.Fatalf("error request counter delta = %v, want 1", got)
	}
}
