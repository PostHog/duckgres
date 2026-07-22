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
