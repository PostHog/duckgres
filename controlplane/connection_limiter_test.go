package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type scheduleAndClaimTestStore struct {
	enqueuedEntry *configstore.OrgConnectionQueueEntry
	scheduleCalls []scheduleAndClaimCall
	scheduleErr   error
	scheduleCtxFn func(context.Context, string, string) (*configstore.OrgConnectionLease, error)
	legacyTries   int
	cancelID      string
	releaseID     string
}

type scheduleAndClaimCall struct {
	requestID    string
	cpInstanceID string
}

func (s *scheduleAndClaimTestStore) EnqueueOrgConnectionRequest(entry *configstore.OrgConnectionQueueEntry) error {
	entryCopy := *entry
	s.enqueuedEntry = &entryCopy
	return nil
}

func (s *scheduleAndClaimTestStore) EnqueueOrgConnectionRequestContext(_ context.Context, entry *configstore.OrgConnectionQueueEntry) error {
	return s.EnqueueOrgConnectionRequest(entry)
}

func (s *scheduleAndClaimTestStore) ScheduleAndClaimOrgConnectionLease(requestID, cpInstanceID string) (*configstore.OrgConnectionLease, error) {
	s.scheduleCalls = append(s.scheduleCalls, scheduleAndClaimCall{
		requestID:    requestID,
		cpInstanceID: cpInstanceID,
	})
	if s.scheduleErr != nil {
		return nil, s.scheduleErr
	}
	if len(s.scheduleCalls) == 1 {
		return nil, nil
	}
	return &configstore.OrgConnectionLease{LeaseID: requestID, RequestID: requestID}, nil
}

func (s *scheduleAndClaimTestStore) ScheduleAndClaimOrgConnectionLeaseContext(ctx context.Context, requestID, cpInstanceID string) (*configstore.OrgConnectionLease, error) {
	if s.scheduleCtxFn != nil {
		s.scheduleCalls = append(s.scheduleCalls, scheduleAndClaimCall{requestID: requestID, cpInstanceID: cpInstanceID})
		return s.scheduleCtxFn(ctx, requestID, cpInstanceID)
	}
	return s.ScheduleAndClaimOrgConnectionLease(requestID, cpInstanceID)
}

func (s *scheduleAndClaimTestStore) TryAcquireOrgConnectionLease(string, configstore.OrgResourceLimits, time.Time) (*configstore.OrgConnectionLease, error) {
	s.legacyTries++
	return nil, errors.New("legacy TryAcquireOrgConnectionLease called")
}

func (s *scheduleAndClaimTestStore) ReleaseOrgConnectionLease(leaseID string) error {
	s.releaseID = leaseID
	return nil
}

func (s *scheduleAndClaimTestStore) ReleaseOrgConnectionLeaseContext(_ context.Context, leaseID string) error {
	return s.ReleaseOrgConnectionLease(leaseID)
}

func (s *scheduleAndClaimTestStore) CancelOrgConnectionRequest(requestID string, _ time.Time) error {
	s.cancelID = requestID
	return nil
}

func (s *scheduleAndClaimTestStore) CancelOrgConnectionRequestContext(_ context.Context, requestID string, canceledAt time.Time) error {
	return s.CancelOrgConnectionRequest(requestID, canceledAt)
}

func TestRuntimeOrgConnectionLimiterPrefersScheduleAndClaimHandshake(t *testing.T) {
	store := &scheduleAndClaimTestStore{}
	limiter := &runtimeOrgConnectionLimiter{
		store:        store,
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
		if call.requestID != "request-1" || call.cpInstanceID != "cp-owner" {
			t.Fatalf("schedule-and-claim call %d = %#v, want request-1/cp-owner", i, call)
		}
	}
	if store.legacyTries != 0 {
		t.Fatalf("legacy TryAcquire calls = %d, want 0", store.legacyTries)
	}
	if limitReads != 0 {
		t.Fatalf("local limit reads = %d, want 0", limitReads)
	}
	if store.cancelID != "" {
		t.Fatalf("unexpected cancellation of %q", store.cancelID)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if store.releaseID != "request-1" {
		t.Fatalf("released lease = %q, want request-1", store.releaseID)
	}
}

func TestRuntimeOrgConnectionLimiterCancelsAfterScheduleAndClaimError(t *testing.T) {
	boom := errors.New("schedule failed")
	store := &scheduleAndClaimTestStore{scheduleErr: boom}
	limiter := &runtimeOrgConnectionLimiter{
		store:        store,
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
	if store.legacyTries != 0 {
		t.Fatalf("legacy TryAcquire calls = %d, want 0", store.legacyTries)
	}
	if store.cancelID != "request-error" {
		t.Fatalf("canceled request = %q, want request-error", store.cancelID)
	}
}

func TestRuntimeOrgConnectionLimiterCancellationInterruptsScheduleAndReclaimsRequest(t *testing.T) {
	entered := make(chan struct{})
	store := &scheduleAndClaimTestStore{}
	store.scheduleCtxFn = func(ctx context.Context, _, _ string) (*configstore.OrgConnectionLease, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	limiter := &runtimeOrgConnectionLimiter{
		store: store, orgID: "org-1", cpInstanceID: "cp-owner",
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
	if store.cancelID != "request-canceled-lock-wait" {
		t.Fatalf("canceled request = %q", store.cancelID)
	}
}

func TestRuntimeOrgConnectionLimiterReclaimsLeaseGrantedWithCancellation(t *testing.T) {
	entered := make(chan struct{})
	store := &scheduleAndClaimTestStore{}
	store.scheduleCtxFn = func(ctx context.Context, requestID, _ string) (*configstore.OrgConnectionLease, error) {
		close(entered)
		<-ctx.Done()
		return &configstore.OrgConnectionLease{LeaseID: requestID, RequestID: requestID}, nil
	}
	limiter := &runtimeOrgConnectionLimiter{
		store: store, orgID: "org-1", cpInstanceID: "cp-owner",
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
	if store.cancelID != "request-grant-cancel-race" {
		t.Fatalf("canceled request = %q", store.cancelID)
	}
}
