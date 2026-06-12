//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testClock is a manually-advanced clock for driving the broker's cache
// expiry checks.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestBroker(clock *testClock, assume assumeRoleFunc) *STSBroker {
	b := newSTSBroker(assume)
	b.now = clock.Now
	return b
}

func TestSTSBrokerCacheHitWithinValidity(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)}
	expiration := clock.Now().Add(stsSessionDuration)
	var calls atomic.Int64
	b := newTestBroker(clock, func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		calls.Add(1)
		return &AssumedCredentials{
			AccessKeyID:     "AKID",
			SecretAccessKey: "secret",
			SessionToken:    "token",
			Expiration:      expiration,
		}, nil
	})

	first, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
	if err != nil {
		t.Fatalf("first AssumeRole: %v", err)
	}
	// Well within validity: 1h creds with the safety margin → cached until
	// expiration minus the margin; stop one minute short of that boundary.
	clock.Advance(stsSessionDuration - stsCacheSafetyMargin - time.Minute)
	second, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
	if err != nil {
		t.Fatalf("second AssumeRole: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 underlying AssumeRole call, got %d", got)
	}
	if *second != *first {
		t.Fatalf("cached creds mismatch: got %+v want %+v", *second, *first)
	}
	// The true STS expiration must be preserved on cache hits (the
	// credential-refresh scheduler stamps it on worker records).
	if !second.Expiration.Equal(expiration) {
		t.Fatalf("cached creds expiration = %v, want true expiration %v", second.Expiration, expiration)
	}
}

func TestSTSBrokerCacheIsPerRoleARN(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)}
	var calls atomic.Int64
	b := newTestBroker(clock, func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		calls.Add(1)
		return &AssumedCredentials{
			AccessKeyID: "AKID-" + roleARN,
			Expiration:  clock.Now().Add(stsSessionDuration),
		}, nil
	})

	a, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
	if err != nil {
		t.Fatalf("AssumeRole org-a: %v", err)
	}
	bCreds, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-b")
	if err != nil {
		t.Fatalf("AssumeRole org-b: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 underlying calls (one per ARN), got %d", got)
	}
	if a.AccessKeyID == bCreds.AccessKeyID {
		t.Fatalf("expected distinct creds per ARN, both got %q", a.AccessKeyID)
	}
}

func TestSTSBrokerRefreshesAfterExpiryMargin(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)}
	var calls atomic.Int64
	b := newTestBroker(clock, func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		n := calls.Add(1)
		return &AssumedCredentials{
			AccessKeyID: "AKID",
			Expiration:  clock.Now().Add(stsSessionDuration),
			// Distinguish minted generations.
			SessionToken: map[int64]string{1: "gen1", 2: "gen2"}[n],
		}, nil
	})

	if _, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a"); err != nil {
		t.Fatalf("first AssumeRole: %v", err)
	}
	// 1h creds with the safety margin: once inside the margin the cached
	// creds are no longer served even though they have not truly expired yet.
	clock.Advance(stsSessionDuration - stsCacheSafetyMargin)
	refreshed, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
	if err != nil {
		t.Fatalf("refresh AssumeRole: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected refresh to call AWS again (2 calls), got %d", got)
	}
	if refreshed.SessionToken != "gen2" {
		t.Fatalf("expected freshly minted creds, got %+v", refreshed)
	}
}

func TestSTSBrokerSingleFlightCollapsesConcurrentCalls(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)}
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var calls atomic.Int64
	b := newTestBroker(clock, func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return &AssumedCredentials{
			AccessKeyID: "AKID",
			Expiration:  clock.Now().Add(stsSessionDuration),
		}, nil
	})

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	creds := make([]*AssumedCredentials, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			creds[i], errs[i] = b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
		}()
	}
	// Wait for the single in-flight AWS call, then let it finish. Any
	// goroutine still on its way either joins the in-flight call or hits the
	// freshly populated cache — never a second AWS call.
	<-entered
	close(release)
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if creds[i] == nil || creds[i].AccessKeyID != "AKID" {
			t.Fatalf("caller %d got unexpected creds %+v", i, creds[i])
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected concurrent calls to collapse to 1 AWS call, got %d", got)
	}
}

func TestSTSBrokerErrorNotCached(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)}
	wantErr := errors.New("throttled")
	var calls atomic.Int64
	b := newTestBroker(clock, func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		if calls.Add(1) == 1 {
			return nil, wantErr
		}
		return &AssumedCredentials{
			AccessKeyID: "AKID",
			Expiration:  clock.Now().Add(stsSessionDuration),
		}, nil
	})

	if _, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a"); !errors.Is(err, wantErr) {
		t.Fatalf("expected first call to fail with %v, got %v", wantErr, err)
	}
	got, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
	if err != nil {
		t.Fatalf("expected retry to succeed after failed call, got %v", err)
	}
	if got.AccessKeyID != "AKID" {
		t.Fatalf("unexpected creds after retry: %+v", got)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 underlying calls (error not cached), got %d", calls.Load())
	}
	// And the success IS cached.
	if _, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a"); err != nil {
		t.Fatalf("cached call: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected success to be cached, got %d underlying calls", calls.Load())
	}
}

func TestSTSBrokerWaiterRespectsContextCancellation(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)}
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	b := newTestBroker(clock, func(ctx context.Context, roleARN string) (*AssumedCredentials, error) {
		entered <- struct{}{}
		<-release
		return &AssumedCredentials{
			AccessKeyID: "AKID",
			Expiration:  clock.Now().Add(stsSessionDuration),
		}, nil
	})

	leaderDone := make(chan error, 1)
	go func() {
		_, err := b.AssumeRole(context.Background(), "arn:aws:iam::123:role/org-a")
		leaderDone <- err
	}()
	<-entered

	// A waiter whose ctx is cancelled must return promptly even though the
	// shared AWS call is still in flight.
	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := b.AssumeRole(ctx, "arn:aws:iam::123:role/org-a")
		waiterDone <- err
	}()
	cancel()
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled waiter did not return while AWS call was in flight")
	}

	// The in-flight call is unaffected by the waiter's cancellation.
	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader AssumeRole failed: %v", err)
	}
}
