package controlplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAdmissionReclaimerMetricFamiliesUseCanonicalNames(t *testing.T) {
	orgConnectionReclaimAttemptsCounter.WithLabelValues(admissionReclaimAttemptOutcomeSuccess)
	orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectFull)

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(
		orgConnectionReclaimPendingGauge,
		orgConnectionReclaimAttemptsCounter,
		orgConnectionReclaimReservationsInUseGauge,
		orgConnectionReclaimReservationCapacityGauge,
		orgConnectionReclaimReservationRejectionsCounter,
	)
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather reclaimer metrics: %v", err)
	}

	want := map[string]bool{
		"duckgres_session_admission_reclaim_pending":                      false,
		"duckgres_session_admission_reclaim_attempts_total":               false,
		"duckgres_session_admission_reclaim_reservations_in_use":          false,
		"duckgres_session_admission_reclaim_reservation_capacity":         false,
		"duckgres_session_admission_reclaim_reservation_rejections_total": false,
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("canonical reclaimer metric %q was not registered", name)
		}
	}

	defaultFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather default metrics registry: %v", err)
	}
	for _, family := range defaultFamilies {
		if strings.HasPrefix(family.GetName(), "duckgres_org_connection_reclaim_") {
			t.Fatalf("retired reclaimer metric %q is still registered", family.GetName())
		}
	}
}

func TestAdmissionReclaimerSubmitRetainsAndDeduplicates(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Second,
	})
	t.Cleanup(r.Shutdown)

	ref := testAdmissionRef("request-1", "org-a")
	if err := r.Submit(ref, admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	r.mu.Lock()
	pendingAfterSubmit := len(r.pending)
	r.mu.Unlock()
	if pendingAfterSubmit != 1 {
		t.Fatalf("pending immediately after Submit = %d, want 1", pendingAfterSubmit)
	}
	if err := r.Submit(ref, admissionReclaimCauseAcquireAbandoned); err != nil {
		t.Fatalf("duplicate Submit: %v", err)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reclaim attempt did not start")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls while duplicate intent is in flight = %d, want 1", got)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("total calls = %d, want one deduplicated reclaim", got)
	}
}

func TestAdmissionReclaimerRetriesWithConfiguredBackoff(t *testing.T) {
	var calls atomic.Int32
	storeErr := errors.New("config store unavailable")
	store := admissionReclaimStoreFunc(func(context.Context, configstore.OrgConnectionAdmissionRef) error {
		if calls.Add(1) <= 3 {
			return storeErr
		}
		return nil
	})

	var backoffMu sync.Mutex
	var backoffFailures []int
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Second,
		RetryBackoff: func(failures int) time.Duration {
			backoffMu.Lock()
			backoffFailures = append(backoffFailures, failures)
			backoffMu.Unlock()
			return time.Millisecond
		},
	})
	t.Cleanup(r.Shutdown)

	if err := r.Submit(testAdmissionRef("request-retry", "org-a"), admissionReclaimCauseAcquireAbandoned); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("reclaim calls = %d, want 4", got)
	}
	backoffMu.Lock()
	gotFailures := append([]int(nil), backoffFailures...)
	backoffMu.Unlock()
	wantFailures := []int{1, 2, 3}
	if len(gotFailures) != len(wantFailures) {
		t.Fatalf("backoff calls = %v, want %v", gotFailures, wantFailures)
	}
	for i := range wantFailures {
		if gotFailures[i] != wantFailures[i] {
			t.Fatalf("backoff calls = %v, want %v", gotFailures, wantFailures)
		}
	}
}

func TestAdmissionReclaimerUsesDetachedBoundedAttemptContexts(t *testing.T) {
	firstStarted := make(chan time.Duration, 1)
	firstCanceled := make(chan error, 1)
	var calls atomic.Int32
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		if calls.Add(1) != 1 {
			return nil
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			firstStarted <- -1
			return errors.New("attempt context has no deadline")
		}
		firstStarted <- time.Until(deadline)
		<-ctx.Done()
		firstCanceled <- ctx.Err()
		return ctx.Err()
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: 20 * time.Millisecond,
		RetryBackoff:   func(int) time.Duration { return 0 },
	})
	t.Cleanup(r.Shutdown)

	if err := r.Submit(testAdmissionRef("request-timeout", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case remaining := <-firstStarted:
		if remaining < 0 {
			t.Fatal("attempt context did not have a deadline")
		}
		if remaining <= 0 || remaining > 100*time.Millisecond {
			t.Fatalf("attempt deadline remaining = %v, want a fresh bounded context", remaining)
		}
	case <-time.After(time.Second):
		t.Fatal("first reclaim attempt did not start")
	}
	select {
	case err := <-firstCanceled:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first attempt context error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first attempt context was not canceled at its deadline")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose after retry: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("reclaim calls = %d, want timed-out attempt plus successful retry", got)
	}
}

func TestAdmissionReclaimerBoundsConcurrencyAndSerializesEachOrg(t *testing.T) {
	store := newBlockingAdmissionReclaimStore()
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     2,
		AttemptTimeout: time.Second,
	})
	t.Cleanup(r.Shutdown)

	refs := []configstore.OrgConnectionAdmissionRef{
		testAdmissionRef("request-a-1", "org-a"),
		testAdmissionRef("request-a-2", "org-a"),
		testAdmissionRef("request-b-1", "org-b"),
	}
	for _, ref := range refs {
		if err := r.Submit(ref, admissionReclaimCauseLeaseRelease); err != nil {
			t.Fatalf("Submit(%s): %v", ref.RequestID, err)
		}
	}

	first := receiveAdmissionRef(t, store.entered)
	second := receiveAdmissionRef(t, store.entered)
	if first.OrgID == second.OrgID {
		t.Fatalf("first two concurrent attempts targeted the same org: %#v, %#v", first, second)
	}
	select {
	case third := <-store.entered:
		t.Fatalf("third attempt started before a worker slot was free: %#v", third)
	case <-time.After(20 * time.Millisecond):
	}

	close(store.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.calls != len(refs) {
		t.Fatalf("reclaim calls = %d, want %d", store.calls, len(refs))
	}
	if store.maxActive > 2 {
		t.Fatalf("maximum concurrent attempts = %d, want at most 2", store.maxActive)
	}
	if store.maxActiveByOrg > 1 {
		t.Fatalf("maximum concurrent attempts for one org = %d, want 1", store.maxActiveByOrg)
	}
}

func TestAdmissionReclaimerDispatchesDueSameOrgIntentsInSubmissionOrder(t *testing.T) {
	const intentCount = 12
	entered := make(chan configstore.OrgConnectionAdmissionRef, intentCount+1)
	releaseGate := make(chan struct{})
	store := admissionReclaimStoreFunc(func(ctx context.Context, ref configstore.OrgConnectionAdmissionRef) error {
		entered <- ref
		if ref.RequestID != "request-gate" {
			return nil
		}
		select {
		case <-releaseGate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Second,
	})
	t.Cleanup(r.Shutdown)

	if err := r.Submit(testAdmissionRef("request-gate", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit gate: %v", err)
	}
	if got := receiveAdmissionRef(t, entered); got.RequestID != "request-gate" {
		t.Fatalf("first attempt = %q, want gate", got.RequestID)
	}

	refs := make([]configstore.OrgConnectionAdmissionRef, 0, intentCount)
	for i := range intentCount {
		ref := testAdmissionRef(fmt.Sprintf("request-%02d", i), "org-a")
		refs = append(refs, ref)
		if err := r.Submit(ref, admissionReclaimCauseLeaseRelease); err != nil {
			t.Fatalf("Submit(%s): %v", ref.RequestID, err)
		}
	}
	// Make the scheduling deadline identical so immutable submission sequence is
	// the only tie-breaker once the per-org gate finishes.
	due := time.Now().Add(-time.Second)
	r.mu.Lock()
	for _, ref := range refs {
		r.pending[ref].nextAttempt = due
	}
	r.mu.Unlock()

	close(releaseGate)
	for i, want := range refs {
		if got := receiveAdmissionRef(t, entered); got != want {
			t.Fatalf("attempt %d = %#v, want oldest due intent %#v", i+1, got, want)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
}

func TestAdmissionReclaimerDrainMakesBackoffIntentImmediatelyEligible(t *testing.T) {
	backoffScheduled := make(chan struct{}, 1)
	var calls atomic.Int32
	store := admissionReclaimStoreFunc(func(context.Context, configstore.OrgConnectionAdmissionRef) error {
		if calls.Add(1) == 1 {
			return errors.New("temporary failure")
		}
		return nil
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Second,
		RetryBackoff: func(int) time.Duration {
			backoffScheduled <- struct{}{}
			return time.Hour
		},
	})
	t.Cleanup(r.Shutdown)

	if err := r.Submit(testAdmissionRef("request-drain-retry", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-backoffScheduled:
	case <-time.After(time.Second):
		t.Fatal("first failed attempt did not schedule its backoff")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose should immediately retry backoff work: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("reclaim calls = %d, want failed attempt plus immediate drain attempt", got)
	}
}

func TestAdmissionReclaimerDrainRetriesPromptlyWithoutHotLooping(t *testing.T) {
	failureRelease := make(chan struct{})
	attempted := make(chan int, 3)
	var calls atomic.Int32
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		call := int(calls.Add(1))
		attempted <- call
		if call > 2 {
			return nil
		}
		select {
		case <-failureRelease:
			return errors.New("temporary failure")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Second,
		RetryBackoff:   func(int) time.Duration { return time.Hour },
	})
	t.Cleanup(r.Shutdown)

	if err := r.Submit(testAdmissionRef("request-drain-repeated-retry", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := receiveAttemptNumber(t, attempted); got != 1 {
		t.Fatalf("first attempt number = %d, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- r.DrainAndClose(ctx) }()

	deadline := time.Now().Add(100 * time.Millisecond)
	for {
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("DrainAndClose did not begin closing the reclaimer")
		}
		time.Sleep(time.Millisecond)
	}

	started := time.Now()
	var secondFailureReleasedAt time.Time
	for want := 2; want <= 3; want++ {
		if want == 2 {
			failureRelease <- struct{}{}
		}
		select {
		case got := <-attempted:
			if got != want {
				t.Fatalf("attempt number = %d, want %d", got, want)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("attempt %d did not start promptly while draining", want)
		}
		if want == 2 {
			secondFailureReleasedAt = time.Now()
			failureRelease <- struct{}{}
		}
	}
	if retryDelay := time.Since(secondFailureReleasedAt); retryDelay < 50*time.Millisecond {
		t.Fatalf("post-drain failure retried after %v, want a bounded delay to avoid hot-looping", retryDelay)
	}

	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("DrainAndClose: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("DrainAndClose retained the configured retry backoff")
	}
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("DrainAndClose took %v, want less than 500ms", elapsed)
	}
}

func TestAdmissionReclaimerRetryBackoffRunsWithoutHoldingMutex(t *testing.T) {
	backoffEntered := make(chan struct{})
	backoffRelease := make(chan struct{})
	var calls atomic.Int32
	store := admissionReclaimStoreFunc(func(context.Context, configstore.OrgConnectionAdmissionRef) error {
		if calls.Add(1) == 1 {
			return errors.New("temporary failure")
		}
		return nil
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Second,
		RetryBackoff: func(int) time.Duration {
			close(backoffEntered)
			<-backoffRelease
			return time.Hour
		},
	})
	t.Cleanup(r.Shutdown)

	if err := r.Submit(testAdmissionRef("request-blocked-backoff", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit first ref: %v", err)
	}
	select {
	case <-backoffEntered:
	case <-time.After(time.Second):
		t.Fatal("retry backoff callback did not start")
	}

	submitted := make(chan error, 1)
	go func() {
		submitted <- r.Submit(testAdmissionRef("request-during-backoff", "org-b"), admissionReclaimCauseLeaseRelease)
	}()
	select {
	case err := <-submitted:
		if err != nil {
			close(backoffRelease)
			t.Fatalf("Submit second ref: %v", err)
		}
		close(backoffRelease)
	case <-time.After(100 * time.Millisecond):
		close(backoffRelease)
		<-submitted
		t.Fatal("Submit blocked while RetryBackoff was executing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
}

func TestAdmissionReclaimerReservationsBoundOwnershipBeforeActivation(t *testing.T) {
	fullRejectionsBefore := testutil.ToFloat64(
		orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectFull),
	)
	attempted := make(chan configstore.OrgConnectionAdmissionRef, 3)
	store := admissionReclaimStoreFunc(func(_ context.Context, ref configstore.OrgConnectionAdmissionRef) error {
		attempted <- ref
		return nil
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:      1,
		MaxReservations: 1,
		AttemptTimeout:  time.Second,
		RetryBackoff:    func(int) time.Duration { return 0 },
	})
	t.Cleanup(r.Shutdown)

	reserved, err := r.Reserve(testAdmissionRef("request-reserved-first", "org-a"))
	if err != nil {
		t.Fatalf("Reserve first intent: %v", err)
	}
	if _, err := r.Reserve(testAdmissionRef("request-over-capacity", "org-b")); !errors.Is(err, ErrAdmissionReclaimerFull) {
		t.Fatalf("Reserve over capacity error = %v, want ErrAdmissionReclaimerFull", err)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectFull)); got != fullRejectionsBefore+1 {
		t.Fatalf("full reservation rejections = %v, want %v", got, fullRejectionsBefore+1)
	}
	select {
	case ref := <-attempted:
		t.Fatalf("held reservation started reclaiming %#v", ref)
	case <-time.After(20 * time.Millisecond):
	}

	firstRef := testAdmissionRef("request-reserved-first", "org-a")
	reserved.Reclaim(admissionReclaimCauseLeaseRelease)
	if got := receiveAdmissionRef(t, attempted); got != firstRef {
		t.Fatalf("first activated ref = %#v, want %#v", got, firstRef)
	}

	deadline := time.Now().Add(time.Second)
	var activated AdmissionReclaimReservation
	activatedRef := testAdmissionRef("request-reserved-second", "org-b")
	for time.Now().Before(deadline) {
		activated, err = r.Reserve(activatedRef)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrAdmissionReclaimerFull) {
			t.Fatalf("Reserve after first reclaim: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Reserve after first reclaim did not regain capacity: %v", err)
	}
	activated.Reclaim(admissionReclaimCauseLeaseRelease)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
	if got := receiveAdmissionRef(t, attempted); got != activatedRef {
		t.Fatalf("second activated ref = %#v, want %#v", got, activatedRef)
	}
}

func TestAdmissionReclaimerSubmitCapacityRejectionCountsOnce(t *testing.T) {
	fullRejectionsBefore := testutil.ToFloat64(
		orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectFull),
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:      1,
		MaxReservations: 1,
		AttemptTimeout:  time.Second,
	})
	t.Cleanup(r.Shutdown)

	if err := r.Submit(testAdmissionRef("request-first", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit first intent: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first reclaim attempt did not start")
	}
	if err := r.Submit(testAdmissionRef("request-over-capacity", "org-b"), admissionReclaimCauseLeaseRelease); !errors.Is(err, ErrAdmissionReclaimerFull) {
		t.Fatalf("Submit over capacity error = %v, want ErrAdmissionReclaimerFull", err)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimReservationRejectionsCounter.WithLabelValues(admissionReclaimReservationRejectFull)); got != fullRejectionsBefore+1 {
		t.Fatalf("full submit rejections = %v, want %v", got, fullRejectionsBefore+1)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
}

func TestAdmissionReclaimerDrainActivatesHeldReservation(t *testing.T) {
	attempted := make(chan configstore.OrgConnectionAdmissionRef, 1)
	store := admissionReclaimStoreFunc(func(_ context.Context, ref configstore.OrgConnectionAdmissionRef) error {
		attempted <- ref
		return nil
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:      1,
		MaxReservations: 1,
		AttemptTimeout:  time.Second,
	})
	t.Cleanup(r.Shutdown)

	ref := testAdmissionRef("request-held-until-drain", "org-a")
	if _, err := r.Reserve(ref); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	select {
	case got := <-attempted:
		t.Fatalf("held reservation reclaimed before drain: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
	if got := receiveAdmissionRef(t, attempted); got != ref {
		t.Fatalf("drain-activated ref = %#v, want %#v", got, ref)
	}
}

func TestAdmissionReclaimerStaleReservationCannotActivateReusedRef(t *testing.T) {
	attempted := make(chan configstore.OrgConnectionAdmissionRef, 2)
	store := admissionReclaimStoreFunc(func(_ context.Context, ref configstore.OrgConnectionAdmissionRef) error {
		attempted <- ref
		return nil
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:      1,
		MaxReservations: 1,
		AttemptTimeout:  time.Second,
	})
	t.Cleanup(r.Shutdown)

	ref := testAdmissionRef("request-reused-reservation", "org-a")
	oldReservation, err := r.Reserve(ref)
	if err != nil {
		t.Fatalf("Reserve old generation: %v", err)
	}
	oldReservation.Reclaim(admissionReclaimCauseLeaseRelease)
	if got := receiveAdmissionRef(t, attempted); got != ref {
		t.Fatalf("old generation ref = %#v, want %#v", got, ref)
	}

	var newReservation AdmissionReclaimReservation
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		newReservation, err = r.Reserve(ref)
		if err == nil {
			break
		}
		if !errors.Is(err, errAdmissionReclaimerDuplicateReservation) {
			t.Fatalf("Reserve reused ref: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Reserve reused ref did not succeed: %v", err)
	}

	oldReservation.Reclaim(admissionReclaimCauseAcquireAbandoned)
	select {
	case got := <-attempted:
		t.Fatalf("stale reservation activated reused ref: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}
	newReservation.Reclaim(admissionReclaimCauseLeaseRelease)
	if got := receiveAdmissionRef(t, attempted); got != ref {
		t.Fatalf("new generation ref = %#v, want %#v", got, ref)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
}

func TestAdmissionReclaimerDrainDeadlineDoesNotWaitForUncooperativeStore(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	store := admissionReclaimStoreFunc(func(context.Context, configstore.OrgConnectionAdmissionRef) error {
		entered <- struct{}{}
		<-release // Deliberately ignore context cancellation.
		return context.Canceled
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Hour,
	})

	if err := r.Submit(testAdmissionRef("request-uncooperative", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reclaim attempt did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- r.DrainAndClose(ctx) }()

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			close(release)
			r.Shutdown()
			t.Fatalf("DrainAndClose error = %v, want deadline exceeded", err)
		}
		close(release)
		r.Shutdown()
	case <-time.After(250 * time.Millisecond):
		close(release)
		err := <-result
		r.Shutdown()
		t.Fatalf("DrainAndClose remained blocked after its deadline; eventual error: %v", err)
	}
}

func TestAdmissionReclaimerPendingGaugeAggregatesInstancesAndReleasesOnShutdown(t *testing.T) {
	baseline := testutil.ToFloat64(orgConnectionReclaimPendingGauge)
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		<-ctx.Done()
		return ctx.Err()
	})
	r1 := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{AttemptTimeout: time.Hour})
	if err := r1.Submit(testAdmissionRef("request-metric-1", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit first instance: %v", err)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != baseline+1 {
		r1.Shutdown()
		t.Fatalf("pending gauge after first submit = %v, want %v", got, baseline+1)
	}

	r2 := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{AttemptTimeout: time.Hour})
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != baseline+1 {
		r1.Shutdown()
		r2.Shutdown()
		t.Fatalf("constructing a second instance changed aggregate gauge to %v, want %v", got, baseline+1)
	}
	if err := r2.Submit(testAdmissionRef("request-metric-2", "org-b"), admissionReclaimCauseLeaseRelease); err != nil {
		r1.Shutdown()
		r2.Shutdown()
		t.Fatalf("Submit second instance: %v", err)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != baseline+2 {
		r1.Shutdown()
		r2.Shutdown()
		t.Fatalf("aggregate pending gauge = %v, want %v", got, baseline+2)
	}

	r1.Shutdown()
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != baseline+1 {
		r2.Shutdown()
		t.Fatalf("pending gauge after first forced shutdown = %v, want %v", got, baseline+1)
	}
	r2.Shutdown()
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != baseline {
		t.Fatalf("pending gauge after both forced shutdowns = %v, want baseline %v", got, baseline)
	}
}

func TestAdmissionReclaimerReservationMetricsSeparateHeldFromPending(t *testing.T) {
	pendingBaseline := testutil.ToFloat64(orgConnectionReclaimPendingGauge)
	inUseBaseline := testutil.ToFloat64(orgConnectionReclaimReservationsInUseGauge)
	capacityBaseline := testutil.ToFloat64(orgConnectionReclaimReservationCapacityGauge)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:      1,
		MaxReservations: 3,
		AttemptTimeout:  time.Second,
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		r.Shutdown()
	})

	if got := testutil.ToFloat64(orgConnectionReclaimReservationCapacityGauge); got != capacityBaseline+3 {
		t.Fatalf("reservation capacity = %v, want %v", got, capacityBaseline+3)
	}
	reservation, err := r.Reserve(testAdmissionRef("request-held-metric", "org-a"))
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimReservationsInUseGauge); got != inUseBaseline+1 {
		t.Fatalf("reservations in use = %v, want %v", got, inUseBaseline+1)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != pendingBaseline {
		t.Fatalf("held reservation changed pending gauge to %v, want %v", got, pendingBaseline)
	}

	reservation.Reclaim(admissionReclaimCauseLeaseRelease)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("activated reservation did not start cleanup")
	}
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != pendingBaseline+1 {
		t.Fatalf("pending gauge after activation = %v, want %v", got, pendingBaseline+1)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimReservationsInUseGauge); got != inUseBaseline+1 {
		t.Fatalf("activation changed reservations in use to %v, want %v", got, inUseBaseline+1)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.DrainAndClose(ctx); err != nil {
		t.Fatalf("DrainAndClose: %v", err)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimPendingGauge); got != pendingBaseline {
		t.Fatalf("pending gauge after drain = %v, want %v", got, pendingBaseline)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimReservationsInUseGauge); got != inUseBaseline {
		t.Fatalf("reservations in use after drain = %v, want %v", got, inUseBaseline)
	}
	if got := testutil.ToFloat64(orgConnectionReclaimReservationCapacityGauge); got != capacityBaseline {
		t.Fatalf("reservation capacity after drain = %v, want %v", got, capacityBaseline)
	}
}

func TestAdmissionReclaimerDrainTimeoutStopsWorkersAndClosesSubmission(t *testing.T) {
	var logs bytes.Buffer
	entered := make(chan struct{}, 1)
	canceled := make(chan error, 1)
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		entered <- struct{}{}
		<-ctx.Done()
		canceled <- ctx.Err()
		return ctx.Err()
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Hour,
		Logger:         slog.New(slog.NewTextHandler(&logs, nil)),
	})

	ref := testAdmissionRef("request-stuck", "org-a")
	if err := r.Submit(ref, admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reclaim attempt did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := r.DrainAndClose(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DrainAndClose error = %v, want deadline exceeded", err)
	}
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("attempt error after forced shutdown = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("forced shutdown did not cancel the detached attempt")
	}
	if err := r.Submit(testAdmissionRef("request-late", "org-b"), admissionReclaimCauseAcquireAbandoned); !errors.Is(err, ErrAdmissionReclaimerClosed) {
		t.Fatalf("Submit after DrainAndClose error = %v, want ErrAdmissionReclaimerClosed", err)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, "Admission reclaimer stopped with pending cleanup intents.") ||
		!strings.Contains(logOutput, "reason=drain_timeout") ||
		!strings.Contains(logOutput, "pending=1") ||
		!strings.Contains(logOutput, "oldest_age=") {
		t.Fatalf("aggregate drain-timeout log missing expected fields: %s", logOutput)
	}
}

func TestAdmissionReclaimerShutdownCancelsAttempt(t *testing.T) {
	var logs bytes.Buffer
	entered := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	store := admissionReclaimStoreFunc(func(ctx context.Context, _ configstore.OrgConnectionAdmissionRef) error {
		entered <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		return ctx.Err()
	})
	r := NewAdmissionReclaimer(store, AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: time.Hour,
		Logger:         slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err := r.Submit(testAdmissionRef("request-shutdown", "org-a"), admissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reclaim attempt did not start")
	}
	r.Shutdown()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not cancel the in-flight attempt")
	}
	if err := r.Submit(testAdmissionRef("request-late", "org-b"), admissionReclaimCauseLeaseRelease); !errors.Is(err, ErrAdmissionReclaimerClosed) {
		t.Fatalf("Submit after Shutdown error = %v, want ErrAdmissionReclaimerClosed", err)
	}
	logOutput := logs.String()
	if !strings.Contains(logOutput, "Admission reclaimer stopped with pending cleanup intents.") ||
		!strings.Contains(logOutput, "reason=shutdown") ||
		!strings.Contains(logOutput, "pending=1") ||
		!strings.Contains(logOutput, "oldest_age=") {
		t.Fatalf("aggregate shutdown log missing expected fields: %s", logOutput)
	}
}

func TestAdmissionReclaimBackoffBaseAndJitterBounds(t *testing.T) {
	tests := []struct {
		failures int
		wantBase time.Duration
	}{
		{failures: 0, wantBase: 250 * time.Millisecond},
		{failures: 1, wantBase: 250 * time.Millisecond},
		{failures: 2, wantBase: 500 * time.Millisecond},
		{failures: 3, wantBase: time.Second},
		{failures: 7, wantBase: 16 * time.Second},
		{failures: 8, wantBase: 30 * time.Second},
		{failures: 100, wantBase: 30 * time.Second},
	}
	for _, tt := range tests {
		if got := admissionReclaimBackoffBase(tt.failures); got != tt.wantBase {
			t.Fatalf("admissionReclaimBackoffBase(%d) = %v, want %v", tt.failures, got, tt.wantBase)
		}
		for range 20 {
			got := defaultAdmissionReclaimBackoff(tt.failures)
			if got < tt.wantBase/2 || got > tt.wantBase {
				t.Fatalf("defaultAdmissionReclaimBackoff(%d) = %v, want [%v, %v]", tt.failures, got, tt.wantBase/2, tt.wantBase)
			}
		}
	}
}

type admissionReclaimStoreFunc func(context.Context, configstore.OrgConnectionAdmissionRef) error

func (f admissionReclaimStoreFunc) ReclaimOrgConnectionAdmissionContext(ctx context.Context, ref configstore.OrgConnectionAdmissionRef) error {
	return f(ctx, ref)
}

type blockingAdmissionReclaimStore struct {
	mu             sync.Mutex
	active         int
	activeByOrg    map[string]int
	maxActive      int
	maxActiveByOrg int
	calls          int
	entered        chan configstore.OrgConnectionAdmissionRef
	release        chan struct{}
}

func newBlockingAdmissionReclaimStore() *blockingAdmissionReclaimStore {
	return &blockingAdmissionReclaimStore{
		activeByOrg: make(map[string]int),
		entered:     make(chan configstore.OrgConnectionAdmissionRef, 8),
		release:     make(chan struct{}),
	}
}

func (s *blockingAdmissionReclaimStore) ReclaimOrgConnectionAdmissionContext(ctx context.Context, ref configstore.OrgConnectionAdmissionRef) error {
	s.mu.Lock()
	s.calls++
	s.active++
	s.activeByOrg[ref.OrgID]++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	if s.activeByOrg[ref.OrgID] > s.maxActiveByOrg {
		s.maxActiveByOrg = s.activeByOrg[ref.OrgID]
	}
	s.mu.Unlock()

	s.entered <- ref
	var err error
	select {
	case <-s.release:
	case <-ctx.Done():
		err = ctx.Err()
	}

	s.mu.Lock()
	s.active--
	s.activeByOrg[ref.OrgID]--
	s.mu.Unlock()
	return err
}

func testAdmissionRef(requestID, orgID string) configstore.OrgConnectionAdmissionRef {
	return configstore.OrgConnectionAdmissionRef{
		RequestID:    requestID,
		OrgID:        orgID,
		CPInstanceID: "cp-test",
	}
}

func receiveAdmissionRef(t *testing.T, refs <-chan configstore.OrgConnectionAdmissionRef) configstore.OrgConnectionAdmissionRef {
	t.Helper()
	select {
	case ref := <-refs:
		return ref
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reclaim attempt")
		return configstore.OrgConnectionAdmissionRef{}
	}
}

func receiveAttemptNumber(t *testing.T, attempts <-chan int) int {
	t.Helper()
	select {
	case attempt := <-attempts:
		return attempt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reclaim attempt")
		return 0
	}
}
