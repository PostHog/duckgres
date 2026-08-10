package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPeerFetchLimiterEnforcesCountAndBytesTogether(t *testing.T) {
	limiter := newPeerFetchLimiter(2, 20)

	first, reason := limiter.acquire(context.Background(), 10)
	if first == nil || reason != "" {
		t.Fatalf("first acquire = (%v, %q), want permit", first, reason)
	}
	second, reason := limiter.acquire(context.Background(), 10)
	if second == nil || reason != "" {
		t.Fatalf("second acquire = (%v, %q), want permit", second, reason)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if permit, gotReason := limiter.acquire(ctx, 1); permit != nil || gotReason != "deadline" {
		t.Fatalf("blocked acquire = (%v, %q), want (nil, deadline)", permit, gotReason)
	}
	if gotCount, gotBytes := limiter.snapshot(); gotCount != 2 || gotBytes != 20 {
		t.Fatalf("in flight = (%d, %d), want (2, 20)", gotCount, gotBytes)
	}

	first.release()
	third, reason := limiter.acquire(context.Background(), 10)
	if third == nil || reason != "" {
		t.Fatalf("acquire after release = (%v, %q), want permit", third, reason)
	}
	second.release()
	third.release()
	if gotCount, gotBytes := limiter.snapshot(); gotCount != 0 || gotBytes != 0 {
		t.Fatalf("in flight after release = (%d, %d), want zero", gotCount, gotBytes)
	}
}

func TestPeerFetchLimiterRejectsReservationAboveCeiling(t *testing.T) {
	limiter := newPeerFetchLimiter(32, 8)
	permit, reason := limiter.acquire(context.Background(), 9)
	if permit != nil || reason != "capacity" {
		t.Fatalf("oversized acquire = (%v, %q), want (nil, capacity)", permit, reason)
	}
}

func TestPeerFetchLimiterReleasesCountPermitWhenByteWaitExpires(t *testing.T) {
	limiter := newPeerFetchLimiter(2, 1)
	first, reason := limiter.acquire(context.Background(), 1)
	if first == nil || reason != "" {
		t.Fatalf("first acquire = (%v, %q), want permit", first, reason)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if permit, gotReason := limiter.acquire(ctx, 1); permit != nil || gotReason != "deadline" {
		t.Fatalf("byte-blocked acquire = (%v, %q), want (nil, deadline)", permit, gotReason)
	}

	// The failed acquire must release exactly its own count permit. Releasing
	// the original permit must not over-release the weighted semaphore.
	first.release()
	if gotCount, gotBytes := limiter.snapshot(); gotCount != 0 || gotBytes != 0 {
		t.Fatalf("in flight after release = (%d, %d), want zero", gotCount, gotBytes)
	}
}

func TestPeerFetchLimiterRejectsExpiredAbsoluteDeadline(t *testing.T) {
	limiter := newPeerFetchLimiter(1, 1)
	permit, reason := limiter.acquireBefore(context.Background(), time.Now().Add(-time.Millisecond), 1)
	if permit != nil || reason != "deadline" {
		t.Fatalf("expired acquire = (%v, %q), want (nil, deadline)", permit, reason)
	}
	if gotCount, gotBytes := limiter.snapshot(); gotCount != 0 || gotBytes != 0 {
		t.Fatalf("in flight after expired acquire = (%d, %d), want zero", gotCount, gotBytes)
	}
}

func TestPeerFetchPolicyAdaptiveHeadStartUsesRollingMedianAndClamps(t *testing.T) {
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	policy := newPeerFetchPolicy(cfg)

	if got := policy.headStart(); got != 25*time.Millisecond {
		t.Fatalf("empty-window head start = %v, want 25ms", got)
	}
	for _, sample := range []time.Duration{5, 10, 15, 20, 30} {
		policy.observePeer(sample*time.Millisecond, true)
	}
	if got := policy.headStart(); got != 25*time.Millisecond {
		t.Fatalf("low-median head start = %v, want lower clamp 25ms", got)
	}
	for i := 0; i < peerLatencyWindowSize; i++ {
		policy.observePeer(500*time.Millisecond, true)
	}
	if got := policy.headStart(); got != 150*time.Millisecond {
		t.Fatalf("high-median head start = %v, want upper clamp 150ms", got)
	}
}

func TestPeerCircuitBreakerRequiresSustainedLatencyRatioAndSamplesRecovery(t *testing.T) {
	now := time.Unix(1_000, 0)
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	cfg.now = func() time.Time { return now }
	cfg.breakerOpenAfter = 3
	cfg.breakerCloseAfter = 2
	cfg.breakerProbeInterval = time.Second
	cfg.breakerMinSamples = 1
	policy := newPeerFetchPolicy(cfg)

	for i := 0; i < 2; i++ {
		policy.observePeer(200*time.Millisecond, true)
		policy.observeOriginFirstBlock(100 * time.Millisecond)
		policy.recordLatencyRatioObservation()
	}
	if policy.breakerOpen() {
		t.Fatal("breaker opened before sustained-ratio threshold")
	}
	policy.observePeer(200*time.Millisecond, true)
	policy.observeOriginFirstBlock(100 * time.Millisecond)
	policy.recordLatencyRatioObservation()
	if !policy.breakerOpen() {
		t.Fatal("breaker remained closed after a sustained peer/origin ratio above 1.5")
	}

	if allowed, sample := policy.allowPeer(true); allowed || sample {
		t.Fatalf("peer allowed before recovery interval: allowed=%v sample=%v", allowed, sample)
	}
	now = now.Add(time.Second)
	allowed, sample := policy.allowPeer(true)
	if !allowed || !sample {
		t.Fatalf("recovery sample = (%v, %v), want (true, true)", allowed, sample)
	}
	if allowedAgain, sampleAgain := policy.allowPeer(true); allowedAgain || sampleAgain {
		t.Fatal("breaker allowed more than one concurrent recovery sample")
	}
	policy.finishProbe(sample, true)

	now = now.Add(time.Second)
	allowed, sample = policy.allowPeer(true)
	if !allowed || !sample {
		t.Fatal("second recovery sample was not admitted")
	}
	policy.finishProbe(sample, true)
	if policy.breakerOpen() {
		t.Fatal("breaker did not close after sustained successful recovery samples")
	}
}

func TestPeerCircuitBreakerDoesNotOpenWithoutLatencyRatioEvidence(t *testing.T) {
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	cfg.breakerOpenAfter = 3
	policy := newPeerFetchPolicy(cfg)

	for i := 0; i < 10; i++ {
		policy.recordLatencyRatioObservation()
	}
	if policy.breakerOpen() {
		t.Fatal("breaker opened on origin wins without comparable peer/origin latency samples")
	}
}

func TestAmbiguousPeerMeasurementIsGloballySampled(t *testing.T) {
	now := time.Unix(1_000, 0)
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	cfg.now = func() time.Time { return now }
	cfg.breakerProbeInterval = time.Second
	policy := newPeerFetchPolicy(cfg)

	if !policy.startAmbiguousMeasurement() {
		t.Fatal("first ambiguous measurement was not admitted")
	}
	if policy.startAmbiguousMeasurement() {
		t.Fatal("second concurrent ambiguous measurement was admitted")
	}
	policy.finishAmbiguousMeasurement()
	if policy.startAmbiguousMeasurement() {
		t.Fatal("ambiguous measurement was admitted before the sample interval")
	}
	now = now.Add(time.Second)
	if !policy.startAmbiguousMeasurement() {
		t.Fatal("ambiguous measurement was not admitted after the sample interval")
	}
	policy.finishAmbiguousMeasurement()
}

func TestPeerWithinBreakerRatioAllowsMarginallySlowerPeer(t *testing.T) {
	if !peerWithinBreakerRatio(130*time.Millisecond, 100*time.Millisecond, 1.5) {
		t.Fatal("peer at 1.3x origin should be a healthy recovery sample")
	}
	if peerWithinBreakerRatio(151*time.Millisecond, 100*time.Millisecond, 1.5) {
		t.Fatal("peer above 1.5x origin should be an unhealthy recovery sample")
	}
}

func TestPeerCircuitBreakerOpensOnSustainedLatencyRatio(t *testing.T) {
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	cfg.breakerMinSamples = 2
	cfg.breakerOpenAfter = 2
	policy := newPeerFetchPolicy(cfg)

	// The first sample warms the comparable EWMA window; the next two
	// slow-ratio decisions satisfy the sustained-loss threshold.
	for i := 0; i < 3; i++ {
		policy.observePeer(200*time.Millisecond, true)
		policy.observeOriginFirstBlock(100 * time.Millisecond)
		policy.recordLatencyRatioObservation()
	}
	if !policy.breakerOpen() {
		t.Fatal("breaker stayed closed with a sustained peer/origin ratio above 1.5")
	}
}

func TestPeerCircuitBreakerUsesThresholdQualifiedCanceledLowerBounds(t *testing.T) {
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	cfg.breakerMinSamples = 2
	cfg.breakerOpenAfter = 2
	policy := newPeerFetchPolicy(cfg)

	for i := 0; i < 3; i++ {
		policy.observeOriginFirstBlock(100 * time.Millisecond)
		policy.observePeerLowerBound(160 * time.Millisecond)
		policy.recordLatencyRatioObservation()
	}
	if !policy.breakerOpen() {
		t.Fatal("breaker stayed closed despite sustained canceled-peer lower bounds above 1.5x origin")
	}
}

func TestPeerCircuitBreakerOpensOnSustainedDirectThresholdEvidence(t *testing.T) {
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	cfg.breakerOpenAfter = 3
	policy := newPeerFetchPolicy(cfg)

	// Model an abrupt regression from a healthy low EWMA. A diagnostic that
	// times out at exactly the ratio boundary proves peer > ratio*origin, even
	// though adding that censored lower bound to the EWMA would take many
	// samples to overcome the old healthy history.
	for i := 0; i < 16; i++ {
		policy.observePeer(20*time.Millisecond, true)
	}
	for i := 0; i < 2; i++ {
		policy.recordThresholdComparison(true)
	}
	if policy.breakerOpen() {
		t.Fatal("breaker opened before the sustained direct-evidence threshold")
	}
	policy.recordThresholdComparison(true)
	if !policy.breakerOpen() {
		t.Fatal("breaker stayed closed after three direct peer>origin ratio observations")
	}
}

func TestPeerCircuitBreakerDirectEvidenceResetsOnKnownHealthyComparison(t *testing.T) {
	cfg := defaultPeerFetchPolicyConfig(32, 32<<20)
	cfg.breakerOpenAfter = 3
	policy := newPeerFetchPolicy(cfg)

	policy.recordThresholdComparison(true)
	policy.recordThresholdComparison(true)
	policy.recordThresholdComparison(false)
	policy.recordThresholdComparison(true)
	policy.recordThresholdComparison(true)
	if policy.breakerOpen() {
		t.Fatal("non-consecutive direct evidence opened after a known healthy comparison")
	}
	policy.recordThresholdComparison(true)
	if !policy.breakerOpen() {
		t.Fatal("breaker did not open after a new sustained run of direct evidence")
	}
}

func TestLatePeerSuccessAndDuplicateBytesAreCounted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fill := &peerFill{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	fill.started.Store(true)

	lateBefore := counterValue(t, latePeerSuccessesTotal)
	duplicateBefore := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("peer"))
	fill.cancelForOrigin()
	fill.finish(controlledPeerFetchResult{ok: true, bytes: 128, started: true})

	if got := counterValue(t, latePeerSuccessesTotal); got != lateBefore+1 {
		t.Fatalf("late peer successes delta = %v, want 1", got-lateBefore)
	}
	if got := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("peer")); got != duplicateBefore+128 {
		t.Fatalf("duplicate peer bytes delta = %v, want 128", got-duplicateBefore)
	}
}

func TestOriginSpanFlightsCancelOnlyAfterLastWaiterLeaves(t *testing.T) {
	var flights originSpanFlights
	started := make(chan struct{})
	producerCanceled := make(chan struct{})
	var starts atomic.Int32

	start := func(ctx context.Context) (originSpanResult, error) {
		if starts.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		close(producerCanceled)
		return originSpanResult{}, ctx.Err()
	}

	first := flights.start("same-span", start)
	second := flights.start("same-span", start)
	<-started
	if first.release() {
		t.Fatal("first waiter canceled a producer still needed by another waiter")
	}
	select {
	case <-producerCanceled:
		t.Fatal("shared producer was canceled while a waiter remained")
	case <-time.After(20 * time.Millisecond):
	}
	if !second.release() {
		t.Fatal("last waiter did not cancel the unneeded producer")
	}
	select {
	case <-producerCanceled:
	case <-time.After(time.Second):
		t.Fatal("producer did not observe cancellation")
	}
	if starts.Load() != 1 {
		t.Fatalf("producer starts = %d, want 1", starts.Load())
	}
}

func TestOriginSpanFlightWaitHonorsCallerCancellation(t *testing.T) {
	var flights originSpanFlights
	releaseProducer := make(chan struct{})
	flight := flights.start("span", func(context.Context) (originSpanResult, error) {
		<-releaseProducer
		return originSpanResult{}, nil
	})
	t.Cleanup(func() { close(releaseProducer) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := flight.wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}
	flight.release()
}

func TestOriginSpanFlightNewCallerDoesNotJoinCanceledCall(t *testing.T) {
	var flights originSpanFlights
	firstCanceled := make(chan struct{})
	allowFirstExit := make(chan struct{})
	secondStarted := make(chan struct{})
	var starts atomic.Int32

	start := func(ctx context.Context) (originSpanResult, error) {
		if starts.Add(1) == 1 {
			<-ctx.Done()
			close(firstCanceled)
			<-allowFirstExit
			return originSpanResult{}, ctx.Err()
		}
		close(secondStarted)
		return originSpanResult{bytesRead: 1}, nil
	}

	first := flights.start("same-span", start)
	first.release()
	<-firstCanceled
	second := flights.start("same-span", start)
	select {
	case <-secondStarted:
	case <-time.After(20 * time.Millisecond):
		close(allowFirstExit)
		second.release()
		t.Fatalf("producer starts = %d, want a fresh producer after last-waiter cancellation", starts.Load())
	}
	close(allowFirstExit)
	if result, err := second.wait(context.Background()); err != nil || result.bytesRead != 1 {
		t.Fatalf("fresh producer result = (%+v, %v), want one byte and no error", result, err)
	}
	second.release()
}

func TestOriginSpanFlightAbandonmentIsNotHedgeCancellation(t *testing.T) {
	var flights originSpanFlights
	cancelBefore := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("origin"))
	duplicateBefore := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("origin"))

	flight := flights.start("abandoned", func(ctx context.Context) (originSpanResult, error) {
		<-ctx.Done()
		return originSpanResult{bytesRead: 64}, ctx.Err()
	})
	if !flight.release() {
		t.Fatal("last abandoned waiter did not cancel its unneeded producer")
	}
	<-flight.call.done

	if got := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("origin")); got != cancelBefore {
		t.Fatalf("ordinary abandonment changed hedge cancellations by %v", got-cancelBefore)
	}
	if got := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("origin")); got != duplicateBefore {
		t.Fatalf("ordinary abandonment changed hedge duplicate bytes by %v", got-duplicateBefore)
	}
}

func TestCompletedUnusedOriginIsCountedAsHedgeDuplication(t *testing.T) {
	var flights originSpanFlights
	cancelBefore := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("origin"))
	duplicateBefore := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("origin"))

	flight := flights.start("completed-loser", func(context.Context) (originSpanResult, error) {
		return originSpanResult{bytesRead: 128}, nil
	})
	if _, err := flight.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if flight.releasePeerWinner() {
		t.Fatal("completed origin was reported as canceled")
	}

	if got := counterValue(t, peerFetchCancellationsTotal.WithLabelValues("origin")); got != cancelBefore {
		t.Fatalf("completed loser cancellation delta = %v, want 0", got-cancelBefore)
	}
	if got := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("origin")); got != duplicateBefore+128 {
		t.Fatalf("completed loser duplicate-byte delta = %v, want 128", got-duplicateBefore)
	}
}

func TestSharedOriginUsedByOneWaiterIsNotDuplicate(t *testing.T) {
	var flights originSpanFlights
	releaseProducer := make(chan struct{})
	duplicateBefore := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("origin"))
	start := func(context.Context) (originSpanResult, error) {
		<-releaseProducer
		return originSpanResult{bytesRead: 256}, nil
	}
	used := flights.start("shared-used", start)
	peerWon := flights.start("shared-used", start)
	close(releaseProducer)
	if _, err := used.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := peerWon.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	used.releaseOriginUsed()
	peerWon.releasePeerWinner()

	if got := counterValue(t, peerHedgeDuplicateBytesTotal.WithLabelValues("origin")); got != duplicateBefore {
		t.Fatalf("origin bytes used by another waiter were counted duplicate: delta=%v", got-duplicateBefore)
	}
}
