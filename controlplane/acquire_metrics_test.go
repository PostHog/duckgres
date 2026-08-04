//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestAcquireMetricsObserveHelpers exercises every observe helper (and every
// label combination they can emit) so a registration conflict or label-arity
// mistake panics here instead of in production.
func TestAcquireMetricsObserveHelpers(t *testing.T) {
	const org = "metrics-test-org"
	gateBefore := histogramVecLabelSampleCount(t, workerAcquireGateWaitHistogram, org, acquireGateOutcomeAcquired)
	observeAcquireGateWait(125*time.Millisecond, org, acquireGateOutcomeAcquired)
	observeAcquireGateWait(-1*time.Second, org, acquireGateOutcomeCanceled) // negative durations clamp to 0
	if got := histogramVecLabelSampleCount(t, workerAcquireGateWaitHistogram, org, acquireGateOutcomeAcquired); got != gateBefore+1 {
		t.Fatalf("gate wait acquired samples = %d, want %d", got, gateBefore+1)
	}

	for _, phase := range []string{acquirePhaseHotIdleClaim, acquirePhaseSpawn, acquirePhaseActivate} {
		okBefore := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, phase, acquireOutcomeOK)
		errBefore := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, phase, acquireOutcomeError)
		observeAcquirePhase(phase, org, 50*time.Millisecond, nil)
		observeAcquirePhase(phase, org, 50*time.Millisecond, errors.New("boom"))
		if got := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, phase, acquireOutcomeOK); got != okBefore+1 {
			t.Fatalf("phase %q ok samples = %d, want %d", phase, got, okBefore+1)
		}
		if got := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, phase, acquireOutcomeError); got != errBefore+1 {
			t.Fatalf("phase %q error samples = %d, want %d", phase, got, errBefore+1)
		}
	}

	for _, source := range []string{acquireSourceIdleReuse, acquireSourceHotIdleClaim, acquireSourceSpawn, acquireSourceNone} {
		for _, outcome := range []string{acquireOutcomeOK, acquireOutcomeCapacity, acquireOutcomeError, acquireOutcomeCanceled} {
			before := histogramVecLabelSampleCount(t, workerAcquireTotalHistogram, org, source, outcome)
			observeAcquireTotal(time.Second, org, source, outcome)
			if got := histogramVecLabelSampleCount(t, workerAcquireTotalHistogram, org, source, outcome); got != before+1 {
				t.Fatalf("total source=%q outcome=%q samples = %d, want %d", source, outcome, got, before+1)
			}
		}
	}
}

func TestAcquireTotalOutcomeClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, acquireOutcomeOK},
		{"capacity", NewWorkerCapacityExhaustedError(time.Second), acquireOutcomeCapacity},
		{"wrapped capacity", fmt.Errorf("acquire: %w", NewWorkerCapacityExhaustedError(time.Second)), acquireOutcomeCapacity},
		{"canceled", context.Canceled, acquireOutcomeCanceled},
		{"deadline", fmt.Errorf("gate: %w", context.DeadlineExceeded), acquireOutcomeCanceled},
		{"other", errors.New("boom"), acquireOutcomeError},
	}
	for _, tc := range cases {
		if got := acquireTotalOutcome(tc.err); got != tc.want {
			t.Errorf("%s: acquireTotalOutcome = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestOrgReservedPoolAcquireObservesPhaseMetrics asserts a successful slow-path
// acquisition (no idle worker → gate → spawn → activate) records one sample in
// each phase histogram plus the gate-wait and end-to-end histograms.
func TestOrgReservedPoolAcquireObservesPhaseMetrics(t *testing.T) {
	const org = "analytics"
	gateBefore := histogramVecLabelSampleCount(t, workerAcquireGateWaitHistogram, org, acquireGateOutcomeAcquired)
	spawnBefore := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, acquirePhaseSpawn, acquireOutcomeOK)
	activateBefore := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, acquirePhaseActivate, acquireOutcomeOK)
	// A no-idle-worker acquire cold-spawns, so the end-to-end total lands under
	// source=spawn (proving the source label tracks the allocation path).
	totalBefore := histogramVecLabelSampleCount(t, workerAcquireTotalHistogram, org, acquireSourceSpawn, acquireOutcomeOK)

	shared, _ := newTestK8sPool(t, 5)
	shared.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	shared.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		shared.mu.Lock()
		shared.workers[id] = &ManagedWorker{ID: id, image: shared.workerImage, done: make(chan struct{})}
		shared.mu.Unlock()
		return nil
	}

	pool := NewOrgReservedPool(shared, org, 2, shared.workerImage, nil)
	pool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := pool.AcquireWorker(ctx, nil); err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}

	if got := histogramVecLabelSampleCount(t, workerAcquireGateWaitHistogram, org, acquireGateOutcomeAcquired); got != gateBefore+1 {
		t.Errorf("gate wait acquired samples = %d, want %d", got, gateBefore+1)
	}
	if got := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, acquirePhaseSpawn, acquireOutcomeOK); got != spawnBefore+1 {
		t.Errorf("spawn ok samples = %d, want %d", got, spawnBefore+1)
	}
	if got := histogramVecLabelSampleCount(t, workerAcquirePhaseHistogram, org, acquirePhaseActivate, acquireOutcomeOK); got != activateBefore+1 {
		t.Errorf("activate ok samples = %d, want %d", got, activateBefore+1)
	}
	if got := histogramVecLabelSampleCount(t, workerAcquireTotalHistogram, org, acquireSourceSpawn, acquireOutcomeOK); got != totalBefore+1 {
		t.Errorf("total spawn/ok samples = %d, want %d", got, totalBefore+1)
	}
}
