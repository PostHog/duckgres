//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMarkWorkerRetiredLocked_TransitionsToRetired(t *testing.T) {
	// Retirement reason is recorded on the durable side via the
	// lifecycle service's CAS path; in-memory bookkeeping just flips
	// the lifecycle state, which is what this test pins.
	pool, _ := newTestK8sPool(t, 5)

	w := makeTestWorker(WorkerLifecycleIdle, nil)
	pool.workers[1] = w

	pool.markWorkerRetiredLocked(w, RetireReasonIdleTimeout)

	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleRetired {
		t.Fatalf("expected retired, got %s", w.SharedState().NormalizedLifecycle())
	}
}

func TestMarkWorkerRetiredLocked_RecordsHotWorkerSessions(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	w := makeTestWorker(WorkerLifecycleHot, &WorkerAssignment{OrgID: "org-1"})
	w.peakSessions = 5
	pool.workers[1] = w

	pool.markWorkerRetiredLocked(w, RetireReasonNormal)

	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleRetired {
		t.Fatalf("expected retired, got %s", w.SharedState().NormalizedLifecycle())
	}
}

func TestReservedAtTracking(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	w := makeTestWorker(WorkerLifecycleIdle, nil)
	pool.workers[1] = w

	before := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := pool.ReserveSharedWorker(ctx, &WorkerAssignment{
		OrgID: "org-1",
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker failed: %v", err)
	}
	after := time.Now()

	if w.reservedAt.Before(before) || w.reservedAt.After(after) {
		t.Fatalf("reservedAt %v not between %v and %v", w.reservedAt, before, after)
	}
}

func TestPeakSessionsTracking(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	w := makeTestWorker(WorkerLifecycleIdle, nil)
	pool.workers[1] = w

	// Simulate session assignments
	pool.mu.Lock()
	w.activeSessions = 3
	if w.activeSessions > w.peakSessions {
		w.peakSessions = w.activeSessions
	}
	pool.mu.Unlock()

	pool.mu.Lock()
	w.activeSessions = 1
	pool.mu.Unlock()

	if w.peakSessions != 3 {
		t.Fatalf("expected peakSessions=3, got %d", w.peakSessions)
	}
}

func TestActivateWorkerForOrgRecordsActivationDurationWhenWorkerAlreadyHot(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	worker := makeTestWorker(WorkerLifecycleReserved, &WorkerAssignment{
		OrgID: "org-1",
	})
	worker.reservedAt = time.Now().Add(-2 * time.Second)
	pool.workers[1] = worker

	orgPool := NewOrgReservedPool(pool, "org-1", 1, pool.workerImage, nil)
	orgPool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		nextState, err := worker.SharedState().Transition(WorkerLifecycleHot, nil)
		if err != nil {
			return err
		}
		return worker.SetSharedState(nextState)
	}

	before := metricHistogramCount(t, "duckgres_activation_duration_seconds")
	if err := orgPool.activateWorkerForOrg(context.Background(), worker); err != nil {
		t.Fatalf("activateWorkerForOrg failed: %v", err)
	}
	after := metricHistogramCount(t, "duckgres_activation_duration_seconds")

	if after-before != 1 {
		t.Fatalf("expected activation duration histogram sample count delta 1, got %d", after-before)
	}
}

func TestReapStuckActivatingWorkers(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.minWorkers = 2
	pool.activatingTimeout = 50 * time.Millisecond

	spawnedIDs := make(chan int, 10)
	pool.spawnWarmWorkerBackgroundFunc = func(id int) {
		spawnedIDs <- id
	}

	// One idle worker (healthy), one stuck activating worker
	idle := makeTestWorker(WorkerLifecycleIdle, nil)
	pool.workers[1] = idle

	stuck := makeTestWorker(WorkerLifecycleActivating, &WorkerAssignment{
		OrgID: "org-1",
	})
	stuck.reservedAt = time.Now().Add(-time.Minute) // reserved 1 minute ago
	pool.workers[2] = stuck

	pool.reapStuckActivatingWorkers()

	// Stuck worker should be removed
	pool.mu.RLock()
	_, stuckExists := pool.workers[2]
	_, idleExists := pool.workers[1]
	pool.mu.RUnlock()

	if stuckExists {
		t.Fatal("stuck worker should have been reaped")
	}
	if !idleExists {
		t.Fatal("idle worker should not have been reaped")
	}

	// Should have spawned a replacement since minWorkers=2 and only 1 remains
	select {
	case <-spawnedIDs:
		// good
	case <-time.After(time.Second):
		t.Fatal("expected replacement worker to be spawned")
	}
}

func TestReapStuckActivatingWorkers_RecentlyReservedNotReaped(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.activatingTimeout = 2 * time.Minute

	w := makeTestWorker(WorkerLifecycleActivating, &WorkerAssignment{
		OrgID: "org-1",
	})
	w.reservedAt = time.Now() // just reserved
	pool.workers[1] = w

	pool.reapStuckActivatingWorkers()

	pool.mu.RLock()
	_, exists := pool.workers[1]
	pool.mu.RUnlock()

	if !exists {
		t.Fatal("recently reserved worker should not be reaped")
	}
}

func TestObserveWarmCapacityMetrics(t *testing.T) {
	image := "duckgres:metrics-test"
	warmCapacityMissesCounter.DeleteLabelValues(image, string(configstore.WorkerClaimMissReasonGlobalCap))

	observeWarmCapacityMiss(image, configstore.WorkerClaimMissReasonGlobalCap)
	if got := counterLabelValues(warmCapacityMissesCounter, image, string(configstore.WorkerClaimMissReasonGlobalCap)); got != 1 {
		t.Fatalf("expected one global-cap warm capacity miss, got %v", got)
	}

	observeWarmCapacityTargets(
		map[string]int{image: 2},
		map[string]int{image: 5},
		10,
	)
	assertGaugeVecValue(t, warmCapacityEffectiveTargetGauge, 5, image)
	assertGaugeValue(t, warmCapacityHeadroomGauge, 5)

	workerLifecycleCountGauge.DeleteLabelValues(image, string(configstore.WorkerStateIdle), "neutral")
	workerLifecycleCountGauge.DeleteLabelValues(image, string(configstore.WorkerStateHot), "org_bound")
	workerLifecycleCountGauge.DeleteLabelValues(image, string(configstore.WorkerStateHotIdle), "org_bound")
	observeWorkerLifecycleStats([]configstore.WorkerLifecycleStats{
		{Image: image, State: configstore.WorkerStateIdle, Binding: "neutral", Count: 2},
		{Image: image, State: configstore.WorkerStateHot, Binding: "org_bound", Count: 1},
		{Image: image, State: configstore.WorkerStateHotIdle, Binding: "org_bound", Count: 3},
	})
	assertGaugeVecValue(t, workerLifecycleCountGauge, 2, image, string(configstore.WorkerStateIdle), "neutral")
	assertGaugeVecValue(t, workerLifecycleCountGauge, 1, image, string(configstore.WorkerStateHot), "org_bound")
	assertGaugeVecValue(t, workerLifecycleCountGauge, 3, image, string(configstore.WorkerStateHotIdle), "org_bound")
	observeWorkerLifecycleStats(nil, []configstore.WorkerLifecycleStats{
		{Image: image, State: configstore.WorkerStateIdle, Binding: "neutral", Count: 2},
		{Image: image, State: configstore.WorkerStateHot, Binding: "org_bound", Count: 1},
		{Image: image, State: configstore.WorkerStateHotIdle, Binding: "org_bound", Count: 3},
	})
	assertGaugeVecValue(t, workerLifecycleCountGauge, 0, image, string(configstore.WorkerStateIdle), "neutral")
	assertGaugeVecValue(t, workerLifecycleCountGauge, 0, image, string(configstore.WorkerStateHot), "org_bound")
	assertGaugeVecValue(t, workerLifecycleCountGauge, 0, image, string(configstore.WorkerStateHotIdle), "org_bound")
}

func TestResetLeaderOwnedClusterMetrics(t *testing.T) {
	image := "duckgres:leader-reset-test"

	observeWarmCapacityTargets(
		map[string]int{image: 2},
		map[string]int{image: 5},
		10,
	)
	observeWorkerLifecycleStats([]configstore.WorkerLifecycleStats{
		{Image: image, State: configstore.WorkerStateIdle, Binding: "neutral", Count: 2},
	})

	resetLeaderOwnedClusterMetrics()

	assertGaugeVecValue(t, warmCapacityEffectiveTargetGauge, 0, image)
	assertGaugeValue(t, warmCapacityHeadroomGauge, 0)
	assertGaugeVecValue(t, workerLifecycleCountGauge, 0, image, string(configstore.WorkerStateIdle), "neutral")
}

// --- Helpers ---

func makeTestWorker(lifecycle WorkerLifecycleState, assignment *WorkerAssignment) *ManagedWorker {
	w := &ManagedWorker{
		done: make(chan struct{}),
	}
	state := SharedWorkerState{Lifecycle: lifecycle, Assignment: assignment}
	_ = w.SetSharedState(state)
	return w
}

func assertGaugeValue(t *testing.T, gauge prometheus.Gauge, expected float64) {
	t.Helper()
	m := &dto.Metric{}
	if err := gauge.Write(m); err != nil {
		t.Fatalf("failed to read gauge: %v", err)
	}
	if got := m.GetGauge().GetValue(); got != expected {
		t.Fatalf("expected gauge value %v, got %v", expected, got)
	}
}

func counterLabelValue(cv *prometheus.CounterVec, label string) float64 {
	return counterLabelValues(cv, label)
}

func counterLabelValues(cv *prometheus.CounterVec, labels ...string) float64 {
	m := &dto.Metric{}
	counter, err := cv.GetMetricWithLabelValues(labels...)
	if err != nil {
		return 0
	}
	if err := counter.Write(m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func assertGaugeVecValue(t *testing.T, gv *prometheus.GaugeVec, expected float64, labels ...string) {
	t.Helper()
	gauge, err := gv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("failed to read gauge labels %v: %v", labels, err)
	}
	m := &dto.Metric{}
	if err := gauge.Write(m); err != nil {
		t.Fatalf("failed to write gauge labels %v: %v", labels, err)
	}
	if got := m.GetGauge().GetValue(); got != expected {
		t.Fatalf("expected gauge labels %v value %v, got %v", labels, expected, got)
	}
}
