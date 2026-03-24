//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// resetMetrics resets all warm pool counters/histograms for test isolation.
func resetMetrics() {
	workerRetirementsCounter.Reset()
}

func TestObserveWarmPoolLifecycleGauges(t *testing.T) {
	workers := map[int]*ManagedWorker{
		1: makeTestWorker(WorkerLifecycleIdle, nil),
		2: makeTestWorker(WorkerLifecycleIdle, nil),
		3: makeTestWorker(WorkerLifecycleReserved, &WorkerAssignment{OrgID: "org-1", LeaseExpiresAt: time.Now().Add(time.Hour)}),
		4: makeTestWorker(WorkerLifecycleActivating, &WorkerAssignment{OrgID: "org-1", LeaseExpiresAt: time.Now().Add(time.Hour)}),
		5: makeTestWorker(WorkerLifecycleHot, &WorkerAssignment{OrgID: "org-2", LeaseExpiresAt: time.Now().Add(time.Hour)}),
		6: makeTestWorker(WorkerLifecycleHot, &WorkerAssignment{OrgID: "org-2", LeaseExpiresAt: time.Now().Add(time.Hour)}),
		7: makeTestWorker(WorkerLifecycleDraining, &WorkerAssignment{OrgID: "org-3", LeaseExpiresAt: time.Now().Add(time.Hour)}),
	}

	observeWarmPoolLifecycleGauges(workers)

	assertGaugeValue(t, warmWorkersGauge, 2)
	assertGaugeValue(t, reservedWorkersGauge, 1)
	assertGaugeValue(t, activatingWorkersGauge, 1)
	assertGaugeValue(t, hotWorkersGauge, 2)
	assertGaugeValue(t, drainingWorkersGauge, 1)
}

func TestObserveWarmPoolLifecycleGauges_SkipsDeadWorkers(t *testing.T) {
	dead := makeTestWorker(WorkerLifecycleIdle, nil)
	close(dead.done) // mark as dead

	workers := map[int]*ManagedWorker{
		1: makeTestWorker(WorkerLifecycleIdle, nil),
		2: dead,
	}

	observeWarmPoolLifecycleGauges(workers)

	assertGaugeValue(t, warmWorkersGauge, 1)
}

func TestMarkWorkerRetiredLocked_RecordsRetirementMetric(t *testing.T) {
	resetMetrics()
	pool, _ := newTestK8sPool(t, 5)

	w := makeTestWorker(WorkerLifecycleIdle, nil)
	pool.workers[1] = w

	pool.markWorkerRetiredLocked(w, RetireReasonIdleTimeout)

	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleRetired {
		t.Fatalf("expected retired, got %s", w.SharedState().NormalizedLifecycle())
	}

	val := counterLabelValue(workerRetirementsCounter, RetireReasonIdleTimeout)
	if val != 1 {
		t.Fatalf("expected 1 retirement with reason idle_timeout, got %v", val)
	}
}

func TestMarkWorkerRetiredLocked_RecordsHotWorkerSessions(t *testing.T) {
	resetMetrics()
	pool, _ := newTestK8sPool(t, 5)

	w := makeTestWorker(WorkerLifecycleHot, &WorkerAssignment{OrgID: "org-1", LeaseExpiresAt: time.Now().Add(time.Hour)})
	w.peakSessions = 5
	pool.workers[1] = w

	pool.markWorkerRetiredLocked(w, RetireReasonNormal)

	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleRetired {
		t.Fatalf("expected retired, got %s", w.SharedState().NormalizedLifecycle())
	}
}

func TestReservedAtTracking(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	w := makeTestWorker(WorkerLifecycleIdle, nil)
	pool.workers[1] = w

	before := time.Now()
	_, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
		OrgID:          "org-1",
		LeaseExpiresAt: time.Now().Add(time.Hour),
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

func TestSpawnMinWorkersUpdatesWarmWorkersGauge(t *testing.T) {
	observeWarmPoolLifecycleGauges(map[int]*ManagedWorker{})
	pool, _ := newTestK8sPool(t, 5)

	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		pool.mu.Lock()
		pool.workers[id] = makeTestWorker(WorkerLifecycleIdle, nil)
		pool.mu.Unlock()
		return nil
	}

	if err := pool.SpawnMinWorkers(1); err != nil {
		t.Fatalf("SpawnMinWorkers failed: %v", err)
	}

	assertGaugeValue(t, warmWorkersGauge, 1)
}

func TestActivateWorkerForOrgUpdatesActivatingGauge(t *testing.T) {
	observeWarmPoolLifecycleGauges(map[int]*ManagedWorker{})
	pool, _ := newTestK8sPool(t, 5)
	worker := makeTestWorker(WorkerLifecycleReserved, &WorkerAssignment{
		OrgID:          "org-1",
		LeaseExpiresAt: time.Now().Add(time.Hour),
	})
	worker.reservedAt = time.Now()
	pool.workers[1] = worker
	observeWarmPoolLifecycleGauges(pool.workers)

	orgPool := NewOrgReservedPool(pool, "org-1", 1)
	orgPool.activateReservedWorker = func(ctx context.Context, worker *ManagedWorker) error {
		assertGaugeValue(t, reservedWorkersGauge, 0)
		assertGaugeValue(t, activatingWorkersGauge, 1)
		return nil
	}

	if err := orgPool.activateWorkerForOrg(context.Background(), worker); err != nil {
		t.Fatalf("activateWorkerForOrg failed: %v", err)
	}
}

func TestActivateWorkerForOrgRecordsActivationDurationWhenWorkerAlreadyHot(t *testing.T) {
	observeWarmPoolLifecycleGauges(map[int]*ManagedWorker{})
	pool, _ := newTestK8sPool(t, 5)
	worker := makeTestWorker(WorkerLifecycleReserved, &WorkerAssignment{
		OrgID:          "org-1",
		LeaseExpiresAt: time.Now().Add(time.Hour),
	})
	worker.reservedAt = time.Now().Add(-2 * time.Second)
	pool.workers[1] = worker

	orgPool := NewOrgReservedPool(pool, "org-1", 1)
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
		OrgID:          "org-1",
		LeaseExpiresAt: time.Now().Add(time.Hour),
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
		OrgID:          "org-1",
		LeaseExpiresAt: time.Now().Add(time.Hour),
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
	m := &dto.Metric{}
	counter, err := cv.GetMetricWithLabelValues(label)
	if err != nil {
		return 0
	}
	if err := counter.Write(m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}
