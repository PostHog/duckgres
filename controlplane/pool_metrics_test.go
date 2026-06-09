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

	pool.markWorkerRetiredLocked(w, RetireReasonIdleTimeout, LifecycleOriginIdleTimeout)

	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleRetired {
		t.Fatalf("expected retired, got %s", w.SharedState().NormalizedLifecycle())
	}
}

func TestMarkWorkerRetiredLocked_TransitionsHotWorkerToRetired(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	w := makeTestWorker(WorkerLifecycleHot, &WorkerAssignment{OrgID: "org-1"})
	w.peakSessions = 5
	pool.workers[1] = w

	pool.markWorkerRetiredLocked(w, RetireReasonNormal, LifecycleOriginPublicAPI)

	if w.SharedState().NormalizedLifecycle() != WorkerLifecycleRetired {
		t.Fatalf("expected retired, got %s", w.SharedState().NormalizedLifecycle())
	}
}

func TestRetireWorkerWithReasonSkipsCleanupWhenDurableRetireFenced(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.runtimeStore = &captureRuntimeWorkerStore{
		upsertErr: configstore.ErrWorkerRecordUpsertFenceMiss,
	}

	w := makeTestWorker(WorkerLifecycleIdle, nil)
	w.ID = 1
	w.podName = pool.podNameForWorker(w.ID)
	w.image = pool.workerImage
	pool.workers[w.ID] = w

	before := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(LifecycleOpRetireLocal),
		string(configstore.TransitionOutcomeFenceMissLease),
		pool.workerImage,
		string(LifecycleOriginPublicAPI),
	)

	if retired := pool.retireWorkerWithReason(w.ID, RetireReasonNormal, LifecycleOriginPublicAPI); retired {
		t.Fatal("expected retireWorkerWithReason to report false on durable fence miss")
	}

	pool.mu.RLock()
	_, stillPresent := pool.workers[w.ID]
	pool.mu.RUnlock()
	if !stillPresent {
		t.Fatal("worker should remain in the local map when durable retire is fenced")
	}
	if got := w.SharedState().NormalizedLifecycle(); got != WorkerLifecycleIdle {
		t.Fatalf("expected lifecycle to remain idle after durable fence miss, got %s", got)
	}

	after := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(LifecycleOpRetireLocal),
		string(configstore.TransitionOutcomeFenceMissLease),
		pool.workerImage,
		string(LifecycleOriginPublicAPI),
	)
	if delta := after - before; delta != 1 {
		t.Fatalf("expected one retire_local fence-miss metric, got delta %v", delta)
	}
}

func TestReservedAtTracking(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	// Spawn-on-demand: register the spawned worker so ReserveSharedWorker can
	// reserve it and stamp reservedAt.
	pool.spawnWorkerFunc = func(ctx context.Context, id int, image string, profile WorkerProfile) error {
		w := makeTestWorker(WorkerLifecycleIdle, nil)
		w.ID = id
		pool.mu.Lock()
		pool.workers[id] = w
		pool.mu.Unlock()
		return nil
	}

	before := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := pool.ReserveSharedWorker(ctx, &WorkerAssignment{
		OrgID: "org-1",
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker failed: %v", err)
	}
	after := time.Now()

	if got.reservedAt.Before(before) || got.reservedAt.After(after) {
		t.Fatalf("reservedAt %v not between %v and %v", got.reservedAt, before, after)
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
	pool.activatingTimeout = 50 * time.Millisecond

	// One idle worker (healthy), one stuck activating worker
	idle := makeTestWorker(WorkerLifecycleIdle, nil)
	pool.workers[1] = idle

	stuck := makeTestWorker(WorkerLifecycleActivating, &WorkerAssignment{
		OrgID: "org-1",
	})
	stuck.reservedAt = time.Now().Add(-time.Minute) // reserved 1 minute ago
	pool.workers[2] = stuck

	pool.reapStuckActivatingWorkers()

	// Stuck worker should be removed; the healthy idle worker left alone. There
	// is no warm pool, so nothing is spawned to replace the reaped worker.
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

func TestObserveWorkerLifecycleStatsSeedsZerosAndDeletesStaleImages(t *testing.T) {
	currentImage := "duckgres:current-lifecycle"
	staleImage := "duckgres:stale-lifecycle"
	workerLifecycleCountGauge.DeleteLabelValues(currentImage, string(configstore.WorkerStateIdle), "neutral")
	workerLifecycleCountGauge.DeleteLabelValues(currentImage, string(configstore.WorkerStateHot), "org_bound")
	workerLifecycleCountGauge.DeleteLabelValues(staleImage, string(configstore.WorkerStateIdle), "neutral")

	previous := []configstore.WorkerLifecycleStats{
		{Image: staleImage, State: configstore.WorkerStateIdle, Binding: "neutral", Count: 3},
	}
	observeWorkerLifecycleStats(previous)
	if _, ok := metricGaugeFamilyLabelValue(t, "duckgres_worker_lifecycle_count", map[string]string{
		"image":   staleImage,
		"state":   string(configstore.WorkerStateIdle),
		"binding": "neutral",
	}); !ok {
		t.Fatal("expected stale image series to exist before replacement observation")
	}

	observeWorkerLifecycleStats([]configstore.WorkerLifecycleStats{
		{Image: currentImage, State: configstore.WorkerStateHot, Binding: "org_bound", Count: 1},
	}, previous)

	if got, ok := metricGaugeFamilyLabelValue(t, "duckgres_worker_lifecycle_count", map[string]string{
		"image":   currentImage,
		"state":   string(configstore.WorkerStateIdle),
		"binding": "neutral",
	}); !ok || got != 0 {
		t.Fatalf("expected seeded idle/neutral zero series for current image, got value=%v ok=%v", got, ok)
	}
	if _, ok := metricGaugeFamilyLabelValue(t, "duckgres_worker_lifecycle_count", map[string]string{
		"image":   staleImage,
		"state":   string(configstore.WorkerStateIdle),
		"binding": "neutral",
	}); ok {
		t.Fatal("expected stale lifecycle image series to be deleted")
	}
}

func TestResetLeaderOwnedClusterMetrics(t *testing.T) {
	image := "duckgres:leader-reset-test"

	observeWorkerLifecycleStats([]configstore.WorkerLifecycleStats{
		{Image: image, State: configstore.WorkerStateIdle, Binding: "org_bound", Count: 2},
	})

	resetLeaderOwnedClusterMetrics()

	assertGaugeVecValue(t, workerLifecycleCountGauge, 0, image, string(configstore.WorkerStateIdle), "org_bound")
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

func metricGaugeFamilyLabelValue(t *testing.T, metricName string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		if fam.GetType() != dto.MetricType_GAUGE {
			t.Fatalf("metric %q is not a gauge", metricName)
		}
		for _, metric := range fam.GetMetric() {
			if metricHasLabels(metric, labels) {
				return metric.GetGauge().GetValue(), true
			}
		}
		return 0, false
	}
	return 0, false
}

func metricHasLabels(metric *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}
	seen := make(map[string]string, len(metric.GetLabel()))
	for _, label := range metric.GetLabel() {
		seen[label.GetName()] = label.GetValue()
	}
	for name, want := range labels {
		if seen[name] != want {
			return false
		}
	}
	return true
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
