//go:build kubernetes

package controlplane

import (
	"testing"
	"time"
)

// TestK8sPoolSetWorkerTTL asserts the pool-side override behind
// SET duckgres.worker_ttl: the worker's profile TTL is updated in place (under
// the pool lock) and unknown workers are reported.
func TestK8sPoolSetWorkerTTL(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{
		ID:             5,
		activeSessions: 1,
		profile:        WorkerProfile{CPU: "8", Memory: "16Gi", TTL: time.Minute},
		done:           make(chan struct{}),
	}
	pool.workers[worker.ID] = worker

	if !pool.SetWorkerTTL(5, 20*time.Minute) {
		t.Fatal("SetWorkerTTL(5) = false, want true")
	}
	ttl, ok := pool.WorkerTTL(5)
	if !ok || ttl != 20*time.Minute {
		t.Fatalf("WorkerTTL(5) = %s, %v; want 20m, true", ttl, ok)
	}

	if pool.SetWorkerTTL(99, time.Minute) {
		t.Fatal("SetWorkerTTL(99) = true, want false (unknown worker)")
	}
	if _, ok := pool.WorkerTTL(99); ok {
		t.Fatal("WorkerTTL(99) ok=true, want false (unknown worker)")
	}
}

// TestK8sPoolSetWorkerTTLPersistsAtPark asserts the override actually governs
// reaping: when the worker's last session ends, the hot_idle record the
// reapers read carries the OVERRIDDEN ttl_minutes, not the connect-time one.
func TestK8sPoolSetWorkerTTLPersistsAtPark(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store
	worker := &ManagedWorker{
		ID:             5,
		activeSessions: 1,
		profile:        WorkerProfile{CPU: "8", Memory: "16Gi", TTL: time.Minute},
		done:           make(chan struct{}),
	}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle:  WorkerLifecycleHot,
		Assignment: &WorkerAssignment{OrgID: "analytics"},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[worker.ID] = worker

	if !pool.SetWorkerTTL(5, 20*time.Minute) {
		t.Fatal("SetWorkerTTL(5) = false, want true")
	}
	if !pool.TransitionToHotIdleIfNoSessions(worker.ID) {
		t.Fatal("expected the worker to park to hot_idle")
	}

	var hotIdle *int
	for i := range store.records {
		if store.records[i].State == "hot_idle" {
			hotIdle = &store.records[i].TTLMinutes
		}
	}
	if hotIdle == nil {
		t.Fatal("no hot_idle record persisted at park")
	}
	if *hotIdle != 20 {
		t.Fatalf("parked record ttl_minutes = %d, want 20 (the override)", *hotIdle)
	}
}
