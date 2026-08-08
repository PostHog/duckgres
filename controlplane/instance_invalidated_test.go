//go:build kubernetes

package controlplane

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

// The reuse gate is the load-bearing half of the fix: a worker whose DuckDB
// instance is poisoned answers RPCs normally, so without this check it stays
// hot-idle and the org's NEXT connection is handed the dead instance. That
// reuse is what made a single bad statement look like "the warehouse is down
// until someone restarts it".
func TestValidateReservedWorkerHealthRejectsInvalidatedInstance(t *testing.T) {
	err := validateReservedWorkerHealth(&healthCheckResult{
		Healthy:               false,
		InstanceInvalidated:   true,
		InstanceInvalidReason: "INTERNAL Error: Calling GetValueInternal on a value that is NULL",
	})
	if err == nil {
		t.Fatal("an invalidated instance must never be reused")
	}
	// The operator needs the originating engine error, not just "unhealthy".
	if !strings.Contains(err.Error(), "GetValueInternal") {
		t.Errorf("expected the engine error in the rejection, got %q", err.Error())
	}

	// A worker that reports the flag but still claims healthy=true (worker
	// newer than this CP's expectations) must be rejected on the flag alone.
	if err := validateReservedWorkerHealth(&healthCheckResult{
		Healthy:             true,
		InstanceInvalidated: true,
	}); err == nil {
		t.Fatal("the flag alone must reject reuse, independent of Healthy")
	}

	// And a genuinely healthy worker still passes — this gate must not become
	// a spurious source of acquisition failures.
	if err := validateReservedWorkerHealth(&healthCheckResult{Healthy: true}); err != nil {
		t.Fatalf("healthy worker must be reusable, got %v", err)
	}
}

// The health-check loop must retire an invalidated worker on the FIRST report.
// It answers RPCs fine, so healthErr is nil and the consecutive-failure counter
// would never fire — waiting for it would leave a dead worker schedulable
// forever.
func TestK8sPoolHealthCheckLoopRetiresInvalidatedInstanceImmediately(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			8: {
				WorkerID:          8,
				PodName:           "test-cp-worker-8",
				State:             configstore.WorkerStateHot,
				OwnerCPInstanceID: pool.cpInstanceID,
				OwnerEpoch:        4,
			},
		},
	}
	pool.runtimeStore = store
	pool.lifecycle = NewWorkerLifecycle(store, pool)

	worker := &ManagedWorker{ID: 8, podName: "test-cp-worker-8", done: make(chan struct{})}
	worker.SetOwnerCPInstanceID(pool.cpInstanceID)
	worker.SetOwnerEpoch(4)
	pool.workers[worker.ID] = worker

	origHealthCheck := doHealthCheckWithMetadata
	doHealthCheckWithMetadata = func(context.Context, *flightsql.Client, server.WorkerHealthCheckPayload) (*healthCheckResult, error) {
		return &healthCheckResult{
			Healthy:               false,
			InstanceInvalidated:   true,
			InstanceInvalidReason: "INTERNAL Error: Calling GetValueInternal on a value that is NULL",
		}, nil
	}
	t.Cleanup(func() { doHealthCheckWithMetadata = origHealthCheck })

	crashed := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.HealthCheckLoop(ctx, time.Millisecond, func(workerID int) {
		select {
		case crashed <- workerID:
		default:
		}
	}, nil)

	// Sessions on the dead instance are notified so the client sees a real
	// error instead of hanging on a worker that can no longer execute anything.
	select {
	case workerID := <-crashed:
		if workerID != worker.ID {
			t.Fatalf("expected crash notification for worker %d, got %d", worker.ID, workerID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an invalidated worker must be retired and its sessions notified")
	}

	// And it must be gone from the pool, so nothing can acquire it.
	deadline := time.After(time.Second)
	for {
		pool.mu.RLock()
		_, stillPooled := pool.workers[worker.ID]
		pool.mu.RUnlock()
		if !stillPooled {
			break
		}
		select {
		case <-deadline:
			t.Fatal("invalidated worker must be removed from the pool")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Conversely, a healthy worker must survive the loop untouched — the new branch
// must not turn ordinary health checks into retirements.
func TestK8sPoolHealthCheckLoopKeepsHealthyWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			8: {
				WorkerID:          8,
				PodName:           "test-cp-worker-8",
				State:             configstore.WorkerStateHot,
				OwnerCPInstanceID: pool.cpInstanceID,
				OwnerEpoch:        4,
			},
		},
	}
	pool.runtimeStore = store
	pool.lifecycle = NewWorkerLifecycle(store, pool)

	worker := &ManagedWorker{ID: 8, podName: "test-cp-worker-8", done: make(chan struct{})}
	worker.SetOwnerCPInstanceID(pool.cpInstanceID)
	worker.SetOwnerEpoch(4)
	pool.workers[worker.ID] = worker

	origHealthCheck := doHealthCheckWithMetadata
	doHealthCheckWithMetadata = func(context.Context, *flightsql.Client, server.WorkerHealthCheckPayload) (*healthCheckResult, error) {
		return &healthCheckResult{Healthy: true}, nil
	}
	t.Cleanup(func() { doHealthCheckWithMetadata = origHealthCheck })

	crashed := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.HealthCheckLoop(ctx, time.Millisecond, func(workerID int) {
		select {
		case crashed <- workerID:
		default:
		}
	}, nil)

	time.Sleep(100 * time.Millisecond)

	select {
	case workerID := <-crashed:
		t.Fatalf("healthy worker must not be retired, got crash for %d", workerID)
	default:
	}
	pool.mu.RLock()
	_, stillPooled := pool.workers[worker.ID]
	pool.mu.RUnlock()
	if !stillPooled {
		t.Fatal("healthy worker must stay in the pool")
	}
}
