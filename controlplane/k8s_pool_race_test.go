//go:build kubernetes && race

package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestK8sPoolHealthCheckLoopSnapshotsLeaseUnderPoolLock(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.runtimeStore = &captureRuntimeWorkerStore{
		markLostErr: errors.New("retry lease validation"),
	}

	worker := &ManagedWorker{ID: 88, done: make(chan struct{})}
	worker.SetOwnerCPInstanceID(pool.cpInstanceID)
	pool.workers[worker.ID] = worker

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.HealthCheckLoop(ctx, time.Nanosecond, nil, nil)
	}()

	deadline := time.After(50 * time.Millisecond)
	for {
		select {
		case <-deadline:
			cancel()
			<-done
			return
		default:
		}

		pool.mu.Lock()
		worker.SetOwnerEpoch(worker.OwnerEpoch() + 1)
		if worker.OwnerCPInstanceID() == pool.cpInstanceID {
			worker.SetOwnerCPInstanceID("other-cp:boot-b")
		} else {
			worker.SetOwnerCPInstanceID(pool.cpInstanceID)
		}
		pool.mu.Unlock()
	}
}
