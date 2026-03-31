//go:build !kubernetes

package controlplane

import (
	"testing"
	"time"
)

func TestControlPlaneWorkerAcquireAndSpawnMetrics(t *testing.T) {
	acquireBefore := metricHistogramCount(t, "duckgres_control_plane_worker_acquire_seconds")
	spawnBefore := metricHistogramCount(t, "duckgres_control_plane_worker_spawn_seconds")

	observeControlPlaneWorkerAcquire(5 * time.Millisecond)
	observeControlPlaneWorkerSpawn(10 * time.Millisecond)

	acquireAfter := metricHistogramCount(t, "duckgres_control_plane_worker_acquire_seconds")
	spawnAfter := metricHistogramCount(t, "duckgres_control_plane_worker_spawn_seconds")

	if acquireAfter-acquireBefore != 1 {
		t.Fatalf("expected acquire histogram sample count delta 1, got %d", acquireAfter-acquireBefore)
	}
	if spawnAfter-spawnBefore != 1 {
		t.Fatalf("expected spawn histogram sample count delta 1, got %d", spawnAfter-spawnBefore)
	}
}

func TestControlPlaneWorkerQueueDepthGauge(t *testing.T) {
	before := metricGaugeValue(t, "duckgres_control_plane_worker_queue_depth")

	observeControlPlaneWorkerQueueDepthDelta(1)
	mid := metricGaugeValue(t, "duckgres_control_plane_worker_queue_depth")
	observeControlPlaneWorkerQueueDepthDelta(-1)
	after := metricGaugeValue(t, "duckgres_control_plane_worker_queue_depth")

	if mid != before+1 {
		t.Fatalf("expected queue depth to increase by 1: before=%f mid=%f", before, mid)
	}
	if after != before {
		t.Fatalf("expected queue depth to return to baseline: before=%f after=%f", before, after)
	}
}
