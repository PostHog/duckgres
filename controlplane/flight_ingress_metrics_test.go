package controlplane

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func metricGaugeValue(t *testing.T, metricName string) float64 {
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
		if len(fam.GetMetric()) == 0 {
			return 0
		}
		return fam.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("metric %q not found", metricName)
	return 0
}

func metricHistogramCount(t *testing.T, metricName string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		if fam.GetType() != dto.MetricType_HISTOGRAM {
			t.Fatalf("metric %q is not a histogram", metricName)
		}
		var total uint64
		for _, metric := range fam.GetMetric() {
			total += metric.GetHistogram().GetSampleCount()
		}
		return total
	}
	t.Fatalf("metric %q not found", metricName)
	return 0
}

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
