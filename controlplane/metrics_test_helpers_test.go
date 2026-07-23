package controlplane

import (
	"testing"

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
	// Match the lazy-init semantics of metricHistogramCount and
	// metricCounterFamilyTotal: a Vec family doesn't appear in
	// Gather() output until at least one label combination has been
	// observed, so "not found" is a legitimate zero rather than a
	// test failure.
	return 0
}

func gaugeVecLabelValue(t *testing.T, gv *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	gauge, err := gv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("gauge labels %v: %v", labels, err)
	}
	metric := &dto.Metric{}
	if err := gauge.Write(metric); err != nil {
		t.Fatalf("gauge write labels %v: %v", labels, err)
	}
	return metric.GetGauge().GetValue()
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
	// HistogramVec families don't appear in Gather() output until a label
	// combination has been observed. Returning 0 here lets "before / after"
	// comparisons work against a fresh metric without forcing callers to
	// prime it.
	return 0
}

// counterVecLabelValue returns the value of a CounterVec series labelled
// with the given values. Returns 0 if the series has never been touched
// (matching prometheus's lazy-initialization semantics).
func counterVecLabelValue(t *testing.T, cv *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	counter, err := cv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("counter labels %v: %v", labels, err)
	}
	m := &dto.Metric{}
	if err := counter.Write(m); err != nil {
		t.Fatalf("counter write labels %v: %v", labels, err)
	}
	return m.GetCounter().GetValue()
}

// histogramVecLabelSampleCount returns the cumulative sample count of a
// HistogramVec series labelled with the given values.
func histogramVecLabelSampleCount(t *testing.T, hv *prometheus.HistogramVec, labels ...string) uint64 {
	t.Helper()
	observer, err := hv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("histogram labels %v: %v", labels, err)
	}
	h, ok := observer.(prometheus.Histogram)
	if !ok {
		t.Fatalf("expected prometheus.Histogram, got %T", observer)
	}
	m := &dto.Metric{}
	if err := h.Write(m); err != nil {
		t.Fatalf("histogram write labels %v: %v", labels, err)
	}
	return m.GetHistogram().GetSampleCount()
}

// metricCounterFamilyTotal sums the value across all series of a counter
// family. Returns 0 if the family has no series yet. Useful for "did
// anything increment?" assertions where the test doesn't care about
// specific labels.
func metricCounterFamilyTotal(t *testing.T, metricName string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		if fam.GetType() != dto.MetricType_COUNTER {
			t.Fatalf("metric %q is not a counter", metricName)
		}
		var total float64
		for _, metric := range fam.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
		return total
	}
	// Family not yet registered (no series) — that's a legitimate zero.
	return 0
}
