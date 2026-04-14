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
