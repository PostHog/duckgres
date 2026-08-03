//go:build kubernetes

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
	return 0
}
