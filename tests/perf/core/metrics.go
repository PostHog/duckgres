package core

import (
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

type RunnerMetrics struct {
	registry *prometheus.Registry

	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	rows     *prometheus.CounterVec
	errors   *prometheus.CounterVec
}

func NewRunnerMetrics() *RunnerMetrics {
	reg := prometheus.NewRegistry()
	m := &RunnerMetrics{
		registry: reg,
		requests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "duckgres_perf_query_requests_total",
				Help: "Total perf harness query requests by protocol/query/status.",
			},
			[]string{"protocol", "query_id", "status"},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "duckgres_perf_query_duration_seconds",
				Help:    "Perf harness query duration by protocol/query.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"protocol", "query_id"},
		),
		rows: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "duckgres_perf_query_rows_total",
				Help: "Total returned/affected rows by protocol/query.",
			},
			[]string{"protocol", "query_id"},
		),
		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "duckgres_perf_query_errors_total",
				Help: "Total perf harness query errors by protocol/query/error_class.",
			},
			[]string{"protocol", "query_id", "error_class"},
		),
	}
	reg.MustRegister(m.requests, m.duration, m.rows, m.errors)
	return m
}

func (m *RunnerMetrics) Observe(result QueryResult) {
	status := result.Status
	if status == "" {
		status = "unknown"
	}
	protocol := string(result.Protocol)
	m.requests.WithLabelValues(protocol, result.QueryID, status).Inc()
	m.duration.WithLabelValues(protocol, result.QueryID).Observe(result.Duration.Seconds())
	if result.Rows > 0 {
		m.rows.WithLabelValues(protocol, result.QueryID).Add(float64(result.Rows))
	}
	if result.Status == "error" {
		errorClass := result.ErrorClass
		if errorClass == "" {
			errorClass = "unknown"
		}
		m.errors.WithLabelValues(protocol, result.QueryID, errorClass).Inc()
	}
}

func (m *RunnerMetrics) OpenMetricsText() (string, error) {
	families, err := m.registry.Gather()
	if err != nil {
		return "", fmt.Errorf("gather runner metrics: %w", err)
	}
	sort.Slice(families, func(i, j int) bool {
		return families[i].GetName() < families[j].GetName()
	})
	var b strings.Builder
	for _, family := range families {
		if _, err := expfmt.MetricFamilyToText(&b, family); err != nil {
			return "", fmt.Errorf("encode metric %s: %w", family.GetName(), err)
		}
	}
	return b.String(), nil
}

func (m *RunnerMetrics) Gatherer() prometheus.Gatherer {
	return m.registry
}
