//go:build kubernetes

package controlplane

import (
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/prometheus/client_golang/prometheus"
)

var trinoOrgMetrics = newTrinoOrgMetrics(prometheus.DefaultRegisterer)

// trinoOrgMetricSet is the per-org Trino query telemetry behind the product
// monitoring series. The usage collector is its only writer.
type trinoOrgMetricSet struct {
	inFlight     *prometheus.GaugeVec
	queries      *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	queued       *prometheus.HistogramVec
	scannedBytes *prometheus.CounterVec
	cpuSeconds   *prometheus.CounterVec
}

func newTrinoOrgMetrics(reg prometheus.Registerer) *trinoOrgMetricSet {
	m := &trinoOrgMetricSet{
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "duckgres_trino_org_queries", Help: "Trino queries in flight for an org at the last successful coordinator poll, by state.",
		}, []string{"org", "state"}),
		queries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "duckgres_trino_org_query_total", Help: "Finished Trino queries observed for an org, by outcome.",
		}, []string{"org", "status", "error_type"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "duckgres_trino_org_query_duration_seconds", Help: "Elapsed time of finished Trino queries for an org.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 900, 1800, 3600},
		}, []string{"org"}),
		queued: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "duckgres_trino_org_query_queued_seconds", Help: "Time finished Trino queries for an org spent queued.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 15, 30, 60, 300, 900},
		}, []string{"org"}),
		scannedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "duckgres_trino_org_query_physical_input_bytes_total", Help: "Physical input bytes read by finished Trino queries for an org.",
		}, []string{"org"}),
		cpuSeconds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "duckgres_trino_org_query_cpu_seconds_total", Help: "CPU seconds used by finished Trino queries for an org.",
		}, []string{"org"}),
	}
	reg.MustRegister(m.inFlight, m.queries, m.duration, m.queued, m.scannedBytes, m.cpuSeconds)
	return m
}

func (m *trinoOrgMetricSet) observeFinished(orgID string, query admin.TrinoQuery) {
	if m == nil {
		return
	}
	status, errorType := "success", "none"
	if query.State == "FAILED" {
		status, errorType = "error", trinoErrorTypeLabel(query.ErrorType)
	}
	m.queries.WithLabelValues(orgID, status, errorType).Inc()
	m.duration.WithLabelValues(orgID).Observe(float64(query.ElapsedMS) / 1000)
	m.queued.WithLabelValues(orgID).Observe(float64(query.QueuedMS) / 1000)
	m.scannedBytes.WithLabelValues(orgID).Add(float64(query.PhysicalInputBytes))
	m.cpuSeconds.WithLabelValues(orgID).Add(float64(query.CPUMS) / 1000)
}

// setInFlight writes every state, so a state with no queries reads zero
// instead of keeping its previous value.
func (m *trinoOrgMetricSet) setInFlight(orgID string, counts map[string]int) {
	if m == nil {
		return
	}
	for _, state := range admin.TrinoInFlightStates {
		m.inFlight.WithLabelValues(orgID, state).Set(float64(counts[state]))
	}
}

func (m *trinoOrgMetricSet) clearInFlight(orgID string) {
	if m == nil {
		return
	}
	for _, state := range admin.TrinoInFlightStates {
		m.inFlight.DeleteLabelValues(orgID, state)
	}
}

// trinoErrorTypeLabel maps Trino's ErrorType onto a bounded label set, so an
// unexpected value from a newer Trino cannot add series.
func trinoErrorTypeLabel(errorType string) string {
	switch errorType {
	case "USER_ERROR":
		return "user"
	case "INTERNAL_ERROR":
		return "internal"
	case "INSUFFICIENT_RESOURCES":
		return "insufficient_resources"
	case "EXTERNAL":
		return "external"
	default:
		return "unknown"
	}
}
