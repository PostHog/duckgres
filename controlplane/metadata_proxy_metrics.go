package controlplane

import (
	"io"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	metadataProxyOutcomeSuccess               = "success"
	metadataProxyOutcomeUnavailable           = "unavailable"
	metadataProxyOutcomeInvalidDatabase       = "invalid_database"
	metadataProxyOutcomeAuthFailed            = "auth_failed"
	metadataProxyOutcomeDraining              = "draining"
	metadataProxyOutcomeCapacity              = "capacity"
	metadataProxyOutcomeTargetResolutionError = "target_resolution_error"
	metadataProxyOutcomeUpstreamConnectError  = "upstream_connect_error"
	metadataProxyOutcomeUpstreamSyncError     = "upstream_sync_error"
	metadataProxyOutcomeUpstreamHijackError   = "upstream_hijack_error"
	metadataProxyOutcomeCancelKeyError        = "cancel_key_error"
	metadataProxyOutcomeHandshakeError        = "handshake_error"

	metadataProxyUpstreamOutcomeSuccess = "success"
	metadataProxyUpstreamOutcomeError   = "error"

	metadataProxyDirectionClientToUpstream = "client_to_upstream"
	metadataProxyDirectionUpstreamToClient = "upstream_to_client"

	metadataProxyCancelOutcomeSessionTerminated = "session_terminated"
	metadataProxyCancelOutcomeNotLocal          = "not_local"
)

// Metadata proxy metrics use a distinct namespace rather than adding a
// protocol label to established Duckgres metric families. In particular,
// duckgres_connections_open intentionally remains the process-wide count of
// every accepted client socket (including metadata proxy sockets), while the
// metrics below isolate the early proxy branch without changing existing
// dashboards or alerts. The org label is bounded by configured tenants and all
// outcome/direction values are closed enums defined above.
var (
	metadataProxyConnectionsOpenGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "duckgres_metadata_proxy_connections_open",
		Help: "Current admitted native metadata Postgres proxy connections, including upstream bootstrap, partitioned by org.",
	}, []string{"org"})

	metadataProxyConnectionAttemptsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_metadata_proxy_connection_attempts_total",
		Help: "Metadata proxy connection attempts partitioned by org and bounded terminal outcome.",
	}, []string{"org", "outcome"})

	metadataProxyConnectionDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "duckgres_metadata_proxy_connection_duration_seconds",
		Help:    "Lifetime of admitted metadata proxy connections, including upstream bootstrap, partitioned by org.",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 18000, 36000, 86400},
	}, []string{"org"})

	metadataProxyUpstreamConnectDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "duckgres_metadata_proxy_upstream_connect_duration_seconds",
		Help:    "Time to connect and authenticate to the internal metadata Postgres target, partitioned by org and bounded outcome.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"org", "outcome"})

	metadataProxyBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_metadata_proxy_bytes_total",
		Help: "Post-authentication pgwire bytes relayed by the metadata proxy, partitioned by org and bounded direction.",
	}, []string{"org", "direction"})

	metadataProxyCancelRequestsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "duckgres_metadata_proxy_cancel_requests_total",
		Help: "Metadata proxy CancelRequest handling partitioned by bounded outcome (session_terminated or not_local).",
	}, []string{"outcome"})
)

type metadataProxyAttemptMetrics struct {
	mu       sync.Mutex
	org      string
	finished bool
}

func newMetadataProxyAttemptMetrics() *metadataProxyAttemptMetrics {
	return &metadataProxyAttemptMetrics{}
}

func (m *metadataProxyAttemptMetrics) SetOrg(org string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if !m.finished {
		m.org = org
	}
	m.mu.Unlock()
}

// Finish records exactly one terminal attempt outcome even when a caller has a
// fallback defer and an explicit success path.
func (m *metadataProxyAttemptMetrics) Finish(outcome string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.finished {
		m.mu.Unlock()
		return
	}
	m.finished = true
	org := m.org
	m.mu.Unlock()
	metadataProxyConnectionAttemptsCounter.WithLabelValues(org, outcome).Inc()
}

func beginMetadataProxyConnection(org string) func() {
	started := time.Now()
	metadataProxyConnectionsOpenGauge.WithLabelValues(org).Inc()
	return func() {
		metadataProxyConnectionsOpenGauge.WithLabelValues(org).Dec()
		duration := time.Since(started)
		if duration < 0 {
			duration = 0
		}
		metadataProxyConnectionDurationHistogram.WithLabelValues(org).Observe(duration.Seconds())
	}
}

func observeMetadataProxyUpstreamConnect(org, outcome string, started time.Time) {
	duration := time.Since(started)
	if duration < 0 {
		duration = 0
	}
	metadataProxyUpstreamConnectDurationHistogram.WithLabelValues(org, outcome).Observe(duration.Seconds())
}

type metadataProxyCountingWriter struct {
	dst     io.Writer
	counter prometheus.Counter
}

func (w metadataProxyCountingWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	if n > 0 {
		w.counter.Add(float64(n))
	}
	return n, err
}

func metadataProxyTrafficWriter(dst io.Writer, org, direction string) io.Writer {
	return metadataProxyCountingWriter{
		dst:     dst,
		counter: metadataProxyBytesCounter.WithLabelValues(org, direction),
	}
}
