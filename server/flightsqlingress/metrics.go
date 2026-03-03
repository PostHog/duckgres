package flightsqlingress

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var flightRPCDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_flight_rpc_duration_seconds",
	Help:    "Duration of Flight SQL ingress RPC handlers by method.",
	Buckets: prometheus.DefBuckets,
}, []string{"method"})

var flightIngressSessionsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_flight_ingress_sessions_total",
	Help: "Total Flight ingress session outcomes.",
}, []string{"outcome"})

func observeFlightRPCDuration(method string, started time.Time) {
	if method == "" {
		method = "unknown"
	}
	d := time.Since(started)
	if d < 0 {
		d = 0
	}
	flightRPCDurationHistogram.WithLabelValues(method).Observe(d.Seconds())
}

func observeFlightIngressSessionOutcome(outcome string) {
	if outcome == "" {
		outcome = "unknown"
	}
	flightIngressSessionsCounter.WithLabelValues(outcome).Inc()
}
