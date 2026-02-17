package controlplane

import "github.com/prometheus/client_golang/prometheus/promauto"
import "github.com/prometheus/client_golang/prometheus"

var flightAuthSessionsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_flight_auth_sessions_active",
	Help: "Number of active Flight auth sessions on the control plane",
})

var controlPlaneWorkersGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_control_plane_workers_active",
	Help: "Number of active control-plane worker processes",
})

var flightSessionsReapedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_flight_sessions_reaped_total",
	Help: "Number of Flight auth sessions reaped",
}, []string{"trigger"})

var flightMaxWorkersRetryCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_flight_max_workers_retry_total",
	Help: "Number of max-worker retry outcomes when creating Flight auth sessions",
}, []string{"outcome"})

func observeFlightAuthSessions(count int) {
	if count < 0 {
		count = 0
	}
	flightAuthSessionsGauge.Set(float64(count))
}

func observeControlPlaneWorkers(count int) {
	if count < 0 {
		count = 0
	}
	controlPlaneWorkersGauge.Set(float64(count))
}

func observeFlightSessionsReaped(trigger string, count int) {
	if count <= 0 {
		return
	}
	flightSessionsReapedCounter.WithLabelValues(trigger).Add(float64(count))
}

func observeFlightMaxWorkersRetry(outcome string) {
	flightMaxWorkersRetryCounter.WithLabelValues(outcome).Inc()
}
