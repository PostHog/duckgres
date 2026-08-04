//go:build kubernetes

package controlplane

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics referenced only from kubernetes-tagged code (the janitor leader
// elector and the hot->hot_idle park). They live in a kubernetes-tagged file so
// the default-tag lint (golangci-lint runs without build tags) does not flag
// these unexported symbols as unused — under the default build neither the
// metric nor its only caller is compiled.

var janitorIsLeaderGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_control_plane_janitor_is_leader",
	Help: "1 if this control-plane replica currently holds the janitor leader lease, else 0. Summed across replicas it should be ~1; a sustained 0 means no replica is running the fleet-wide reapers (hot-idle TTL, orphan/stuck sweeps, headroom) — alert on it.",
})

var workerHotIdlePersistFailuresCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_hot_idle_persist_failures_total",
	Help: "Failures to durably persist a worker's hot->hot_idle transition. The worker is held Hot (and stays reusable by its org) rather than advanced to an in-memory-hot_idle / durable-hot split that would be invisible to BOTH reuse and the TTL reaper. A sustained nonzero rate points at runtime-store write problems.",
}, []string{"image"})

// setJanitorIsLeader records whether this replica currently holds the janitor
// leader lease (1) or not (0).
func setJanitorIsLeader(isLeader bool) {
	if isLeader {
		janitorIsLeaderGauge.Set(1)
		return
	}
	janitorIsLeaderGauge.Set(0)
}

// observeHotIdlePersistFailure increments the hot_idle persist-failure counter.
// Empty image falls back to "unknown".
func observeHotIdlePersistFailure(image string) {
	img := strings.TrimSpace(image)
	if img == "" {
		img = "unknown"
	}
	workerHotIdlePersistFailuresCounter.WithLabelValues(img).Inc()
}
