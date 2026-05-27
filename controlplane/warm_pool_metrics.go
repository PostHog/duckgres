//go:build kubernetes

package controlplane

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- Activation latency and failure counters ---

var activationDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_activation_duration_seconds",
	Help:    "Time from worker reservation to the worker becoming hot, partitioned by image.",
	Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
}, []string{"image"})

var activationFailuresCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_activation_failures_total",
	Help: "Worker activations that failed before reaching hot, partitioned by image. The companion lifecycle transition fires at retireWorkerWithReason with reason=activation_failure.",
}, []string{"image"})

func observeActivationDuration(d time.Duration, image string) {
	img := strings.TrimSpace(image)
	if img == "" {
		img = "unknown"
	}
	if d < 0 {
		d = 0
	}
	activationDurationHistogram.WithLabelValues(img).Observe(d.Seconds())
}

func observeActivationFailure(image string) {
	img := strings.TrimSpace(image)
	if img == "" {
		img = "unknown"
	}
	activationFailuresCounter.WithLabelValues(img).Inc()
}
