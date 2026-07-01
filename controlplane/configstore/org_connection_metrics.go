package configstore

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	orgConnectionAdmissionOutcomeGranted         = "granted"
	orgConnectionAdmissionOutcomeGrantedOther    = "granted_other"
	orgConnectionAdmissionOutcomeAlreadyGranted  = "already_granted"
	orgConnectionAdmissionOutcomeBlockedOrgVCPU  = "blocked_org_vcpu"
	orgConnectionAdmissionOutcomeBlockedUserVCPU = "blocked_user_vcpu"
	orgConnectionAdmissionOutcomeInactive        = "inactive_request"
	orgConnectionAdmissionOutcomeMissing         = "missing_request"
	orgConnectionAdmissionOutcomeRetry           = "retry"
	orgConnectionAdmissionOutcomeWaiting         = "waiting"
	orgConnectionAdmissionOutcomeError           = "error"
)

var orgConnectionAdmissionDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_org_connection_admission_duration_seconds",
	Help:    "Duration of one org connection admission evaluation, including the DB-backed per-org scheduler transaction.",
	Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
}, []string{"outcome"})

var orgConnectionAdmissionAttemptsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_connection_admission_attempts_total",
	Help: "Total org connection admission evaluations by outcome.",
}, []string{"outcome"})

var orgConnectionAdmissionQueueDepthHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_org_connection_admission_queue_depth",
	Help:    "Pending org connection queue depth observed during admission evaluation.",
	Buckets: prometheus.ExponentialBuckets(1, 2, 14),
})

var orgConnectionAdmissionUserQueuesHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_org_connection_admission_user_queues",
	Help:    "Number of per-user queue heads considered during org connection admission evaluation.",
	Buckets: prometheus.ExponentialBuckets(1, 2, 12),
})

var orgConnectionAdmissionUserLimitSkipsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_org_connection_admission_user_limit_skips_total",
	Help: "Total per-user queue heads skipped during org connection admission because the user was at its vCPU limit.",
})

type orgConnectionAdmissionStats struct {
	queueDepth     int64
	userQueues     int
	userLimitSkips int
}

func observeOrgConnectionAdmission(d time.Duration, outcome string, stats orgConnectionAdmissionStats) {
	if d < 0 {
		d = 0
	}
	if outcome == "" {
		outcome = orgConnectionAdmissionOutcomeWaiting
	}
	orgConnectionAdmissionDurationHistogram.WithLabelValues(outcome).Observe(d.Seconds())
	orgConnectionAdmissionAttemptsCounter.WithLabelValues(outcome).Inc()
	orgConnectionAdmissionQueueDepthHistogram.Observe(float64(nonNegativeInt64(stats.queueDepth)))
	orgConnectionAdmissionUserQueuesHistogram.Observe(float64(nonNegativeInt(stats.userQueues)))
	if stats.userLimitSkips > 0 {
		orgConnectionAdmissionUserLimitSkipsCounter.Add(float64(stats.userLimitSkips))
	}
}

func nonNegativeInt(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

func nonNegativeInt64(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
