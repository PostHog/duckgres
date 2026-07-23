package controlplane

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var sessionAdmissionWaitHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_session_admission_wait_seconds",
	Help:    "Time a successfully enqueued session admission request waited for a terminal result, partitioned by org, outcome, and retained blocking reason.",
	Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
}, []string{"org", "outcome", "reason"})

var sessionAdmissionRequestsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_session_admission_requests_total",
	Help: "Total successfully enqueued session admission requests by terminal outcome and retained blocking reason.",
}, []string{"org", "outcome", "reason"})

var sessionAdmissionQueueDepthGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_session_admission_queue_depth",
	Help: "Current session admission callers waiting in this control-plane process; sum by org across replicas for cluster-wide logical queue depth.",
}, []string{"org"})

var sessionAdmissionActiveVCPUsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_session_admission_active_vcpus",
	Help: "Requested vCPUs held by live local admission lease handles; cleanup-pending durable rows are excluded.",
}, []string{"org"})

var sessionAdmissionLimitVCPUsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_session_admission_limit_vcpus",
	Help: "Effective org session-admission vCPU limit for active org stacks, reconciled from this control-plane process's current config snapshot; zero means unlimited and max by org collapses replica duplication.",
}, []string{"org"})

func observeSessionAdmissionTerminal(org, outcome, reason string, wait time.Duration) {
	if wait < 0 {
		wait = 0
	}
	outcome = normalizedSessionAdmissionOutcome(outcome)
	reason = normalizedSessionAdmissionReason(reason)
	sessionAdmissionWaitHistogram.WithLabelValues(org, outcome, reason).Observe(wait.Seconds())
	sessionAdmissionRequestsCounter.WithLabelValues(org, outcome, reason).Inc()
}

func normalizedSessionAdmissionOutcome(outcome string) string {
	switch outcome {
	case "granted", "rejected", "timeout", "canceled", "error":
		return outcome
	default:
		return "error"
	}
}

func normalizedSessionAdmissionReason(reason string) string {
	switch reason {
	case "", "none":
		return "none"
	case "org_vcpu", "user_vcpu", "org_user_vcpu", "user_ineligible", "resharding", "fifo", "store_error", "mixed":
		return reason
	default:
		return "store_error"
	}
}

type sessionAdmissionReasons struct {
	firstNonVCPU string
	mixedNonVCPU bool
	orgVCPU      bool
	userVCPU     bool
}

func (r *sessionAdmissionReasons) add(reason string) {
	reason = normalizedSessionAdmissionReason(reason)
	if reason == "none" {
		return
	}
	switch reason {
	case "org_vcpu":
		r.orgVCPU = true
		return
	case "user_vcpu":
		r.userVCPU = true
		return
	case "org_user_vcpu":
		r.orgVCPU = true
		r.userVCPU = true
		return
	}
	if r.firstNonVCPU == "" {
		r.firstNonVCPU = reason
		return
	}
	if r.firstNonVCPU != reason {
		r.mixedNonVCPU = true
	}
}

func (r sessionAdmissionReasons) value() string {
	if r.orgVCPU && r.userVCPU {
		return "org_user_vcpu"
	}
	if r.orgVCPU {
		return "org_vcpu"
	}
	if r.userVCPU {
		return "user_vcpu"
	}
	if r.mixedNonVCPU {
		return "mixed"
	}
	if r.firstNonVCPU == "" {
		return "none"
	}
	return r.firstNonVCPU
}
