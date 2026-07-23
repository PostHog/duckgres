package configstore

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	orgConnectionAdmissionOutcomeGranted            = "granted"
	orgConnectionAdmissionOutcomeAlreadyGranted     = "already_granted"
	orgConnectionAdmissionOutcomeBlockedOrgVCPU     = "blocked_org_vcpu"
	orgConnectionAdmissionOutcomeBlockedUserVCPU    = "blocked_user_vcpu"
	orgConnectionAdmissionOutcomeBlockedOrgUserVCPU = "blocked_org_user_vcpu"
	orgConnectionAdmissionOutcomeRejectedOrgVCPU    = "rejected_org_vcpu"
	orgConnectionAdmissionOutcomeRejectedUserVCPU   = "rejected_user_vcpu"
	orgConnectionAdmissionOutcomeIneligibleUser     = "ineligible_user"
	orgConnectionAdmissionOutcomeInactive           = "inactive_request"
	orgConnectionAdmissionOutcomeMissing            = "missing_request"
	orgConnectionAdmissionOutcomeWaiting            = "waiting"
	orgConnectionAdmissionOutcomeCanceled           = "canceled"
	orgConnectionAdmissionOutcomeTimeout            = "timeout"
	orgConnectionAdmissionOutcomeError              = "error"
	orgConnectionAdmissionOutcomeResharding         = "blocked_resharding"
)

var orgConnectionAdmissionDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_session_admission_evaluation_duration_seconds",
	Help:    "Duration of one DB-backed session admission request poll, partitioned by its decision and reason.",
	Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
}, []string{"decision", "reason"})

var orgConnectionAdmissionAttemptsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_session_admission_evaluations_total",
	Help: "Total DB-backed session admission request polls, partitioned by decision and reason.",
}, []string{"decision", "reason"})

// OrgConnectionAdmissionEvaluation describes one DB-backed admission poll for
// the polling request. Decision records what happened to that request; Reason
// records why it could not be granted when it waited, was blocked, or rejected.
type OrgConnectionAdmissionEvaluation struct {
	Decision string
	Reason   string
}

func observeOrgConnectionAdmission(d time.Duration, outcome string) OrgConnectionAdmissionEvaluation {
	if d < 0 {
		d = 0
	}
	evaluation := orgConnectionAdmissionEvaluationForOutcome(outcome)
	orgConnectionAdmissionDurationHistogram.WithLabelValues(evaluation.Decision, evaluation.Reason).Observe(d.Seconds())
	orgConnectionAdmissionAttemptsCounter.WithLabelValues(evaluation.Decision, evaluation.Reason).Inc()
	return evaluation
}

func orgConnectionAdmissionEvaluationForOutcome(outcome string) OrgConnectionAdmissionEvaluation {
	switch outcome {
	case orgConnectionAdmissionOutcomeGranted:
		return OrgConnectionAdmissionEvaluation{Decision: "granted_current", Reason: "none"}
	case orgConnectionAdmissionOutcomeAlreadyGranted:
		return OrgConnectionAdmissionEvaluation{Decision: "already_granted", Reason: "none"}
	case orgConnectionAdmissionOutcomeBlockedOrgVCPU:
		return OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "org_vcpu"}
	case orgConnectionAdmissionOutcomeBlockedUserVCPU:
		return OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "user_vcpu"}
	case orgConnectionAdmissionOutcomeBlockedOrgUserVCPU:
		return OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "org_user_vcpu"}
	case orgConnectionAdmissionOutcomeRejectedOrgVCPU:
		return OrgConnectionAdmissionEvaluation{Decision: "rejected", Reason: "org_vcpu"}
	case orgConnectionAdmissionOutcomeRejectedUserVCPU:
		return OrgConnectionAdmissionEvaluation{Decision: "rejected", Reason: "user_vcpu"}
	case orgConnectionAdmissionOutcomeIneligibleUser:
		return OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "user_ineligible"}
	case orgConnectionAdmissionOutcomeResharding:
		return OrgConnectionAdmissionEvaluation{Decision: "blocked", Reason: "resharding"}
	case orgConnectionAdmissionOutcomeInactive:
		return OrgConnectionAdmissionEvaluation{Decision: "inactive", Reason: "none"}
	case orgConnectionAdmissionOutcomeMissing:
		return OrgConnectionAdmissionEvaluation{Decision: "missing", Reason: "none"}
	case orgConnectionAdmissionOutcomeWaiting, "":
		return OrgConnectionAdmissionEvaluation{Decision: "waiting", Reason: "fifo"}
	case orgConnectionAdmissionOutcomeCanceled:
		return OrgConnectionAdmissionEvaluation{Decision: "canceled", Reason: "none"}
	case orgConnectionAdmissionOutcomeTimeout:
		return OrgConnectionAdmissionEvaluation{Decision: "timeout", Reason: "none"}
	case orgConnectionAdmissionOutcomeError:
		return OrgConnectionAdmissionEvaluation{Decision: "error", Reason: "store_error"}
	default:
		return OrgConnectionAdmissionEvaluation{Decision: "error", Reason: "store_error"}
	}
}
