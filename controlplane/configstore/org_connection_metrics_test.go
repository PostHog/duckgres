package configstore

import "testing"

func TestOrgConnectionAdmissionEvaluationForOutcome(t *testing.T) {
	tests := []struct {
		outcome  string
		decision string
		reason   string
	}{
		{orgConnectionAdmissionOutcomeGranted, "granted_current", "none"},
		{orgConnectionAdmissionOutcomeAlreadyGranted, "already_granted", "none"},
		{orgConnectionAdmissionOutcomeBlockedOrgVCPU, "blocked", "org_vcpu"},
		{orgConnectionAdmissionOutcomeBlockedUserVCPU, "blocked", "user_vcpu"},
		{orgConnectionAdmissionOutcomeBlockedOrgUserVCPU, "blocked", "org_user_vcpu"},
		{orgConnectionAdmissionOutcomeRejectedOrgVCPU, "rejected", "org_vcpu"},
		{orgConnectionAdmissionOutcomeRejectedUserVCPU, "rejected", "user_vcpu"},
		{orgConnectionAdmissionOutcomeIneligibleUser, "blocked", "user_ineligible"},
		{orgConnectionAdmissionOutcomeResharding, "blocked", "resharding"},
		{orgConnectionAdmissionOutcomeInactive, "inactive", "none"},
		{orgConnectionAdmissionOutcomeMissing, "missing", "none"},
		{orgConnectionAdmissionOutcomeWaiting, "waiting", "fifo"},
		{orgConnectionAdmissionOutcomeCanceled, "canceled", "none"},
		{orgConnectionAdmissionOutcomeTimeout, "timeout", "none"},
		{orgConnectionAdmissionOutcomeError, "error", "store_error"},
	}
	for _, tt := range tests {
		t.Run(tt.outcome, func(t *testing.T) {
			got := orgConnectionAdmissionEvaluationForOutcome(tt.outcome)
			if got.Decision != tt.decision || got.Reason != tt.reason {
				t.Fatalf("evaluation = %#v, want decision=%q reason=%q", got, tt.decision, tt.reason)
			}
		})
	}
}
