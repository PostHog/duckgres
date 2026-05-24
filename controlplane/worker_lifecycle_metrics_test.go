package controlplane

import (
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestObserveLifecycleTransition_RecordsLabels(t *testing.T) {
	image := "duckgres:lifecycle-metric-labels"
	op := LifecycleOpRetireFromSnapshot
	outcome := configstore.TransitionOutcomeTransitioned
	origin := LifecycleOriginJanitorHotIdleTTL

	before := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(op), string(outcome), image, string(origin))

	observeLifecycleTransition(op, outcome, image, origin)

	after := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(op), string(outcome), image, string(origin))
	if delta := after - before; delta != 1 {
		t.Fatalf("expected one transition increment, got delta %v", delta)
	}
}

func TestObserveLifecycleTransition_NormalizesEmptyOriginAndImage(t *testing.T) {
	op := LifecycleOpDrain
	outcome := configstore.TransitionOutcomeFenceMissOwner

	beforeUnknown := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(op), string(outcome), "unknown", string(LifecycleOriginUnknown))

	observeLifecycleTransition(op, outcome, "  ", "  ")

	afterUnknown := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(op), string(outcome), "unknown", string(LifecycleOriginUnknown))
	if delta := afterUnknown - beforeUnknown; delta != 1 {
		t.Fatalf("expected one increment on (image=unknown, origin=unknown), got delta %v", delta)
	}
}

func TestObserveLifecycleTransition_EmptyOutcomeFallsBackToStoreError(t *testing.T) {
	op := LifecycleOpRetireDrained
	image := "duckgres:lifecycle-metric-empty-outcome"
	origin := LifecycleOriginShutdownAll

	before := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(op), string(configstore.TransitionOutcomeStoreError), image, string(origin))

	observeLifecycleTransition(op, configstore.TransitionOutcomeReason(""), image, origin)

	after := counterVecLabelValue(t, workerLifecycleTransitionsCounter,
		string(op), string(configstore.TransitionOutcomeStoreError), image, string(origin))
	if delta := after - before; delta != 1 {
		t.Fatalf("expected empty outcome to fall back to store_error; delta %v", delta)
	}
}

func TestObserveLifecycleTransition_EmptyOperationIsDropped(t *testing.T) {
	// Total across the whole transitions family should be unchanged after
	// an empty-op call — there's nothing to record under a useful label.
	before := metricCounterFamilyTotal(t, "duckgres_worker_lifecycle_transitions_total")
	observeLifecycleTransition(LifecycleOperation(""), configstore.TransitionOutcomeTransitioned,
		"duckgres:does-not-matter", LifecycleOriginCredRefresh)
	after := metricCounterFamilyTotal(t, "duckgres_worker_lifecycle_transitions_total")
	if after != before {
		t.Fatalf("expected empty-op call to be dropped; family total moved %v → %v", before, after)
	}
}

func TestObserveLifecycleTransitionDuration_RecordsSample(t *testing.T) {
	op := LifecycleOpMarkLostFromLease

	before := histogramVecLabelSampleCount(t, workerLifecycleTransitionDurationHistogram, string(op))
	observeLifecycleTransitionDuration(op, 12*time.Millisecond)
	after := histogramVecLabelSampleCount(t, workerLifecycleTransitionDurationHistogram, string(op))

	if after-before != 1 {
		t.Fatalf("expected one duration sample, got delta %d", after-before)
	}
}

func TestObserveLifecycleTransitionDuration_NegativeDurationCoercedToZero(t *testing.T) {
	op := LifecycleOpRefreshLease

	before := histogramVecLabelSampleCount(t, workerLifecycleTransitionDurationHistogram, string(op))
	observeLifecycleTransitionDuration(op, -5*time.Second)
	after := histogramVecLabelSampleCount(t, workerLifecycleTransitionDurationHistogram, string(op))

	if after-before != 1 {
		t.Fatalf("expected negative duration to still record one sample (clamped to 0), got delta %d", after-before)
	}
}

func TestObserveLifecycleTransitionDuration_EmptyOperationIsDropped(t *testing.T) {
	before := metricHistogramCount(t, "duckgres_worker_lifecycle_transition_duration_seconds")
	observeLifecycleTransitionDuration(LifecycleOperation(""), 10*time.Millisecond)
	after := metricHistogramCount(t, "duckgres_worker_lifecycle_transition_duration_seconds")
	if before != after {
		t.Fatalf("expected empty-op duration call to be dropped; samples moved %d → %d", before, after)
	}
}

func TestObserveStrandedPodReconciled(t *testing.T) {
	before := counterVecLabelValue(t, workerStrandedPodsReconciledCounter, string(StrandedOutcomeDeleted))
	observeStrandedPodReconciled(StrandedOutcomeDeleted)
	after := counterVecLabelValue(t, workerStrandedPodsReconciledCounter, string(StrandedOutcomeDeleted))
	if after-before != 1 {
		t.Fatalf("expected one stranded-pod-deleted increment, got delta %v", after-before)
	}

	beforeFailed := counterVecLabelValue(t, workerStrandedPodsReconciledCounter, string(StrandedOutcomeDeleteFailed))
	observeStrandedPodReconciled(StrandedOutcomeDeleteFailed)
	afterFailed := counterVecLabelValue(t, workerStrandedPodsReconciledCounter, string(StrandedOutcomeDeleteFailed))
	if afterFailed-beforeFailed != 1 {
		t.Fatalf("expected one stranded-pod-delete-failed increment, got delta %v", afterFailed-beforeFailed)
	}

	// Empty outcome is a no-op — family total must not move.
	familyBefore := metricCounterFamilyTotal(t, "duckgres_worker_stranded_pods_reconciled_total")
	observeStrandedPodReconciled("  ")
	familyAfter := metricCounterFamilyTotal(t, "duckgres_worker_stranded_pods_reconciled_total")
	if familyAfter != familyBefore {
		t.Fatalf("expected empty stranded-pod outcome to be dropped; family total moved %v → %v", familyBefore, familyAfter)
	}
}

func TestObserveStrandedSecretReconciled(t *testing.T) {
	before := counterVecLabelValue(t, workerStrandedSecretsReconciledCounter, string(StrandedOutcomeKept))
	observeStrandedSecretReconciled(StrandedOutcomeKept)
	after := counterVecLabelValue(t, workerStrandedSecretsReconciledCounter, string(StrandedOutcomeKept))
	if after-before != 1 {
		t.Fatalf("expected one stranded-secret-kept increment, got delta %v", after-before)
	}

	// Empty outcome is a no-op — family total must not move.
	familyBefore := metricCounterFamilyTotal(t, "duckgres_worker_stranded_secrets_reconciled_total")
	observeStrandedSecretReconciled("")
	familyAfter := metricCounterFamilyTotal(t, "duckgres_worker_stranded_secrets_reconciled_total")
	if familyAfter != familyBefore {
		t.Fatalf("expected empty stranded-secret outcome to be dropped; family total moved %v → %v", familyBefore, familyAfter)
	}
}

func TestRecordJanitorRecoverySuccess(t *testing.T) {
	ts := time.Unix(1_700_000_000, 0)
	recordJanitorRecoverySuccess(ts)
	if got := gaugeValue(t, janitorRecoveryLastSuccessGauge); got != float64(ts.Unix()) {
		t.Fatalf("expected gauge=%d, got %v", ts.Unix(), got)
	}

	later := time.Unix(1_700_000_500, 0)
	recordJanitorRecoverySuccess(later)
	if got := gaugeValue(t, janitorRecoveryLastSuccessGauge); got != float64(later.Unix()) {
		t.Fatalf("expected gauge=%d after second call, got %v", later.Unix(), got)
	}
}

func TestObserveEpochLockWait(t *testing.T) {
	op := EpochLockOpRefreshAtomic

	before := histogramVecLabelSampleCount(t, workerEpochLockWaitHistogram, string(op))
	observeEpochLockWait(op, 2*time.Millisecond)
	after := histogramVecLabelSampleCount(t, workerEpochLockWaitHistogram, string(op))
	if after-before != 1 {
		t.Fatalf("expected one wait sample for %s, got delta %d", op, after-before)
	}

	// Negative durations coerce to zero rather than dropping.
	beforeZero := histogramVecLabelSampleCount(t, workerEpochLockWaitHistogram, string(EpochLockOpGet))
	observeEpochLockWait(EpochLockOpGet, -time.Hour)
	afterZero := histogramVecLabelSampleCount(t, workerEpochLockWaitHistogram, string(EpochLockOpGet))
	if afterZero-beforeZero != 1 {
		t.Fatalf("expected negative duration to be coerced to 0 and recorded; delta %d", afterZero-beforeZero)
	}

	// Empty op is dropped — family total must not move.
	familyBefore := metricHistogramCount(t, "duckgres_worker_epoch_lock_wait_seconds")
	observeEpochLockWait("  ", 5*time.Millisecond)
	familyAfter := metricHistogramCount(t, "duckgres_worker_epoch_lock_wait_seconds")
	if familyAfter != familyBefore {
		t.Fatalf("expected empty-op epoch wait call to be dropped; family samples moved %d → %d", familyBefore, familyAfter)
	}
}
