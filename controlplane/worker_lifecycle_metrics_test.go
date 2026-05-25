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
	before := counterVecLabelValue(t, workerStrandedSecretsReconciledCounter, string(StrandedOutcomeDeleted))
	observeStrandedSecretReconciled(StrandedOutcomeDeleted)
	after := counterVecLabelValue(t, workerStrandedSecretsReconciledCounter, string(StrandedOutcomeDeleted))
	if after-before != 1 {
		t.Fatalf("expected one stranded-secret-deleted increment, got delta %v", after-before)
	}

	familyBefore := metricCounterFamilyTotal(t, "duckgres_worker_stranded_secrets_reconciled_total")
	observeStrandedSecretReconciled("  ")
	familyAfter := metricCounterFamilyTotal(t, "duckgres_worker_stranded_secrets_reconciled_total")
	if familyAfter != familyBefore {
		t.Fatalf("expected empty stranded-secret outcome to be dropped; family total moved %v -> %v", familyBefore, familyAfter)
	}
}

func TestObserveSpawnFailure(t *testing.T) {
	image := "duckgres:spawn-fail-test"
	before := counterVecLabelValue(t, workerSpawnFailuresCounter, string(SpawnFailureReasonPodCreate), image)
	observeSpawnFailure(SpawnFailureReasonPodCreate, image)
	after := counterVecLabelValue(t, workerSpawnFailuresCounter, string(SpawnFailureReasonPodCreate), image)
	if after-before != 1 {
		t.Fatalf("expected one spawn-failure increment for (pod_create, %s), got delta %v", image, after-before)
	}

	// Empty image falls back to "unknown".
	beforeUnknown := counterVecLabelValue(t, workerSpawnFailuresCounter, string(SpawnFailureReasonGRPCConnect), "unknown")
	observeSpawnFailure(SpawnFailureReasonGRPCConnect, "  ")
	afterUnknown := counterVecLabelValue(t, workerSpawnFailuresCounter, string(SpawnFailureReasonGRPCConnect), "unknown")
	if afterUnknown-beforeUnknown != 1 {
		t.Fatalf("expected empty image to fall back to unknown; delta %v", afterUnknown-beforeUnknown)
	}

	// Empty reason drops the sample.
	familyBefore := metricCounterFamilyTotal(t, "duckgres_worker_spawn_failures_total")
	observeSpawnFailure("", image)
	familyAfter := metricCounterFamilyTotal(t, "duckgres_worker_spawn_failures_total")
	if familyAfter != familyBefore {
		t.Fatalf("expected empty-reason spawn-failure to drop; family total moved %v → %v", familyBefore, familyAfter)
	}
}

func TestObserveDrainTotalDuration(t *testing.T) {
	before := metricHistogramCount(t, "duckgres_worker_drain_total_duration_seconds")
	observeDrainTotalDuration(150 * time.Millisecond)
	mid := metricHistogramCount(t, "duckgres_worker_drain_total_duration_seconds")
	if mid-before != 1 {
		t.Fatalf("expected one drain-duration sample, got delta %d", mid-before)
	}

	// Negative durations coerce to zero rather than drop.
	observeDrainTotalDuration(-time.Second)
	after := metricHistogramCount(t, "duckgres_worker_drain_total_duration_seconds")
	if after-mid != 1 {
		t.Fatalf("expected negative duration to record one sample at 0; delta %d", after-mid)
	}
}

func TestObserveHealthCheck(t *testing.T) {
	image := "duckgres:hc-test"
	before := counterVecLabelValue(t, workerHealthChecksCounter, string(HealthCheckResultPass), image)
	observeHealthCheck(HealthCheckResultPass, image)
	after := counterVecLabelValue(t, workerHealthChecksCounter, string(HealthCheckResultPass), image)
	if after-before != 1 {
		t.Fatalf("expected one HC pass increment, got delta %v", after-before)
	}

	failBefore := counterVecLabelValue(t, workerHealthChecksCounter, string(HealthCheckResultFail), image)
	observeHealthCheck(HealthCheckResultFail, image)
	failAfter := counterVecLabelValue(t, workerHealthChecksCounter, string(HealthCheckResultFail), image)
	if failAfter-failBefore != 1 {
		t.Fatalf("expected one HC fail increment, got delta %v", failAfter-failBefore)
	}

	// Empty result drops.
	familyBefore := metricCounterFamilyTotal(t, "duckgres_worker_health_checks_total")
	observeHealthCheck("", image)
	familyAfter := metricCounterFamilyTotal(t, "duckgres_worker_health_checks_total")
	if familyAfter != familyBefore {
		t.Fatalf("expected empty-result HC to drop; family total moved %v → %v", familyBefore, familyAfter)
	}
}
