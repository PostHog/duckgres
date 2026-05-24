package controlplane

import (
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// LifecycleOperation is the stable, low-cardinality label that identifies
// a public WorkerLifecycle method on lifecycle-transition metrics. New
// values must be added here (not invented at the call site) so the
// metric's label set stays bounded.
type LifecycleOperation string

const (
	LifecycleOpRetireFromSnapshot            LifecycleOperation = "retire_from_snapshot"
	LifecycleOpRetireOrphanFromSnapshot      LifecycleOperation = "retire_orphan_from_snapshot"
	LifecycleOpRetireIdleVariantFromSnapshot LifecycleOperation = "retire_idle_variant_from_snapshot"
	LifecycleOpMarkLostFromLease             LifecycleOperation = "mark_lost_from_lease"
	LifecycleOpDrain                         LifecycleOperation = "drain"
	LifecycleOpRetireDrained                 LifecycleOperation = "retire_drained"
	LifecycleOpRefreshLease                  LifecycleOperation = "refresh_lease"
)

// LifecycleOrigin identifies the caller that asked WorkerLifecycle for a
// transition. The same operation (e.g. retire_from_snapshot) is invoked
// from many sites with very different triggering conditions, and a CAS
// miss from the janitor's hot-idle reaper tells a different story than
// one from cred-refresh — so we record both axes. Like LifecycleOperation,
// new values must be added here.
type LifecycleOrigin string

const (
	LifecycleOriginJanitorOrphan           LifecycleOrigin = "janitor_orphan"
	LifecycleOriginJanitorHotIdleTTL       LifecycleOrigin = "janitor_hot_idle_ttl"
	LifecycleOriginJanitorStuckActivating  LifecycleOrigin = "janitor_stuck_activating"
	LifecycleOriginMismatchedVersionReaper LifecycleOrigin = "mismatched_version_reaper"
	LifecycleOriginShutdownAll             LifecycleOrigin = "shutdown_all"
	LifecycleOriginHealthCheckCrash        LifecycleOrigin = "health_check_crash"
	LifecycleOriginSpawnFailure            LifecycleOrigin = "spawn_failure"
	LifecycleOriginReserveImageMismatch    LifecycleOrigin = "reserve_image_mismatch"
	LifecycleOriginCredRefresh             LifecycleOrigin = "cred_refresh"
	// LifecycleOriginReserveFailure marks retire paths that fire when
	// ReserveSharedWorker observes a claim that cannot be activated
	// (stale-claim retries excepted) and falls back to retire-and-retry.
	LifecycleOriginReserveFailure LifecycleOrigin = "reserve_failure"
	// LifecycleOriginUnknown is the fallback label applied when an empty
	// origin reaches the observer. We always emit a sample rather than
	// silently drop it; an "unknown" bucket showing up on dashboards is
	// the signal that a new call site forgot to thread the origin.
	LifecycleOriginUnknown LifecycleOrigin = "unknown"
)

// EpochLockOp identifies which ManagedWorker.epochMu accessor is being
// observed for wait time. Cred-refresh holds the lock across a DB
// round-trip, so we want to see whether that wait bleeds into
// hot-path readers.
type EpochLockOp string

const (
	EpochLockOpGet           EpochLockOp = "get"
	EpochLockOpSet           EpochLockOp = "set"
	EpochLockOpIncrement     EpochLockOp = "increment"
	EpochLockOpRefreshAtomic EpochLockOp = "refresh_atomic"
)

// StrandedOutcome categorizes what the janitor recovery sweep did with
// each stranded pod or secret it observed. "kept" means the artifact
// was claimed by a current runtime row (i.e. it wasn't actually
// stranded); "deleted" means the API delete succeeded; "delete_failed"
// means the delete returned an error and the artifact is still around.
type StrandedOutcome string

const (
	StrandedOutcomeDeleted      StrandedOutcome = "deleted"
	StrandedOutcomeKept         StrandedOutcome = "kept"
	StrandedOutcomeDeleteFailed StrandedOutcome = "delete_failed"
)

// SpawnFailureReason categorizes why a worker spawn returned an error.
// Buckets follow the spawnWorker control-flow stages so dashboards
// can localize regressions: an uptick in "pod_ready" points at the
// scheduler / image-pull, an uptick in "secret_create" points at
// Kubernetes API auth, and so on.
type SpawnFailureReason string

const (
	SpawnFailureReasonRuntimeStore    SpawnFailureReason = "runtime_store"
	SpawnFailureReasonConfigMap       SpawnFailureReason = "config_map"
	SpawnFailureReasonSecretCreate    SpawnFailureReason = "secret_create"
	SpawnFailureReasonPodCreate       SpawnFailureReason = "pod_create"
	SpawnFailureReasonPodReady        SpawnFailureReason = "pod_ready"
	SpawnFailureReasonSecretRead      SpawnFailureReason = "secret_read"
	SpawnFailureReasonGRPCConnect     SpawnFailureReason = "grpc_connect"
	SpawnFailureReasonContextCanceled SpawnFailureReason = "context_canceled"
	SpawnFailureReasonOther           SpawnFailureReason = "other"
)

// HealthCheckResult records the outcome of a single worker health
// check probe. "pass" and "fail" are the only first-class outcomes;
// recoverWorkerPanic-caught panics surface as "fail" because the
// caller sees them as an error return.
type HealthCheckResult string

const (
	HealthCheckResultPass HealthCheckResult = "pass"
	HealthCheckResultFail HealthCheckResult = "fail"
)

// InventoryDivergenceKind identifies which side of the in-memory ↔
// durable comparison has the extra worker. "in_memory_only" means a
// ManagedWorker exists with no matching durable row in an active
// state (suggests a missed CAS to terminal); "durable_only" means a
// durable row in an active state has no in-memory worker on this CP
// (suggests a missed claim/takeover or a CP that should be reaping).
type InventoryDivergenceKind string

const (
	InventoryDivergenceKindInMemoryOnly InventoryDivergenceKind = "in_memory_only"
	InventoryDivergenceKindDurableOnly  InventoryDivergenceKind = "durable_only"
)

// --- Metric definitions ---

var workerLifecycleTransitionsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_lifecycle_transitions_total",
	Help: "Worker lifecycle transitions attempted through WorkerLifecycle, partitioned by operation, outcome (CAS success or specific fence miss), worker image, and originating caller.",
}, []string{"operation", "outcome", "image", "origin"})

var workerLifecycleTransitionDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_lifecycle_transition_duration_seconds",
	Help:    "Wall-clock latency of WorkerLifecycle.* invocations including the durable CAS round-trip and any inline cleanup scheduling, partitioned by operation.",
	Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
}, []string{"operation"})

var workerStrandedPodsReconciledCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_stranded_pods_reconciled_total",
	Help: "Stranded worker pods inspected by the janitor recovery sweep, partitioned by reconciliation outcome.",
}, []string{"outcome"})

var workerStrandedSecretsReconciledCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_stranded_secrets_reconciled_total",
	Help: "Stranded worker RPC secrets inspected by the janitor recovery sweep, partitioned by reconciliation outcome.",
}, []string{"outcome"})

var janitorRecoveryLastSuccessGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_janitor_recovery_last_success_seconds",
	Help: "Unix timestamp of the most recent fully-successful janitor recovery sweep. Stays at 0 until the first success, then advances on every clean run; staleness is the alert signal.",
})

var workerEpochLockWaitHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "duckgres_worker_epoch_lock_wait_seconds",
	Help:    "Time spent waiting to acquire ManagedWorker.epochMu before performing the accessor, partitioned by accessor. Cred-refresh holds the lock across a DB round-trip; this histogram exposes whether that wait stalls hot-path readers.",
	Buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1},
}, []string{"op"})

var workerSpawnFailuresCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_spawn_failures_total",
	Help: "Worker spawn failures partitioned by failure reason (which spawn stage returned the error) and image. The companion lifecycle transition fires under origin=spawn_failure; this counter localizes the root cause.",
}, []string{"reason", "image"})

var workerDrainTotalDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_worker_drain_total_duration_seconds",
	Help:    "End-to-end wall-clock duration of a single worker drain (Drain CAS + pod delete + RetireDrained CAS) within ShutdownAll. Complements the per-CAS _transition_duration_seconds, which can't show the pod-delete window between the two CAS steps.",
	Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
})

var workerHealthChecksCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_health_checks_total",
	Help: "Worker RPC health-check probes partitioned by result (pass|fail) and image. Pass-rate complements the mark-lost transitions counter: a worker that's intermittently failing health checks but not yet crossing the consecutive-failure threshold is invisible without this.",
}, []string{"result", "image"})

var workerInventoryDivergenceGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_worker_inventory_divergence",
	Help: "Workers present on exactly one side of the in-memory map / durable runtime store comparison, partitioned by kind. Non-zero values indicate a missed CAS to terminal (in_memory_only) or a missed claim/takeover (durable_only).",
}, []string{"kind"})

// --- Observation helpers ---

// observeLifecycleTransition records one transition attempt on
// duckgres_worker_lifecycle_transitions_total. Inputs are normalized:
// empty operation drops the sample (we'd have no useful label), empty
// outcome falls back to "store_error" (the safest default — an unknown
// outcome is almost certainly a code path that returned early on
// failure), empty image falls back to "unknown", and empty origin falls
// back to "unknown" so call sites missing an origin still show up on
// dashboards instead of being silently lost.
func observeLifecycleTransition(op LifecycleOperation, outcome configstore.TransitionOutcomeReason, image string, origin LifecycleOrigin) {
	opStr := strings.TrimSpace(string(op))
	if opStr == "" {
		return
	}
	outcomeStr := strings.TrimSpace(string(outcome))
	if outcomeStr == "" {
		outcomeStr = string(configstore.TransitionOutcomeStoreError)
	}
	imageStr := strings.TrimSpace(image)
	if imageStr == "" {
		imageStr = "unknown"
	}
	originStr := strings.TrimSpace(string(origin))
	if originStr == "" {
		originStr = string(LifecycleOriginUnknown)
	}
	workerLifecycleTransitionsCounter.WithLabelValues(opStr, outcomeStr, imageStr, originStr).Inc()
}

// observeLifecycleTransitionDuration records elapsed time for a
// WorkerLifecycle.* invocation against the duration histogram.
// Negative durations (clock skew) are coerced to zero.
func observeLifecycleTransitionDuration(op LifecycleOperation, d time.Duration) {
	opStr := strings.TrimSpace(string(op))
	if opStr == "" {
		return
	}
	if d < 0 {
		d = 0
	}
	workerLifecycleTransitionDurationHistogram.WithLabelValues(opStr).Observe(d.Seconds())
}

// observeStrandedPodReconciled records one outcome of the janitor's
// stranded-pod sweep. Empty outcomes are dropped — the caller has no
// useful classification to record.
func observeStrandedPodReconciled(outcome StrandedOutcome) {
	v := strings.TrimSpace(string(outcome))
	if v == "" {
		return
	}
	workerStrandedPodsReconciledCounter.WithLabelValues(v).Inc()
}

// observeStrandedSecretReconciled records one outcome of the janitor's
// stranded-secret sweep. Empty outcomes are dropped.
func observeStrandedSecretReconciled(outcome StrandedOutcome) {
	v := strings.TrimSpace(string(outcome))
	if v == "" {
		return
	}
	workerStrandedSecretsReconciledCounter.WithLabelValues(v).Inc()
}

// recordJanitorRecoverySuccess sets the last-success gauge to the given
// timestamp's Unix-seconds. Called by the janitor only when the
// recovery sweep completed without an error so that alerts can fire on
// staleness ("recovery hasn't succeeded in N minutes") rather than on
// absolute liveness.
func recordJanitorRecoverySuccess(now time.Time) {
	janitorRecoveryLastSuccessGauge.Set(float64(now.Unix()))
}

// observeEpochLockWait records how long a caller waited on
// ManagedWorker.epochMu before acquiring it. Negative durations are
// coerced to zero. Wired by Step 5 of the observability migration.
func observeEpochLockWait(op EpochLockOp, d time.Duration) {
	v := strings.TrimSpace(string(op))
	if v == "" {
		return
	}
	if d < 0 {
		d = 0
	}
	workerEpochLockWaitHistogram.WithLabelValues(v).Observe(d.Seconds())
}

// observeSpawnFailure increments the spawn-failure counter for the
// given (reason, image) tuple. Empty reason drops the sample (the
// counter would be useless without a category); empty image falls
// back to "unknown" so a forgotten image at the call site still
// records something.
func observeSpawnFailure(reason SpawnFailureReason, image string) {
	r := strings.TrimSpace(string(reason))
	if r == "" {
		return
	}
	img := strings.TrimSpace(image)
	if img == "" {
		img = "unknown"
	}
	workerSpawnFailuresCounter.WithLabelValues(r, img).Inc()
}

// observeDrainTotalDuration records the wall-clock duration of one
// complete worker drain (Drain CAS + pod delete + RetireDrained CAS)
// inside ShutdownAll. Negative durations coerce to zero rather than
// drop, mirroring the other histogram helpers.
func observeDrainTotalDuration(d time.Duration) {
	if d < 0 {
		d = 0
	}
	workerDrainTotalDurationHistogram.Observe(d.Seconds())
}

// observeHealthCheck records the result of one health-check probe.
// Empty result drops the sample.
func observeHealthCheck(result HealthCheckResult, image string) {
	r := strings.TrimSpace(string(result))
	if r == "" {
		return
	}
	img := strings.TrimSpace(image)
	if img == "" {
		img = "unknown"
	}
	workerHealthChecksCounter.WithLabelValues(r, img).Inc()
}

// recordInventoryDivergence sets the divergence gauge for the given
// kind. Negative counts coerce to zero. Callers compute the count
// from a single comparison pass and call this twice per tick (once
// per kind) so the gauge always reflects the most recent sweep.
func recordInventoryDivergence(kind InventoryDivergenceKind, count int) {
	k := strings.TrimSpace(string(kind))
	if k == "" {
		return
	}
	if count < 0 {
		count = 0
	}
	workerInventoryDivergenceGauge.WithLabelValues(k).Set(float64(count))
}
