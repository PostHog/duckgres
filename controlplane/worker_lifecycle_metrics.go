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
	// LifecycleOpRetireLocal covers the in-memory retirement chain
	// (markWorkerRetiredLocked → persistWorkerRecord via UpsertWorkerRecord)
	// which doesn't go through the lifecycle CAS service. Used by
	// public RetireWorker, idle-timeout reaping, stuck-activating
	// reaping, and activation-failure / liveness-recheck fallbacks.
	// outcome is normally transitioned, but UpsertWorkerRecord has a
	// fence-miss path (ErrWorkerRecordUpsertFenceMiss) — markWorkerRetiredLocked
	// gates the emission on the upsert result, so retire_local
	// samples reflect transitions that actually landed durably.
	LifecycleOpRetireLocal LifecycleOperation = "retire_local"
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
	// LifecycleOriginDrainReleaseIdle marks the hot -> hot_idle park performed
	// by OrgRouter.ReleaseIdleHotWorkers at CP drain start: idle (zero-session)
	// Hot workers are released so the unfenced hot-idle TTL reaper reclaims them
	// during the drain wait instead of pinning vCPU until the CP is declared
	// expired. A nonzero rate here during a rollout is the leak fix working.
	LifecycleOriginDrainReleaseIdle LifecycleOrigin = "drain_release_idle"
	LifecycleOriginHealthCheckCrash        LifecycleOrigin = "health_check_crash"
	LifecycleOriginWorkerDrain             LifecycleOrigin = "worker_drain"
	LifecycleOriginSpawnFailure            LifecycleOrigin = "spawn_failure"
	LifecycleOriginReserveImageMismatch    LifecycleOrigin = "reserve_image_mismatch"
	LifecycleOriginCredRefresh             LifecycleOrigin = "cred_refresh"
	// LifecycleOriginReserveFailure marks retire paths that fire when
	// ReserveSharedWorker observes a claim that cannot be activated
	// (stale-claim retries excepted) and falls back to retire-and-retry.
	LifecycleOriginReserveFailure LifecycleOrigin = "reserve_failure"
	// LifecycleOriginIdleTimeout marks retire paths from the idle-worker
	// reaper (reapIdleWorkers).
	LifecycleOriginIdleTimeout LifecycleOrigin = "idle_timeout"
	// LifecycleOriginPublicAPI marks retire paths invoked through the
	// public RetireWorker / RetireWorkerIfNoSessions / RetireIfDrainingAndEmpty
	// surface (admin tooling, orchestration callbacks).
	LifecycleOriginPublicAPI LifecycleOrigin = "public_api"
	// LifecycleOriginActivationFailure marks retire paths fired after a
	// worker activation returned an error (org pool's
	// activateWorkerForOrg / ReconnectFlightWorker).
	LifecycleOriginActivationFailure LifecycleOrigin = "activation_failure"
	// LifecycleOriginCrashGeneric marks retire paths driven by a generic
	// "worker died" signal that isn't covered by a more specific origin
	// (no current callers after the explicit-origin refactor; retained as
	// a safe fallback bucket for future generic-crash sites).
	LifecycleOriginCrashGeneric LifecycleOrigin = "crash_generic"
	// LifecycleOriginInformerCrash marks the cleanDeadWorkersLocked path
	// driven by the K8s pod-informer firing w.done. Distinct from
	// LifecycleOriginHealthCheckCrash (which is the periodic
	// HealthCheckLoop probe) so dashboards can separate cluster-driven
	// pod terminations (eviction, OOM, manual delete, node drain) from
	// our own health-check decisions.
	LifecycleOriginInformerCrash LifecycleOrigin = "informer_crash"
	// LifecycleOriginPerCPHotIdleTTL marks the per-CP fallback hot-idle reaper
	// (per_cp_hot_idle_reaper.go), which runs on EVERY replica independent of the
	// janitor leader lease. Distinct from LifecycleOriginJanitorHotIdleTTL (the
	// leader-only janitor reaper) so dashboards can tell when the fallback —
	// rather than the leader — is doing the reaping, which is the signal that
	// leadership is wedged.
	LifecycleOriginPerCPHotIdleTTL LifecycleOrigin = "per_cp_hot_idle_ttl"
	// LifecycleOriginPerCPOrphan marks the per-CP fallback reaper retiring a
	// hot-idle worker whose OWNING CP instance is no longer live (rollout/crash
	// orphan). Distinct from LifecycleOriginJanitorOrphan (the leader-only orphan
	// sweep) so dashboards can see the fallback doing cross-CP orphan reclamation
	// during a leaderless window. Every CP start mints a fresh cpInstanceID, so a
	// graceful rollout orphans the prior process's hot-idle rows; without this
	// path they would wait on the leader.
	LifecycleOriginPerCPOrphan LifecycleOrigin = "per_cp_orphan"
	// LifecycleOriginPoolStuckActivating marks the pool-local
	// reapStuckActivatingWorkers loop, which runs every minute on every
	// CP. Distinct from LifecycleOriginJanitorStuckActivating (which is
	// the leader-only janitor reaper) so operators can tell whether
	// pool-side or janitor-side reaping is doing the work.
	LifecycleOriginPoolStuckActivating LifecycleOrigin = "pool_stuck_activating"
	// LifecycleOriginOrgShutdown marks per-org ShutdownAll on
	// OrgReservedPool. Distinct from LifecycleOriginShutdownAll (which is
	// the pool-wide K8sWorkerPool.ShutdownAll) so operators can tell
	// org-offboarding events from CP rollouts on dashboards.
	LifecycleOriginOrgShutdown LifecycleOrigin = "org_shutdown"
	// LifecycleOriginUnknown is the fallback label applied when an empty
	// origin reaches the observer. We always emit a sample rather than
	// silently drop it; an "unknown" bucket showing up on dashboards is
	// the signal that a new call site forgot to thread the origin.
	LifecycleOriginUnknown LifecycleOrigin = "unknown"
)

// StrandedOutcome categorizes what the janitor recovery sweep did with
// each stranded pod it observed. "kept" means the artifact was claimed
// by a current runtime row (i.e. it wasn't actually stranded); "deleted"
// means the API delete succeeded; "delete_failed" means the delete
// returned an error and the artifact is still around.
type StrandedOutcome string

const (
	StrandedOutcomeDeleted            StrandedOutcome = "deleted"
	StrandedOutcomeDeletedMissingRow  StrandedOutcome = "deleted_missing_row"
	StrandedOutcomeDeletedTerminalRow StrandedOutcome = "deleted_terminal_row"
	StrandedOutcomeKept               StrandedOutcome = "kept"
	StrandedOutcomeDeleteFailed       StrandedOutcome = "delete_failed"
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

// Retirement reason constants. Passed as the `reason` argument to
// WorkerLifecycle.* methods and surfaced on
// duckgres_worker_lifecycle_transitions_total as part of the operation
// context (also fed into lifecycleOriginForRetireReason for retire_local
// observations). Defined here (no build tag) rather than in
// warm_pool_metrics.go (kubernetes-tagged) so the no-tag lifecycle
// observation helpers can reference them.
const (
	RetireReasonNormal            = "normal"
	RetireReasonActivationFailure = "activation_failure"
	RetireReasonOrphaned          = "orphaned"
	RetireReasonCrash             = "crash"
	RetireReasonShutdown          = "shutdown"
	RetireReasonIdleTimeout       = "idle_timeout"
	RetireReasonStuckActivating   = "stuck_activating"
	RetireReasonMismatchedVersion = "mismatched_version"
	// RetireReasonSpawnFailure marks a spawning-slot row freed on the request
	// thread after its pod spawn failed (k8s_pool_acquire.go), so the org+global
	// cap is released immediately instead of waiting for the stale-spawning sweep.
	RetireReasonSpawnFailure = "spawn_failure"
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
	Help: "Stranded worker RPC secrets inspected by the janitor recovery sweep, partitioned by reconciliation outcome. Restored after review pointed out that secret-leak detection isn't reachable from the pods counter — the two reapers cover disjoint failure modes (orphan secret with no pod vs. orphan pod with retired durable row).",
}, []string{"outcome"})

var workerSpawnFailuresCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_spawn_failures_total",
	Help: "Worker spawn failures partitioned by failure reason (which spawn stage returned the error) and image. The companion lifecycle transition fires under origin=spawn_failure; this counter localizes the root cause.",
}, []string{"reason", "image"})

var workerDrainTotalDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "duckgres_worker_drain_total_duration_seconds",
	Help:    "Wall-clock duration of a single worker drain within ShutdownAll, measured from the Drain CAS through successful pod-delete (the pod-gone milestone). Only emitted when the pod was actually deleted — Drain CAS misses and pod-delete failures are excluded so p99 alerts track the operationally meaningful tail rather than being dominated by microsecond fence-misses. The best-effort terminal CAS that follows is intentionally not part of the timing.",
	Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
})

var workerHealthChecksCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_worker_health_checks_total",
	Help: "Worker RPC health-check probes partitioned by result (pass|fail) and image. Pass-rate complements the mark-lost transitions counter: a worker that's intermittently failing health checks but not yet crossing the consecutive-failure threshold is invisible without this.",
}, []string{"result", "image"})

// hotIdleReapLastRunGauge is updated by both the leader janitor (janitor.go,
// untagged) and the per-CP fallback reaper (per_cp_hot_idle_reaper.go, untagged),
// so it stays in this shared file. The kubernetes-only janitor_is_leader and
// hot_idle_persist_failures metrics live in worker_lifecycle_metrics_k8s.go so
// they are not flagged unused under the default-tag lint.
var hotIdleReapLastRunGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_control_plane_hot_idle_reap_last_run_timestamp_seconds",
	Help: "Unix timestamp of the most recent successful hot-idle reap pass on THIS replica — either the leader janitor or the per-CP fallback reaper. Alert when max() across replicas falls far behind now(): idle worker pods are not being retired and node capacity is leaking.",
})

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
// stranded-secret sweep.
func observeStrandedSecretReconciled(outcome StrandedOutcome) {
	v := strings.TrimSpace(string(outcome))
	if v == "" {
		return
	}
	workerStrandedSecretsReconciledCounter.WithLabelValues(v).Inc()
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

// observeHotIdleReapRun stamps the last-successful-hot-idle-reap gauge with the
// given time. Called by both the leader janitor and the per-CP fallback reaper
// (both untagged), so it lives here rather than in the kubernetes-tagged file.
func observeHotIdleReapRun(t time.Time) {
	hotIdleReapLastRunGauge.Set(float64(t.Unix()))
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
