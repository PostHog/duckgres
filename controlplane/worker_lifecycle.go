package controlplane

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// WorkerPhysicalCleanup performs the post-CAS K8s/in-memory side of a
// worker retirement: pod delete, RPC secret delete, removal from any
// in-process pool maps. WorkerLifecycle calls it after a durable CAS to
// terminal has landed.
//
// Cleanup is fire-and-forget (the method returns synchronously after
// scheduling the goroutine that performs the delete). The lifecycle
// service does not depend on or report cleanup completion — the orphan
// reconciler + cleanupOrphanedWorkerPods janitor are the safety net for
// failed deletes.
type WorkerPhysicalCleanup interface {
	// DeleteWorkerArtifacts schedules deletion of the pod and RPC secret
	// for the named worker. Called only after the durable runtime row
	// has transitioned to terminal. Implementations may also clean up
	// in-memory pool state keyed by workerID.
	DeleteWorkerArtifacts(workerID int, podName string, reason string)
}

// workerLifecycleStore is the narrow subset of RuntimeWorkerStore that
// WorkerLifecycle needs. Declared here so the service can be wired and
// tested against a minimal mock without spelling out the full store
// surface.
type workerLifecycleStore interface {
	MarkWorkerTerminalIfCurrent(record *configstore.WorkerRecord, targetState configstore.WorkerState, reason string) (bool, error)
	RetireOrphanWorker(record *configstore.WorkerRecord, reason string) (bool, error)
	RetireIdleOrHotIdleWorker(record *configstore.WorkerRecord, reason string) (bool, error)
	MarkWorkerLostIfCurrentLease(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, reason string) (bool, error)
	MarkWorkerDraining(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64) (bool, error)
	RetireDrainingWorker(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, reason string) (bool, error)
	BumpWorkerEpoch(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64) (int64, error)
}

// WorkerLifecycle is the central typed lifecycle-transition API. It is
// the seam where post-#615 hand-rolled "observe → fence → CAS → cleanup"
// patterns are collapsed into one place.
//
// All transitions take either a WorkerSnapshot (for observed-row paths
// like the janitor reapers) or a WorkerLease (for owned-row paths like
// ShutdownAll and the health checker). The two types cannot be
// interchanged at the call site, which is the load-bearing safety
// property — callers without a real lease physically cannot invoke the
// lease-only methods, and a snapshot's frozen fields are exactly what
// the underlying CAS fences against.
//
// On a successful terminal CAS the service kicks the configured
// WorkerPhysicalCleanup. On a CAS miss the cleanup is skipped — by
// construction we have no proof of ownership over the pod, so deleting
// it is unsafe.
//
// Every public transition method observes
// duckgres_worker_lifecycle_transitions_total (keyed by operation,
// outcome, image, and origin) plus a per-operation latency histogram.
// origin is a caller-supplied LifecycleOrigin so dashboards can see, for
// example, that "fence_miss_owner on retire_from_snapshot from
// janitor_orphan" is a different signal from the same outcome coming
// from cred_refresh.
type WorkerLifecycle struct {
	store   workerLifecycleStore
	cleanup WorkerPhysicalCleanup
}

// NewWorkerLifecycle wires the service. cleanup may be nil if the
// caller only needs durable CAS transitions and no physical cleanup
// (e.g. tests that assert on store calls in isolation).
func NewWorkerLifecycle(store workerLifecycleStore, cleanup WorkerPhysicalCleanup) *WorkerLifecycle {
	return &WorkerLifecycle{store: store, cleanup: cleanup}
}

// RetireFromSnapshot moves the observed worker row to a terminal state
// (retired or lost) fenced by the snapshot. Used by paths that already
// own the worker but observed a separate snapshot (e.g. the janitor's
// hot-idle TTL reaper) — the broad MarkWorkerTerminalIfCurrent fence
// ensures we don't trample a row that has been taken over since the
// snapshot was captured.
func (l *WorkerLifecycle) RetireFromSnapshot(snap configstore.WorkerSnapshot, target configstore.WorkerState, reason string, origin LifecycleOrigin) (configstore.TransitionOutcome, error) {
	const op = LifecycleOpRetireFromSnapshot
	start := time.Now()
	defer func() { observeLifecycleTransitionDuration(op, time.Since(start)) }()

	if l == nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, errors.New("worker lifecycle service not configured")
	}
	if target != configstore.WorkerStateRetired && target != configstore.WorkerStateLost {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, fmt.Errorf("worker %d unsupported terminal state %q", snap.WorkerID(), target)
	}

	record := snap.Record()
	transitioned, err := l.store.MarkWorkerTerminalIfCurrent(&record, target, reason)
	if err != nil {
		slog.Warn("Lifecycle retire CAS failed.",
			"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "target", target, "reason", reason, "origin", origin, "error", err)
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, err
	}
	if !transitioned {
		slog.Debug("Lifecycle retire CAS missed; pod cleanup skipped.",
			"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "target", target, "reason", reason, "origin", origin)
		outcome := configstore.TransitionOutcome{Reason: classifySnapshotMiss(snap)}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, nil
	}
	l.scheduleCleanup(snap.WorkerID(), snap.PodName(), reason)
	outcome := configstore.TransitionOutcome{
		Transitioned:             true,
		PhysicalCleanupScheduled: l.cleanup != nil,
		Reason:                   configstore.TransitionOutcomeTransitioned,
	}
	observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
	return outcome, nil
}

// RetireOrphanFromSnapshot is the orphan-specific retire: it permits any
// active observed state (spawning through draining) and additionally
// enforces that the worker's owner control-plane is still expired (or
// absent). Used by the janitor's orphan-cleanup loop where the listing
// step already filtered by owner-expired-or-missing, so the snapshot's
// CP must remain so for the retire to be safe.
func (l *WorkerLifecycle) RetireOrphanFromSnapshot(snap configstore.WorkerSnapshot, reason string, origin LifecycleOrigin) (configstore.TransitionOutcome, error) {
	const op = LifecycleOpRetireOrphanFromSnapshot
	start := time.Now()
	defer func() { observeLifecycleTransitionDuration(op, time.Since(start)) }()

	if l == nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, errors.New("worker lifecycle service not configured")
	}
	record := snap.Record()
	transitioned, err := l.store.RetireOrphanWorker(&record, reason)
	if err != nil {
		slog.Warn("Lifecycle orphan retire CAS failed.",
			"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "reason", reason, "origin", origin, "error", err)
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, err
	}
	if !transitioned {
		// The orphan CAS rejects on any of: state changed, owner changed,
		// epoch advanced, updated_at advanced, or the supposed-orphan's
		// owner CP is no longer expired. Without an extra round-trip we
		// can't tell which — report the generic snapshot-miss label and
		// let PR 6 add a follow-up classifier once metrics need the
		// finer dimensions.
		miss := classifySnapshotMiss(snap)
		slog.Debug("Lifecycle orphan retire CAS missed; pod cleanup skipped.",
			"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "reason", reason, "origin", origin, "miss", miss)
		outcome := configstore.TransitionOutcome{Reason: miss}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, nil
	}
	l.scheduleCleanup(snap.WorkerID(), snap.PodName(), reason)
	outcome := configstore.TransitionOutcome{
		Transitioned:             true,
		PhysicalCleanupScheduled: l.cleanup != nil,
		Reason:                   configstore.TransitionOutcomeTransitioned,
	}
	observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
	return outcome, nil
}

// RetireIdleVariantFromSnapshot retires a worker observed as idle or
// hot_idle, fenced by the snapshot. The state restriction maps onto
// the legacy RetireIdleOrHotIdleWorker store CAS. Today's only caller
// is the mismatched-version reaper, which deliberately reaps only
// idle/hot_idle pods so a busy worker isn't yanked mid-session. The
// hot-idle TTL janitor previously used this helper but was promoted to
// the broader RetireFromSnapshot once the snapshot already narrowed
// the candidate set to state=hot_idle by virtue of ListExpiredHotIdleSnapshots.
func (l *WorkerLifecycle) RetireIdleVariantFromSnapshot(snap configstore.WorkerSnapshot, reason string, origin LifecycleOrigin) (configstore.TransitionOutcome, error) {
	const op = LifecycleOpRetireIdleVariantFromSnapshot
	start := time.Now()
	defer func() { observeLifecycleTransitionDuration(op, time.Since(start)) }()

	if l == nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, errors.New("worker lifecycle service not configured")
	}
	record := snap.Record()
	transitioned, err := l.store.RetireIdleOrHotIdleWorker(&record, reason)
	if err != nil {
		slog.Warn("Lifecycle idle-variant retire CAS failed.",
			"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "reason", reason, "origin", origin, "error", err)
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, err
	}
	if !transitioned {
		miss := classifySnapshotMiss(snap)
		// If the snapshot was not idle/hot_idle to begin with, that's a
		// state-restriction miss rather than an ownership change.
		if snap.State() != configstore.WorkerStateIdle && snap.State() != configstore.WorkerStateHotIdle {
			miss = configstore.TransitionOutcomeFenceMissState
		}
		slog.Debug("Lifecycle idle-variant retire CAS missed; pod cleanup skipped.",
			"worker", snap.WorkerID(), "worker_pod", snap.PodName(), "org", snap.OrgID(), "reason", reason, "origin", origin, "miss", miss)
		outcome := configstore.TransitionOutcome{Reason: miss}
		observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
		return outcome, nil
	}
	l.scheduleCleanup(snap.WorkerID(), snap.PodName(), reason)
	outcome := configstore.TransitionOutcome{
		Transitioned:             true,
		PhysicalCleanupScheduled: l.cleanup != nil,
		Reason:                   configstore.TransitionOutcomeTransitioned,
	}
	observeLifecycleTransition(op, outcome.Reason, snap.Image(), origin)
	return outcome, nil
}

// MarkLostFromLease performs the lease-fenced CAS that transitions a
// worker row to lost. Used by the health-checker after it has
// confirmed the worker is unresponsive. Does NOT schedule pod
// cleanup — consistent with the other lease-based transitions
// (Drain, RetireDrained, RefreshLease), the caller orchestrates
// physical cleanup so it can interleave replenishment decisions,
// in-memory pool removal, and pod delete in the right order. (The
// snapshot-based variants RetireFromSnapshot/RetireOrphanFromSnapshot/
// RetireIdleVariantFromSnapshot do bundle cleanup because their
// callers don't have post-CAS choreography.)
func (l *WorkerLifecycle) MarkLostFromLease(lease configstore.WorkerLease, reason string, origin LifecycleOrigin) (configstore.TransitionOutcome, error) {
	const op = LifecycleOpMarkLostFromLease
	start := time.Now()
	defer func() { observeLifecycleTransitionDuration(op, time.Since(start)) }()

	if l == nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, errors.New("worker lifecycle service not configured")
	}
	transitioned, err := l.store.MarkWorkerLostIfCurrentLease(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch(), reason)
	if err != nil {
		slog.Warn("Lifecycle mark-lost CAS failed.",
			"worker_id", lease.WorkerID(), "reason", reason, "origin", origin, "error", err)
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, err
	}
	if !transitioned {
		slog.Debug("Lifecycle mark-lost CAS missed.",
			"worker_id", lease.WorkerID(), "reason", reason, "origin", origin)
		// Lease-fenced CAS missed: state vs. owner_epoch vs.
		// state-restriction can't be distinguished from rowsAffected
		// alone. See TransitionOutcomeFenceMissLease.
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeFenceMissLease}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, nil
	}
	outcome := configstore.TransitionOutcome{
		Transitioned: true,
		Reason:       configstore.TransitionOutcomeTransitioned,
	}
	observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
	return outcome, nil
}

// Drain transitions a lease-owned worker into the draining state. This
// is the first step of ShutdownAll's 3-step chain; it does NOT trigger
// physical cleanup (the caller orchestrates the pod delete between
// Drain and RetireDrained). Returns Transitioned=false on a CAS miss so
// the caller can skip the remaining steps.
func (l *WorkerLifecycle) Drain(lease configstore.WorkerLease, origin LifecycleOrigin) (configstore.TransitionOutcome, error) {
	const op = LifecycleOpDrain
	start := time.Now()
	defer func() { observeLifecycleTransitionDuration(op, time.Since(start)) }()

	if l == nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, errors.New("worker lifecycle service not configured")
	}
	transitioned, err := l.store.MarkWorkerDraining(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch())
	if err != nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, err
	}
	if !transitioned {
		// MarkWorkerDraining's WHERE clause filters by both state and
		// (owner, epoch); rowsAffected==0 can mean any of them. Use
		// the generic lease-miss label.
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeFenceMissLease}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, nil
	}
	outcome := configstore.TransitionOutcome{
		Transitioned: true,
		Reason:       configstore.TransitionOutcomeTransitioned,
	}
	observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
	return outcome, nil
}

// RetireDrained is the third step of ShutdownAll's chain: it transitions
// a draining row to retired, fenced by the lease. The caller is
// responsible for the pod delete between Drain and RetireDrained; if
// that delete failed the row should be left in draining (don't call
// this method) so the orphan sweep can reconcile.
func (l *WorkerLifecycle) RetireDrained(lease configstore.WorkerLease, reason string, origin LifecycleOrigin) (configstore.TransitionOutcome, error) {
	const op = LifecycleOpRetireDrained
	start := time.Now()
	defer func() { observeLifecycleTransitionDuration(op, time.Since(start)) }()

	if l == nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, errors.New("worker lifecycle service not configured")
	}
	transitioned, err := l.store.RetireDrainingWorker(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch(), reason)
	if err != nil {
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, err
	}
	if !transitioned {
		// Lease-fenced; either the row moved out of draining (e.g.
		// orphan sweep already retired it) or the lease aged out.
		outcome := configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeFenceMissLease}
		observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
		return outcome, nil
	}
	outcome := configstore.TransitionOutcome{
		Transitioned: true,
		Reason:       configstore.TransitionOutcomeTransitioned,
	}
	observeLifecycleTransition(op, outcome.Reason, lease.Image(), origin)
	return outcome, nil
}

// RefreshLease bumps the durable owner_epoch under the current lease
// and returns a fresh lease at the new epoch. Equivalent to calling
// BumpWorkerEpoch directly; the value is the typed lease passing, not
// race avoidance — the caller still has to mirror the new epoch onto
// the in-memory worker (and the window between BumpWorkerEpoch
// returning and that mirror update is unchanged). PR 5 is where the
// in-memory-epoch race actually gets closed. Returns
// ErrWorkerOwnerEpochMismatch (via the error return) when the lease no
// longer matches the durable row.
func (l *WorkerLifecycle) RefreshLease(lease configstore.WorkerLease, origin LifecycleOrigin) (configstore.WorkerLease, error) {
	const op = LifecycleOpRefreshLease
	start := time.Now()
	defer func() { observeLifecycleTransitionDuration(op, time.Since(start)) }()

	if l == nil {
		observeLifecycleTransition(op, configstore.TransitionOutcomeStoreError, lease.Image(), origin)
		return configstore.WorkerLease{}, errors.New("worker lifecycle service not configured")
	}
	newEpoch, err := l.store.BumpWorkerEpoch(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch())
	if err != nil {
		// BumpWorkerEpoch returns ErrWorkerOwnerEpochMismatch only when
		// rowsAffected == 0 (a real lease mismatch). Any other error
		// bubbled up from GORM/the driver — DB unavailable, statement
		// timeout, etc. — is a store error. Conflating them under
		// fence_miss_owner would hide outages on dashboards.
		outcome := configstore.TransitionOutcomeStoreError
		if errors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
			outcome = configstore.TransitionOutcomeFenceMissOwner
		}
		observeLifecycleTransition(op, outcome, lease.Image(), origin)
		return configstore.WorkerLease{}, err
	}
	observeLifecycleTransition(op, configstore.TransitionOutcomeTransitioned, lease.Image(), origin)
	return configstore.NewWorkerLease(lease.WorkerID(), lease.OwnerCPInstanceID(), newEpoch, lease.Image()), nil
}

// scheduleCleanup invokes WorkerPhysicalCleanup if configured. Separated
// so every successful-CAS path goes through one helper, making it easy
// to audit "all terminal transitions trigger pod cleanup."
func (l *WorkerLifecycle) scheduleCleanup(workerID int, podName, reason string) {
	if l.cleanup == nil {
		return
	}
	l.cleanup.DeleteWorkerArtifacts(workerID, podName, reason)
}

// classifySnapshotMiss returns the default reason label for a
// snapshot-fenced CAS miss. Without an extra round-trip to read the
// current row we cannot distinguish which fence (state, owner, epoch,
// updated_at) rejected the update, so the label is intentionally
// generic. PR 6 can replace this with a follow-up GetWorkerRecord +
// field-by-field comparison when the lifecycle metrics need
// per-fence-cause dimensions.
//
// Callers that already have more information (e.g. they know the
// snapshot's observed state was not in the target's eligible set)
// override the label at their call site with a specific
// TransitionOutcomeFenceMiss* constant.
func classifySnapshotMiss(snap configstore.WorkerSnapshot) configstore.TransitionOutcomeReason {
	_ = snap
	return configstore.TransitionOutcomeFenceMissSnapshot
}
