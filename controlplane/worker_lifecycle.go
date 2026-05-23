package controlplane

import (
	"errors"
	"fmt"
	"log/slog"

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
// own the worker but observed a separate snapshot (e.g.
// retireCurrentRuntimeWorker before slot retirement) — the broad
// MarkWorkerTerminalIfCurrent fence ensures we don't trample a row that
// has been taken over since the snapshot was captured.
func (l *WorkerLifecycle) RetireFromSnapshot(snap configstore.WorkerSnapshot, target configstore.WorkerState, reason string) (configstore.TransitionOutcome, error) {
	if l == nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, errors.New("worker lifecycle service not configured")
	}
	if target != configstore.WorkerStateRetired && target != configstore.WorkerStateLost {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, fmt.Errorf("worker %d unsupported terminal state %q", snap.WorkerID(), target)
	}

	record := snap.Record()
	transitioned, err := l.store.MarkWorkerTerminalIfCurrent(&record, target, reason)
	if err != nil {
		slog.Warn("Lifecycle retire CAS failed.",
			"worker_id", snap.WorkerID(), "target", target, "reason", reason, "error", err)
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, err
	}
	if !transitioned {
		slog.Debug("Lifecycle retire CAS missed; pod cleanup skipped.",
			"worker_id", snap.WorkerID(), "target", target, "reason", reason)
		return configstore.TransitionOutcome{Reason: classifySnapshotMiss(snap)}, nil
	}
	l.scheduleCleanup(snap.WorkerID(), snap.PodName(), reason)
	return configstore.TransitionOutcome{
		Transitioned:             true,
		PhysicalCleanupScheduled: l.cleanup != nil,
		Reason:                   configstore.TransitionOutcomeTransitioned,
	}, nil
}

// RetireOrphanFromSnapshot is the orphan-specific retire: it permits any
// active observed state (spawning through draining) and additionally
// enforces that the worker's owner control-plane is still expired (or
// absent). Used by the janitor's orphan-cleanup loop where the listing
// step already filtered by owner-expired-or-missing, so the snapshot's
// CP must remain so for the retire to be safe.
func (l *WorkerLifecycle) RetireOrphanFromSnapshot(snap configstore.WorkerSnapshot, reason string) (configstore.TransitionOutcome, error) {
	if l == nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, errors.New("worker lifecycle service not configured")
	}
	record := snap.Record()
	transitioned, err := l.store.RetireOrphanWorker(&record, reason)
	if err != nil {
		slog.Warn("Lifecycle orphan retire CAS failed.",
			"worker_id", snap.WorkerID(), "reason", reason, "error", err)
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, err
	}
	if !transitioned {
		// Could be a regular snapshot miss or the CP-revival fence;
		// classifySnapshotMiss handles the former, and we layer the
		// orphan-specific case on top.
		miss := classifySnapshotMiss(snap)
		if miss == configstore.TransitionOutcomeFenceMissOwner && snap.OwnerCPInstanceID() != "" {
			miss = configstore.TransitionOutcomeFenceMissCPRevived
		}
		slog.Debug("Lifecycle orphan retire CAS missed; pod cleanup skipped.",
			"worker_id", snap.WorkerID(), "reason", reason, "miss", miss)
		return configstore.TransitionOutcome{Reason: miss}, nil
	}
	l.scheduleCleanup(snap.WorkerID(), snap.PodName(), reason)
	return configstore.TransitionOutcome{
		Transitioned:             true,
		PhysicalCleanupScheduled: l.cleanup != nil,
		Reason:                   configstore.TransitionOutcomeTransitioned,
	}, nil
}

// RetireIdleVariantFromSnapshot retires a worker observed as idle or
// hot_idle, fenced by the snapshot. Used by the version-aware reaper
// and the hot-idle TTL reaper, both of which only act on workers in
// those two states.
func (l *WorkerLifecycle) RetireIdleVariantFromSnapshot(snap configstore.WorkerSnapshot, reason string) (configstore.TransitionOutcome, error) {
	if l == nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, errors.New("worker lifecycle service not configured")
	}
	record := snap.Record()
	transitioned, err := l.store.RetireIdleOrHotIdleWorker(&record, reason)
	if err != nil {
		slog.Warn("Lifecycle idle-variant retire CAS failed.",
			"worker_id", snap.WorkerID(), "reason", reason, "error", err)
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, err
	}
	if !transitioned {
		miss := classifySnapshotMiss(snap)
		// If the snapshot was not idle/hot_idle to begin with, that's a
		// state-restriction miss rather than an ownership change.
		if snap.State() != configstore.WorkerStateIdle && snap.State() != configstore.WorkerStateHotIdle {
			miss = configstore.TransitionOutcomeFenceMissState
		}
		slog.Debug("Lifecycle idle-variant retire CAS missed; pod cleanup skipped.",
			"worker_id", snap.WorkerID(), "reason", reason, "miss", miss)
		return configstore.TransitionOutcome{Reason: miss}, nil
	}
	l.scheduleCleanup(snap.WorkerID(), snap.PodName(), reason)
	return configstore.TransitionOutcome{
		Transitioned:             true,
		PhysicalCleanupScheduled: l.cleanup != nil,
		Reason:                   configstore.TransitionOutcomeTransitioned,
	}, nil
}

// MarkLostFromLease marks a lease-owned worker as lost. Used by the
// health checker after it has confirmed the worker is unresponsive.
// Triggers pod cleanup on success.
func (l *WorkerLifecycle) MarkLostFromLease(lease configstore.WorkerLease, podName, reason string) (configstore.TransitionOutcome, error) {
	if l == nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, errors.New("worker lifecycle service not configured")
	}
	transitioned, err := l.store.MarkWorkerLostIfCurrentLease(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch(), reason)
	if err != nil {
		slog.Warn("Lifecycle mark-lost CAS failed.",
			"worker_id", lease.WorkerID(), "reason", reason, "error", err)
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, err
	}
	if !transitioned {
		slog.Debug("Lifecycle mark-lost CAS missed; pod cleanup skipped.",
			"worker_id", lease.WorkerID(), "reason", reason)
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeFenceMissOwner}, nil
	}
	l.scheduleCleanup(lease.WorkerID(), podName, reason)
	return configstore.TransitionOutcome{
		Transitioned:             true,
		PhysicalCleanupScheduled: l.cleanup != nil,
		Reason:                   configstore.TransitionOutcomeTransitioned,
	}, nil
}

// Drain transitions a lease-owned worker into the draining state. This
// is the first step of ShutdownAll's 3-step chain; it does NOT trigger
// physical cleanup (the caller orchestrates the pod delete between
// Drain and RetireDrained). Returns Transitioned=false on a CAS miss so
// the caller can skip the remaining steps.
func (l *WorkerLifecycle) Drain(lease configstore.WorkerLease) (configstore.TransitionOutcome, error) {
	if l == nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, errors.New("worker lifecycle service not configured")
	}
	transitioned, err := l.store.MarkWorkerDraining(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch())
	if err != nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, err
	}
	if !transitioned {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeFenceMissOwner}, nil
	}
	return configstore.TransitionOutcome{
		Transitioned: true,
		Reason:       configstore.TransitionOutcomeTransitioned,
	}, nil
}

// RetireDrained is the third step of ShutdownAll's chain: it transitions
// a draining row to retired, fenced by the lease. The caller is
// responsible for the pod delete between Drain and RetireDrained; if
// that delete failed the row should be left in draining (don't call
// this method) so the orphan sweep can reconcile.
func (l *WorkerLifecycle) RetireDrained(lease configstore.WorkerLease, reason string) (configstore.TransitionOutcome, error) {
	if l == nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, errors.New("worker lifecycle service not configured")
	}
	transitioned, err := l.store.RetireDrainingWorker(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch(), reason)
	if err != nil {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeStoreError}, err
	}
	if !transitioned {
		return configstore.TransitionOutcome{Reason: configstore.TransitionOutcomeFenceMissOwner}, nil
	}
	return configstore.TransitionOutcome{
		Transitioned: true,
		Reason:       configstore.TransitionOutcomeTransitioned,
	}, nil
}

// RefreshLease bumps the durable owner_epoch under the current lease
// and returns the new lease. Replaces the previous two-step pattern of
// BumpWorkerEpoch + worker.SetOwnerEpoch where a concurrent ShutdownAll
// could observe a stale in-memory epoch between the two operations.
// Returns ErrWorkerOwnerEpochMismatch (via the error return) when the
// lease no longer matches the durable row.
func (l *WorkerLifecycle) RefreshLease(lease configstore.WorkerLease) (configstore.WorkerLease, error) {
	if l == nil {
		return configstore.WorkerLease{}, errors.New("worker lifecycle service not configured")
	}
	newEpoch, err := l.store.BumpWorkerEpoch(lease.WorkerID(), lease.OwnerCPInstanceID(), lease.OwnerEpoch())
	if err != nil {
		return configstore.WorkerLease{}, err
	}
	return configstore.NewWorkerLease(lease.WorkerID(), lease.OwnerCPInstanceID(), newEpoch), nil
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
// generic — PR 6 can add a follow-up read for better classification if
// the lifecycle metrics need finer-grained miss accounting.
//
// Variants that have more information available (e.g. the orphan-path
// CP-revival case, or callers that already know their snapshot was
// constructed with a non-eligible state) refine the label at their
// call site.
func classifySnapshotMiss(snap configstore.WorkerSnapshot) configstore.TransitionOutcomeReason {
	_ = snap
	return configstore.TransitionOutcomeFenceMissOwner
}
