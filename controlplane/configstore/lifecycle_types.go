package configstore

import "time"

// WorkerSnapshot is an opaque, point-in-time observation of a worker row,
// captured by a ConfigStore read (ObserveWorker, ListOrphanedWorkerSnapshots,
// etc.). It is the only argument accepted by lifecycle transitions that
// operate on an *observed* row — i.e. paths where the caller did not just
// take ownership, but rather discovered a row by listing and now wants to
// move it to a terminal state.
//
// The unexported field means callers outside this package cannot fabricate
// a snapshot from a WorkerRecord. The only way to get one is to ask the
// store for it, which guarantees the snapshot reflects a real durable read
// rather than a value the caller invented or mutated.
//
// Snapshots are immutable: there are no setters and the inner record is
// returned by value. A snapshot whose underlying row has since changed
// will cause downstream CAS attempts to miss, which is the desired
// behavior — the snapshot is the fence.
type WorkerSnapshot struct {
	record WorkerRecord
}

// newWorkerSnapshot wraps a WorkerRecord into a snapshot. Package-private
// so only configstore methods can construct snapshots.
func newWorkerSnapshot(record WorkerRecord) WorkerSnapshot {
	return WorkerSnapshot{record: record}
}

// newWorkerSnapshotPtr wraps a *WorkerRecord into a *WorkerSnapshot, or
// returns nil if the record is nil. Package-private companion to
// newWorkerSnapshot for the (*WorkerRecord, error) → (*WorkerSnapshot, error)
// store methods.
func newWorkerSnapshotPtr(record *WorkerRecord) *WorkerSnapshot {
	if record == nil {
		return nil
	}
	snap := newWorkerSnapshot(*record)
	return &snap
}

// WorkerID returns the worker id this snapshot was captured for.
func (s WorkerSnapshot) WorkerID() int { return s.record.WorkerID }

// State returns the worker's state at observation time.
func (s WorkerSnapshot) State() WorkerState { return s.record.State }

// PodName returns the K8s pod name recorded for this worker.
func (s WorkerSnapshot) PodName() string { return s.record.PodName }

// Image returns the container image recorded for this worker.
func (s WorkerSnapshot) Image() string { return s.record.Image }

// OrgID returns the org id the worker was assigned to at observation time.
// Empty for neutral warm workers.
func (s WorkerSnapshot) OrgID() string { return s.record.OrgID }

// OwnerCPInstanceID returns the control-plane id that owned the worker at
// observation time. Empty for unowned rows.
func (s WorkerSnapshot) OwnerCPInstanceID() string { return s.record.OwnerCPInstanceID }

// OwnerEpoch returns the owner epoch at observation time.
func (s WorkerSnapshot) OwnerEpoch() int64 { return s.record.OwnerEpoch }

// UpdatedAt returns the row's updated_at timestamp at observation time.
func (s WorkerSnapshot) UpdatedAt() time.Time { return s.record.UpdatedAt }

// Record returns a copy of the underlying WorkerRecord. Provided for
// callers that still need to thread the record through legacy APIs during
// the lifecycle migration; new code should rely on the typed accessors.
func (s WorkerSnapshot) Record() WorkerRecord { return s.record }

// recordPtr is the package-internal accessor used by lifecycle CAS methods
// that take a *WorkerRecord. Keeps the unexported field truly unexported
// outside this package while still letting in-package CAS code use it.
func (s WorkerSnapshot) recordPtr() *WorkerRecord {
	rec := s.record
	return &rec
}

// WorkerLease is an opaque proof that the holder currently owns a worker
// row at a specific epoch. It is the only argument accepted by lifecycle
// transitions that act on a row the caller already owns — Drain on
// shutdown, MarkLost from the health checker, RefreshLease from the
// credential-refresh scheduler.
//
// Leases are produced by the store methods that establish ownership:
// ClaimIdleWorker, ClaimHotIdleWorker, TakeOverWorker, the Create*Slot
// variants, and RefreshLease. Outside this package they cannot be
// constructed by hand, which means a caller without a real lease cannot
// invoke the lease-only lifecycle methods.
type WorkerLease struct {
	workerID          int
	ownerCPInstanceID string
	ownerEpoch        int64
}

// newWorkerLease constructs a WorkerLease. Package-private so leases can
// only be minted by store methods that have actually established
// ownership.
func newWorkerLease(workerID int, ownerCPInstanceID string, ownerEpoch int64) WorkerLease {
	return WorkerLease{
		workerID:          workerID,
		ownerCPInstanceID: ownerCPInstanceID,
		ownerEpoch:        ownerEpoch,
	}
}

// newWorkerLeaseFromRecord constructs a lease from a record whose
// ownership fields are trusted (i.e. just-returned by a claim or takeover
// inside this package). Returns nil if the record is nil.
func newWorkerLeaseFromRecord(record *WorkerRecord) *WorkerLease {
	if record == nil {
		return nil
	}
	lease := newWorkerLease(record.WorkerID, record.OwnerCPInstanceID, record.OwnerEpoch)
	return &lease
}

// WorkerID returns the worker id this lease is for.
func (l WorkerLease) WorkerID() int { return l.workerID }

// OwnerCPInstanceID returns the control-plane id that owns the worker
// under this lease.
func (l WorkerLease) OwnerCPInstanceID() string { return l.ownerCPInstanceID }

// OwnerEpoch returns the epoch this lease was minted at. Subsequent
// epoch-bumping operations (e.g. RefreshLease) produce a new lease; the
// previous one becomes stale and any CAS attempted with it will miss.
func (l WorkerLease) OwnerEpoch() int64 { return l.ownerEpoch }

// TransitionOutcomeReason classifies why a lifecycle transition did or
// did not happen. The values are stable and meant for telemetry — PR 6
// will hang per-image metrics off these labels.
type TransitionOutcomeReason string

const (
	// TransitionOutcomeTransitioned indicates the durable CAS landed and
	// the row moved to the requested target state.
	TransitionOutcomeTransitioned TransitionOutcomeReason = "transitioned"

	// TransitionOutcomeFenceMissState indicates the CAS missed because
	// the observed state no longer matched the durable row.
	TransitionOutcomeFenceMissState TransitionOutcomeReason = "fence_miss_state"

	// TransitionOutcomeFenceMissOwner indicates the CAS missed because
	// the durable owner_cp_instance_id no longer matched.
	TransitionOutcomeFenceMissOwner TransitionOutcomeReason = "fence_miss_owner"

	// TransitionOutcomeFenceMissEpoch indicates the CAS missed because
	// the durable owner_epoch no longer matched.
	TransitionOutcomeFenceMissEpoch TransitionOutcomeReason = "fence_miss_epoch"

	// TransitionOutcomeFenceMissUpdatedAt indicates the CAS missed
	// because the durable updated_at had advanced past the snapshot's
	// observed value (the row was touched after listing).
	TransitionOutcomeFenceMissUpdatedAt TransitionOutcomeReason = "fence_miss_updated_at"

	// TransitionOutcomeFenceMissCPRevived indicates the orphan CAS missed
	// because the supposed-orphan's owner CP was no longer expired.
	TransitionOutcomeFenceMissCPRevived TransitionOutcomeReason = "fence_miss_cp_revived"

	// TransitionOutcomeRowMissing indicates the row could not be found in
	// the runtime store (hard-deleted or never created).
	TransitionOutcomeRowMissing TransitionOutcomeReason = "row_missing"

	// TransitionOutcomeStoreError indicates the underlying CAS query
	// returned a database error. The caller's err return carries the
	// underlying cause.
	TransitionOutcomeStoreError TransitionOutcomeReason = "store_error"
)

// TransitionOutcome is the result of a lifecycle transition attempt. It
// distinguishes "the durable CAS landed" from "the physical cleanup
// (pod/secret delete) also completed", and surfaces a stable reason
// string so callers don't have to interpret bool returns.
type TransitionOutcome struct {
	// Transitioned is true when the durable CAS landed and the worker
	// row reached the target terminal/intermediate state.
	Transitioned bool

	// PhysicalCleanupScheduled is true when the post-CAS pod/secret/local
	// cleanup was kicked off. Cleanup is fire-and-forget today, so this
	// only reports that it was scheduled, not that it completed.
	PhysicalCleanupScheduled bool

	// Reason is a stable label classifying the outcome — useful for both
	// log structured fields and per-image metric labels.
	Reason TransitionOutcomeReason
}
