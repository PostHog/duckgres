package configstore

import "time"

// ObserveWorker returns a frozen snapshot of the named worker row, or nil
// if no row exists. Use this where the legacy API would have returned a
// *WorkerRecord that the caller then read fields off of and passed to a
// fenced CAS: feeding a WorkerSnapshot to a lifecycle method binds the
// observation to the subsequent transition by construction.
//
// A non-store error from the underlying read is wrapped and returned;
// (nil, nil) means "no row" exactly as GetWorkerRecord does.
func (cs *ConfigStore) ObserveWorker(workerID int) (*WorkerSnapshot, error) {
	record, err := cs.GetWorkerRecord(workerID)
	if err != nil {
		return nil, err
	}
	return newWorkerSnapshotPtr(record), nil
}

// ListOrphanedWorkerSnapshots is the snapshot-typed variant of
// ListOrphanedWorkers used by the orphan-cleanup path. Each returned
// snapshot binds the observation (state, owner, epoch, updated_at) to
// the subsequent retire CAS, so callers cannot accidentally retire a
// row that has changed under them since the listing.
func (cs *ConfigStore) ListOrphanedWorkerSnapshots(before time.Time) ([]WorkerSnapshot, error) {
	records, err := cs.ListOrphanedWorkers(before)
	if err != nil {
		return nil, err
	}
	return workerSnapshotsFromRecords(records), nil
}

// ListExpiredHotIdleSnapshots is the snapshot-typed variant of
// ListExpiredHotIdleWorkers used by the janitor's hot-idle TTL reaper.
func (cs *ConfigStore) ListExpiredHotIdleSnapshots(now time.Time, defaultTTL time.Duration) ([]WorkerSnapshot, error) {
	records, err := cs.ListExpiredHotIdleWorkers(now, defaultTTL)
	if err != nil {
		return nil, err
	}
	return workerSnapshotsFromRecords(records), nil
}

// ListExpiredHotIdleSnapshotsForCP is the snapshot-typed, owner-scoped variant
// of ListExpiredHotIdleWorkers used by the per-CP fallback reaper.
func (cs *ConfigStore) ListExpiredHotIdleSnapshotsForCP(ownerCPInstanceID string, now time.Time, defaultTTL time.Duration) ([]WorkerSnapshot, error) {
	records, err := cs.ListExpiredHotIdleWorkersForCP(ownerCPInstanceID, now, defaultTTL)
	if err != nil {
		return nil, err
	}
	return workerSnapshotsFromRecords(records), nil
}

// ListStuckWorkerSnapshots is the snapshot-typed variant of
// ListStuckWorkers used by the janitor's stuck-spawn/activate reaper.
func (cs *ConfigStore) ListStuckWorkerSnapshots(spawningBefore, activatingBefore time.Time) ([]WorkerSnapshot, error) {
	records, err := cs.ListStuckWorkers(spawningBefore, activatingBefore)
	if err != nil {
		return nil, err
	}
	return workerSnapshotsFromRecords(records), nil
}

// ListWorkerRecordSnapshotsByStatesBefore is the snapshot-typed variant
// of ListWorkerRecordsByStatesBefore. Currently unused in production but
// provided for symmetry — every WorkerRecord-returning list method has a
// snapshot-typed counterpart so PR 4 can remove the legacy variants
// without leaving observation paths dangling.
func (cs *ConfigStore) ListWorkerRecordSnapshotsByStatesBefore(states []WorkerState, updatedBefore time.Time) ([]WorkerSnapshot, error) {
	records, err := cs.ListWorkerRecordsByStatesBefore(states, updatedBefore)
	if err != nil {
		return nil, err
	}
	return workerSnapshotsFromRecords(records), nil
}

// workerSnapshotsFromRecords converts a record slice to a snapshot slice
// in one place so the wrapper methods don't each spell out the loop.
func workerSnapshotsFromRecords(records []WorkerRecord) []WorkerSnapshot {
	if len(records) == 0 {
		return nil
	}
	snaps := make([]WorkerSnapshot, len(records))
	for i, r := range records {
		snaps[i] = newWorkerSnapshot(r)
	}
	return snaps
}

// NewWorkerLease constructs a WorkerLease from explicit identity
// fields. The expected callers are paths that already hold ownership
// information out-of-band — most notably the K8sWorkerPool, whose
// in-memory ManagedWorker caches owner_cp_instance_id and owner_epoch
// from the most recent claim/takeover/refresh. The lifecycle CAS itself
// is what enforces freshness: a stale lease will simply miss.
//
// image, when supplied, is the worker image the lease was minted
// against. It is not part of the CAS fence (the lease's
// owner_cp_instance_id + owner_epoch already establish identity) — it
// is carried so the lifecycle service can label transition metrics by
// image without a separate snapshot/read. The argument is variadic to
// preserve compatibility with legacy callers that constructed leases
// before image labeling existed.
func NewWorkerLease(workerID int, ownerCPInstanceID string, ownerEpoch int64, image ...string) WorkerLease {
	leaseImage := ""
	if len(image) > 0 {
		leaseImage = image[0]
	}
	return newWorkerLease(workerID, ownerCPInstanceID, ownerEpoch, leaseImage)
}
