package configstore

import "time"

// NewWorkerSnapshotForTesting constructs a WorkerSnapshot from explicit
// field values. This is the sanctioned escape hatch for tests in other
// packages that need to drive lifecycle code with specific observed
// fields — production code MUST go through ObserveWorker or the
// snapshot-returning List* methods, which is what makes WorkerSnapshot a
// real "freshly observed from the store" type.
//
// The "ForTesting" suffix exists so linters and reviewers can flag any
// accidental production import. It is the only externally-visible way
// to construct a WorkerSnapshot.
func NewWorkerSnapshotForTesting(workerID int, state WorkerState, ownerCPInstanceID string, ownerEpoch int64, podName string, updatedAt time.Time) WorkerSnapshot {
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	return WorkerSnapshot{record: WorkerRecord{
		WorkerID:          workerID,
		PodName:           podName,
		State:             state,
		OwnerCPInstanceID: ownerCPInstanceID,
		OwnerEpoch:        ownerEpoch,
		UpdatedAt:         updatedAt,
	}}
}
