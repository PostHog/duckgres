package configstore

import "time"

// NewWorkerSnapshotForTesting wraps a WorkerRecord into a WorkerSnapshot.
// This is the sanctioned escape hatch for tests in other packages that
// need to drive lifecycle code with specific observed fields — production
// code MUST go through ObserveWorker or the snapshot-returning List*
// methods, which is what makes WorkerSnapshot a real "freshly observed
// from the store" type.
//
// The "ForTesting" suffix exists so linters and reviewers can flag any
// accidental production import. It is the only externally-visible way
// to construct a WorkerSnapshot.
func NewWorkerSnapshotForTesting(record WorkerRecord) WorkerSnapshot {
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = time.Now()
	}
	return WorkerSnapshot{record: record}
}
