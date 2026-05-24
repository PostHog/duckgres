package configstore

// NewWorkerSnapshot wraps a WorkerRecord into a WorkerSnapshot. The
// preferred way to obtain a snapshot is still through ObserveWorker or
// the snapshot-returning List* methods — those guarantee the snapshot
// reflects a real durable read at a specific instant. This constructor
// exists for two specific call sites:
//
//   - Production callers that already hold a freshly-issued record
//     from a CAS-producing store call (Claim*/TakeOver/Create*Slot).
//     The record is as fresh as a fresh observation would have been,
//     and an extra ObserveWorker round-trip just to wrap it would burn
//     a query.
//
//   - Tests in other packages that need to drive lifecycle code with
//     specific observed-field values, without standing up a real
//     ConfigStore.
//
// Callers passing in a stale record will simply CAS-miss downstream
// because the snapshot's owner_cp_instance_id / owner_epoch / state
// won't match. A zero UpdatedAt is preserved verbatim and means
// "no updated_at fence" — MarkWorkerTerminalIfCurrent only adds the
// updated_at predicate when the snapshot's UpdatedAt is non-zero, so
// callers without a meaningful updated_at observation can pass it
// through without polluting the fence.
//
// The compiler distinction between WorkerSnapshot and WorkerLease is
// the load-bearing protection here.
func NewWorkerSnapshot(record WorkerRecord) WorkerSnapshot {
	return WorkerSnapshot{record: record}
}
