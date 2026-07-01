// Pure, stable ordering for the Live tables. The API returns rows in
// non-deterministic order (per-org map iteration + the cross-CP merge), so every
// 3s poll reshuffles them and the tables flicker. A deterministic client-side
// sort with a cluster-unique tiebreaker (worker id) pins row order across
// refreshes — the same rows stay put, only genuinely new/gone rows move.

export interface StartedRow {
  started_at?: string;
  worker_id: number;
}

export interface WorkerRow {
  worker_id: number;
}

// compareByStarted orders by session start time, OLDEST first — long-lived
// sessions stay pinned at the top (where an operator watching for stuck/runaway
// sessions wants them) and a brand-new row appends at the bottom rather than
// shoving everything down. worker id is the tiebreaker so equal-or-missing
// timestamps never reorder between polls. Timestamps are UTC RFC3339, so a
// lexicographic compare is chronological; missing timestamps sort last.
export function compareByStarted(a: StartedRow, b: StartedRow): number {
  const as = a.started_at || "";
  const bs = b.started_at || "";
  if (as !== bs) {
    if (!as) return 1;
    if (!bs) return -1;
    return as < bs ? -1 : 1;
  }
  return a.worker_id - b.worker_id;
}

// compareByWorker is the stable ordering for rows with no start time (sessions):
// the cluster-unique worker id alone is a total order, enough to stop flicker.
export function compareByWorker(a: WorkerRow, b: WorkerRow): number {
  return a.worker_id - b.worker_id;
}
