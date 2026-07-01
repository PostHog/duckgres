import { describe, expect, it } from "vitest";
import { compareByStarted, compareByWorker } from "./liveSort";

describe("compareByStarted", () => {
  it("orders oldest start first", () => {
    const rows = [
      { started_at: "2026-07-01T10:00:02Z", worker_id: 2 },
      { started_at: "2026-07-01T10:00:01Z", worker_id: 1 },
      { started_at: "2026-07-01T10:00:03Z", worker_id: 3 },
    ];
    expect([...rows].sort(compareByStarted).map((r) => r.worker_id)).toEqual([1, 2, 3]);
  });

  it("is stable across reshuffled input (kills flicker)", () => {
    const a = { started_at: "2026-07-01T10:00:01Z", worker_id: 5 };
    const b = { started_at: "2026-07-01T10:00:02Z", worker_id: 9 };
    const c = { started_at: "2026-07-01T10:00:03Z", worker_id: 1 };
    // Any input permutation yields the same output order.
    const want = [5, 9, 1];
    expect([a, b, c].sort(compareByStarted).map((r) => r.worker_id)).toEqual(want);
    expect([c, a, b].sort(compareByStarted).map((r) => r.worker_id)).toEqual(want);
    expect([b, c, a].sort(compareByStarted).map((r) => r.worker_id)).toEqual(want);
  });

  it("breaks ties on worker id so equal timestamps never reorder", () => {
    const t = "2026-07-01T10:00:00Z";
    const rows = [
      { started_at: t, worker_id: 30 },
      { started_at: t, worker_id: 10 },
      { started_at: t, worker_id: 20 },
    ];
    expect([...rows].sort(compareByStarted).map((r) => r.worker_id)).toEqual([10, 20, 30]);
  });

  it("sorts missing timestamps last (still deterministic)", () => {
    const rows = [
      { worker_id: 7 },
      { started_at: "2026-07-01T10:00:01Z", worker_id: 2 },
      { worker_id: 3 },
    ];
    expect([...rows].sort(compareByStarted).map((r) => r.worker_id)).toEqual([2, 3, 7]);
  });
});

describe("compareByWorker", () => {
  it("orders by cluster-unique worker id", () => {
    const rows = [{ worker_id: 30 }, { worker_id: 10 }, { worker_id: 20 }];
    expect([...rows].sort(compareByWorker).map((r) => r.worker_id)).toEqual([10, 20, 30]);
  });
});
