import { describe, expect, it } from "vitest";
import {
  IDLE_LEAK_THRESHOLD,
  isIdleLeak,
  orgLoadPercent,
  summarizeFleet,
  topOrgsByLoad,
} from "./fleet";
import type { FleetStat, OrgStatus } from "@/types/api";

// Build a FleetStat row; image/binding don't matter for the cluster rollup.
const fs = (state: string, count: number): FleetStat => ({
  image: "img",
  state,
  binding: "org_bound",
  count,
  cpu_cores: 0,
  memory_bytes: 0,
});

describe("summarizeFleet", () => {
  it("splits busy (hot) from idle (hot_idle) — distinct states, not derived from each other", () => {
    const s = summarizeFleet([fs("hot", 12), fs("hot_idle", 3), fs("spawning", 2)]);
    expect(s.busy).toBe(12);
    expect(s.idle).toBe(3);
    expect(s.total).toBe(17);
    expect(s.byState).toEqual({ hot: 12, hot_idle: 3, spawning: 2 });
  });

  // Regression: the reported prod screenshot — 150 workers all `hot`, 150
  // sessions (one per worker), 0 hot_idle. The old code computed idle as
  // `hot - total_workers` with total_workers always 0, so it showed
  // "150 hot · 150 idle" AND fired the leak warning while every worker was busy.
  it("reports 150 hot / 0 idle for a fully-busy fleet (no false leak)", () => {
    const s = summarizeFleet([fs("hot", 150)]);
    expect(s.busy).toBe(150);
    expect(s.idle).toBe(0);
    expect(s.total).toBe(150);
    expect(isIdleLeak(s.idle)).toBe(false); // the warning MUST NOT fire
  });

  it("sums duplicate states across images (rollout: two images, same state)", () => {
    const s = summarizeFleet([fs("hot", 5), fs("hot", 7), fs("hot_idle", 1)]);
    expect(s.busy).toBe(12);
    expect(s.idle).toBe(1);
    expect(s.total).toBe(13);
  });

  it("handles undefined / empty fleet", () => {
    expect(summarizeFleet(undefined)).toEqual({ total: 0, busy: 0, idle: 0, byState: {} });
    expect(summarizeFleet([])).toEqual({ total: 0, busy: 0, idle: 0, byState: {} });
  });
});

describe("isIdleLeak", () => {
  it("fires only at/above the threshold of parked hot-idle workers", () => {
    expect(isIdleLeak(0)).toBe(false);
    expect(isIdleLeak(IDLE_LEAK_THRESHOLD - 1)).toBe(false);
    expect(isIdleLeak(IDLE_LEAK_THRESHOLD)).toBe(true);
    expect(isIdleLeak(IDLE_LEAK_THRESHOLD + 100)).toBe(true);
  });

  it("is independent of busy workers (a big hot count never trips it)", () => {
    const s = summarizeFleet([fs("hot", 999), fs("hot_idle", 0)]);
    expect(isIdleLeak(s.idle)).toBe(false);
  });
});

describe("orgLoadPercent", () => {
  it("is workers/cap as a percentage", () => {
    expect(orgLoadPercent(2, 4)).toBe(50);
    expect(orgLoadPercent(4, 4)).toBe(100);
  });

  it("clamps to 100 when workers exceed the cap", () => {
    expect(orgLoadPercent(8, 4)).toBe(100);
  });

  it("never divides by zero for an unbounded org (max_workers <= 0)", () => {
    expect(orgLoadPercent(0, 0)).toBe(0); // 0 workers, unbounded → empty bar
    expect(orgLoadPercent(3, 0)).toBe(100); // any workers, unbounded → full bar
    expect(Number.isFinite(orgLoadPercent(5, 0))).toBe(true);
  });

  it("is 0 (not NaN) for a brand-new org with no workers", () => {
    expect(orgLoadPercent(0, 8)).toBe(0);
  });
});

describe("topOrgsByLoad", () => {
  const org = (name: string, sessions: number): OrgStatus => ({
    name,
    workers: 0,
    active_sessions: sessions,
    max_workers: 0,
  });

  it("sorts by active sessions desc and caps at n", () => {
    const orgs = [org("a", 1), org("b", 9), org("c", 5)];
    expect(topOrgsByLoad(orgs, 2).map((o) => o.name)).toEqual(["b", "c"]);
  });

  it("does not mutate the input array", () => {
    const orgs = [org("a", 1), org("b", 9)];
    topOrgsByLoad(orgs, 5);
    expect(orgs.map((o) => o.name)).toEqual(["a", "b"]);
  });

  it("handles undefined", () => {
    expect(topOrgsByLoad(undefined, 5)).toEqual([]);
  });
});
