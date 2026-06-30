// Pure, testable derivations for the Overview "Workers" + per-org load views.
//
// These live outside the component on purpose: the worker-count math has shipped
// wrong more than once (hot/idle showing the same number; a leak warning firing
// while every worker was busy), and a pure function is the only thing we can pin
// with a fast unit test. Keep all fleet/load arithmetic here, not in the JSX.

import type { FleetStat, OrgStatus } from "@/types/api";

// IDLE_LEAK_THRESHOLD is the hot-idle worker count above which the Overview
// raises the "parked workers reserving vCPU/memory" warning. Parked-idle workers
// are normal up to a point; a large persistent count is the cost/leak signal.
export const IDLE_LEAK_THRESHOLD = 20;

export interface FleetSummary {
  total: number; // every worker across all lifecycle states
  busy: number; // `hot` = holding a session
  idle: number; // `hot_idle` = parked warm, no session
  byState: Record<string, number>;
}

// summarizeFleet folds the per-(image,state,binding) fleet rows into cluster
// totals. busy and idle come from DISTINCT lifecycle states (`hot` vs
// `hot_idle`) of the SAME source, so they can never be equal-by-construction —
// the bug where idle was `hot - total_workers` (and total_workers was always 0,
// so idle collapsed onto the hot count) cannot recur here.
export function summarizeFleet(fleet: FleetStat[] | undefined): FleetSummary {
  const byState: Record<string, number> = {};
  let total = 0;
  for (const f of fleet ?? []) {
    byState[f.state] = (byState[f.state] ?? 0) + f.count;
    total += f.count;
  }
  return {
    total,
    busy: byState["hot"] ?? 0,
    idle: byState["hot_idle"] ?? 0,
    byState,
  };
}

// isIdleLeak reports whether the parked-idle worker count warrants the warning.
export function isIdleLeak(idle: number): boolean {
  return idle >= IDLE_LEAK_THRESHOLD;
}

// orgLoadPercent is the per-org load bar fill: assigned workers over the org's
// cap, clamped to [0,100]. With no cap (max_workers <= 0) the org is unbounded,
// so the bar tracks against max(workers, 1) — never divide by zero, never
// exceed 100.
export function orgLoadPercent(workers: number, maxWorkers: number): number {
  const cap = maxWorkers > 0 ? maxWorkers : Math.max(workers, 1);
  return Math.min(100, Math.max(0, (workers / cap) * 100));
}

// topOrgsByLoad sorts orgs by active sessions (desc) and takes the first n, for
// the Overview's per-org load list.
export function topOrgsByLoad(orgs: OrgStatus[] | undefined, n: number): OrgStatus[] {
  return [...(orgs ?? [])].sort((a, b) => b.active_sessions - a.active_sessions).slice(0, n);
}
