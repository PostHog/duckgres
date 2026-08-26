// Pricing-sensitivity logic for the Usage page's cost calculator. Pure
// functions — the feature is entirely client-side over the monthly usage
// endpoint's rows (per-team usage units per org per month), so the math here
// is the whole feature and is unit-tested directly.

import type { MonthlyUsageRow } from "@/types/api";

// PriceScenario is one named set of unit prices ("what if CPU cost X?").
// Prices are USD per usage unit, matching the units the Usage page displays.
export interface PriceScenario {
  id: string;
  name: string;
  cpuPerMin: number; // $ per CPU-minute
  memPerGiBMin: number; // $ per GiB·minute of memory
  storagePerGiBH: number; // $ per GiB·hour of S3
}

// OrgUsageTotals is one org's month totals in display units (the sums of its
// teams' rows).
export interface OrgUsageTotals {
  orgId: string;
  cpuMinutes: number;
  memGiBMinutes: number;
  storageGiBHours: number;
}

// orgTotals aggregates per-team monthly rows into one totals line per org,
// sorted by org id for a stable table.
export function orgTotals(rows: MonthlyUsageRow[]): OrgUsageTotals[] {
  const byOrg = new Map<string, OrgUsageTotals>();
  for (const r of rows) {
    let t = byOrg.get(r.org_id);
    if (!t) {
      t = { orgId: r.org_id, cpuMinutes: 0, memGiBMinutes: 0, storageGiBHours: 0 };
      byOrg.set(r.org_id, t);
    }
    t.cpuMinutes += r.cpu_seconds / 60;
    t.memGiBMinutes += r.memory_seconds / 60;
    t.storageGiBHours += Number(r.gib_seconds) / 3600;
  }
  return [...byOrg.values()].sort((a, b) => a.orgId.localeCompare(b.orgId));
}

// scenarioCost prices one org's month under one scenario.
export function scenarioCost(t: OrgUsageTotals, s: PriceScenario): number {
  return t.cpuMinutes * s.cpuPerMin + t.memGiBMinutes * s.memPerGiBMin + t.storageGiBHours * s.storagePerGiBH;
}

// Grounded defaults for a fresh scenario, so the calculator is useful on
// first open — roughly EC2 on-demand m6i/r6gd-class economics (≈$0.024/vCPU·h
// and ≈$0.006/GiB·h RAM, i.e. half the instance price attributed to each)
// plus S3 standard storage (≈$0.023/GiB·mo ≈ $0.00003/GiB·h). These are
// editable starting points for sensitivity analysis, not PostHog pricing.
export const DEFAULT_CPU_PER_MIN = 0.0004;
export const DEFAULT_MEM_PER_GIB_MIN = 0.0001;
export const DEFAULT_STORAGE_PER_GIB_H = 0.00003;

// parsePrice reads a price input: non-negative finite numbers pass through,
// anything else (empty, NaN, negative) is 0 — a half-typed input must never
// make a cost cell NaN.
export function parsePrice(raw: string): number {
  const v = Number.parseFloat(raw);
  return Number.isFinite(v) && v >= 0 ? v : 0;
}

// fmtMoney formats a USD amount with grouping and two decimals.
export function fmtMoney(n: number): string {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 });
}
