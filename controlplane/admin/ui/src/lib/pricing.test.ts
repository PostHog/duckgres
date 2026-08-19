import { describe, expect, it } from "vitest";
import { fmtMoney, orgTotals, parsePrice, scenarioCost, type PriceScenario } from "./pricing";
import type { MonthlyUsageRow } from "@/types/api";

const ROWS: MonthlyUsageRow[] = [
  // acme: two teams in the same month → org totals must sum across teams.
  { month: "2026-08", org_id: "acme", team_id: 5, schema_name: "team_5", cpu_seconds: 7200, memory_seconds: 3600, gib_seconds: 3600 },
  { month: "2026-08", org_id: "acme", team_id: 6, schema_name: "team_6", cpu_seconds: 600, memory_seconds: 600, gib_seconds: 7200 },
  { month: "2026-08", org_id: "globex", team_id: 9, schema_name: "team_9", cpu_seconds: 1200, memory_seconds: 0, gib_seconds: 0 },
];

describe("orgTotals", () => {
  it("sums usage units per org across teams", () => {
    const totals = orgTotals(ROWS);
    expect(totals).toHaveLength(2);
    // Sorted by org id for a stable table.
    expect(totals[0].orgId).toBe("acme");
    // acme: cpu (7200+600)/60 = 130 min; mem (3600+600)/60 = 70 GiB·min; storage (3600+7200)/3600 = 3 GiB·h.
    expect(totals[0].cpuMinutes).toBeCloseTo(130);
    expect(totals[0].memGiBMinutes).toBeCloseTo(70);
    expect(totals[0].storageGiBHours).toBeCloseTo(3);
    expect(totals[1].orgId).toBe("globex");
    expect(totals[1].cpuMinutes).toBeCloseTo(20);
  });

  it("returns an empty list for no rows", () => {
    expect(orgTotals([])).toEqual([]);
  });
});

describe("scenarioCost", () => {
  it("multiplies each usage unit by its price", () => {
    const s: PriceScenario = { id: "a", name: "A", cpuPerMin: 0.1, memPerGiBMin: 0.01, storagePerGiBH: 2 };
    // acme: 130×0.1 + 70×0.01 + 3×2 = 13 + 0.7 + 6 = 19.7
    const t = orgTotals(ROWS)[0];
    expect(scenarioCost(t, s)).toBeCloseTo(19.7);
  });

  it("zero prices cost zero", () => {
    const s: PriceScenario = { id: "z", name: "Z", cpuPerMin: 0, memPerGiBMin: 0, storagePerGiBH: 0 };
    expect(scenarioCost(orgTotals(ROWS)[1], s)).toBe(0);
  });
});

describe("parsePrice", () => {
  it("accepts non-negative numbers", () => {
    expect(parsePrice("0.05")).toBeCloseTo(0.05);
    expect(parsePrice("2")).toBe(2);
    expect(parsePrice("")).toBe(0);
  });
  it("rejects NaN and negatives as 0", () => {
    expect(parsePrice("abc")).toBe(0);
    expect(parsePrice("-1")).toBe(0);
  });
});

describe("fmtMoney", () => {
  it("formats USD with two decimals", () => {
    expect(fmtMoney(19.7)).toBe("$19.70");
    expect(fmtMoney(0)).toBe("$0.00");
    expect(fmtMoney(1234567.891)).toBe("$1,234,567.89");
  });
});
