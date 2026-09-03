import { describe, expect, it } from "vitest";
import {
  customerStoragePrice,
  fmtMoney,
  hoursInUTCMonth,
  priceStorageByOrg,
  storageGiBMonths,
  type OrgUsageTotals,
} from "./pricing";

describe("storage pricing", () => {
  it("converts GiB-hours using the actual UTC calendar month", () => {
    expect(hoursInUTCMonth("2026-02")).toBe(672);
    expect(hoursInUTCMonth("2028-02")).toBe(696);
    expect(hoursInUTCMonth("2026-08")).toBe(744);
    expect(storageGiBMonths(600 * 744, "2026-08")).toBe(600);
  });

  it("applies the customer US tiers progressively with binary TiB boundaries", () => {
    expect(customerStoragePrice(100, "US")).toBe(0);
    expect(customerStoragePrice(600, "US")).toBeCloseTo(19.5);
    // The supplied definition says 1 TiB = 1024 GiB. Consequently 37,200
    // GiB prices to $971.34; $969.90 would use contradictory decimal-TB cuts.
    expect(customerStoragePrice(37_200, "US")).toBeCloseTo(971.34);
  });

  it("applies the EU rates to the same progressive boundaries", () => {
    expect(customerStoragePrice(600, "EU")).toBeCloseTo(21.45);
  });

  it.each([
    ["US", 100, 0.04],
    ["US", 500, 0.035],
    ["US", 1_024, 0.03],
    ["US", 10_240, 0.0245],
    ["US", 51_200, 0.0235],
    ["EU", 100, 0.044],
    ["EU", 500, 0.0385],
    ["EU", 1_024, 0.033],
    ["EU", 10_240, 0.027],
    ["EU", 51_200, 0.026],
  ] as const)("uses the %s marginal rate immediately above %d GiB-month", (region, boundary, rate) => {
    expect(customerStoragePrice(boundary + 1, region) - customerStoragePrice(boundary, region)).toBeCloseTo(rate);
  });

  it("uses aggregate us-east-1 S3 tiers and allocates cost back to orgs", () => {
    const totals: OrgUsageTotals[] = [
      { orgId: "org-a", storageGiBHours: 40_000 * 744 },
      { orgId: "org-b", storageGiBHours: 20_000 * 744 },
    ];
    const priced = priceStorageByOrg(totals, "2026-08", "US");
    // Aggregate cost: first 50 TiB (51,200 GiB) at .023, remaining 8,800 at .022.
    expect(priced.summary.cost).toBeCloseTo(51_200 * 0.023 + 8_800 * 0.022);
    expect(priced.rows[0].cost + priced.rows[1].cost).toBeCloseTo(priced.summary.cost);
    expect(priced.rows[0].cost / priced.rows[1].cost).toBeCloseTo(2);
  });

  it.each([
    [51_200, 0.022],
    [512_000, 0.021],
  ])("uses the next AWS marginal rate above %d aggregate GiB-month", (boundary, rate) => {
    const costAt = (gibMonths: number) =>
      priceStorageByOrg([{ orgId: "org-a", storageGiBHours: gibMonths * 744 }], "2026-08", "US").summary.cost;
    expect(costAt(boundary + 1) - costAt(boundary)).toBeCloseTo(rate);
  });

  it("prices each customer separately, then sums price, gross profit, and margin", () => {
    const totals: OrgUsageTotals[] = [
      { orgId: "org-a", storageGiBHours: 600 * 744 },
      { orgId: "org-b", storageGiBHours: 50 * 744 },
    ];
    const priced = priceStorageByOrg(totals, "2026-08", "US");

    expect(priced.rows[0]).toMatchObject({ orgId: "org-a", gibMonths: 600 });
    expect(priced.rows[0].cost).toBeCloseTo(13.8);
    expect(priced.rows[0].price).toBeCloseTo(19.5);
    expect(priced.rows[0].grossProfit).toBeCloseTo(5.7);
    expect(priced.rows[0].grossMarginPercent).toBeCloseTo(29.2307);

    expect(priced.rows[1].cost).toBeCloseTo(1.15);
    expect(priced.rows[1].price).toBe(0);
    expect(priced.rows[1].grossProfit).toBeCloseTo(-1.15);
    expect(priced.rows[1].grossMarginPercent).toBeNull();

    expect(priced.summary.cost).toBeCloseTo(14.95);
    expect(priced.summary.price).toBeCloseTo(19.5);
    expect(priced.summary.grossProfit).toBeCloseTo(4.55);
    expect(priced.summary.grossMarginPercent).toBeCloseTo(23.3333);
  });
});

describe("fmtMoney", () => {
  it("formats USD with two decimals", () => {
    expect(fmtMoney(19.7)).toBe("$19.70");
    expect(fmtMoney(-1.15)).toBe("-$1.15");
    expect(fmtMoney(1234567.891)).toBe("$1,234,567.89");
  });
});
