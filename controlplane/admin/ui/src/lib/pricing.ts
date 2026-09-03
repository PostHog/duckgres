// Storage pricing for the Usage page. Duckgres meters GiB-hours; both AWS
// cost and customer pricing are monthly, progressive schedules.

import type { MonthlyUsageRow } from "@/types/api";

export type PricingRegion = "US" | "EU";

export interface OrgUsageTotals {
  orgId: string;
  storageGiBHours: number;
}

export interface OrgStoragePricing extends OrgUsageTotals {
  gibMonths: number;
  cost: number;
  price: number;
  grossProfit: number;
  grossMarginPercent: number | null;
  priceAt80PercentMargin: number;
}

export interface StoragePricingSummary {
  storageGiBHours: number;
  gibMonths: number;
  cost: number;
  price: number;
  grossProfit: number;
  grossMarginPercent: number | null;
  priceAt80PercentMargin: number;
}

type StorageTier = {
  upperBoundGiBMonths: number;
  rate: number;
};

const TIB_IN_GIB = 1024;

// Each customer tier applies only to the corresponding portion of one org's
// monthly usage. Bounds follow the supplied binary convention: 1 TB = 1024 GB.
const CUSTOMER_TIERS: Record<PricingRegion, StorageTier[]> = {
  US: [
    { upperBoundGiBMonths: 100, rate: 0 },
    { upperBoundGiBMonths: 500, rate: 0.04 },
    { upperBoundGiBMonths: TIB_IN_GIB, rate: 0.035 },
    { upperBoundGiBMonths: 10 * TIB_IN_GIB, rate: 0.03 },
    { upperBoundGiBMonths: 50 * TIB_IN_GIB, rate: 0.0245 },
    { upperBoundGiBMonths: Number.POSITIVE_INFINITY, rate: 0.0235 },
  ],
  EU: [
    { upperBoundGiBMonths: 100, rate: 0 },
    { upperBoundGiBMonths: 500, rate: 0.044 },
    { upperBoundGiBMonths: TIB_IN_GIB, rate: 0.0385 },
    { upperBoundGiBMonths: 10 * TIB_IN_GIB, rate: 0.033 },
    { upperBoundGiBMonths: 50 * TIB_IN_GIB, rate: 0.027 },
    { upperBoundGiBMonths: Number.POSITIVE_INFINITY, rate: 0.026 },
  ],
};

// Public S3 Standard list rates for us-east-1, verified 2026-09-03. AWS
// applies these tiers to combined regional usage, not independently per bucket.
const AWS_US_EAST_1_TIERS: StorageTier[] = [
  { upperBoundGiBMonths: 50 * TIB_IN_GIB, rate: 0.023 },
  { upperBoundGiBMonths: 500 * TIB_IN_GIB, rate: 0.022 },
  { upperBoundGiBMonths: Number.POSITIVE_INFINITY, rate: 0.021 },
];

export const GIB_HOURS_TOOLTIP =
  "S3 GiB·h measures storage over time, not current bucket size or a transfer rate. Duckgres samples tracked DuckLake file bytes every 30 minutes. Each sample contributes tracked GiB × 0.5 hours; for example, 10 GiB stored for 24 hours is 240 GiB·h. This view includes only samples still in the retained billing buffer.";

export const AWS_COST_TOOLTIP =
  "Estimated S3 Standard storage cost at public us-east-1 rates, not an AWS invoice or CUR charge. GiB·h is divided by the number of hours in the selected UTC month to get GiB-month. AWS tiers the combined monthly usage in this view: $0.023/GiB-month for the first 50 TiB, $0.022 for the next 450 TiB, and $0.021 thereafter. Storage capacity only; excludes requests, transfer, taxes, credits, negotiated discounts, and storage outside this view. Per-org cost is allocated in proportion to usage.";

export function customerPriceTooltip(region: PricingRegion): string {
  const rates =
    region === "US"
      ? "$0.040 from 100–500 GiB, $0.035 from 500 GiB–1 TiB, $0.030 from 1–10 TiB, $0.0245 from 10–50 TiB, and $0.0235 above 50 TiB"
      : "$0.044 from 100–500 GiB, $0.0385 from 500 GiB–1 TiB, $0.033 from 1–10 TiB, $0.027 from 10–50 TiB, and $0.026 above 50 TiB";
  return `Estimated customer charge under the progressive ${region} monthly tiers. GiB·h is divided by the number of hours in the selected UTC month to get GiB-month. Each organization is priced independently, with its first 100 GiB-month free, then ${rates}. Each rate applies only to usage within that tier.`;
}

export const GROSS_MARGIN_TOOLTIP =
  "Gross margin is gross profit divided by customer price, where gross profit is customer price minus allocated AWS storage cost. The table shows both. Free-tier organizations can have negative gross profit because S3 still costs us; margin is unavailable when customer price is $0.";

export const PRICE_AT_80_PERCENT_MARGIN_TOOLTIP =
  "Price that would produce an 80% storage gross margin for this organization using its allocated estimated AWS storage cost: target price = allocated cost ÷ (1 − 0.80) = 5 × allocated cost. This is a what-if value, not the current progressive customer price, and it inherits the AWS cost estimate's exclusions and allocation assumptions. At zero allocated cost it displays $0.00; margin itself is undefined when both cost and price are zero.";

export const BINARY_UNITS_NOTE =
  "Storage follows AWS's binary convention: 1 GB = 2^30 bytes (1 GiB), and 1 TB = 2^40 bytes (1024 GB).";

export function orgTotals(rows: MonthlyUsageRow[]): OrgUsageTotals[] {
  const byOrg = new Map<string, OrgUsageTotals>();
  for (const row of rows) {
    let total = byOrg.get(row.org_id);
    if (!total) {
      total = { orgId: row.org_id, storageGiBHours: 0 };
      byOrg.set(row.org_id, total);
    }
    total.storageGiBHours += Number(row.gib_seconds) / 3600;
  }
  return [...byOrg.values()].sort((a, b) => a.orgId.localeCompare(b.orgId));
}

export function hoursInUTCMonth(month: string): number {
  const match = /^(\d{4})-(\d{2})$/.exec(month);
  if (!match) throw new Error(`invalid UTC month: ${month}`);
  const year = Number(match[1]);
  const monthIndex = Number(match[2]) - 1;
  if (monthIndex < 0 || monthIndex > 11) throw new Error(`invalid UTC month: ${month}`);
  return (Date.UTC(year, monthIndex + 1, 1) - Date.UTC(year, monthIndex, 1)) / 3_600_000;
}

export function storageGiBMonths(storageGiBHours: number, month: string): number {
  return Math.max(0, storageGiBHours) / hoursInUTCMonth(month);
}

function progressiveCharge(quantity: number, tiers: StorageTier[]): number {
  let charge = 0;
  let lowerBound = 0;
  const usage = Math.max(0, quantity);
  for (const tier of tiers) {
    const inTier = Math.min(Math.max(usage - lowerBound, 0), tier.upperBoundGiBMonths - lowerBound);
    charge += inTier * tier.rate;
    lowerBound = tier.upperBoundGiBMonths;
    if (usage <= lowerBound) break;
  }
  return charge;
}

export function customerStoragePrice(gibMonths: number, region: PricingRegion): number {
  return progressiveCharge(gibMonths, CUSTOMER_TIERS[region]);
}

function awsStorageCost(gibMonths: number): number {
  return progressiveCharge(gibMonths, AWS_US_EAST_1_TIERS);
}

export function priceStorageByOrg(
  totals: OrgUsageTotals[],
  month: string,
  region: PricingRegion,
): { rows: OrgStoragePricing[]; summary: StoragePricingSummary } {
  const withMonths = totals.map((total) => ({ ...total, gibMonths: storageGiBMonths(total.storageGiBHours, month) }));
  const totalGiBMonths = withMonths.reduce((sum, row) => sum + row.gibMonths, 0);
  const totalCost = awsStorageCost(totalGiBMonths);

  const rows = withMonths.map((row): OrgStoragePricing => {
    // AWS tiers combined regional usage. Allocate that aggregate list cost by
    // metered usage so per-org cost and margin rows add exactly to the total.
    const cost = totalGiBMonths === 0 ? 0 : totalCost * (row.gibMonths / totalGiBMonths);
    const price = customerStoragePrice(row.gibMonths, region);
    const grossProfit = price - cost;
    return {
      ...row,
      cost,
      price,
      grossProfit,
      grossMarginPercent: price > 0 ? (grossProfit / price) * 100 : null,
      priceAt80PercentMargin: cost / (1 - 0.8),
    };
  });

  const storageGiBHours = totals.reduce((sum, row) => sum + row.storageGiBHours, 0);
  const price = rows.reduce((sum, row) => sum + row.price, 0);
  const grossProfit = price - totalCost;
  return {
    rows,
    summary: {
      storageGiBHours,
      gibMonths: totalGiBMonths,
      cost: totalCost,
      price,
      grossProfit,
      grossMarginPercent: price > 0 ? (grossProfit / price) * 100 : null,
      priceAt80PercentMargin: totalCost / (1 - 0.8),
    },
  };
}

export function fmtMoney(value: number): string {
  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}
