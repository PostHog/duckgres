import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { ArrowDown, ArrowUp, ArrowUpDown } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { EmptyState } from "@/components/states";
import { InfoTooltip } from "@/components/InfoTooltip";
import { OrgRef } from "@/components/OrgRef";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { fmtUnits } from "@/lib/format";
import {
  AWS_COST_TOOLTIP,
  BINARY_UNITS_NOTE,
  GIB_HOURS_TOOLTIP,
  GROSS_MARGIN_TOOLTIP,
  PRICE_AT_80_PERCENT_MARGIN_TOOLTIP,
  customerPriceTooltip,
  fmtMoney,
  orgTotals,
  priceStorageByOrg,
  type PricingRegion,
} from "@/lib/pricing";
import type { MonthlyUsageRow } from "@/types/api";

function fmtMargin(profit: number, percent: number | null): string {
  return `${percent == null ? "N/A" : `${percent.toFixed(1)}%`} (${fmtMoney(profit)} profit)`;
}

type SortKey =
  | "org"
  | "storageGiBHours"
  | "cost"
  | "price"
  | "priceAt80PercentMargin"
  | "grossMarginPercent";
type SortDirection = "asc" | "desc";

const orgCollator = new Intl.Collator("en", { numeric: true, sensitivity: "base" });

function SortableHeader({
  sortKey,
  label,
  activeKey,
  direction,
  onSort,
  tooltipLabel,
  tooltipText,
  align = "left",
}: {
  sortKey: SortKey;
  label: string;
  activeKey: SortKey;
  direction: SortDirection;
  onSort: (key: SortKey) => void;
  tooltipLabel?: string;
  tooltipText?: string;
  align?: "left" | "right";
}) {
  const active = activeKey === sortKey;
  const SortIcon = active ? (direction === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <TableHead
      className={align === "right" ? "text-right" : undefined}
      aria-sort={active ? (direction === "asc" ? "ascending" : "descending") : undefined}
    >
      <span className={`inline-flex items-center gap-1.5 ${align === "right" ? "w-full justify-end" : ""}`}>
        <button
          type="button"
          className="inline-flex items-center gap-1 rounded-sm hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          aria-label={`Sort by ${label}`}
          onClick={() => onSort(sortKey)}
        >
          <span>{label}</span>
          <SortIcon className="h-3.5 w-3.5" aria-hidden="true" />
        </button>
        {tooltipLabel && tooltipText ? <InfoTooltip label={tooltipLabel} text={tooltipText} /> : null}
      </span>
    </TableHead>
  );
}

export function UsagePricing({
  rows,
  labels,
  month,
  region,
}: {
  rows: MonthlyUsageRow[];
  labels?: Map<string, string>;
  month: string;
  region: PricingRegion;
}) {
  const [sort, setSort] = useState<{ key: SortKey; direction: SortDirection }>({
    key: "org",
    direction: "asc",
  });
  const totals = useMemo(() => orgTotals(rows), [rows]);
  const pricing = useMemo(() => priceStorageByOrg(totals, month, region), [totals, month, region]);
  const sortedRows = useMemo(() => {
    const displayOrg = (orgId: string) => labels?.get(orgId)?.trim() || orgId;
    const tieBreak = (a: (typeof pricing.rows)[number], b: (typeof pricing.rows)[number]) =>
      orgCollator.compare(displayOrg(a.orgId), displayOrg(b.orgId)) ||
      orgCollator.compare(a.orgId, b.orgId);
    return [...pricing.rows].sort((a, b) => {
      if (sort.key === "org") {
        const comparison = tieBreak(a, b);
        return sort.direction === "asc" ? comparison : -comparison;
      }
      const av = a[sort.key];
      const bv = b[sort.key];
      const aMissing = av == null || !Number.isFinite(av);
      const bMissing = bv == null || !Number.isFinite(bv);
      if (aMissing !== bMissing) return aMissing ? 1 : -1;
      if (aMissing && bMissing) return tieBreak(a, b);
      const comparison = (av as number) - (bv as number);
      return comparison === 0 ? tieBreak(a, b) : sort.direction === "asc" ? comparison : -comparison;
    });
  }, [labels, pricing.rows, sort]);

  const handleSort = (key: SortKey) => {
    setSort((current) =>
      current.key === key
        ? { key, direction: current.direction === "asc" ? "desc" : "asc" }
        : { key, direction: key === "org" ? "asc" : "desc" },
    );
  };

  return (
    <Card className="mt-4">
      <CardHeader>
        <div>
          <CardTitle>Storage economics</CardTitle>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Estimated AWS storage cost, customer price, and gross margin for retained usage in {month}.
          </p>
          <p className="mt-1 text-xs text-muted-foreground">{BINARY_UNITS_NOTE}</p>
        </div>
      </CardHeader>
      <CardContent>
        {pricing.rows.length === 0 ? (
          <EmptyState title="No usage rows" description="Pick a month with retained storage usage above to price it." />
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <SortableHeader
                  sortKey="org"
                  label="Org"
                  activeKey={sort.key}
                  direction={sort.direction}
                  onSort={handleSort}
                />
                <SortableHeader
                  sortKey="storageGiBHours"
                  label="S3 GiB·h"
                  activeKey={sort.key}
                  direction={sort.direction}
                  onSort={handleSort}
                  tooltipLabel="Explain S3 GiB·h"
                  tooltipText={GIB_HOURS_TOOLTIP}
                />
                <SortableHeader
                  sortKey="cost"
                  label="Allocated AWS cost"
                  activeKey={sort.key}
                  direction={sort.direction}
                  onSort={handleSort}
                  tooltipLabel="Explain AWS storage cost"
                  tooltipText={AWS_COST_TOOLTIP}
                  align="right"
                />
                <SortableHeader
                  sortKey="price"
                  label={`Customer price (${region})`}
                  activeKey={sort.key}
                  direction={sort.direction}
                  onSort={handleSort}
                  tooltipLabel="Explain customer price"
                  tooltipText={customerPriceTooltip(region)}
                  align="right"
                />
                <SortableHeader
                  sortKey="priceAt80PercentMargin"
                  label="Price at 80% margin"
                  activeKey={sort.key}
                  direction={sort.direction}
                  onSort={handleSort}
                  tooltipLabel="Explain price at 80% margin"
                  tooltipText={PRICE_AT_80_PERCENT_MARGIN_TOOLTIP}
                  align="right"
                />
                <SortableHeader
                  sortKey="grossMarginPercent"
                  label="Gross margin"
                  activeKey={sort.key}
                  direction={sort.direction}
                  onSort={handleSort}
                  tooltipLabel="Explain gross margin"
                  tooltipText={GROSS_MARGIN_TOOLTIP}
                  align="right"
                />
              </TableRow>
            </TableHeader>
            <TableBody>
              {sortedRows.map((row) => (
                <TableRow key={row.orgId}>
                  <TableCell>
                    <Link to={`/orgs/${encodeURIComponent(row.orgId)}`} className="block hover:underline">
                      <OrgRef id={row.orgId} label={labels?.get(row.orgId)} copyable={false} />
                    </Link>
                  </TableCell>
                  <TableCell className="font-mono text-xs">{fmtUnits(row.storageGiBHours)}</TableCell>
                  <TableCell className="text-right font-mono text-xs">{fmtMoney(row.cost)}</TableCell>
                  <TableCell className="text-right font-mono text-xs">{fmtMoney(row.price)}</TableCell>
                  <TableCell className="text-right font-mono text-xs">
                    {fmtMoney(row.priceAt80PercentMargin)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs font-medium">
                    {fmtMargin(row.grossProfit, row.grossMarginPercent)}
                  </TableCell>
                </TableRow>
              ))}
              <TableRow className="border-t-2 font-semibold">
                <TableCell className="text-xs">All orgs</TableCell>
                <TableCell className="font-mono text-xs">{fmtUnits(pricing.summary.storageGiBHours)}</TableCell>
                <TableCell className="text-right font-mono text-xs">{fmtMoney(pricing.summary.cost)}</TableCell>
                <TableCell className="text-right font-mono text-xs">{fmtMoney(pricing.summary.price)}</TableCell>
                <TableCell className="text-right font-mono text-xs">
                  {fmtMoney(pricing.summary.priceAt80PercentMargin)}
                </TableCell>
                <TableCell className="text-right font-mono text-xs">
                  {fmtMargin(pricing.summary.grossProfit, pricing.summary.grossMarginPercent)}
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}
