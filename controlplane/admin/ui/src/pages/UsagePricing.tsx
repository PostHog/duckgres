import { useMemo, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
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
  customerPriceTooltip,
  fmtMoney,
  orgTotals,
  priceStorageByOrg,
  type PricingRegion,
} from "@/lib/pricing";
import type { MonthlyUsageRow } from "@/types/api";

function HeaderWithTooltip({
  children,
  label,
  text,
}: {
  children: ReactNode;
  label: string;
  text: string;
}) {
  return (
    <span className="inline-flex items-center gap-1.5">
      {children}
      <InfoTooltip label={label} text={text} />
    </span>
  );
}

function fmtMargin(profit: number, percent: number | null): string {
  return `${percent == null ? "N/A" : `${percent.toFixed(1)}%`} (${fmtMoney(profit)} profit)`;
}

export function UsagePricing({
  rows,
  labels,
  month,
  region,
  onRegionChange,
}: {
  rows: MonthlyUsageRow[];
  labels?: Map<string, string>;
  month: string;
  region: PricingRegion;
  onRegionChange: (region: PricingRegion) => void;
}) {
  const totals = useMemo(() => orgTotals(rows), [rows]);
  const pricing = useMemo(() => priceStorageByOrg(totals, month, region), [totals, month, region]);

  return (
    <Card className="mt-4">
      <CardHeader className="flex-row items-start justify-between gap-3">
        <div>
          <CardTitle>Storage economics</CardTitle>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Estimated AWS storage cost, customer price, and gross margin for retained usage in {month}.
          </p>
          <p className="mt-1 text-xs text-muted-foreground">{BINARY_UNITS_NOTE}</p>
        </div>
        <div className="flex shrink-0 items-center gap-1" aria-label="Customer pricing region">
          <span className="mr-1 text-xs text-muted-foreground">Customer pricing</span>
          {(["US", "EU"] as const).map((value) => (
            <Button
              key={value}
              size="sm"
              variant={region === value ? "secondary" : "ghost"}
              className="h-7 px-2 text-xs"
              aria-label={`${value} pricing`}
              onClick={() => onRegionChange(value)}
            >
              {value}
            </Button>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        {pricing.rows.length === 0 ? (
          <EmptyState title="No usage rows" description="Pick a month with retained storage usage above to price it." />
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>Org</TableHead>
                <TableHead>
                  <HeaderWithTooltip label="Explain S3 GiB·h" text={GIB_HOURS_TOOLTIP}>
                    S3 GiB·h
                  </HeaderWithTooltip>
                </TableHead>
                <TableHead className="text-right">
                  <HeaderWithTooltip label="Explain AWS storage cost" text={AWS_COST_TOOLTIP}>
                    Allocated AWS cost
                  </HeaderWithTooltip>
                </TableHead>
                <TableHead className="text-right">
                  <HeaderWithTooltip label="Explain customer price" text={customerPriceTooltip(region)}>
                    Customer price ({region})
                  </HeaderWithTooltip>
                </TableHead>
                <TableHead className="text-right">
                  <HeaderWithTooltip label="Explain gross margin" text={GROSS_MARGIN_TOOLTIP}>
                    Gross margin
                  </HeaderWithTooltip>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {pricing.rows.map((row) => (
                <TableRow key={row.orgId}>
                  <TableCell>
                    <Link to={`/orgs/${encodeURIComponent(row.orgId)}`} className="block hover:underline">
                      <OrgRef id={row.orgId} label={labels?.get(row.orgId)} copyable={false} />
                    </Link>
                  </TableCell>
                  <TableCell className="font-mono text-xs">{fmtUnits(row.storageGiBHours)}</TableCell>
                  <TableCell className="text-right font-mono text-xs">{fmtMoney(row.cost)}</TableCell>
                  <TableCell className="text-right font-mono text-xs">{fmtMoney(row.price)}</TableCell>
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
