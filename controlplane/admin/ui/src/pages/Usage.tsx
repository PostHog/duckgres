import { useMemo, useState } from "react";
import { Coins, ShieldAlert } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { InfoTooltip } from "@/components/InfoTooltip";
import { StatCard } from "@/components/StatCard";
import { UsagePricing } from "@/pages/UsagePricing";
import { Card } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useIdentity } from "@/components/IdentityProvider";
import { useMonthlyUsage, useOrgLabels } from "@/hooks/useApi";
import { fmtTime } from "@/lib/format";
import {
  AWS_COST_TOOLTIP,
  GROSS_MARGIN_TOOLTIP,
  customerPriceTooltip,
  fmtMoney,
  orgTotals,
  priceStorageByOrg,
  type PricingRegion,
} from "@/lib/pricing";

function currentMonth(): string {
  const d = new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
}

export function Usage() {
  const { isAdmin } = useIdentity();
  const [months, setMonths] = useState(6);
  const [pricingRegion, setPricingRegion] = useState<PricingRegion>("US");
  const usage = useMonthlyUsage(months);
  const orgLabels = useOrgLabels();
  const rows = useMemo(() => usage.data?.rows ?? [], [usage.data]);

  // Month tabs: every month present in the data plus the current (possibly
  // still empty) month, newest first. Default to the latest month WITH data
  // so the page never opens on a blank current month.
  const monthOptions = useMemo(() => {
    const s = new Set<string>(rows.map((r) => r.month));
    s.add(currentMonth());
    return [...s].sort().reverse();
  }, [rows]);
  const [selected, setSelected] = useState<string | null>(null);
  const month =
    selected && monthOptions.includes(selected)
      ? selected
      : (monthOptions.find((m) => rows.some((r) => r.month === m)) ?? currentMonth());

  const monthRows = useMemo(() => rows.filter((r) => r.month === month), [rows, month]);
  // Storage is org-scoped. The API still carries its historical informational
  // team stamp, so collapse those rows before display rather than showing
  // duplicate-looking org lines when that stamp changed within the month.
  const orgRows = useMemo(() => orgTotals(monthRows), [monthRows]);
  const pricing = useMemo(
    () => priceStorageByOrg(orgRows, month, pricingRegion),
    [orgRows, month, pricingRegion],
  );

  // Organization cost data is admin-only (the API enforces RequireAdmin; this is
  // just the friendly notice, matching the Operators page).
  if (!isAdmin) {
    return (
      <>
        <PageHeader title="Usage" description="Monthly storage usage by organization." />
        <PageBody>
          <EmptyState
            icon={<ShieldAlert className="h-6 w-6 text-warning" />}
            title="Admin only"
            description="Organization usage and cost data requires the admin role."
          />
        </PageBody>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Usage"
        description="S3 storage-time (GiB·h) by organization, summed over the retained billing buffer."
        actions={
          <>
            <Select value={month} onValueChange={setSelected}>
              <SelectTrigger className="w-32" aria-label="Month">
                <SelectValue placeholder="Month" />
              </SelectTrigger>
              <SelectContent>
                {monthOptions.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={String(months)} onValueChange={(v) => setMonths(Number(v))}>
              <SelectTrigger className="w-36" aria-label="Window">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {[1, 3, 6, 12, 24].map((n) => (
                  <SelectItem key={n} value={String(n)}>
                    Last {n} mo
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </>
        }
      />
      <PageBody>
        {usage.data?.watermark_low && (
          <Card className="mb-4 border-warning/40 bg-warning/5 p-3 text-xs text-muted-foreground">
            Usage at or before {fmtTime(usage.data.watermark_low)} has been billed and removed from the buffer, so
            earlier months may be partial or absent. This page is an operations view, not an invoice.
          </Card>
        )}
        {usage.isError ? (
          <ErrorState error={usage.error} onRetry={() => usage.refetch()} />
        ) : usage.isLoading ? (
          <TableSkeleton />
        ) : (
          <>
            <div className="mb-4 grid grid-cols-1 gap-3 sm:grid-cols-3">
              <StatCard
                label="Total cost"
                value={fmtMoney(pricing.summary.cost)}
                icon={<InfoTooltip label="Explain AWS storage cost" text={AWS_COST_TOOLTIP} />}
                hint="AWS us-east-1 storage estimate"
              />
              <StatCard
                label="Total price"
                value={fmtMoney(pricing.summary.price)}
                icon={<InfoTooltip label="Explain customer price" text={customerPriceTooltip(pricingRegion)} />}
                hint={`${pricingRegion} progressive customer tiers`}
              />
              <StatCard
                label="Total gross margin"
                value={
                  pricing.summary.grossMarginPercent == null
                    ? "N/A"
                    : `${pricing.summary.grossMarginPercent.toFixed(1)}%`
                }
                icon={<InfoTooltip label="Explain gross margin" text={GROSS_MARGIN_TOOLTIP} />}
                hint={`${fmtMoney(pricing.summary.grossProfit)} gross profit`}
                accent={pricing.summary.grossProfit < 0 ? "destructive" : "success"}
              />
            </div>
            {monthRows.length === 0 ? (
              <EmptyState
                icon={<Coins className="h-8 w-8" />}
                title="No usage"
                description={`No usage has been recorded for ${month} in the retained billing buffer.`}
              />
            ) : (
              <UsagePricing
                rows={monthRows}
                labels={orgLabels}
                month={month}
                region={pricingRegion}
                onRegionChange={setPricingRegion}
              />
            )}
          </>
        )}
      </PageBody>
    </>
  );
}
