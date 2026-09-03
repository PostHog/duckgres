import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Tooltip as RTooltip, ResponsiveContainer, XAxis, YAxis } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { InfoTooltip } from "@/components/InfoTooltip";
import { useIdentity } from "@/components/IdentityProvider";
import { useOrgDailyUsage } from "@/hooks/useApi";
import { fmtTime, fmtUnits } from "@/lib/format";
import { GIB_HOURS_TOOLTIP } from "@/lib/pricing";
import { cn } from "@/lib/utils";
import type { DailyUsageRow } from "@/types/api";

type PeriodKey = "7d" | "14d" | "30d" | "wtd" | "mtd";

function periodDays(period: PeriodKey, now = new Date()): number {
  if (period === "wtd") {
    const weekday = now.getUTCDay();
    return weekday === 0 ? 7 : weekday;
  }
  if (period === "mtd") return now.getUTCDate();
  return Number.parseInt(period, 10);
}

const PERIODS: { key: PeriodKey; label: string }[] = [
  { key: "7d", label: "7d" },
  { key: "14d", label: "14d" },
  { key: "30d", label: "30d" },
  { key: "wtd", label: "WTD" },
  { key: "mtd", label: "MTD" },
];

// The API retains an informational team stamp, but storage belongs to the org.
// Collapse all stamps into one value per UTC date.
function dailyStorage(rows: DailyUsageRow[]) {
  const byDate = new Map<string, number>();
  for (const r of rows) {
    byDate.set(r.date, (byDate.get(r.date) ?? 0) + Number(r.gib_seconds) / 3600);
  }
  return [...byDate.entries()]
    .map(([date, storage]) => ({ date, storage }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function UsageChart({ rows }: { rows: DailyUsageRow[] }) {
  const data = useMemo(() => dailyStorage(rows), [rows]);
  const total = useMemo(() => data.reduce((sum, row) => sum + row.storage, 0), [data]);
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-1.5">
          S3 GiB·hours
          <InfoTooltip label="Explain S3 GiB·h" text={GIB_HOURS_TOOLTIP} />
        </CardTitle>
        <p className="text-xs text-muted-foreground">{fmtUnits(total)} total in window</p>
      </CardHeader>
      <CardContent>
        {data.length === 0 ? (
          <p className="py-12 text-center text-sm text-muted-foreground">No data in the selected window.</p>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: -8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey="date"
                tickFormatter={(d) => String(d).slice(5)} // MM-DD
                stroke="hsl(var(--muted-foreground))"
                fontSize={10}
              />
              <YAxis stroke="hsl(var(--muted-foreground))" fontSize={10} width={56} tickFormatter={fmtUnits} />
              <RTooltip
                contentStyle={{
                  background: "hsl(var(--popover))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: 8,
                  fontSize: 12,
                }}
                formatter={(v: number) => [`${fmtUnits(v)} GiB·h`, "S3 storage"]}
              />
              <Bar dataKey="storage" fill="hsl(var(--primary))" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}

// OrgUsageSection renders the org's daily S3 usage over a selectable window.
// Cost data is admin-only —
// viewers get nothing at all (the API 403s them anyway; this keeps the page
// clean and avoids the wasted request).
export function OrgUsageSection({ orgId }: { orgId: string }) {
  const { isAdmin } = useIdentity();
  const [period, setPeriod] = useState<PeriodKey>("14d");
  const days = periodDays(period);
  const usage = useOrgDailyUsage(orgId, days);

  if (!isAdmin) return null;

  const rows = usage.data?.rows ?? [];
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3">
        <div>
          <CardTitle>Usage</CardTitle>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Daily S3 storage-time (GiB·h) for this organization, summed over the retained billing buffer.
          </p>
        </div>
        <div className="flex items-center gap-1">
          {PERIODS.map(({ key, label }) => (
            <Button
              key={key}
              size="sm"
              variant={period === key ? "secondary" : "ghost"}
              className={cn("h-7 px-2 text-xs", period === key && "font-semibold")}
              onClick={() => setPeriod(key)}
            >
              {label}
            </Button>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        {usage.data?.watermark_low && (
          <p className="mb-3 rounded-md border border-warning/40 bg-warning/5 p-2 text-xs text-muted-foreground">
            Usage at or before {fmtTime(usage.data.watermark_low)} has been billed and removed from the buffer, so the
            left edge of the selected period may be partial.
          </p>
        )}
        {usage.isError ? (
          <ErrorState error={usage.error} onRetry={() => usage.refetch()} />
        ) : usage.isLoading ? (
          <LoadingState />
        ) : rows.length === 0 ? (
          <EmptyState title="No usage recorded" description={`No usage for this org in the last ${days} days of the retained buffer.`} />
        ) : (
          <UsageChart rows={rows} />
        )}
      </CardContent>
    </Card>
  );
}
