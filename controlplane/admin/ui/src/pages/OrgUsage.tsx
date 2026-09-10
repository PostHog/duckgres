import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Tooltip as RTooltip, ResponsiveContainer, XAxis, YAxis } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { InfoTooltip } from "@/components/InfoTooltip";
import { useIdentity } from "@/components/IdentityProvider";
import { useOrgDailyUsage } from "@/hooks/useApi";
import { fmtBytes, fmtUnits } from "@/lib/format";
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
function dailyUsage(rows: DailyUsageRow[], metric: "storage" | "scan") {
  const byDate = new Map<string, number>();
  for (const r of rows) {
    byDate.set(r.date, (byDate.get(r.date) ?? 0) + (metric === "storage" ? Number(r.gib_seconds) / 3600 : Number(r.bytes_scanned)));
  }
  return [...byDate.entries()]
    .map(([date, value]) => ({ date, value }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function UsageChart({ rows, metric }: { rows: DailyUsageRow[]; metric: "storage" | "scan" }) {
  const data = useMemo(() => dailyUsage(rows, metric), [rows, metric]);
  const total = useMemo(() => data.reduce((sum, row) => sum + row.value, 0), [data]);
  const format = metric === "storage" ? fmtUnits : fmtBytes;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-1.5">
          {metric === "storage" ? "S3 GiB·hours" : "Bytes scanned"}
          {metric === "storage" && <InfoTooltip label="Explain S3 GiB·h" text={GIB_HOURS_TOOLTIP} />}
        </CardTitle>
        <p className="text-xs text-muted-foreground">{format(total)} total in window</p>
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
              <YAxis stroke="hsl(var(--muted-foreground))" fontSize={10} width={56} tickFormatter={format} />
              <RTooltip
                contentStyle={{
                  background: "hsl(var(--popover))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: 8,
                  fontSize: 12,
                }}
                formatter={(v: number) => metric === "storage" ? [`${fmtUnits(v)} GiB·h`, "S3 storage"] : [fmtBytes(v), "Bytes scanned"]}
              />
              <Bar dataKey="value" fill="hsl(var(--primary))" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}

// OrgUsageSection renders retained daily scan and S3 usage.
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
            Daily query scan bytes and S3 storage-time (GiB·h). Failed and cancelled queries are included.
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
        {usage.isError ? (
          <ErrorState error={usage.error} onRetry={() => usage.refetch()} />
        ) : usage.isLoading ? (
          <LoadingState />
        ) : rows.length === 0 ? (
          <EmptyState title="No usage recorded" description={`No usage for this org in the last ${days} days.`} />
        ) : (
          <div className="grid gap-3 lg:grid-cols-2">
            <UsageChart rows={rows} metric="scan" />
            <UsageChart rows={rows} metric="storage" />
          </div>
        )}
      </CardContent>
    </Card>
  );
}
