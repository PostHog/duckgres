import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Legend, Tooltip as RTooltip, ResponsiveContainer, XAxis, YAxis } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { useIdentity } from "@/components/IdentityProvider";
import { useOrgDailyUsage } from "@/hooks/useApi";
import { hashColor } from "@/lib/colors";
import { fmtTime } from "@/lib/format";
import { cn } from "@/lib/utils";
import type { DailyUsageRow } from "@/types/api";

const PERIODS = [7, 14, 30];

type Metric = {
  key: string;
  title: string;
  // derive the chart value (display units) from a raw daily row
  value: (r: DailyUsageRow) => number;
  unit: string;
};

const METRICS: Metric[] = [
  { key: "cpu", title: "CPU-minutes", value: (r) => r.cpu_seconds / 60, unit: "CPU-min" },
  { key: "mem", title: "Memory GiB·minutes", value: (r) => r.memory_seconds / 60, unit: "GiB·min" },
  { key: "storage", title: "S3 GiB·hours", value: (r) => Number(r.gib_seconds) / 3600, unit: "GiB·h" },
];

function fmtUnits(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function teamLabel(r: DailyUsageRow): string {
  return r.schema_name ?? `team ${r.team_id}`;
}

// Pivot rows into recharts shape: one object per date, one key per team.
function pivot(rows: DailyUsageRow[], metric: Metric) {
  const teams = [...new Set(rows.map(teamLabel))].sort();
  const byDate = new Map<string, Record<string, number | string>>();
  for (const r of rows) {
    let d = byDate.get(r.date);
    if (!d) {
      d = { date: r.date };
      byDate.set(r.date, d);
    }
    const k = teamLabel(r);
    d[k] = ((d[k] as number) ?? 0) + metric.value(r);
  }
  return { data: [...byDate.values()].sort((a, b) => String(a.date).localeCompare(String(b.date))), teams };
}

function UsageChart({ metric, rows }: { metric: Metric; rows: DailyUsageRow[] }) {
  const { data, teams } = useMemo(() => pivot(rows, metric), [rows, metric]);
  const total = useMemo(() => rows.reduce((s, r) => s + metric.value(r), 0), [rows, metric]);
  return (
    <Card>
      <CardHeader>
        <CardTitle>{metric.title}</CardTitle>
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
                formatter={(v: number, name) => [`${fmtUnits(v)} ${metric.unit}`, name]}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              {teams.map((t) => (
                <Bar key={t} dataKey={t} stackId="usage" fill={hashColor(t)} isAnimationActive={false} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}

// OrgUsageSection renders the org's daily usage charts (CPU / memory / S3,
// stacked by team) over a selectable window. Cost data is admin-only —
// viewers get nothing at all (the API 403s them anyway; this keeps the page
// clean and avoids the wasted request).
export function OrgUsageSection({ orgId }: { orgId: string }) {
  const { isAdmin } = useIdentity();
  const [days, setDays] = useState(14);
  const usage = useOrgDailyUsage(orgId, days);

  if (!isAdmin) return null;

  const rows = usage.data?.rows ?? [];
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3">
        <div>
          <CardTitle>Usage</CardTitle>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Daily compute and storage per team, summed over the retained billing buffer.
          </p>
        </div>
        <div className="flex items-center gap-1">
          {PERIODS.map((n) => (
            <Button
              key={n}
              size="sm"
              variant={days === n ? "secondary" : "ghost"}
              className={cn("h-7 px-2 text-xs", days === n && "font-semibold")}
              onClick={() => setDays(n)}
            >
              {n}d
            </Button>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        {usage.data?.watermark_low && (
          <p className="mb-3 rounded-md border border-warning/40 bg-warning/5 p-2 text-xs text-muted-foreground">
            Usage at or before {fmtTime(usage.data.watermark_low)} has been billed and removed from the buffer, and
            buckets older than 30 days are garbage-collected — the left edge of a long window may be partial.
          </p>
        )}
        {usage.isError ? (
          <ErrorState error={usage.error} onRetry={() => usage.refetch()} />
        ) : usage.isLoading ? (
          <LoadingState />
        ) : rows.length === 0 ? (
          <EmptyState title="No usage recorded" description={`No usage for this org in the last ${days} days of the retained buffer.`} />
        ) : (
          <div className="grid gap-4 lg:grid-cols-3">
            {METRICS.map((m) => (
              <UsageChart key={m.key} metric={m} rows={rows} />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
