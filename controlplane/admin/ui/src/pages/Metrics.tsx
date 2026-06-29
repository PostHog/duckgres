import { useMemo, useState } from "react";
import { AlertTriangle } from "lucide-react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip as RTooltip,
  XAxis,
  YAxis,
} from "recharts";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { LoadingState } from "@/components/states";
import { useMetricRange, useMetricsPanels, useOrgs } from "@/hooks/useApi";
import { promToSeries } from "@/lib/format";

// Window selector. The backend caps the step at ~250 points per window.
const WINDOWS = ["15m", "1h", "6h", "24h"];
const COLORS = ["#22d3ee", "#f59e0b", "#a78bfa", "#34d399", "#f472b6", "#60a5fa", "#fb7185", "#facc15"];

// Allow-listed panel keys (admin/metrics_proxy.go rangePanels) + presentation.
const PANELS: { key: string; title: string; unit: string }[] = [
  { key: "query_rate", title: "Query rate by outcome", unit: "ops/s" },
  { key: "error_ratio", title: "Error ratio", unit: "" },
  { key: "duration_p95", title: "Query duration p95", unit: "s" },
  { key: "duration_p50", title: "Query duration p50", unit: "s" },
  { key: "sessions_active", title: "Active sessions", unit: "" },
  { key: "s3_bytes_rate", title: "S3 read bytes rate", unit: "B/s" },
  { key: "worker_states", title: "Workers by state", unit: "" },
  { key: "queue_depth", title: "Connection queue depth", unit: "" },
];

export function Metrics() {
  const orgs = useOrgs();
  const panels = useMetricsPanels();
  const [org, setOrg] = useState<string>("__all__");
  const [window, setWindow] = useState("1h");
  const orgArg = org === "__all__" ? undefined : org;

  const configured = panels.data?.configured ?? false;
  const available = new Set(panels.data?.panels ?? []);
  // Only render panels the backend actually serves; fall back to the full set
  // before the panels list loads.
  const visible = panels.isSuccess ? PANELS.filter((p) => available.has(p.key)) : PANELS;

  return (
    <>
      <PageHeader
        title="Metrics"
        description="Named Prometheus panels via GET /api/v1/metrics/query_range (server-built PromQL allow-list)."
        actions={
          <div className="flex items-center gap-2">
            <Select value={org} onValueChange={setOrg}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="All orgs" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All orgs</SelectItem>
                {(orgs.data ?? []).map((o) => (
                  <SelectItem key={o.name} value={o.name}>
                    {o.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={window} onValueChange={setWindow}>
              <SelectTrigger className="w-28">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {WINDOWS.map((w) => (
                  <SelectItem key={w} value={w}>
                    {w}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        }
      />
      <PageBody>
        {panels.isSuccess && !configured && (
          <div className="mb-4 flex items-center gap-2 rounded-md border border-warning/40 bg-warning/10 px-4 py-2.5 text-sm">
            <AlertTriangle className="h-4 w-4 text-warning" />
            <span>Metrics not configured (DUCKGRES_PROMETHEUS_URL unset). Panels will be empty.</span>
          </div>
        )}
        <div className="grid gap-4 lg:grid-cols-2">
          {visible.map((p) => (
            <MetricCard
              key={p.key}
              title={p.title}
              panelKey={p.key}
              org={orgArg}
              window={window}
              unit={p.unit}
              enabled={configured}
            />
          ))}
        </div>
      </PageBody>
    </>
  );
}

function MetricCard({
  title,
  panelKey,
  org,
  window,
  unit,
  enabled,
}: {
  title: string;
  panelKey: string;
  org: string | undefined;
  window: string;
  unit: string;
  enabled: boolean;
}) {
  const q = useMetricRange(panelKey, org, window, enabled);
  const series = useMemo(() => promToSeries(q.data), [q.data]);

  // Merge series into a single time-indexed dataset for Recharts.
  const { data, keys } = useMemo(() => {
    const byT = new Map<number, Record<string, number>>();
    const keys: string[] = [];
    series.forEach((s, i) => {
      const key = s.name || `series ${i + 1}`;
      keys.push(key);
      for (const p of s.points) {
        const row = byT.get(p.t) ?? { t: p.t };
        row[key] = p.v;
        byT.set(p.t, row);
      }
    });
    const data = [...byT.values()].sort((a, b) => (a.t as number) - (b.t as number));
    return { data, keys };
  }, [series]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <p className="font-mono text-[11px] text-muted-foreground">{panelKey}</p>
      </CardHeader>
      <CardContent>
        {!enabled ? (
          <p className="py-12 text-center text-sm text-muted-foreground">Metrics not configured.</p>
        ) : q.isLoading ? (
          <LoadingState />
        ) : data.length === 0 ? (
          <p className="py-12 text-center text-sm text-muted-foreground">
            No data for this panel in the selected window.
          </p>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: -8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis
                dataKey="t"
                tickFormatter={(t) => new Date(t).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" })}
                stroke="hsl(var(--muted-foreground))"
                fontSize={10}
              />
              <YAxis stroke="hsl(var(--muted-foreground))" fontSize={10} width={48} />
              <RTooltip
                contentStyle={{
                  background: "hsl(var(--popover))",
                  border: "1px solid hsl(var(--border))",
                  borderRadius: 8,
                  fontSize: 12,
                }}
                labelFormatter={(t) => new Date(t as number).toLocaleString()}
                formatter={(v: number, name) => [`${v}${unit ? ` ${unit}` : ""}`, name]}
              />
              {keys.map((k, i) => (
                <Line
                  key={k}
                  type="monotone"
                  dataKey={k}
                  stroke={COLORS[i % COLORS.length]}
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}
