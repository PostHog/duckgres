import { useMemo } from "react";
import { Building2, Cpu, Layers, ListOrdered, Server, Users } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { StatCard } from "@/components/StatCard";
import { Sparkline } from "@/components/Sparkline";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { StateBadge } from "@/components/StateBadge";
import { useClusterStatus, useFleet, useMetricRange, useModel } from "@/hooks/useApi";
import { fmtInt, fmtPercent, promToSeries } from "@/lib/format";
import { isIdleLeak, orgLoadPercent, summarizeFleet, topOrgsByLoad } from "@/lib/fleet";
import type { WorkerLifecycleState } from "@/types/api";

const LIFECYCLE: WorkerLifecycleState[] = [
  "hot",
  "hot_idle",
  "spawning",
  "activating",
  "draining",
];

export function Overview() {
  const status = useClusterStatus();
  const fleet = useFleet();
  const queue = useModel("org-connection-queue");
  const cps = useModel("cp-instances");
  const qTotal = useMetricRange("query_rate", undefined, "1h");

  // Worker fleet math (busy/idle/total/byState) lives in lib/fleet.ts so it can
  // be unit-tested — see fleet.test.ts. Keep derivations there, not in the JSX.
  const fleetSummary = useMemo(() => summarizeFleet(fleet.data), [fleet.data]);
  const stateCounts = fleetSummary.byState;
  const totalFleet = fleetSummary.total;

  const queueDepth = (queue.data?.rows ?? []).filter((r) => !r["granted_at"]).length;
  const cpRows = (cps.data?.rows ?? []) as { state?: string; pod_name?: string }[];
  const activeCPs = cpRows.filter((r) => (r.state ?? "").toLowerCase() === "active").length;

  // Derive a query-rate sparkline + error% from the `query_rate` panel (a
  // per-second rate labeled by outcome). Each point is ops/s, so we average
  // across the window per outcome rather than diffing a counter.
  const { rateSeries, errorPct } = useMemo(() => {
    const series = promToSeries(qTotal.data);
    if (series.length === 0) return { rateSeries: [] as number[], errorPct: null as number | null };
    let success = 0;
    let error = 0;
    const totalByT = new Map<number, number>();
    for (const s of series) {
      const outcome = s.labels?.outcome ?? "";
      const avg = s.points.length ? s.points.reduce((n, p) => n + p.v, 0) / s.points.length : 0;
      if (outcome === "error" || outcome === "failure") error += avg;
      else success += avg;
      for (const p of s.points) totalByT.set(p.t, (totalByT.get(p.t) ?? 0) + p.v);
    }
    const pts = [...totalByT.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v);
    const total = success + error;
    return { rateSeries: pts, errorPct: total > 0 ? (error / total) * 100 : 0 };
  }, [qTotal.data]);

  // Busy vs idle split, both from the durable fleet (/workers/fleet) so they
  // share scope: `hot` = holding a session (busy), `hot_idle` = parked idle
  // (warm, reserving the pod but running no query).
  const busyWorkers = fleetSummary.busy;
  const idleWorkers = fleetSummary.idle;

  return (
    <>
      <PageHeader title="Overview" description="Cluster-wide rollup of orgs, workers, sessions, and traffic." />
      <PageBody>
        <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
          <StatCard label="Organizations" value={fmtInt(status.data?.total_orgs)} icon={<Building2 className="h-4 w-4" />} />
          <StatCard
            label="Workers"
            value={fleet.isSuccess ? fmtInt(totalFleet) : "—"}
            hint={fleet.isSuccess ? `${fmtInt(busyWorkers)} hot · ${fmtInt(idleWorkers)} idle` : undefined}
            accent={isIdleLeak(idleWorkers) ? "warning" : "default"}
            icon={<Server className="h-4 w-4" />}
          />
          <StatCard label="Sessions" value={fmtInt(status.data?.total_sessions)} icon={<Users className="h-4 w-4" />} />
          <StatCard
            label="Queue depth"
            value={queue.isSuccess ? fmtInt(queueDepth) : "—"}
            accent={queueDepth > 0 ? "warning" : "default"}
            icon={<ListOrdered className="h-4 w-4" />}
          />
          <StatCard
            label="CP replicas"
            value={cps.isSuccess ? fmtInt(activeCPs || cpRows.length) : "—"}
            hint={activeCPs > 0 ? "leader elected" : "no active CP"}
            accent={activeCPs > 0 ? "success" : "warning"}
            icon={<Layers className="h-4 w-4" />}
          />
          <StatCard
            label="Error rate"
            value={errorPct == null ? "—" : fmtPercent(errorPct)}
            accent={errorPct != null && errorPct > 5 ? "destructive" : "success"}
            icon={<Cpu className="h-4 w-4" />}
          />
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-3">
          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Workers by lifecycle state</CardTitle>
            </CardHeader>
            <CardContent>
              {fleet.isSuccess && totalFleet > 0 ? (
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 md:grid-cols-5">
                  {LIFECYCLE.map((st) => (
                    <div key={st} className="rounded-md border border-border bg-background/40 p-3">
                      <div className="mb-1.5">
                        <StateBadge state={st} />
                      </div>
                      <div className="text-2xl font-semibold tabular-nums">{fmtInt(stateCounts[st] ?? 0)}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="py-6 text-center text-sm text-muted-foreground">
                  Fleet detail unavailable (GET /api/v1/workers/fleet). Showing totals only.
                </p>
              )}
              {fleet.isSuccess && isIdleLeak(idleWorkers) ? (
                <p className="mt-3 rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
                  ⚠ {fmtInt(idleWorkers)} workers are parked <span className="font-medium">hot-idle</span> —
                  warm but holding no session, reserving vCPU &amp; memory. Check the Workers page for the
                  fleet breakdown and owner.
                </p>
              ) : null}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Cluster query rate (1h)</CardTitle>
            </CardHeader>
            <CardContent>
              {rateSeries.length > 0 ? (
                <>
                  <Sparkline data={rateSeries} height={64} />
                  <div className="mt-2 flex items-center justify-between text-xs text-muted-foreground">
                    <span>duckgres_query_total</span>
                    <Badge variant={errorPct && errorPct > 5 ? "destructive" : "success"}>
                      {errorPct == null ? "—" : `${fmtPercent(errorPct)} errors`}
                    </Badge>
                  </div>
                </>
              ) : (
                <p className="py-6 text-center text-sm text-muted-foreground">
                  Metrics endpoint unavailable (GET /api/v1/metrics/query_range).
                </p>
              )}
            </CardContent>
          </Card>
        </div>

        <Card className="mt-4">
          <CardHeader>
            <CardTitle>Per-org load</CardTitle>
          </CardHeader>
          <CardContent>
            {(status.data?.orgs?.length ?? 0) === 0 ? (
              <p className="py-6 text-center text-sm text-muted-foreground">No org activity reported.</p>
            ) : (
              <div className="space-y-1.5">
                {topOrgsByLoad(status.data?.orgs, 12).map((o) => {
                  const pct = orgLoadPercent(o.workers, o.max_workers);
                  return (
                    <div key={o.name} className="flex items-center gap-3 text-sm">
                      <span className="w-44 truncate font-mono text-xs">{o.name}</span>
                      <div className="h-2 flex-1 overflow-hidden rounded-full bg-muted">
                        <div className="h-full rounded-full bg-primary" style={{ width: `${pct}%` }} />
                      </div>
                      <span className="w-28 text-right tabular-nums text-xs text-muted-foreground">
                        {o.workers}/{o.max_workers || "∞"} wk · {o.active_sessions} sess
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}
