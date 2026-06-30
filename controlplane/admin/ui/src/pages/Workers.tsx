import { useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { Search, Server } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { StateBadge } from "@/components/StateBadge";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useFleet, useWorkers } from "@/hooks/useApi";
import { fmtBytes, fmtDuration, fmtInt } from "@/lib/format";
import type { WorkerStatus } from "@/types/api";

// Canonical lifecycle-state order for the rollup chips.
const STATE_ORDER = ["spawning", "idle", "reserved", "activating", "hot", "hot_idle", "draining"];

export function Workers() {
  const fleet = useFleet();
  const workers = useWorkers();
  const [filter, setFilter] = useState("");

  const fleetRows = fleet.data ?? [];
  const workerRows = workers.data ?? [];

  // Per-state rollup: sum the durable runtime-store counts grouped by lifecycle
  // state (a state may span several image/binding rows).
  const byState = useMemo(() => {
    const m: Record<string, number> = {};
    for (const f of fleetRows) m[f.state] = (m[f.state] ?? 0) + f.count;
    const known = STATE_ORDER.filter((s) => s in m).map((s) => [s, m[s]] as const);
    const extra = Object.entries(m)
      .filter(([s]) => !STATE_ORDER.includes(s))
      .sort((a, b) => b[1] - a[1]);
    return [...known, ...extra];
  }, [fleetRows]);

  const totalFleet = useMemo(() => fleetRows.reduce((n, f) => n + f.count, 0), [fleetRows]);
  const totalCpu = useMemo(() => fleetRows.reduce((n, f) => n + (f.cpu_cores ?? 0), 0), [fleetRows]);
  const totalMem = useMemo(() => fleetRows.reduce((n, f) => n + (f.memory_bytes ?? 0), 0), [fleetRows]);

  const columns = useMemo<ColumnDef<WorkerStatus, any>[]>(
    () => [
      {
        accessorKey: "id",
        header: "ID",
        cell: ({ getValue }) => <span className="font-mono text-xs">#{String(getValue())}</span>,
      },
      {
        accessorKey: "org",
        header: "Org",
        cell: ({ getValue }) => <span className="font-mono text-xs">{String(getValue())}</span>,
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ getValue }) => <StateBadge state={getValue() as string} />,
      },
      {
        accessorKey: "active_sessions",
        header: "Sessions",
        cell: ({ getValue }) => {
          const v = getValue() as number;
          return <span className={v > 0 ? "font-medium tabular-nums" : "tabular-nums text-muted-foreground"}>{v}</span>;
        },
      },
      {
        accessorKey: "cpu",
        header: "CPU",
        cell: ({ getValue }) => {
          const v = getValue() as string;
          return v ? <span className="tabular-nums">{v}</span> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "memory",
        header: "Memory",
        cell: ({ getValue }) => {
          const v = getValue() as string;
          return v ? <span className="tabular-nums">{v}</span> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "ttl_seconds",
        header: "TTL",
        cell: ({ getValue }) => {
          const v = getValue() as number;
          return v > 0 ? (
            <span className="tabular-nums">{fmtDuration(v)}</span>
          ) : (
            <span className="text-muted-foreground">default</span>
          );
        },
      },
    ],
    [],
  );

  return (
    <>
      <PageHeader
        title="Workers"
        description="Fleet rollup by lifecycle state (GET /api/v1/workers/fleet) plus session-holding workers (GET /api/v1/workers)."
        actions={
          <div className="relative">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter…" className="w-64 pl-8" />
          </div>
        }
      />
      <PageBody>
        <div className="mb-4 grid gap-4 lg:grid-cols-3">
          <Card className="lg:col-span-2">
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle>Fleet by state</CardTitle>
              <div className="flex items-center gap-1.5">
                <Badge variant="secondary">{fmtInt(totalFleet)} total</Badge>
                <Badge variant="muted">{fmtInt(totalCpu)} cores</Badge>
                <Badge variant="muted">{fmtBytes(totalMem)}</Badge>
              </div>
            </CardHeader>
            <CardContent>
              {fleet.isError ? (
                <p className="py-4 text-center text-sm text-destructive">Fleet endpoint error.</p>
              ) : byState.length === 0 ? (
                <p className="py-4 text-center text-sm text-muted-foreground">No workers in the fleet.</p>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {byState.map(([state, n]) => (
                    <div key={state} className="flex items-center gap-2 rounded-md border border-border bg-background/40 px-3 py-2">
                      <StateBadge state={state} />
                      <span className="text-lg font-semibold tabular-nums">{fmtInt(n)}</span>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Fleet by image</CardTitle>
            </CardHeader>
            <CardContent className="space-y-1.5">
              {fleetRows.length === 0 ? (
                <p className="py-2 text-center text-sm text-muted-foreground">No workers.</p>
              ) : (
                fleetRows
                  .slice()
                  .sort((a, b) => b.count - a.count)
                  .slice(0, 8)
                  .map((f, i) => (
                    <div key={`${f.image}-${f.state}-${f.binding}-${i}`} className="flex items-center justify-between gap-2 text-sm">
                      <span className="truncate font-mono text-xs" title={`${f.image} · ${f.binding}`}>
                        {shortImage(f.image)}
                      </span>
                      <div className="flex items-center gap-1.5">
                        <StateBadge state={f.state} />
                        <Badge variant="secondary">{f.count}</Badge>
                      </div>
                    </div>
                  ))
              )}
            </CardContent>
          </Card>
        </div>

        <Card className="overflow-hidden">
          <CardHeader>
            <CardTitle>Session-holding workers</CardTitle>
          </CardHeader>
          {workers.isLoading ? (
            <TableSkeleton cols={7} />
          ) : workers.isError ? (
            <ErrorState error={workers.error} onRetry={() => workers.refetch()} />
          ) : (
            <DataTable
              data={workerRows}
              columns={columns}
              globalFilter={filter}
              onGlobalFilterChange={setFilter}
              initialSorting={[{ id: "active_sessions", desc: true }]}
              empty={
                <EmptyState
                  icon={<Server className="h-6 w-6" />}
                  title="No active workers"
                  description="No workers are currently holding a session."
                />
              }
            />
          )}
        </Card>
      </PageBody>
    </>
  );
}

function shortImage(image: string): string {
  if (!image) return "—";
  const at = image.indexOf("@");
  const base = at >= 0 ? image.slice(0, at) : image;
  const parts = base.split("/");
  return parts[parts.length - 1] || base;
}
