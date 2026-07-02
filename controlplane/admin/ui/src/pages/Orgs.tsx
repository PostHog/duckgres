import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { type ColumnDef } from "@tanstack/react-table";
import { AlertTriangle, Building2, Search } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { StateBadge } from "@/components/StateBadge";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useDucklingDrift, useOrgs } from "@/hooks/useApi";
import { ducklingName, fmtInt } from "@/lib/format";
import type { DucklingDrift, Org } from "@/types/api";

export function Orgs() {
  const orgs = useOrgs();
  const drift = useDucklingDrift();
  const navigate = useNavigate();
  const [filter, setFilter] = useState("");

  // Live drift keyed by org, so a row whose config state is "ready" but whose
  // Duckling CR is actually missing still gets flagged (config state alone
  // can't see that).
  const driftByOrg = useMemo(() => {
    const m = new Map<string, DucklingDrift>();
    for (const e of drift.data?.entries ?? []) {
      if (e.org) m.set(e.org, e);
    }
    return m;
  }, [drift.data]);

  const columns = useMemo<ColumnDef<Org, any>[]>(
    () => [
      {
        accessorKey: "name",
        header: "Org",
        cell: ({ row }) => <span className="font-mono text-xs font-medium">{row.original.name}</span>,
      },
      {
        accessorKey: "database_name",
        header: "Database",
        cell: ({ getValue }) => <span className="font-mono text-xs text-muted-foreground">{String(getValue() ?? "")}</span>,
      },
      {
        accessorKey: "hostname_alias",
        header: "Alias",
        cell: ({ getValue }) => {
          const v = getValue() as string | null;
          return v ? <span className="font-mono text-xs">{v}</span> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "default_team_id",
        header: "Default team",
        cell: ({ getValue }) => {
          const v = getValue() as string | null;
          return v ? <span className="font-mono text-xs">{v}</span> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "max_workers",
        header: "Max workers",
        cell: ({ getValue }) => {
          const v = getValue() as number;
          return <span className="tabular-nums">{v === 0 ? "∞" : fmtInt(v)}</span>;
        },
      },
      {
        accessorKey: "max_vcpus",
        header: "Max vCPUs",
        cell: ({ getValue }) => {
          const v = getValue() as number;
          return <span className="tabular-nums">{v === 0 ? "∞" : fmtInt(v)}</span>;
        },
      },
      {
        id: "users",
        header: "Users",
        accessorFn: (o) => o.users?.length ?? 0,
        cell: ({ getValue }) => <span className="tabular-nums">{fmtInt(getValue() as number)}</span>,
      },
      {
        id: "warehouse",
        header: "Warehouse",
        accessorFn: (o) => o.warehouse?.state ?? "",
        cell: ({ row }) => {
          const o = row.original;
          if (!o.warehouse) return <Badge variant="muted">none</Badge>;
          // Warning is fed from the live drift result (same source as the
          // banner), NOT config state — so a ready-but-CR-missing row still
          // flags here. Kept next to the status badge for easy scanning.
          const d = driftByOrg.get(o.name);
          return (
            <div className="flex items-center gap-1.5">
              <StateBadge state={o.warehouse.state} />
              {d && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <AlertTriangle className="h-4 w-4 text-warning" />
                  </TooltipTrigger>
                  <TooltipContent>{d.message}</TooltipContent>
                </Tooltip>
              )}
            </div>
          );
        },
      },
      {
        id: "duckling",
        header: "Duckling",
        accessorFn: (o) => (o.warehouse ? o.warehouse.duckling_name || ducklingName(o.name) : ""),
        cell: ({ row }) => {
          const o = row.original;
          const name = o.warehouse ? o.warehouse.duckling_name || ducklingName(o.name) : "";
          return <span className="font-mono text-xs text-muted-foreground">{name || "—"}</span>;
        },
      },
    ],
    [driftByOrg],
  );

  return (
    <>
      <PageHeader
        title="Organizations"
        description="Tenants and their per-org limits. Click a row to edit config and warehouse."
        actions={
          <div className="relative">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Filter orgs…"
              className="w-64 pl-8"
            />
          </div>
        }
      />
      <PageBody>
        {drift.data?.available && drift.data.entries.length > 0 && (
          <DriftBanner entries={drift.data.entries} onOpenOrg={(org) => navigate(`/orgs/${encodeURIComponent(org)}`)} />
        )}
        <Card className="overflow-hidden">
          {orgs.isLoading ? (
            <TableSkeleton cols={8} />
          ) : orgs.isError ? (
            <ErrorState error={orgs.error} onRetry={() => orgs.refetch()} />
          ) : (
            <DataTable
              data={orgs.data ?? []}
              columns={columns}
              globalFilter={filter}
              onGlobalFilterChange={setFilter}
              initialSorting={[{ id: "name", desc: false }]}
              onRowClick={(o) => navigate(`/orgs/${encodeURIComponent(o.name)}`)}
              empty={
                <EmptyState
                  icon={<Building2 className="h-6 w-6" />}
                  title="No organizations"
                  description="No tenants are registered in the config store yet."
                />
              }
            />
          )}
        </Card>
      </PageBody>
    </>
  );
}


function DriftRow({ e, onOpenOrg }: { e: DucklingDrift; onOpenOrg: (org: string) => void }) {
  const label = e.org || e.duckling_name || "orphan";
  const inner = (
    <>
      <span className="shrink-0 font-mono text-xs font-medium" title={label}>
        {label}
      </span>
      <span className="flex-1 truncate text-xs text-muted-foreground" title={e.message}>
        {e.message || "—"}
      </span>
      {e.warehouse_state && (
        <span className="shrink-0 font-mono text-xs text-muted-foreground">{e.warehouse_state}</span>
      )}
    </>
  );
  const className =
    "flex items-center justify-between gap-3 rounded-md border border-border/60 bg-background/40 px-3 py-1.5";
  return e.org !== "" ? (
    <button
      type="button"
      onClick={() => onOpenOrg(e.org)}
      className={`${className} w-full text-left hover:bg-background/70`}
    >
      {inner}
    </button>
  ) : (
    <div className={className}>{inner}</div>
  );
}

function DriftGroup({
  title,
  entries,
  onOpenOrg,
}: {
  title: string;
  entries: DucklingDrift[];
  onOpenOrg: (org: string) => void;
}) {
  if (entries.length === 0) return null;
  return (
    <div className="space-y-1">
      <div className="px-1 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        {title} ({entries.length})
      </div>
      <div className="max-h-56 space-y-1 overflow-y-auto">
        {entries.map((e, i) => (
          <DriftRow key={`${e.org}:${e.duckling_name}:${i}`} e={e} onOpenOrg={onOpenOrg} />
        ))}
      </div>
    </div>
  );
}

function DriftBanner({
  entries,
  onOpenOrg,
}: {
  entries: DucklingDrift[];
  onOpenOrg: (org: string) => void;
}) {
  // Orphans (CR with no warehouse row) need the opposite fix from drift
  // (config exists, infra gone) — split them so remediation is obvious.
  const orphans = entries.filter((e) => e.org === "" || e.issue === "orphan");
  const drift = entries.filter((e) => !(e.org === "" || e.issue === "orphan"));
  return (
    <Card className="border-warning/40 bg-warning/5">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-warning">
          <AlertTriangle className="h-4 w-4" />
          {entries.length} duckling issue{entries.length === 1 ? "" : "s"} detected
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <DriftGroup title="Warehouse drift" entries={drift} onOpenOrg={onOpenOrg} />
        <DriftGroup title="Orphaned ducklings" entries={orphans} onOpenOrg={onOpenOrg} />
      </CardContent>
    </Card>
  );
}
