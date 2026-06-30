import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { type ColumnDef } from "@tanstack/react-table";
import { AlertTriangle, Building2, Search } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Input } from "@/components/ui/input";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { StateBadge } from "@/components/StateBadge";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useDucklingDrift, useOrgs } from "@/hooks/useApi";
import { ducklingBroken, ducklingName, fmtInt } from "@/lib/format";
import type { DucklingDrift, DucklingDriftIssue, Org } from "@/types/api";

export function Orgs() {
  const orgs = useOrgs();
  const drift = useDucklingDrift();
  const navigate = useNavigate();
  const [filter, setFilter] = useState("");

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
        accessorKey: "max_workers",
        header: "Max workers",
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
          const broken = ducklingBroken(o.warehouse.state);
          return (
            <div className="flex items-center gap-1.5">
              <StateBadge state={o.warehouse.state} />
              {broken && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <AlertTriangle className="h-4 w-4 text-warning" />
                  </TooltipTrigger>
                  <TooltipContent>Duckling unhealthy (state: {o.warehouse.state})</TooltipContent>
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
          if (!o.warehouse) {
            return (
              <div className="flex items-center gap-1.5">
                <span className="text-muted-foreground">—</span>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <AlertTriangle className="h-4 w-4 text-warning" />
                  </TooltipTrigger>
                  <TooltipContent>No duckling provisioned for this org</TooltipContent>
                </Tooltip>
              </div>
            );
          }
          return (
            <span className="font-mono text-xs text-muted-foreground">
              {o.warehouse.duckling_name || ducklingName(o.name)}
            </span>
          );
        },
      },
    ],
    [],
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
            <TableSkeleton cols={7} />
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

// missing/orphan are hard breaks (a row or CR is absent) → destructive;
// not_ready/state_mismatch are transient/soft drift → warning.
const ISSUE_VARIANT: Record<DucklingDriftIssue, BadgeProps["variant"]> = {
  missing: "destructive",
  orphan: "destructive",
  not_ready: "warning",
  state_mismatch: "warning",
  check_error: "destructive",
};

function DriftBanner({
  entries,
  onOpenOrg,
}: {
  entries: DucklingDrift[];
  onOpenOrg: (org: string) => void;
}) {
  return (
    <Card className="border-warning/40 bg-warning/5">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-warning">
          <AlertTriangle className="h-4 w-4" />
          {entries.length} duckling issue{entries.length === 1 ? "" : "s"} detected
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="max-h-64 space-y-1 overflow-y-auto">
          {entries.map((e, i) => {
            const hasOrg = e.org !== "";
            return (
              <div
                key={`${e.org}:${e.duckling_name}:${i}`}
                className="grid grid-cols-[8rem_1fr_7rem_7rem] items-center gap-3 rounded-md border border-border/60 bg-background/40 px-3 py-1.5"
              >
                {hasOrg ? (
                  <button
                    type="button"
                    onClick={() => onOpenOrg(e.org)}
                    className="truncate text-left font-mono text-xs font-medium hover:underline"
                    title={e.org}
                  >
                    {e.org}
                  </button>
                ) : (
                  <span className="text-xs text-muted-foreground">orphan</span>
                )}
                <span className="truncate font-mono text-xs text-muted-foreground" title={e.duckling_name}>
                  {e.duckling_name || "—"}
                </span>
                <Badge variant={ISSUE_VARIANT[e.issue] ?? "warning"}>{e.issue}</Badge>
                <span className="font-mono text-xs text-muted-foreground">
                  {e.warehouse_state || "—"}
                </span>
                {e.message && (
                  <span className="col-span-4 text-xs text-muted-foreground">{e.message}</span>
                )}
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
