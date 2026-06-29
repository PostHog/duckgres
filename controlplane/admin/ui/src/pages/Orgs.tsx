import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { type ColumnDef } from "@tanstack/react-table";
import { Building2, Search } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { StateBadge } from "@/components/StateBadge";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useOrgs } from "@/hooks/useApi";
import { fmtInt } from "@/lib/format";
import type { Org } from "@/types/api";

export function Orgs() {
  const orgs = useOrgs();
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
        accessorKey: "max_connections",
        header: "Max conns",
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
        cell: ({ row }) =>
          row.original.warehouse ? (
            <StateBadge state={row.original.warehouse.state} />
          ) : (
            <Badge variant="muted">none</Badge>
          ),
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
