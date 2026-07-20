import { useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { Layers, Pencil, Plus, Search, Trash2 } from "lucide-react";
import { Link } from "react-router-dom";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { AdminGate } from "@/components/AdminOnly";
import {
  BackfillBadge,
  CreateTeamDialog,
  DeleteTeamDialog,
  EditTeamDialog,
} from "@/components/OrgTeamDialogs";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useAllOrgTeams, useOrgs } from "@/hooks/useApi";
import { fmtTime } from "@/lib/format";
import type { OrgTeam } from "@/types/api";

export function OrgTeams() {
  const teams = useAllOrgTeams();
  const orgs = useOrgs();
  const [filter, setFilter] = useState("");
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<OrgTeam | null>(null);
  const [deleting, setDeleting] = useState<OrgTeam | null>(null);

  // Per-org row counts feed the delete dialog's last-team refusal.
  const countByOrg = useMemo(() => {
    const m = new Map<string, number>();
    for (const t of teams.data ?? []) {
      m.set(t.org_id, (m.get(t.org_id) ?? 0) + 1);
    }
    return m;
  }, [teams.data]);

  const columns = useMemo<ColumnDef<OrgTeam, any>[]>(
    () => [
      {
        accessorKey: "org_id",
        header: "Org",
        cell: ({ getValue }) => {
          const org = String(getValue());
          return (
            <Link
              to={`/orgs/${encodeURIComponent(org)}`}
              className="font-mono text-xs font-medium hover:underline"
              onClick={(e) => e.stopPropagation()}
            >
              {org}
            </Link>
          );
        },
      },
      {
        accessorKey: "team_id",
        header: "Team id",
        cell: ({ getValue }) => <span className="font-mono text-xs tabular-nums">{String(getValue())}</span>,
      },
      {
        accessorKey: "schema_name",
        header: "Schema",
        cell: ({ getValue }) => <span className="font-mono text-xs">{String(getValue())}</span>,
      },
      {
        accessorKey: "enabled",
        header: "Enabled",
        cell: ({ getValue }) =>
          getValue() ? <Badge variant="secondary">enabled</Badge> : <Badge variant="destructive">disabled</Badge>,
      },
      {
        id: "billing",
        header: "Billing",
        accessorFn: (t) => Boolean(t.is_billing_team),
        cell: ({ getValue }) =>
          getValue() ? <Badge variant="success">billing</Badge> : <span className="text-muted-foreground">—</span>,
      },
      {
        id: "backfill",
        header: "Backfill",
        accessorFn: (t) => t.backfill_enabled ?? null,
        cell: ({ row }) => <BackfillBadge value={row.original.backfill_enabled} />,
      },
      {
        accessorKey: "created_at",
        header: "Created",
        cell: ({ getValue }) => <span className="text-xs text-muted-foreground">{fmtTime(getValue() as string)}</span>,
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <div className="-my-1 flex justify-end gap-1">
            <AdminGate>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                title="Edit"
                onClick={() => setEditing(row.original)}
              >
                <Pencil className="h-3.5 w-3.5" />
              </Button>
            </AdminGate>
            <AdminGate>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                title="Delete"
                onClick={() => setDeleting(row.original)}
              >
                <Trash2 className="h-3.5 w-3.5 text-destructive" />
              </Button>
            </AdminGate>
          </div>
        ),
      },
    ],
    [],
  );

  return (
    <>
      <PageHeader
        title="Org teams"
        description="PostHog teams mapped to orgs and the warehouse schema each team's data lives in."
        actions={
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder="Filter teams…"
                className="w-64 pl-8"
              />
            </div>
            <AdminGate>
              <Button size="sm" onClick={() => setCreating(true)}>
                <Plus className="h-4 w-4" /> Add team
              </Button>
            </AdminGate>
          </div>
        }
      />
      <PageBody>
        <Card className="overflow-hidden">
          {teams.isLoading ? (
            <TableSkeleton cols={8} />
          ) : teams.isError ? (
            <ErrorState error={teams.error} onRetry={() => teams.refetch()} />
          ) : (
            <DataTable
              data={teams.data ?? []}
              columns={columns}
              globalFilter={filter}
              onGlobalFilterChange={setFilter}
              initialSorting={[
                { id: "org_id", desc: false },
                { id: "team_id", desc: false },
              ]}
              empty={
                <EmptyState
                  icon={<Layers className="h-6 w-6" />}
                  title="No org teams"
                  description="No PostHog teams are mapped to any org yet."
                />
              }
            />
          )}
        </Card>
      </PageBody>

      <CreateTeamDialog
        open={creating}
        onClose={() => setCreating(false)}
        orgs={(orgs.data ?? []).map((o) => o.name)}
      />
      {editing && <EditTeamDialog team={editing} onClose={() => setEditing(null)} />}
      {deleting && (
        <DeleteTeamDialog
          team={deleting}
          teamCount={countByOrg.get(deleting.org_id) ?? 1}
          onClose={() => setDeleting(null)}
        />
      )}
    </>
  );
}
