import { useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { Layers, Pencil, Plus, Search, Trash2 } from "lucide-react";
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
  EarliestEventDateCell,
  EditTeamDialog,
  LegacyNamesBadge,
} from "@/components/OrgTeamDialogs";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { OrgRef } from "@/components/OrgRef";
import { useAllOrgTeams, useOrgs } from "@/hooks/useApi";
import { fmtTime, orgLabel } from "@/lib/format";
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

  // Readable org names keyed by org id. Shown instead of the raw id — an
  // operator scans for "Posthog", not a UUID. The id stays visible under
  // the label and via the copy button.
  const orgLabels = useMemo(() => {
    const m = new Map<string, string>();
    for (const o of orgs.data ?? []) {
      m.set(o.name, orgLabel(o));
    }
    return m;
  }, [orgs.data]);

  const columns = useMemo<ColumnDef<OrgTeam, any>[]>(
    () => [
      {
        accessorKey: "org_id",
        header: "Org",
        // Human-readable database name first; the org id sits under it.
        // Deliberately NOT a link: click-through opens the team edit dialog
        // (row click), matching the rest of this page's action model.
        cell: ({ row }) => <OrgRef id={row.original.org_id} label={orgLabels.get(row.original.org_id)} />,
      },
      {
        accessorKey: "team_id",
        header: "Team id",
        cell: ({ getValue }) => <span className="font-mono text-xs tabular-nums">{String(getValue())}</span>,
      },
      {
        accessorKey: "schema_name",
        header: "Schema",
        cell: ({ row, getValue }) => (
          <span className="flex items-center gap-1.5">
            <span className="font-mono text-xs">{String(getValue())}</span>
            <LegacyNamesBadge team={row.original} />
          </span>
        ),
      },
      {
        accessorKey: "enabled",
        header: "Enabled",
        cell: ({ getValue }) =>
          getValue() ? <Badge variant="secondary">enabled</Badge> : <Badge variant="destructive">disabled</Badge>,
      },
      {
        accessorKey: "backfill_enabled",
        header: "Backfill",
        cell: ({ row }) => <BackfillBadge value={row.original.backfill_enabled} />,
      },
      {
        id: "earliest_event",
        header: "Earliest event",
        accessorFn: (t) => t.earliest_event_date ?? "",
        cell: ({ row }) => <EarliestEventDateCell value={row.original.earliest_event_date} />,
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
          <div className="-my-1 flex justify-end gap-1" onClick={(e) => e.stopPropagation()}>
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
    [orgLabels],
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
              // Click anywhere on a team row opens it (the edit dialog) —
              // the Org cell is deliberately not a link to the org page.
              onRowClick={(team) => setEditing(team)}
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
        orgs={(orgs.data ?? []).map((o) => ({ name: o.name, label: orgLabel(o) }))}
      />
      {editing && (
        <EditTeamDialog
          team={editing}
          orgLabel={orgLabels.get(editing.org_id)}
          onClose={() => setEditing(null)}
        />
      )}
      {deleting && (
        <DeleteTeamDialog
          team={deleting}
          teamCount={countByOrg.get(deleting.org_id) ?? 1}
          orgLabel={orgLabels.get(deleting.org_id)}
          onClose={() => setDeleting(null)}
        />
      )}
    </>
  );
}
