import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { type ColumnDef } from "@tanstack/react-table";
import { Coins, Cpu, Database, MemoryStick, ShieldAlert } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { OrgRef } from "@/components/OrgRef";
import { StatCard } from "@/components/StatCard";
import { Card } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useIdentity } from "@/components/IdentityProvider";
import { useMonthlyUsage, useOrgLabels } from "@/hooks/useApi";
import { fmtTime } from "@/lib/format";
import type { MonthlyUsageRow } from "@/types/api";

// Whole-unit-friendly: 120 stays "120", 1.5 stays "1.5", thousands group.
function fmtUnits(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

const cpuMinutes = (r: MonthlyUsageRow) => r.cpu_seconds / 60;
const memGiBMinutes = (r: MonthlyUsageRow) => r.memory_seconds / 60;
const storageGiBHours = (r: MonthlyUsageRow) => Number(r.gib_seconds) / 3600;

function currentMonth(): string {
  const d = new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
}

export function Usage() {
  const { isAdmin } = useIdentity();
  const navigate = useNavigate();
  const [months, setMonths] = useState(6);
  const usage = useMonthlyUsage(months);
  const orgLabels = useOrgLabels();
  const rows = useMemo(() => usage.data?.rows ?? [], [usage.data]);

  // Month tabs: every month present in the data plus the current (possibly
  // still empty) month, newest first. Default to the latest month WITH data
  // so the page never opens on a blank current month.
  const monthOptions = useMemo(() => {
    const s = new Set<string>(rows.map((r) => r.month));
    s.add(currentMonth());
    return [...s].sort().reverse();
  }, [rows]);
  const [selected, setSelected] = useState<string | null>(null);
  const month =
    selected && monthOptions.includes(selected)
      ? selected
      : (monthOptions.find((m) => rows.some((r) => r.month === m)) ?? currentMonth());

  const monthRows = useMemo(() => rows.filter((r) => r.month === month), [rows, month]);
  const totals = useMemo(
    () =>
      monthRows.reduce(
        (acc, r) => ({
          cpu: acc.cpu + cpuMinutes(r),
          mem: acc.mem + memGiBMinutes(r),
          storage: acc.storage + storageGiBHours(r),
        }),
        { cpu: 0, mem: 0, storage: 0 },
      ),
    [monthRows],
  );

  const columns = useMemo<ColumnDef<MonthlyUsageRow, any>[]>(
    () => [
      {
        accessorKey: "org_id",
        header: "Org",
        cell: ({ row }) => <OrgRef id={row.original.org_id} label={orgLabels.get(row.original.org_id)} />,
      },
      {
        id: "team",
        header: "Team",
        accessorFn: (r) => r.schema_name ?? `team ${r.team_id}`,
        cell: ({ row }) => {
          const r = row.original;
          return (
            <div className="flex flex-col gap-0.5">
              <span className="font-mono text-xs">{r.schema_name ?? `team ${r.team_id}`}</span>
              <span className="text-[11px] text-muted-foreground">id {r.team_id}</span>
            </div>
          );
        },
      },
      {
        id: "cpu_minutes",
        header: "CPU-min",
        accessorFn: cpuMinutes,
        cell: ({ getValue }) => <span className="font-mono text-xs">{fmtUnits(getValue() as number)}</span>,
      },
      {
        id: "mem_gib_minutes",
        header: "Memory GiB·min",
        accessorFn: memGiBMinutes,
        cell: ({ getValue }) => <span className="font-mono text-xs">{fmtUnits(getValue() as number)}</span>,
      },
      {
        id: "storage_gib_hours",
        header: "S3 GiB·h",
        accessorFn: storageGiBHours,
        cell: ({ getValue }) => <span className="font-mono text-xs">{fmtUnits(getValue() as number)}</span>,
      },
    ],
    [orgLabels],
  );

  // Per-team cost data is admin-only (the API enforces RequireAdmin; this is
  // just the friendly notice, matching the Operators page).
  if (!isAdmin) {
    return (
      <>
        <PageHeader title="Usage" description="Monthly per-team compute and storage usage." />
        <PageBody>
          <EmptyState
            icon={<ShieldAlert className="h-6 w-6 text-warning" />}
            title="Admin only"
            description="Per-team usage and cost data requires the admin role."
          />
        </PageBody>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Usage"
        description="Cumulative monthly compute and storage per team, summed over the retained billing buffer."
        actions={
          <>
            <Select value={month} onValueChange={setSelected}>
              <SelectTrigger className="w-32" aria-label="Month">
                <SelectValue placeholder="Month" />
              </SelectTrigger>
              <SelectContent>
                {monthOptions.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={String(months)} onValueChange={(v) => setMonths(Number(v))}>
              <SelectTrigger className="w-36" aria-label="Window">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {[1, 3, 6, 12, 24].map((n) => (
                  <SelectItem key={n} value={String(n)}>
                    Last {n} mo
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </>
        }
      />
      <PageBody>
        {usage.data?.watermark_low && (
          <Card className="mb-4 border-warning/40 bg-warning/5 p-3 text-xs text-muted-foreground">
            Usage at or before {fmtTime(usage.data.watermark_low)} has been billed and removed from the buffer, and
            buckets older than 30 days are garbage-collected — earlier months may be partial or absent. This page is
            an operations view, not an invoice.
          </Card>
        )}
        {usage.isError ? (
          <ErrorState error={usage.error} onRetry={() => usage.refetch()} />
        ) : usage.isLoading ? (
          <TableSkeleton />
        ) : (
          <>
            <div className="mb-4 grid grid-cols-1 gap-3 sm:grid-cols-3">
              <StatCard label="CPU-min" value={fmtUnits(totals.cpu)} icon={<Cpu className="h-4 w-4" />} hint={`${month} total`} />
              <StatCard
                label="Memory GiB·min"
                value={fmtUnits(totals.mem)}
                icon={<MemoryStick className="h-4 w-4" />}
                hint={`${month} total`}
              />
              <StatCard
                label="S3 GiB·h"
                value={fmtUnits(totals.storage)}
                icon={<Database className="h-4 w-4" />}
                hint={`${month} total`}
              />
            </div>
            {monthRows.length === 0 ? (
              <EmptyState
                icon={<Coins className="h-8 w-8" />}
                title="No usage"
                description={`No usage has been recorded for ${month} in the retained billing buffer.`}
              />
            ) : (
              <Card>
                <DataTable
                  data={monthRows}
                  columns={columns}
                  initialSorting={[{ id: "cpu_minutes", desc: true }]}
                  // Click through to the org's daily usage charts.
                  onRowClick={(r) => navigate(`/orgs/${encodeURIComponent(r.org_id)}`)}
                  rowClassName={() => "cursor-pointer"}
                />
              </Card>
            )}
          </>
        )}
      </PageBody>
    </>
  );
}
