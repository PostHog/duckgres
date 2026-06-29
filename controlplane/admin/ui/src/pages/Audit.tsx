import { useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { ScrollText, Search, ShieldAlert } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { useIdentity } from "@/components/IdentityProvider";
import { useAudit } from "@/hooks/useApi";
import { fmtTime } from "@/lib/format";
import type { AuditEntry } from "@/types/api";

function statusVariant(s: number): "success" | "warning" | "destructive" | "muted" {
  if (s >= 500) return "destructive";
  if (s >= 400) return "warning";
  if (s >= 200 && s < 300) return "success";
  return "muted";
}

function methodVariant(m: string): "default" | "warning" | "destructive" | "secondary" {
  switch (m.toUpperCase()) {
    case "DELETE":
      return "destructive";
    case "POST":
    case "PUT":
    case "PATCH":
      return "warning";
    default:
      return "secondary";
  }
}

export function Audit() {
  const { isAdmin } = useIdentity();
  // `org` filters server-side (backend supports org/actor/limit); the free-text
  // box is a client-side table filter over the fetched rows.
  const [q, setQ] = useState("");
  const [org, setOrg] = useState("");
  const audit = useAudit({ org });

  const columns = useMemo<ColumnDef<AuditEntry, any>[]>(
    () => [
      {
        accessorKey: "ts",
        header: "Time",
        cell: ({ getValue }) => <span className="whitespace-nowrap text-xs text-muted-foreground">{fmtTime(getValue() as string)}</span>,
      },
      {
        accessorKey: "actor",
        header: "Actor",
        cell: ({ row }) => (
          <div className="flex items-center gap-1.5">
            <span className="font-mono text-xs">{row.original.actor}</span>
            {row.original.role && <Badge variant="muted">{row.original.role}</Badge>}
          </div>
        ),
      },
      {
        accessorKey: "action",
        header: "Action",
        cell: ({ getValue }) => <span className="text-xs">{String(getValue() ?? "")}</span>,
      },
      {
        accessorKey: "method",
        header: "Method",
        cell: ({ getValue }) => <Badge variant={methodVariant(String(getValue() ?? ""))}>{String(getValue() ?? "")}</Badge>,
      },
      {
        accessorKey: "path",
        header: "Path",
        cell: ({ getValue }) => <span className="font-mono text-xs text-muted-foreground">{String(getValue() ?? "")}</span>,
      },
      {
        accessorKey: "org",
        header: "Org",
        cell: ({ getValue }) => {
          const v = getValue() as string | undefined;
          return v ? <span className="font-mono text-xs">{v}</span> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "target_user",
        header: "Target",
        cell: ({ getValue }) => {
          const v = getValue() as string | undefined;
          return v ? <span className="font-mono text-xs">{v}</span> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "sql_redacted",
        header: "SQL",
        cell: ({ getValue }) => {
          const v = getValue() as string | undefined;
          return v ? (
            <code className="line-clamp-1 block max-w-xs break-all font-mono text-xs text-muted-foreground" title={v}>
              {v}
            </code>
          ) : (
            <span className="text-muted-foreground">—</span>
          );
        },
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ getValue }) => {
          const s = getValue() as number;
          return <Badge variant={statusVariant(s)}>{s}</Badge>;
        },
      },
    ],
    [],
  );

  if (!isAdmin) {
    return (
      <>
        <PageHeader title="Audit" />
        <EmptyState
          icon={<ShieldAlert className="h-6 w-6 text-warning" />}
          title="Admin only"
          description="The audit log is visible to admins only."
        />
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Audit"
        description="Admin API activity log. Server-side filtering by text and org."
        actions={
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search…" className="w-56 pl-8" />
            </div>
            <Input value={org} onChange={(e) => setOrg(e.target.value)} placeholder="Org" className="w-36" />
          </div>
        }
      />
      <PageBody>
        <Card className="overflow-hidden">
          {audit.isLoading ? (
            <TableSkeleton cols={9} />
          ) : audit.isError ? (
            <ErrorState error={audit.error} onRetry={() => audit.refetch()} />
          ) : (
            <DataTable
              data={audit.data ?? []}
              columns={columns}
              globalFilter={q}
              onGlobalFilterChange={setQ}
              initialSorting={[{ id: "ts", desc: true }]}
              empty={
                <EmptyState
                  icon={<ScrollText className="h-6 w-6" />}
                  title="No audit entries"
                  description="No audit records were returned (or the endpoint is not wired up)."
                />
              }
            />
          )}
        </Card>
      </PageBody>
    </>
  );
}
