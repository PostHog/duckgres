import { useMemo, useState } from "react";
import { AlertTriangle, Ban, Database, Gauge, Hourglass, Timer } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { StatCard } from "@/components/StatCard";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { OrgRef } from "@/components/OrgRef";
import { AdminGate } from "@/components/AdminOnly";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useKillTrinoQuery, useOrgLabels, useTrinoQueries, useTrinoStatus } from "@/hooks/useApi";
import { fmtBytes, fmtCompact, fmtDurationMs } from "@/lib/format";
import {
  isActiveTrinoQuery,
  summarizeTrinoQueries,
  trinoQueryFlag,
  trinoScanEfficiency,
  trinoStateVariant,
  trinoUnavailableMessage,
  trinoUnavailableReason,
  type TrinoQueryFlag,
} from "@/lib/trino";
import type { TrinoQuery } from "@/types/api";

const FLAG_LABEL: Record<TrinoQueryFlag, string> = {
  failed: "failed",
  blocked: "blocked",
  queued: "queued",
  long_running: "long running",
};

const FLAG_VARIANT: Record<TrinoQueryFlag, "destructive" | "warning"> = {
  failed: "destructive",
  blocked: "destructive",
  queued: "warning",
  long_running: "warning",
};

// KillDialog makes the operator name a reason, because that text is
// delivered to the TENANT as their query's failure message. An unexplained
// cancellation turns into a support ticket; a reason turns into an answer.
function KillDialog({
  query,
  orgLabel,
  onClose,
}: {
  query: TrinoQuery;
  orgLabel: string;
  onClose: () => void;
}) {
  const [reason, setReason] = useState("");
  const kill = useKillTrinoQuery();
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Kill query</DialogTitle>
          <DialogDescription>
            Fails <span className="font-mono">{query.query_id}</span>
            {orgLabel ? <> for {orgLabel}</> : null}. The reason below is delivered to the tenant as
            the query&apos;s error message.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <pre className="max-h-32 overflow-auto rounded bg-muted p-2 font-mono text-xs">
            {query.query}
          </pre>
          <Input
            autoFocus
            placeholder="Reason (shown to the tenant)"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
          />
          {kill.isError && (
            <p className="text-xs text-destructive">{(kill.error as Error).message}</p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="destructive"
            disabled={kill.isPending}
            onClick={() =>
              kill.mutate(
                { id: query.query_id, reason: reason.trim() },
                { onSuccess: onClose },
              )
            }
          >
            {kill.isPending ? "Killing…" : "Kill query"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export function TrinoQueries() {
  // active=true is the live view: the states an operator can still act on.
  // Unchecking it brings in recently-finished queries, which is the closest
  // thing to history the coordinator holds in memory.
  const [activeOnly, setActiveOnly] = useState(true);
  const [orgFilter, setOrgFilter] = useState("");
  const [killing, setKilling] = useState<TrinoQuery | null>(null);

  const status = useTrinoStatus();
  const queries = useTrinoQueries({ active: activeOnly });
  const orgLabels = useOrgLabels();

  const rows = useMemo(() => {
    const all = queries.data?.queries ?? [];
    if (orgFilter === "") return all;
    const needle = orgFilter.toLowerCase();
    return all.filter(
      (q) =>
        q.org.toLowerCase().includes(needle) ||
        q.principal.toLowerCase().includes(needle) ||
        (orgLabels.get(q.org) ?? "").toLowerCase().includes(needle),
    );
  }, [queries.data, orgFilter, orgLabels]);

  const summary = useMemo(() => summarizeTrinoQueries(rows), [rows]);
  const reason = trinoUnavailableReason(status.data);

  return (
    <>
      <PageHeader
        title="Trino queries"
        description={
          status.data?.cell.id
            ? `Live queries across cell ${status.data.cell.id}. SQL text is redacted by the control plane.`
            : "Live queries across the Trino cell."
        }
        actions={
          <div className="flex items-center gap-2">
            <Input
              className="h-8 w-56"
              placeholder="Filter by org or principal…"
              value={orgFilter}
              onChange={(e) => setOrgFilter(e.target.value)}
            />
            <Button
              size="sm"
              variant={activeOnly ? "default" : "outline"}
              onClick={() => setActiveOnly((v) => !v)}
            >
              {activeOnly ? "Active only" : "Including finished"}
            </Button>
          </div>
        }
      />
      <PageBody>
        {reason && (
          <Card className="mb-4 border-warning/40">
            <CardContent className="flex items-start gap-2 p-4 text-sm">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-warning" />
              <span>{trinoUnavailableMessage(reason)}</span>
            </CardContent>
          </Card>
        )}

        <div className="mb-4 grid grid-cols-2 gap-3 lg:grid-cols-5">
          <StatCard label="Running" value={fmtCompact(summary.running)} icon={<Gauge className="h-4 w-4" />} />
          <StatCard
            label="Queued"
            value={fmtCompact(summary.queued)}
            accent={summary.queued > 0 ? "warning" : "default"}
            hint="waiting on a resource group"
            icon={<Hourglass className="h-4 w-4" />}
          />
          <StatCard
            label="Blocked"
            value={fmtCompact(summary.blocked)}
            accent={summary.blocked > 0 ? "destructive" : "default"}
            hint="every driver waiting on I/O"
            icon={<Ban className="h-4 w-4" />}
          />
          <StatCard
            label="Longest"
            value={fmtDurationMs(summary.longestMs)}
            hint="in-flight only"
            icon={<Timer className="h-4 w-4" />}
          />
          <StatCard
            label="Scanned"
            value={fmtBytes(summary.scannedBytes)}
            hint="physical input, listed queries"
            icon={<Database className="h-4 w-4" />}
          />
        </div>

        <Card>
          <CardHeader>
            <CardTitle>{activeOnly ? "Active queries" : "Queries held by the coordinator"}</CardTitle>
          </CardHeader>
          <CardContent>
            {queries.isLoading ? (
              <TableSkeleton cols={8} />
            ) : queries.isError ? (
              <ErrorState error={queries.error} onRetry={() => void queries.refetch()} />
            ) : rows.length === 0 ? (
              <EmptyState
                title={activeOnly ? "No active queries" : "No queries"}
                description="The coordinator is holding nothing that matches this filter."
              />
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>State</TableHead>
                    <TableHead>Org</TableHead>
                    <TableHead>Query</TableHead>
                    <TableHead className="text-right">Elapsed</TableHead>
                    <TableHead className="text-right">CPU</TableHead>
                    <TableHead className="text-right">Scanned</TableHead>
                    <TableHead className="text-right">Peak mem</TableHead>
                    <TableHead />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((q) => {
                    const flag = trinoQueryFlag(q);
                    const perRow = trinoScanEfficiency(q);
                    return (
                      <TableRow key={q.query_id}>
                        <TableCell className="whitespace-nowrap">
                          <Badge variant={trinoStateVariant(q.state)}>{q.state}</Badge>
                          {flag && (
                            <Badge className="ml-1" variant={FLAG_VARIANT[flag]}>
                              {FLAG_LABEL[flag]}
                            </Badge>
                          )}
                        </TableCell>
                        <TableCell className="max-w-[14rem]">
                          {q.org ? (
                            <OrgRef id={q.org} label={orgLabels.get(q.org)} />
                          ) : (
                            // No org means a control-plane principal: the
                            // reconcile loop's DDL or this console's own
                            // reads, both tagged by X-Trino-Source.
                            <span className="text-xs text-muted-foreground">
                              {q.source || q.principal || "—"}
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="max-w-[24rem]">
                          <span className="block truncate font-mono text-xs" title={q.query}>
                            {q.query}
                          </span>
                          <span className="block truncate text-[11px] text-muted-foreground">
                            {q.resource_group || "unassigned"}
                            {q.progress_percentage !== null
                              ? ` · ${q.progress_percentage.toFixed(0)}%`
                              : ""}
                            {perRow !== null ? ` · ${fmtBytes(perRow)}/row` : ""}
                          </span>
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {fmtDurationMs(q.elapsed_ms)}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {fmtDurationMs(q.cpu_ms)}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {fmtBytes(q.physical_input_bytes)}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {fmtBytes(q.peak_memory_bytes)}
                        </TableCell>
                        <TableCell className="text-right">
                          {isActiveTrinoQuery(q) && (
                            <AdminGate reason="Killing a query requires the admin role">
                              <Button size="sm" variant="ghost" onClick={() => setKilling(q)}>
                                Kill
                              </Button>
                            </AdminGate>
                          )}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </PageBody>

      {killing && (
        <KillDialog
          query={killing}
          orgLabel={killing.org ? (orgLabels.get(killing.org) ?? killing.org) : ""}
          onClose={() => setKilling(null)}
        />
      )}
    </>
  );
}
