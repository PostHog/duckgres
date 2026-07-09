import { Link } from "react-router-dom";
import { ArrowRight } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent } from "@/components/ui/card";
import { StateBadge } from "@/components/StateBadge";
import { ErrorState, LoadingState } from "@/components/states";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { fmtTime } from "@/lib/format";
import { useAllReshards } from "@/hooks/useApi";
import type { ReshardOperation } from "@/types/api";

function describeStore(kind: string, shard: string, endpoint: string): string {
  if (kind === "cnpg-shard") return shard ? `cnpg ${shard}` : "cnpg";
  return endpoint ? `external ${endpoint}` : "external";
}

function duration(from: string | null, to: string | null): string {
  if (!from) return "—";
  const end = to ? new Date(to).getTime() : Date.now();
  const s = Math.round((end - new Date(from).getTime()) / 1000);
  if (s < 0) return "—";
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

// Global list of metadata-store reshard operations across every org, newest
// first. Each row links to the operation page (live log, cancel). Starting a
// reshard lives on the org detail page — this page is the fleet overview.
export function Reshards() {
  const reshards = useAllReshards();
  const ops = reshards.data ?? [];

  return (
    <>
      <PageHeader
        title="Reshards"
        description="Metadata-store migrations across all orgs. Start one from an org's page."
      />
      <PageBody>
        <Card>
          <CardContent className="pt-4">
            {reshards.isLoading ? (
              <LoadingState />
            ) : reshards.isError ? (
              <ErrorState error={reshards.error} onRetry={() => reshards.refetch()} />
            ) : ops.length === 0 ? (
              <p className="py-8 text-center text-sm text-muted-foreground">
                No reshard operations yet. Start one from an org's detail page (Danger zone →
                "Reshard metadata store…").
              </p>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Op</TableHead>
                    <TableHead>Org</TableHead>
                    <TableHead>Migration</TableHead>
                    <TableHead>State</TableHead>
                    <TableHead>Step</TableHead>
                    <TableHead>Started</TableHead>
                    <TableHead>Runtime</TableHead>
                    <TableHead>Maintenance</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {ops.map((op: ReshardOperation) => (
                    <TableRow key={op.id}>
                      <TableCell>
                        <Link to={`/reshards/${op.id}`} className="font-mono text-xs text-primary hover:underline">
                          #{op.id}
                        </Link>
                      </TableCell>
                      <TableCell>
                        <Link
                          to={`/orgs/${encodeURIComponent(op.org_id)}`}
                          className="font-mono text-xs hover:underline"
                        >
                          {op.org_id}
                        </Link>
                      </TableCell>
                      <TableCell>
                        <span className="flex items-center gap-1 font-mono text-xs">
                          {describeStore(op.source_kind, op.from_shard, op.source_endpoint)}
                          <ArrowRight className="h-3 w-3 text-muted-foreground" />
                          {describeStore(op.target_kind, op.to_shard, op.target_endpoint)}
                        </span>
                      </TableCell>
                      <TableCell>
                        <StateBadge state={op.state} />
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {op.state === "running" || op.state === "pending" ? op.step || "—" : "—"}
                      </TableCell>
                      <TableCell className="text-xs">{op.started_at ? fmtTime(op.started_at) : "—"}</TableCell>
                      <TableCell className="text-xs">{duration(op.started_at, op.finished_at)}</TableCell>
                      <TableCell className="text-xs">
                        {op.blocked_at ? duration(op.blocked_at, op.unblocked_at) : "—"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}
