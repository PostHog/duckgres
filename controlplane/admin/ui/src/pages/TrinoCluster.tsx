import { useMemo } from "react";
import { Link } from "react-router-dom";
import { AlertTriangle, Boxes, Cpu, Network, ServerCog } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { StatCard } from "@/components/StatCard";
import { StateBadge } from "@/components/StateBadge";
import { EmptyState, TableSkeleton } from "@/components/states";
import { OrgRef } from "@/components/OrgRef";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useOrgLabels, useTrinoNodes, useTrinoOrgs, useTrinoStatus } from "@/hooks/useApi";
import { fmtDurationMs, fmtInt, fmtPercent, fmtTime } from "@/lib/format";
import {
  summarizeTrinoNodes,
  trinoOrgsNeedingAttention,
  trinoUnavailableMessage,
  trinoUnavailableReason,
} from "@/lib/trino";

export function TrinoCluster() {
  const status = useTrinoStatus();
  const nodes = useTrinoNodes();
  const orgs = useTrinoOrgs();
  const orgLabels = useOrgLabels();

  const nodeHealth = useMemo(() => summarizeTrinoNodes(nodes.data?.nodes ?? []), [nodes.data]);
  const orgRows = useMemo(() => orgs.data?.orgs ?? [], [orgs.data]);
  const needAttention = useMemo(() => trinoOrgsNeedingAttention(orgRows), [orgRows]);
  const reason = trinoUnavailableReason(status.data);
  const server = status.data?.server;

  return (
    <>
      <PageHeader
        title="Trino cell"
        description={
          status.data?.cell.id
            ? `${status.data.cell.id} · ${status.data.cell.coordinator_url}`
            : "The shared multi-tenant Trino cell."
        }
      />
      <PageBody>
        {reason && (
          <Card className="mb-4 border-warning/40">
            <CardContent className="flex items-start gap-2 p-4 text-sm">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-warning" />
              <span>
                {trinoUnavailableMessage(reason)}
                {status.data?.error && (
                  <span className="mt-1 block font-mono text-[11px] text-muted-foreground">
                    {status.data.error}
                  </span>
                )}
              </span>
            </CardContent>
          </Card>
        )}

        <div className="mb-4 grid grid-cols-2 gap-3 lg:grid-cols-5">
          <StatCard
            label="Coordinator"
            value={server?.version ?? "—"}
            hint={
              server
                ? server.starting
                  ? "starting — queries are rejected"
                  : `${server.environment} · up ${fmtDurationMs(server.uptime_ms)}`
                : "unreachable"
            }
            accent={server ? (server.starting ? "warning" : "success") : "destructive"}
            icon={<ServerCog className="h-4 w-4" />}
          />
          <StatCard
            label="Nodes"
            value={fmtInt(nodeHealth.total)}
            hint={
              nodeHealth.failed > 0 || nodeHealth.degraded > 0
                ? `${nodeHealth.failed} failed · ${nodeHealth.degraded} degraded`
                : "all healthy"
            }
            accent={nodeHealth.failed > 0 ? "destructive" : nodeHealth.degraded > 0 ? "warning" : "success"}
            icon={<Network className="h-4 w-4" />}
          />
          <StatCard
            label="Running"
            value={fmtInt(status.data?.queries_by_state?.RUNNING ?? 0)}
            hint={`${fmtInt(status.data?.queries_by_state?.QUEUED ?? 0)} queued`}
            icon={<Cpu className="h-4 w-4" />}
          />
          <StatCard
            label="Blocked"
            value={fmtInt(status.data?.blocked_queries ?? 0)}
            hint="waiting on metadata or S3"
            accent={(status.data?.blocked_queries ?? 0) > 0 ? "destructive" : "default"}
          />
          <StatCard
            label="Tenants"
            value={fmtInt(status.data?.total_orgs ?? 0)}
            hint={
              needAttention.length > 0
                ? `${needAttention.length} need attention`
                : "all provisioned"
            }
            accent={needAttention.length > 0 ? "warning" : "success"}
            icon={<Boxes className="h-4 w-4" />}
          />
        </div>

        <Card className="mb-4">
          <CardHeader>
            <CardTitle>Tenants on this cell</CardTitle>
          </CardHeader>
          <CardContent>
            {orgs.isLoading ? (
              <TableSkeleton cols={7} />
            ) : orgRows.length === 0 ? (
              <EmptyState
                title="No Trino-enabled orgs"
                description="Enable Trino for an org to give it a catalog on this cell."
              />
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Org</TableHead>
                    <TableHead>Catalog</TableHead>
                    <TableHead>Tier</TableHead>
                    <TableHead>State</TableHead>
                    <TableHead className="text-right">Running</TableHead>
                    <TableHead className="text-right">Queued</TableHead>
                    <TableHead>Last reconcile</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {orgRows.map((o) => (
                    <TableRow key={o.org}>
                      <TableCell className="max-w-[14rem]">
                        <Link to={`/orgs/${encodeURIComponent(o.org)}`} className="hover:underline">
                          <OrgRef id={o.org} label={orgLabels.get(o.org)} copyable={false} />
                        </Link>
                      </TableCell>
                      <TableCell className="font-mono text-xs">{o.catalog}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{o.tier || "default"}</Badge>
                      </TableCell>
                      <TableCell>
                        <StateBadge state={o.state} />
                        {o.status_message && (
                          // The reconcile detail is the actionable part of a
                          // failed provision — it names the step that broke.
                          <span
                            className="mt-0.5 block max-w-[22rem] truncate text-[11px] text-muted-foreground"
                            title={o.status_message}
                          >
                            {o.status_message}
                          </span>
                        )}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {orgs.data?.available ? fmtInt(o.running_queries) : "—"}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {orgs.data?.available ? fmtInt(o.queued_queries) : "—"}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {o.failed_at ? fmtTime(o.failed_at) : o.ready_at ? fmtTime(o.ready_at) : "—"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Coordinator&apos;s view of the fleet</CardTitle>
          </CardHeader>
          <CardContent>
            {nodes.isLoading ? (
              <TableSkeleton cols={5} />
            ) : (nodes.data?.nodes ?? []).length === 0 ? (
              <EmptyState
                title="No nodes reported"
                description="The coordinator's failure detector has not reported any peers."
              />
            ) : (
              <>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Node</TableHead>
                      <TableHead>Health</TableHead>
                      <TableHead className="text-right">Failure ratio</TableHead>
                      <TableHead className="text-right">Recent requests</TableHead>
                      <TableHead>Seen for</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(nodes.data?.nodes ?? []).map((n) => (
                      <TableRow key={n.uri}>
                        <TableCell className="font-mono text-xs">{n.uri}</TableCell>
                        <TableCell>
                          {n.failed ? (
                            <Badge variant="destructive">failed</Badge>
                          ) : n.recent_failure_ratio > 0 ? (
                            <Badge variant="warning">degraded</Badge>
                          ) : (
                            <Badge variant="success">healthy</Badge>
                          )}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {fmtPercent(n.recent_failure_ratio * 100)}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {fmtInt(Math.round(n.recent_successes + n.recent_failures))}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {fmtDurationMs(n.age_ms)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                <p className="mt-3 text-[11px] text-muted-foreground">
                  Trino&apos;s <code>/v1/node</code> reports heartbeat health keyed by URI and
                  carries no node id or version, so worker version skew is not visible here — check
                  the running images on the Nodes page after a rollout.
                </p>
              </>
            )}
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}
