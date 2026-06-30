import { useMemo, useState } from "react";
import { Activity, Ban, RefreshCw } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ProgressBar } from "@/components/ProgressBar";
import { AdminGate } from "@/components/AdminOnly";
import { EmptyState } from "@/components/states";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useCancelSession, useQueries, useSessions } from "@/hooks/useApi";
import { fmtInt } from "@/lib/format";
import { QueryDetailDialog } from "@/components/QueryDetailDialog";

export function Live() {
  const queries = useQueries();
  const sessions = useSessions();
  const cancel = useCancelSession();
  const [org, setOrg] = useState("");
  const [user, setUser] = useState("");
  const [detailPid, setDetailPid] = useState<number | null>(null);

  const matchOrg = (o?: string) => !org || (o ?? "").toLowerCase().includes(org.toLowerCase());
  const matchUser = (u?: string) => !user || (u ?? "").toLowerCase().includes(user.toLowerCase());

  const liveQueries = useMemo(
    () => (queries.data ?? []).filter((q) => matchOrg(q.org) && matchUser(q.user)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [queries.data, org, user],
  );
  const liveSessions = useMemo(
    () => (sessions.data ?? []).filter((s) => matchOrg(s.org) && matchUser(s.user)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [sessions.data, org, user],
  );

  return (
    <>
      <PageHeader
        title="Live"
        description="Running queries and active sessions across the cluster. Auto-refreshes every 3s."
        actions={
          <div className="flex items-center gap-2">
            <Input value={org} onChange={(e) => setOrg(e.target.value)} placeholder="Filter org…" className="w-40" />
            <Input value={user} onChange={(e) => setUser(e.target.value)} placeholder="Filter user…" className="w-40" />
            <Button
              variant="outline"
              size="icon"
              onClick={() => {
                queries.refetch();
                sessions.refetch();
              }}
              title="Refresh now"
            >
              <RefreshCw className="h-4 w-4" />
            </Button>
          </div>
        }
      />
      <PageBody>
        <Card className="mb-4">
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-4 w-4" /> Running queries
            </CardTitle>
            <Badge variant="secondary">{fmtInt(liveQueries.length)} active</Badge>
          </CardHeader>
          <CardContent className="p-0">
            {!queries.isSuccess && queries.isLoading ? (
              <p className="p-6 text-sm text-muted-foreground">Loading…</p>
            ) : liveQueries.length === 0 ? (
              <EmptyState
                icon={<Activity className="h-6 w-6" />}
                title="No running queries"
                description="No in-flight queries match the current filter."
              />
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>PID</TableHead>
                    <TableHead>Org</TableHead>
                    <TableHead>User</TableHead>
                    <TableHead>Worker</TableHead>
                    <TableHead>Protocol</TableHead>
                    <TableHead className="w-56">Progress</TableHead>
                    <TableHead className="text-right">Action</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {liveQueries.map((q) => {
                    // percentage is a flat 0..100 value; treat <=0 as "no
                    // progress reported yet" → indeterminate animation.
                    const pct = q.percentage > 0 ? q.percentage : undefined;
                    return (
                      <TableRow
                        key={`${q.pid}-${q.worker_id}`}
                        className="cursor-pointer"
                        onClick={() => setDetailPid(q.pid)}
                        title="View query detail"
                      >
                        <TableCell className="font-mono text-xs">{q.pid}</TableCell>
                        <TableCell className="font-mono text-xs">{q.org}</TableCell>
                        <TableCell className="font-mono text-xs">{q.user || "—"}</TableCell>
                        <TableCell className="font-mono text-xs">#{q.worker_id}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{q.protocol || "pg"}</Badge>
                        </TableCell>
                        <TableCell>
                          <ProgressBar percentage={pct} stalled={q.stalled} indeterminate={pct == null} />
                          <div className="mt-1 flex items-center justify-between text-[10px] text-muted-foreground">
                            <span>
                              {q.rows > 0
                                ? `${fmtInt(q.rows)}${q.total_rows > 0 ? ` / ${fmtInt(q.total_rows)}` : ""} rows`
                                : pct != null
                                  ? `${pct.toFixed(0)}%`
                                  : "running"}
                            </span>
                            {q.stalled && <span className="text-warning">stalled</span>}
                          </div>
                        </TableCell>
                        <TableCell className="text-right">
                          <AdminGate>
                            <Button
                              variant="ghost"
                              size="sm"
                              disabled={cancel.isPending}
                              onClick={(e) => {
                                e.stopPropagation();
                                cancel.mutate(q.pid);
                              }}
                            >
                              <Ban className="h-4 w-4 text-destructive" /> Cancel
                            </Button>
                          </AdminGate>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle>Sessions & connections</CardTitle>
            <Badge variant="secondary">{fmtInt(liveSessions.length)} sessions</Badge>
          </CardHeader>
          <CardContent className="p-0">
            {liveSessions.length === 0 ? (
              <EmptyState title="No active sessions" description="No sessions match the current filter." />
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>PID</TableHead>
                    <TableHead>Org</TableHead>
                    <TableHead>User</TableHead>
                    <TableHead>Worker</TableHead>
                    <TableHead>Protocol</TableHead>
                    <TableHead className="text-right">Action</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {liveSessions.map((s) => (
                    <TableRow key={`${s.pid}-${s.worker_id}`}>
                      <TableCell className="font-mono text-xs">{s.pid}</TableCell>
                      <TableCell className="font-mono text-xs">{s.org}</TableCell>
                      <TableCell className="font-mono text-xs">{s.user || "—"}</TableCell>
                      <TableCell className="font-mono text-xs">#{s.worker_id}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{s.protocol || "pg"}</Badge>
                      </TableCell>
                      <TableCell className="text-right">
                        <AdminGate>
                          <Button variant="ghost" size="sm" disabled={cancel.isPending} onClick={() => cancel.mutate(s.pid)}>
                            <Ban className="h-4 w-4 text-destructive" /> Cancel
                          </Button>
                        </AdminGate>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </PageBody>
      <QueryDetailDialog pid={detailPid} onClose={() => setDetailPid(null)} onCancel={(pid) => cancel.mutate(pid)} />
    </>
  );
}
