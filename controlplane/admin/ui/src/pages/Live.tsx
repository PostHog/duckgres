import { useMemo, useState } from "react";
import { Activity, Ban, CircleSlash, RefreshCw, Zap } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ProgressBar } from "@/components/ProgressBar";
import { AdminGate } from "@/components/AdminOnly";
import { EmptyState } from "@/components/states";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useCancelSession, useKillUserSessions, useQueries, useSessions } from "@/hooks/useApi";
import { fmtAge, fmtDurationMs, fmtInt, fmtTime } from "@/lib/format";
import { idleInTransaction, isIdleSession, sessionStateLabel } from "@/lib/session";
import { compareByStarted, compareByWorker } from "@/lib/liveSort";
import { QueryDetailDialog } from "@/components/QueryDetailDialog";

export function Live() {
  const queries = useQueries();
  const sessions = useSessions();
  const cancel = useCancelSession();
  const killUser = useKillUserSessions();
  const [org, setOrg] = useState("");
  const [user, setUser] = useState("");
  const [detailWid, setDetailWid] = useState<number | null>(null);
  const [killOpen, setKillOpen] = useState(false);
  // The kill endpoint targets an EXACT (org, user); the filter boxes are
  // substring matches, so only offer it once both are set, and pass them verbatim.
  const canKillUser = org.trim() !== "" && user.trim() !== "";

  const matchOrg = (o?: string) => !org || (o ?? "").toLowerCase().includes(org.toLowerCase());
  const matchUser = (u?: string) => !user || (u ?? "").toLowerCase().includes(user.toLowerCase());

  // Sort deterministically so the tables don't reshuffle on every 3s poll (the
  // API returns rows in non-deterministic order). Queries by start time (oldest
  // first); sessions by worker id (they carry no start time).
  const liveQueries = useMemo(
    () => (queries.data ?? []).filter((q) => matchOrg(q.org) && matchUser(q.user)).sort(compareByStarted),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [queries.data, org, user],
  );
  const liveSessions = useMemo(
    () => (sessions.data ?? []).filter((s) => matchOrg(s.org) && matchUser(s.user)).sort(compareByWorker),
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
            <AdminGate>
              <Button
                variant="outline"
                size="sm"
                disabled={!canKillUser}
                title={canKillUser ? "Kill all sessions for this org + user" : "Set both org and user filters first"}
                onClick={() => setKillOpen(true)}
              >
                <Zap className="h-4 w-4 text-destructive" /> Kill user
              </Button>
            </AdminGate>
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

      <Dialog open={killOpen} onOpenChange={setKillOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Kill all sessions for "{user}"?</DialogTitle>
            <DialogDescription>
              Immediately terminates every active session and in-flight query for user{" "}
              <span className="font-mono">{user}</span> in org <span className="font-mono">{org}</span> across
              all control-plane replicas. The user can reconnect right away. To also block new connections,
              use Disable on the Users page.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setKillOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              disabled={killUser.isPending || !canKillUser}
              onClick={async () => {
                await killUser.mutateAsync({ org: org.trim(), username: user.trim() });
                setKillOpen(false);
              }}
            >
              {killUser.isPending ? "Killing…" : "Kill sessions"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
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
                    <TableHead>Started</TableHead>
                    <TableHead>Worker</TableHead>
                    <TableHead>Protocol</TableHead>
                    <TableHead>State</TableHead>
                    <TableHead>Duration</TableHead>
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
                        className="cursor-pointer [&>td]:py-1.5"
                        onClick={() => setDetailWid(q.worker_id)}
                        title="View query detail"
                      >
                        <TableCell className="font-mono text-xs">{q.pid}</TableCell>
                        <TableCell className="font-mono text-xs">{q.org}</TableCell>
                        <TableCell className="font-mono text-xs">{q.user || "—"}</TableCell>
                        <TableCell className="font-mono text-xs tabular-nums text-muted-foreground">
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span>{fmtAge(q.started_at)}</span>
                            </TooltipTrigger>
                            <TooltipContent>{fmtTime(q.started_at)}</TooltipContent>
                          </Tooltip>
                        </TableCell>
                        <TableCell className="font-mono text-xs">#{q.worker_id}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{q.protocol || "pg"}</Badge>
                        </TableCell>
                        <TableCell>
                          {isIdleSession(q.state) ? (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span
                                  className={idleInTransaction(q.state) ? "text-warning" : "text-muted-foreground"}
                                  aria-label={`no in-flight query — ${sessionStateLabel(q.state)}`}
                                >
                                  <CircleSlash className="h-4 w-4" />
                                </span>
                              </TooltipTrigger>
                              <TooltipContent>No in-flight query — {sessionStateLabel(q.state)}</TooltipContent>
                            </Tooltip>
                          ) : (
                            <span className="text-muted-foreground/30">·</span>
                          )}
                        </TableCell>
                        <TableCell className="font-mono text-xs tabular-nums">{fmtDurationMs(q.elapsed_ms)}</TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <ProgressBar
                              percentage={pct}
                              stalled={q.stalled}
                              indeterminate={pct == null}
                              className="flex-1"
                            />
                            <span className="whitespace-nowrap text-[10px] text-muted-foreground">
                              {q.rows > 0
                                ? `${fmtInt(q.rows)}${q.total_rows > 0 ? ` / ${fmtInt(q.total_rows)}` : ""} rows`
                                : pct != null
                                  ? `${pct.toFixed(0)}%`
                                  : "running"}
                            </span>
                            {q.stalled && <span className="text-[10px] text-warning">stalled</span>}
                          </div>
                        </TableCell>
                        <TableCell className="text-right">
                          <AdminGate>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="-my-1 h-6"
                              disabled={cancel.isPending}
                              onClick={(e) => {
                                e.stopPropagation();
                                cancel.mutate(q.worker_id);
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
                    <TableRow key={`${s.pid}-${s.worker_id}`} className="[&>td]:py-1.5">
                      <TableCell className="font-mono text-xs">{s.pid}</TableCell>
                      <TableCell className="font-mono text-xs">{s.org}</TableCell>
                      <TableCell className="font-mono text-xs">{s.user || "—"}</TableCell>
                      <TableCell className="font-mono text-xs">#{s.worker_id}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{s.protocol || "pg"}</Badge>
                      </TableCell>
                      <TableCell className="text-right">
                        <AdminGate>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="-my-1 h-6"
                            disabled={cancel.isPending}
                            onClick={() => cancel.mutate(s.worker_id)}
                          >
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
      <QueryDetailDialog
        workerId={detailWid}
        onClose={() => setDetailWid(null)}
        onCancel={(workerId) => cancel.mutate(workerId)}
      />
    </>
  );
}
