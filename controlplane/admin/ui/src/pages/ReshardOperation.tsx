import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { ArrowLeft, ArrowRight, OctagonX } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { StateBadge } from "@/components/StateBadge";
import { AdminGate } from "@/components/AdminOnly";
import { ErrorState, LoadingState } from "@/components/states";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { fmtBytes, fmtInt, fmtTime } from "@/lib/format";
import { useCancelReshard, useReshard, useReshardLog } from "@/hooks/useApi";
import type { ReshardOperation as ReshardOp } from "@/types/api";

// Human labels for the runner's step machine.
const STEP_LABELS: Record<string, string> = {
  blocking: "Blocking new connections…",
  draining: "Waiting for all connections to drain…",
  pausing_compaction: "Pausing compaction…",
  cutover: "Cutting over the duckling…",
  copying: "Copying catalog tables…",
  verifying: "Verifying the copy…",
  cleaning_up: "Cleaning up the source…",
  finalizing: "Finalizing…",
};

function describeStore(kind: string, shard: string, endpoint: string): string {
  if (kind === "cnpg-shard") return shard ? `cnpg ${shard}` : "cnpg";
  return endpoint ? `external ${endpoint}` : "external";
}

function duration(from: string | null, to: string | null): string {
  if (!from) return "—";
  const end = to ? new Date(to).getTime() : Date.now();
  const ms = end - new Date(from).getTime();
  if (ms < 0) return "—";
  const s = Math.round(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (h > 0) return `${h}h ${m}m ${s % 60}s`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

export function ReshardOperation() {
  const { opId = "" } = useParams();
  const id = Number(opId) || null;
  const op = useReshard(id);
  const entries = useReshardLog(id, op.data?.state);
  const cancel = useCancelReshard();
  const [confirmCancel, setConfirmCancel] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [msg, setMsg] = useState<string | null>(null);
  const logRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (autoScroll && logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [entries, autoScroll]);

  if (op.isLoading) {
    return (
      <>
        <PageHeader title={`Reshard #${opId}`} />
        <PageBody>
          <LoadingState />
        </PageBody>
      </>
    );
  }
  if (op.isError || !op.data) {
    return (
      <>
        <PageHeader title={`Reshard #${opId}`} />
        <PageBody>
          <ErrorState error={op.error} onRetry={() => op.refetch()} />
        </PageBody>
      </>
    );
  }

  const o: ReshardOp = op.data;
  const running = o.state === "pending" || o.state === "running";
  const stepLabel = running ? (STEP_LABELS[o.step] ?? o.step) : undefined;

  const doCancel = async () => {
    setMsg(null);
    try {
      await cancel.mutateAsync(o.id);
      setMsg("Cancel requested — the runner rolls back from the current step.");
    } catch (e) {
      setMsg(e instanceof Error ? e.message : "cancel failed");
    }
    setConfirmCancel(false);
  };

  return (
    <>
      <PageHeader
        title={`Reshard #${o.id}`}
        actions={
          <div className="flex items-center gap-2">
            {running && (
              <AdminGate>
                <Button variant="destructive" size="sm" onClick={() => setConfirmCancel(true)}>
                  <OctagonX className="h-4 w-4" /> Cancel
                </Button>
              </AdminGate>
            )}
            <Button variant="outline" size="sm" asChild>
              <Link to={`/orgs/${encodeURIComponent(o.org_id)}`}>
                <ArrowLeft className="h-4 w-4" /> {o.org_id}
              </Link>
            </Button>
          </div>
        }
      />
      <PageBody>
        <div className="space-y-4">
          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-3">
                <Link to={`/orgs/${encodeURIComponent(o.org_id)}`} className="hover:underline">
                  {o.org_id}
                </Link>
                <span className="flex items-center gap-2 font-mono text-xs font-normal text-muted-foreground">
                  {describeStore(o.source_kind, o.from_shard, o.source_endpoint)}
                  <ArrowRight className="h-3 w-3" />
                  {describeStore(o.target_kind, o.to_shard, o.target_endpoint)}
                </span>
              </CardTitle>
              <div className="flex items-center gap-2">
                <StateBadge state={o.state} />
                {stepLabel && <span className="text-xs text-muted-foreground">{stepLabel}</span>}
              </div>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                <HeaderStat label="Started" value={o.started_at ? fmtTime(o.started_at) : "—"} />
                <HeaderStat label="Finished" value={o.finished_at ? fmtTime(o.finished_at) : "—"} />
                <HeaderStat label="Total runtime" value={duration(o.started_at, o.finished_at)} />
                <HeaderStat
                  label="Maintenance mode"
                  value={o.blocked_at ? duration(o.blocked_at, o.unblocked_at) : "not entered"}
                  hint={
                    o.blocked_at
                      ? `${fmtTime(o.blocked_at)} → ${o.unblocked_at ? fmtTime(o.unblocked_at) : "ongoing"}`
                      : undefined
                  }
                />
                <HeaderStat label="Tables copied" value={fmtInt(o.tables_copied)} />
                <HeaderStat label="Rows copied" value={fmtInt(o.rows_copied)} />
                <HeaderStat label="Bytes copied" value={fmtBytes(o.bytes_copied)} />
                <HeaderStat label="Runner" value={o.runner_cp || "—"} mono />
              </div>
              {o.error && (
                <p className="mt-3 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 font-mono text-xs text-destructive">
                  {o.error}
                </p>
              )}
              {msg && <p className="mt-2 text-xs text-muted-foreground">{msg}</p>}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle>Operation log</CardTitle>
              <div className="flex items-center gap-2">
                <Label htmlFor="autoscroll" className="text-xs text-muted-foreground">
                  Auto-scroll
                </Label>
                <Switch id="autoscroll" checked={autoScroll} onCheckedChange={setAutoScroll} />
              </div>
            </CardHeader>
            <CardContent>
              <div
                ref={logRef}
                className="max-h-[32rem] overflow-auto rounded-md border border-border bg-background/60 p-3 font-mono text-xs leading-5"
              >
                {entries.length === 0 ? (
                  <span className="text-muted-foreground">waiting for log entries…</span>
                ) : (
                  entries.map((e) => (
                    <div key={e.id} className="whitespace-pre-wrap">
                      <span className="text-muted-foreground">{fmtTime(e.ts)} </span>
                      <span
                        className={
                          e.level === "error"
                            ? "text-destructive"
                            : e.level === "warn"
                              ? "text-warning"
                              : ""
                        }
                      >
                        {e.message}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </PageBody>

      <Dialog open={confirmCancel} onOpenChange={setConfirmCancel}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Cancel reshard #{o.id}?</DialogTitle>
            <DialogDescription>
              The runner rolls back from the current step and returns the org to service on its
              original metadata store. A cnpg→external operation that already cut over recovers by
              copying back instead.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setConfirmCancel(false)}>
              Keep running
            </Button>
            <Button variant="destructive" size="sm" onClick={doCancel} disabled={cancel.isPending}>
              {cancel.isPending ? "Cancelling…" : "Cancel operation"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function HeaderStat({
  label,
  value,
  hint,
  mono,
}: {
  label: string;
  value: string;
  hint?: string;
  mono?: boolean;
}) {
  return (
    <div className="rounded-md border border-border bg-background/40 p-2">
      <div className="text-[10px] uppercase text-muted-foreground">{label}</div>
      <div className={mono ? "truncate font-mono text-xs" : "text-sm"} title={hint ?? value}>
        {value}
      </div>
      {hint && <div className="truncate text-[10px] text-muted-foreground">{hint}</div>}
    </div>
  );
}
