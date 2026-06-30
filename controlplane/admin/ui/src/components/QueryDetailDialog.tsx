import { Ban } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ProgressBar } from "@/components/ProgressBar";
import { AdminGate } from "@/components/AdminOnly";
import { useQueryDetail } from "@/hooks/useApi";
import { fmtInt, fmtTime } from "@/lib/format";

// Human-readable elapsed from a millisecond duration.
function fmtElapsed(ms: number): string {
  if (!ms || ms < 0) return "—";
  const s = ms / 1000;
  if (s < 1) return `${ms}ms`;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  const rem = Math.round(s % 60);
  if (m < 60) return `${m}m ${rem}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function Field({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={mono ? "font-mono text-xs break-all" : "text-xs"}>{value ?? "—"}</span>
    </div>
  );
}

// Detail view for a single in-flight query, loaded on demand by pid. The SQL is
// redacted server-side (usersecrets.RedactForLog) — credentials in CREATE
// SECRET statements never reach the UI. Detail is scoped to the control-plane
// replica that owns the connection, so a 404 renders a clear notice.
export function QueryDetailDialog({
  pid,
  onClose,
  onCancel,
}: {
  pid: number | null;
  onClose: () => void;
  onCancel: (pid: number) => void;
}) {
  const detail = useQueryDetail(pid);
  const d = detail.data;
  const notFound = detail.isError && (detail.error as { status?: number })?.status === 404;
  const pct = d && d.percentage > 0 ? d.percentage : undefined;

  return (
    <Dialog open={pid != null} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            Query detail
            {d && <Badge variant="outline">PID {d.pid}</Badge>}
            {d && <Badge variant={d.state.startsWith("active") ? "default" : "secondary"}>{d.state}</Badge>}
          </DialogTitle>
          <DialogDescription>
            Live, redacted view of one in-flight query. Refreshes every few seconds while open.
          </DialogDescription>
        </DialogHeader>

        {detail.isLoading ? (
          <p className="py-8 text-center text-sm text-muted-foreground">Loading…</p>
        ) : notFound ? (
          <p className="py-8 text-center text-sm text-muted-foreground">
            This query has finished, or its control-plane replica is briefly unreachable.
          </p>
        ) : !d ? (
          <p className="py-8 text-center text-sm text-muted-foreground">No detail available.</p>
        ) : (
          <div className="space-y-4">
            <div>
              <span className="text-[10px] uppercase tracking-wide text-muted-foreground">SQL</span>
              <pre className="mt-1 max-h-64 overflow-auto rounded-md bg-muted p-3 text-xs leading-relaxed whitespace-pre-wrap break-words">
                {d.query || "(no active statement)"}
              </pre>
            </div>

            <div>
              <ProgressBar percentage={pct} stalled={d.stalled} indeterminate={d.state.startsWith("active") && pct == null} />
              <div className="mt-1 flex items-center justify-between text-[10px] text-muted-foreground">
                <span>
                  {d.rows > 0
                    ? `${fmtInt(d.rows)}${d.total_rows > 0 ? ` / ${fmtInt(d.total_rows)}` : ""} rows`
                    : pct != null
                      ? `${pct.toFixed(0)}%`
                      : d.state.startsWith("active")
                        ? "running"
                        : "idle"}
                </span>
                {d.stalled && <span className="text-warning">stalled</span>}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3">
              <Field label="Org" value={d.org} mono />
              <Field label="User" value={d.user || "—"} mono />
              <Field label="Protocol" value={d.protocol || "pg"} />
              <Field label="Worker" value={`#${d.worker_id}`} mono />
              <Field label="Worker pod" value={d.worker_pod || "—"} mono />
              <Field label="Database" value={d.database || "—"} mono />
              <Field label="Application" value={d.application_name || "—"} mono />
              <Field
                label="Client"
                value={d.client_addr ? `${d.client_addr}${d.client_port ? `:${d.client_port}` : ""}` : "—"}
                mono
              />
              <Field label="Elapsed" value={fmtElapsed(d.elapsed_ms)} />
              <Field label="Query start" value={d.query_start ? fmtTime(d.query_start) : "—"} />
              <Field label="Backend start" value={d.backend_start ? fmtTime(d.backend_start) : "—"} />
            </div>
          </div>
        )}

        <DialogFooter>
          {d && (
            <AdminGate>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => {
                  onCancel(d.pid);
                  onClose();
                }}
              >
                <Ban className="h-4 w-4 text-destructive" /> Cancel query
              </Button>
            </AdminGate>
          )}
          <Button variant="outline" size="sm" onClick={onClose}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
