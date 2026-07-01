import { useMemo, useState } from "react";
import { AlertTriangle, Bug, RefreshCw, ServerCrash } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { EmptyState } from "@/components/states";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useErrors } from "@/hooks/useApi";
import { fmtAge, fmtInt, fmtTime } from "@/lib/format";
import { categoryLabel, categoryVariant, isSystemError } from "@/lib/errors";
import type { ErrorEntry } from "@/types/api";

// "any" is the sentinel for no category filter (Select can't hold "").
const CATEGORIES = ["any", "system", "user", "conflict", "metadata_connection_lost"] as const;

function Field({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={mono ? "font-mono text-xs break-all" : "text-xs"}>{value ?? "—"}</span>
    </div>
  );
}

// Detail view for one captured error. The list already carries full (redacted)
// detail, so this reads the passed row directly — no extra fetch. query +
// message are redacted server-side (a CREATE SECRET error never carries the
// credential), so it is safe to render both verbatim here.
function ErrorDetailDialog({ error, onClose }: { error: ErrorEntry | null; onClose: () => void }) {
  return (
    <Dialog open={error != null} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            Error detail
            {error && <Badge variant="outline">{error.sqlstate || "—"}</Badge>}
            {error && <Badge variant={categoryVariant(error.category)}>{categoryLabel(error.category)}</Badge>}
          </DialogTitle>
          <DialogDescription>Redacted snapshot of one failed query captured on the owning control plane.</DialogDescription>
        </DialogHeader>
        {error && (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
              <Field label="Time" value={fmtTime(error.time)} mono />
              <Field label="Org" value={error.org} mono />
              <Field label="User" value={error.user || "—"} mono />
              <Field label="PID" value={error.pid} mono />
              <Field label="Worker" value={`#${error.worker_id}`} mono />
              <Field label="Worker pod" value={error.worker_pod || "—"} mono />
              <Field label="Client" value={error.client_addr || "—"} mono />
              <Field label="Trace ID" value={error.trace_id || "—"} mono />
            </div>
            <div>
              <span className="text-[10px] uppercase tracking-wide text-muted-foreground">Message</span>
              <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap rounded-md bg-muted p-3 text-xs text-destructive">
                {error.message || "—"}
              </pre>
            </div>
            <div>
              <span className="text-[10px] uppercase tracking-wide text-muted-foreground">Query (redacted)</span>
              <pre className="mt-1 max-h-56 overflow-auto whitespace-pre-wrap rounded-md bg-muted p-3 font-mono text-xs">
                {error.query || "—"}
              </pre>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

export function Errors() {
  const [org, setOrg] = useState("");
  const [user, setUser] = useState("");
  const [sqlstate, setSqlstate] = useState("");
  const [category, setCategory] = useState<string>("any");
  const [detail, setDetail] = useState<ErrorEntry | null>(null);

  // Filters are applied server-side (after the cross-CP merge). Trim so a stray
  // space doesn't over-filter; empty string → omitted by the api layer.
  const filters = useMemo(
    () => ({
      org: org.trim() || undefined,
      user: user.trim() || undefined,
      sqlstate: sqlstate.trim() || undefined,
      category: category === "any" ? undefined : category,
    }),
    [org, user, sqlstate, category],
  );
  const errors = useErrors(filters);
  const rows = useMemo(() => errors.data ?? [], [errors.data]);
  const systemCount = useMemo(() => rows.filter((e) => isSystemError(e.category)).length, [rows]);

  return (
    <>
      <PageHeader
        title="Errors"
        description="Recent failed queries across the cluster (redacted), newest first. Auto-refreshes every 5s. Live-triage buffer — long-term history lives in the query-log pipeline."
        actions={
          <div className="flex items-center gap-2">
            <Input value={org} onChange={(e) => setOrg(e.target.value)} placeholder="Org…" className="w-36" />
            <Input value={user} onChange={(e) => setUser(e.target.value)} placeholder="User…" className="w-32" />
            <Input
              value={sqlstate}
              onChange={(e) => setSqlstate(e.target.value)}
              placeholder="SQLSTATE…"
              className="w-28"
            />
            <Select value={category} onValueChange={setCategory}>
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {CATEGORIES.map((c) => (
                  <SelectItem key={c} value={c}>
                    {c === "any" ? "All categories" : categoryLabel(c)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button variant="outline" size="icon" onClick={() => errors.refetch()} title="Refresh now">
              <RefreshCw className="h-4 w-4" />
            </Button>
          </div>
        }
      />
      <PageBody>
        <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-2">
          <Card>
            <CardContent className="flex items-center gap-3 py-4">
              <Bug className="h-5 w-5 text-muted-foreground" />
              <div>
                <div className="text-2xl font-semibold tabular-nums">{fmtInt(rows.length)}</div>
                <div className="text-xs text-muted-foreground">recent errors (matching filter)</div>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="flex items-center gap-3 py-4">
              <ServerCrash className={`h-5 w-5 ${systemCount > 0 ? "text-destructive" : "text-muted-foreground"}`} />
              <div>
                <div className="text-2xl font-semibold tabular-nums">{fmtInt(systemCount)}</div>
                <div className="text-xs text-muted-foreground">system-attributable (page-worthy)</div>
              </div>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <AlertTriangle className="h-4 w-4" /> Recent errors
            </CardTitle>
            <Badge variant="secondary">{fmtInt(rows.length)}</Badge>
          </CardHeader>
          <CardContent className="p-0">
            {!errors.isSuccess && errors.isLoading ? (
              <p className="p-6 text-sm text-muted-foreground">Loading…</p>
            ) : rows.length === 0 ? (
              <EmptyState
                icon={<AlertTriangle className="h-6 w-6" />}
                title="No errors"
                description="No recent errors match the current filter. That's a good thing."
              />
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>Time</TableHead>
                    <TableHead>Org</TableHead>
                    <TableHead>User</TableHead>
                    <TableHead>SQLSTATE</TableHead>
                    <TableHead>Category</TableHead>
                    <TableHead>Message</TableHead>
                    <TableHead>Worker</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((e, i) => (
                    <TableRow
                      key={`${e.trace_id || ""}-${e.worker_id}-${e.time}-${i}`}
                      className="cursor-pointer [&>td]:py-1.5"
                      onClick={() => setDetail(e)}
                      title="View error detail"
                    >
                      <TableCell className="font-mono text-xs tabular-nums text-muted-foreground">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span>{fmtAge(e.time)}</span>
                          </TooltipTrigger>
                          <TooltipContent>{fmtTime(e.time)}</TooltipContent>
                        </Tooltip>
                      </TableCell>
                      <TableCell className="font-mono text-xs">{e.org}</TableCell>
                      <TableCell className="font-mono text-xs">{e.user || "—"}</TableCell>
                      <TableCell className="font-mono text-xs">{e.sqlstate || "—"}</TableCell>
                      <TableCell>
                        <Badge variant={categoryVariant(e.category)}>{categoryLabel(e.category)}</Badge>
                      </TableCell>
                      <TableCell className="max-w-md truncate text-xs" title={e.message}>
                        {e.message || "—"}
                      </TableCell>
                      <TableCell className="font-mono text-xs">#{e.worker_id}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </PageBody>
      <ErrorDetailDialog error={detail} onClose={() => setDetail(null)} />
    </>
  );
}
