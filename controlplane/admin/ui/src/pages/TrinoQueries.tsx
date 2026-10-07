import { Fragment, useMemo, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { AlertTriangle, ArrowDown, ArrowUp, Ban, ChevronDown, ChevronRight, Database, Gauge, Hourglass, Timer } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { StatCard } from "@/components/StatCard";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import { OrgRef } from "@/components/OrgRef";
import { TrinoCellPicker } from "@/components/TrinoCellPicker";
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
  groupTrinoQueries,
  isActiveTrinoQuery,
  sortTrinoQueries,
  summarizeTrinoQueries,
  trinoSqlSummary,
  trinoStateBucket,
  trinoQueryFlag,
  trinoScanEfficiency,
  trinoStateVariant,
  trinoUnavailableMessage,
  trinoUnavailableReason,
  type TrinoQueryFlag,
  type TrinoQueryGroup,
  type TrinoQuerySortKey,
  type TrinoStateBucket,
} from "@/lib/trino";
import { cn } from "@/lib/utils";
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
  const [params] = useSearchParams();
  const kill = useKillTrinoQuery(params.get("cell") ?? undefined);
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

// SortHeader is a clickable column header. Clicking the active column flips
// direction; clicking another column starts descending, because "biggest
// first" is the question every numeric column here answers.
function SortHeader({
  label,
  column,
  sort,
  onSort,
  align = "left",
}: {
  label: string;
  column: TrinoQuerySortKey;
  sort: { key: TrinoQuerySortKey; dir: "asc" | "desc" };
  onSort: (key: TrinoQuerySortKey) => void;
  align?: "left" | "right";
}) {
  const active = sort.key === column;
  return (
    <TableHead className={align === "right" ? "text-right" : undefined}>
      <button
        type="button"
        className={cn(
          "inline-flex items-center gap-1 hover:text-foreground",
          active && "text-foreground",
          align === "right" && "flex-row-reverse",
        )}
        aria-sort={active ? (sort.dir === "asc" ? "ascending" : "descending") : "none"}
        onClick={() => onSort(column)}
      >
        {label}
        {active && (sort.dir === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />)}
      </button>
    </TableHead>
  );
}

function ProgressBar({ value }: { value: number | null }) {
  if (value === null) return <span className="text-xs text-muted-foreground">—</span>;
  const pct = Math.max(0, Math.min(100, value));
  return (
    <div className="flex items-center gap-2" title={`${pct.toFixed(1)}%`}>
      <div className="h-1.5 w-16 overflow-hidden rounded bg-muted">
        <div className="h-full bg-primary" style={{ width: `${pct}%` }} />
      </div>
      <span className="w-8 text-right text-xs tabular-nums text-muted-foreground">{pct.toFixed(0)}%</span>
    </div>
  );
}

function Detail({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <dt className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</dt>
      <dd className="font-mono text-xs">{children}</dd>
    </div>
  );
}

// Breakdown answers "who is using the cell right now" in one glance, and
// clicking a row narrows the list to it.
function Breakdown({
  title,
  groups,
  label,
  selected,
  onSelect,
}: {
  title: string;
  groups: TrinoQueryGroup[];
  label: (key: string) => ReactNode;
  selected: string | null;
  onSelect: (key: string | null) => void;
}) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead />
              <TableHead className="text-right">Running</TableHead>
              <TableHead className="text-right">Waiting</TableHead>
              <TableHead className="text-right">CPU</TableHead>
              <TableHead className="text-right">Scanned</TableHead>
              <TableHead className="text-right">Peak mem</TableHead>
              <TableHead className="text-right">Longest</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {groups.map((g) => (
              <TableRow
                key={g.key}
                data-testid={`group-${g.key}`}
                className={cn("cursor-pointer", selected === g.key && "bg-muted")}
                onClick={() => onSelect(selected === g.key ? null : g.key)}
              >
                <TableCell className="max-w-[14rem] truncate">{label(g.key)}</TableCell>
                <TableCell className="text-right tabular-nums">
                  {g.running}
                  {g.blocked > 0 && <span className="ml-1 text-xs text-destructive">({g.blocked} blocked)</span>}
                </TableCell>
                <TableCell className={cn("text-right tabular-nums", g.queued > 0 && "text-warning")}>{g.queued}</TableCell>
                <TableCell className="text-right tabular-nums">{fmtDurationMs(g.cpuMs)}</TableCell>
                <TableCell className="text-right tabular-nums">{fmtBytes(g.scannedBytes)}</TableCell>
                <TableCell className="text-right tabular-nums">{fmtBytes(g.peakMemoryBytes)}</TableCell>
                <TableCell className="text-right tabular-nums">{fmtDurationMs(g.longestMs)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

const BUCKETS: Array<{ key: TrinoStateBucket | "all"; label: string; activeOnly?: boolean }> = [
  { key: "all", label: "All" },
  { key: "running", label: "Running" },
  { key: "waiting", label: "Waiting" },
  { key: "failed", label: "Failed", activeOnly: false },
  { key: "finished", label: "Finished", activeOnly: false },
];

// orgKey groups a query by tenant; a control-plane query has no org and is
// grouped by its X-Trino-Source instead, as the Org column shows it.
const orgKey = (q: TrinoQuery) => q.org || q.source || q.principal || "—";

export function TrinoQueries() {
  // active=true is the live view: the states an operator can still act on.
  // Unchecking it brings in recently-finished queries, which is the closest
  // thing to history the coordinator holds in memory.
  const [activeOnly, setActiveOnly] = useState(true);
  const [search, setSearch] = useState("");
  const [bucket, setBucket] = useState<TrinoStateBucket | "all">("all");
  const [orgPick, setOrgPick] = useState<string | null>(null);
  const [instancePick, setInstancePick] = useState<string | null>(null);
  const [sort, setSort] = useState<{ key: TrinoQuerySortKey; dir: "asc" | "desc" }>({ key: "elapsed", dir: "desc" });
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set());
  const [killing, setKilling] = useState<TrinoQuery | null>(null);

  const [params] = useSearchParams();
  const cell = params.get("cell") ?? undefined;
  const status = useTrinoStatus(cell);
  const queries = useTrinoQueries({ active: activeOnly, ...(cell ? { cell } : {}) });
  const orgLabels = useOrgLabels();

  const all = useMemo(() => queries.data?.queries ?? [], [queries.data]);
  const orgName = (key: string) => orgLabels.get(key) ?? key;

  // Text search covers who (org label/id, principal) and what (SQL, query id),
  // so "portola", "events__" and "20261007_" all find what an operator means.
  const searched = useMemo(() => {
    if (search === "") return all;
    const needle = search.toLowerCase();
    return all.filter(
      (q) =>
        q.org.toLowerCase().includes(needle) ||
        q.principal.toLowerCase().includes(needle) ||
        (orgLabels.get(q.org) ?? "").toLowerCase().includes(needle) ||
        q.query_id.toLowerCase().includes(needle) ||
        q.query.toLowerCase().includes(needle),
    );
  }, [all, search, orgLabels]);

  // The headline numbers and breakdowns follow the search, not the state
  // chips or a breakdown pick — those narrow the list below, and summary
  // cards that changed with them would stop describing the cell.
  const summary = useMemo(() => summarizeTrinoQueries(searched), [searched]);
  const byOrg = useMemo(() => groupTrinoQueries(searched, orgKey), [searched]);
  const hasInstances = searched.some((q) => q.instance);
  const byInstance = useMemo(
    () => (hasInstances ? groupTrinoQueries(searched, (q) => q.instance || "—") : []),
    [searched, hasInstances],
  );
  const bucketCounts = useMemo(() => {
    const counts: Record<TrinoStateBucket, number> = { running: 0, waiting: 0, failed: 0, finished: 0 };
    for (const q of searched) counts[trinoStateBucket(q)] += 1;
    return counts;
  }, [searched]);

  const rows = useMemo(() => {
    const filtered = searched.filter(
      (q) =>
        (bucket === "all" || trinoStateBucket(q) === bucket) &&
        (orgPick === null || orgKey(q) === orgPick) &&
        (instancePick === null || (q.instance || "—") === instancePick),
    );
    return sortTrinoQueries(filtered, sort.key, sort.dir);
  }, [searched, bucket, orgPick, instancePick, sort]);

  const reason = trinoUnavailableReason(status.data);
  const onSort = (key: TrinoQuerySortKey) =>
    setSort((prev) => (prev.key === key ? { key, dir: prev.dir === "asc" ? "desc" : "asc" } : { key, dir: key === "org" ? "asc" : "desc" }));
  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  const colCount = hasInstances ? 10 : 9;

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
            <TrinoCellPicker />
            <Input
              className="h-8 w-64"
              placeholder="Search org, principal, SQL or query id…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
            <Button
              size="sm"
              variant={activeOnly ? "default" : "outline"}
              onClick={() => {
                setActiveOnly((v) => !v);
                setBucket("all");
              }}
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
            label="Waiting"
            value={fmtCompact(summary.queued)}
            accent={summary.queued > 0 ? "warning" : "default"}
            hint="queued, waiting for resources, planning or starting"
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
            hint={`physical input · ${fmtDurationMs(summary.cpuMs)} CPU`}
            icon={<Database className="h-4 w-4" />}
          />
        </div>

        {byOrg.length > 0 && (
          <div className={cn("mb-4 grid gap-4", hasInstances && "xl:grid-cols-2")}>
            <Breakdown
              title="By org"
              groups={byOrg}
              label={(key) => (all.some((q) => q.org === key) ? <OrgRef id={key} label={orgLabels.get(key)} /> : <span className="text-xs text-muted-foreground">{key}</span>)}
              selected={orgPick}
              onSelect={setOrgPick}
            />
            {hasInstances && (
              <Breakdown
                title="By pool instance"
                groups={byInstance}
                label={(key) => <span className="font-mono text-xs">{key}</span>}
                selected={instancePick}
                onSelect={setInstancePick}
              />
            )}
          </div>
        )}

        <Card>
          <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-2 space-y-0">
            <CardTitle>{activeOnly ? "Active queries" : "Queries held by the coordinator"}</CardTitle>
            <div className="flex flex-wrap items-center gap-1" role="group" aria-label="State filter">
              {BUCKETS.filter((b) => !(activeOnly && b.activeOnly === false)).map((b) => (
                <Button
                  key={b.key}
                  size="sm"
                  variant={bucket === b.key ? "default" : "outline"}
                  className="h-7"
                  onClick={() => setBucket(b.key)}
                >
                  {b.label}
                  <span className="ml-1 tabular-nums opacity-70">
                    {b.key === "all" ? searched.length : bucketCounts[b.key]}
                  </span>
                </Button>
              ))}
              {(orgPick !== null || instancePick !== null) && (
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-7"
                  onClick={() => {
                    setOrgPick(null);
                    setInstancePick(null);
                  }}
                >
                  Clear {[orgPick !== null && orgName(orgPick), instancePick].filter(Boolean).join(" · ")} ✕
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {queries.isLoading ? (
              <TableSkeleton cols={colCount} />
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
                    <TableHead className="w-6" />
                    <SortHeader label="State" column="state" sort={sort} onSort={onSort} />
                    <SortHeader label="Org" column="org" sort={sort} onSort={onSort} />
                    <TableHead>Query</TableHead>
                    {hasInstances && <TableHead>Instance</TableHead>}
                    <SortHeader label="Elapsed" column="elapsed" sort={sort} onSort={onSort} align="right" />
                    <SortHeader label="Progress" column="progress" sort={sort} onSort={onSort} />
                    <SortHeader label="CPU" column="cpu" sort={sort} onSort={onSort} align="right" />
                    <SortHeader label="Scanned" column="scanned" sort={sort} onSort={onSort} align="right" />
                    <SortHeader label="Peak mem" column="memory" sort={sort} onSort={onSort} align="right" />
                    <TableHead />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((q) => {
                    const flag = trinoQueryFlag(q);
                    const perRow = trinoScanEfficiency(q);
                    const sql = trinoSqlSummary(q.query);
                    const open = expanded.has(q.query_id);
                    return (
                      <Fragment key={q.query_id}>
                        <TableRow
                          data-testid={`query-${q.query_id}`}
                          className="cursor-pointer"
                          onClick={() => toggle(q.query_id)}
                        >
                          <TableCell className="pr-0 text-muted-foreground">
                            {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                          </TableCell>
                          <TableCell className="whitespace-nowrap">
                            <Badge variant={trinoStateVariant(q.state)}>{q.state}</Badge>
                            {flag && (
                              <Badge className="ml-1" variant={FLAG_VARIANT[flag]}>
                                {FLAG_LABEL[flag]}
                              </Badge>
                            )}
                          </TableCell>
                          <TableCell className="max-w-[12rem]">
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
                            {q.org && q.principal && (
                              <span className="block truncate text-[11px] text-muted-foreground">{q.principal}</span>
                            )}
                          </TableCell>
                          <TableCell className="max-w-[28rem]">
                            {sql.verb && (
                              <span className="block truncate text-xs">
                                <span className="font-semibold">{sql.verb}</span>
                                {sql.target && <span className="ml-1 font-mono">{sql.target}</span>}
                              </span>
                            )}
                            <span className="block truncate font-mono text-[11px] text-muted-foreground" title={q.query}>
                              {sql.text || q.query}
                            </span>
                          </TableCell>
                          {hasInstances && (
                            <TableCell className="whitespace-nowrap font-mono text-xs">{q.instance || "—"}</TableCell>
                          )}
                          <TableCell className="text-right tabular-nums">
                            {fmtDurationMs(q.elapsed_ms)}
                            {q.queued_ms > 1000 && (
                              <span className="block text-[11px] text-muted-foreground">{fmtDurationMs(q.queued_ms)} queued</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <ProgressBar value={q.progress_percentage} />
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{fmtDurationMs(q.cpu_ms)}</TableCell>
                          <TableCell className="text-right tabular-nums">
                            {fmtBytes(q.physical_input_bytes)}
                            {perRow !== null && (
                              <span className="block text-[11px] text-muted-foreground">{fmtBytes(perRow)}/row</span>
                            )}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{fmtBytes(q.peak_memory_bytes)}</TableCell>
                          <TableCell className="text-right" onClick={(e) => e.stopPropagation()}>
                            {isActiveTrinoQuery(q) && (
                              <AdminGate reason="Killing a query requires the admin role">
                                <Button size="sm" variant="ghost" onClick={() => setKilling(q)}>
                                  Kill
                                </Button>
                              </AdminGate>
                            )}
                          </TableCell>
                        </TableRow>
                        {open && (
                          <TableRow className="bg-muted/40 hover:bg-muted/40">
                            <TableCell />
                            <TableCell colSpan={colCount}>
                              <dl className="mb-3 grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-6">
                                <Detail label="Query id">{q.query_id}</Detail>
                                <Detail label="Principal">{q.principal || "—"}</Detail>
                                <Detail label="Source">{q.source || "—"}</Detail>
                                <Detail label="Resource group">{q.resource_group || "unassigned"}</Detail>
                                <Detail label="Drivers">
                                  {q.running_drivers} running · {q.queued_drivers} queued · {q.completed_drivers}/{q.total_drivers} done
                                </Detail>
                                <Detail label="Rows read">{fmtCompact(q.processed_input_rows)}</Detail>
                                <Detail label="Network">{fmtBytes(q.internal_network_bytes)}</Detail>
                                <Detail label="Spilled">{fmtBytes(q.spilled_bytes)}</Detail>
                                <Detail label="Queued for">{fmtDurationMs(q.queued_ms)}</Detail>
                                {sql.tag && <Detail label="Client tag">{sql.tag}</Detail>}
                                {q.error_type && (
                                  <Detail label="Error">
                                    {q.error_type}
                                    {q.error_code ? ` · ${q.error_code}` : ""}
                                  </Detail>
                                )}
                              </dl>
                              <pre className="max-h-96 overflow-auto whitespace-pre-wrap break-all rounded bg-background p-3 font-mono text-xs">
                                {q.query}
                              </pre>
                            </TableCell>
                          </TableRow>
                        )}
                      </Fragment>
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
