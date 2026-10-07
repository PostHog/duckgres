// Pure derivations for the Trino cell views.
//
// This math lives here, not inline in the pages, because the console's
// derived numbers have shipped wrong before (worker hot/idle counts, a leak
// warning firing while every worker was busy). Everything below is a pure
// function with a test in trino.test.ts.

import type { BadgeProps } from "@/components/ui/badge";
import type { TrinoNode, TrinoNodeSource, TrinoOrgStatus, TrinoQuery, TrinoStatus } from "@/types/api";

// Trino's terminal states, mirroring QueryState.isDone(). Everything else --
// QUEUED, WAITING_FOR_RESOURCES, DISPATCHING, PLANNING, STARTING, RUNNING,
// FINISHING -- is still in flight and still killable.
//
// Defined as the terminal pair rather than as an allowlist of interesting
// states so this cannot drift out of step with the server's `active=1`
// filter, and so a query stuck in PLANNING (for a DuckLake-backed cell,
// planning talks to a per-tenant Postgres, so this is a real pathology)
// stays visible instead of being quietly filtered away.
export const TRINO_TERMINAL_STATES = ["FINISHED", "FAILED"] as const;

// A query is "actionable" when killing it would still do something.
export function isActiveTrinoQuery(q: TrinoQuery): boolean {
  return !(TRINO_TERMINAL_STATES as readonly string[]).includes(q.state);
}

// States in which a query holds a slot in the list but is not yet executing.
// QUEUED is only the resource-group queue; a query then passes through
// WAITING_FOR_RESOURCES (cluster minimum workers / memory), DISPATCHING,
// PLANNING and STARTING before it runs. Counting only QUEUED reported "0
// queued" while dozens of queries sat in WAITING_FOR_RESOURCES.
export const TRINO_WAITING_STATES = [
  "QUEUED",
  "WAITING_FOR_RESOURCES",
  "DISPATCHING",
  "PLANNING",
  "STARTING",
] as const;

export function isWaitingTrinoQuery(q: Pick<TrinoQuery, "state">): boolean {
  return (TRINO_WAITING_STATES as readonly string[]).includes(q.state.toUpperCase());
}

export function trinoStateVariant(state: string): BadgeProps["variant"] {
  switch (state.toUpperCase()) {
    case "RUNNING":
    case "FINISHING":
      return "success";
    case "WAITING_FOR_RESOURCES":
    case "QUEUED":
      // Queued is not an error, but it IS the state an operator is looking
      // for when a tenant reports slowness — resource-group saturation.
      return "warning";
    case "PLANNING":
    case "STARTING":
      return "default";
    case "FINISHED":
      return "muted";
    case "FAILED":
      return "destructive";
    default:
      return "outline";
  }
}

// Why a query is worth an operator's attention, most severe first. Returns
// null when nothing stands out.
export type TrinoQueryFlag = "failed" | "blocked" | "queued" | "long_running";

// A query running this long is worth a second look. Not a limit and not an
// alert — the cell enforces its own per-query caps; this only decides what
// gets highlighted in a list an operator is scanning.
export const TRINO_LONG_RUNNING_MS = 5 * 60_000;

export function trinoQueryFlag(q: TrinoQuery): TrinoQueryFlag | null {
  if (q.state === "FAILED") return "failed";
  // fully_blocked matters more than elapsed time: it means every driver is
  // waiting on the metadata store or on S3, which is a cell-level problem
  // wearing one query's clothes.
  if (q.state === "RUNNING" && q.fully_blocked) return "blocked";
  if (isWaitingTrinoQuery(q)) return "queued";
  if (isActiveTrinoQuery(q) && q.elapsed_ms >= TRINO_LONG_RUNNING_MS) return "long_running";
  return null;
}

export interface TrinoQuerySummary {
  total: number;
  running: number;
  // queued counts every waiting state (TRINO_WAITING_STATES), not only QUEUED.
  queued: number;
  blocked: number;
  failed: number;
  // scannedBytes is the physical input across the listed queries — what the
  // cell is actually pulling from object storage right now.
  scannedBytes: number;
  // cpuMs across the listed queries.
  cpuMs: number;
  longestMs: number;
}

export function summarizeTrinoQueries(queries: TrinoQuery[]): TrinoQuerySummary {
  const s: TrinoQuerySummary = {
    total: queries.length,
    running: 0,
    queued: 0,
    blocked: 0,
    failed: 0,
    scannedBytes: 0,
    cpuMs: 0,
    longestMs: 0,
  };
  for (const q of queries) {
    if (q.state === "RUNNING" || q.state === "FINISHING") s.running += 1;
    if (isWaitingTrinoQuery(q)) s.queued += 1;
    if (q.state === "FAILED") s.failed += 1;
    if (q.state === "RUNNING" && q.fully_blocked) s.blocked += 1;
    s.scannedBytes += q.physical_input_bytes ?? 0;
    s.cpuMs += q.cpu_ms ?? 0;
    // Elapsed time of a FINISHED query says nothing about current load, so
    // the "longest" headline tracks only queries still in flight.
    if (isActiveTrinoQuery(q)) s.longestMs = Math.max(s.longestMs, q.elapsed_ms ?? 0);
  }
  return s;
}

// Trino's NodeState for a node that is scheduling work normally. Anything
// else is draining, shutting down or already gone.
export const TRINO_ACTIVE_NODE_STATE = "ACTIVE";

export interface TrinoNodeHealth {
  total: number;
  // healthKnown=false means the cell reported membership only (the announce
  // inventory), so failed/degraded/worstFailureRatio are zero because
  // nothing was measured. A view that renders them as "all healthy" is
  // making a claim the coordinator never made — check this first.
  healthKnown: boolean;
  failed: number;
  // degraded counts nodes the coordinator still schedules onto but which
  // are losing heartbeats. They are the early warning the `failed` count
  // gives only after the fact.
  degraded: number;
  worstFailureRatio: number;
  // detailKnown=true means the source carried lifecycle state and version
  // (system.runtime.nodes). inactive and versions are meaningful only then.
  detailKnown: boolean;
  // Nodes not in the ACTIVE state: draining, shutting down or inactive.
  inactive: number;
  // Distinct node versions, sorted. More than one means a rollout is in
  // flight — or stuck, which is the case worth seeing.
  versions: string[];
}

// A node that is still scheduled but failing this share of its heartbeats
// is degraded. Below the failure detector's own threshold on purpose: the
// point is to see trouble before the coordinator evicts the node.
export const TRINO_DEGRADED_FAILURE_RATIO = 0.1;

export function summarizeTrinoNodes(
  nodes: TrinoNode[],
  source: TrinoNodeSource | undefined = "failure_detector",
): TrinoNodeHealth {
  const healthKnown = source === "failure_detector";
  const detailKnown = source === "system_table";
  const h: TrinoNodeHealth = {
    total: nodes.length,
    healthKnown,
    failed: 0,
    degraded: 0,
    worstFailureRatio: 0,
    detailKnown,
    inactive: 0,
    versions: [],
  };
  if (detailKnown) {
    const seen = new Set<string>();
    for (const n of nodes) {
      if (n.state && n.state !== TRINO_ACTIVE_NODE_STATE) h.inactive += 1;
      if (n.version) seen.add(n.version);
    }
    h.versions = [...seen].sort();
    return h;
  }
  // The announce inventory carries a URI and nothing else. Counting its
  // zeroed heartbeat fields would manufacture a clean bill of health.
  if (!healthKnown) return h;
  for (const n of nodes) {
    const ratio = n.recent_failure_ratio ?? 0;
    if (n.failed) {
      h.failed += 1;
    } else if (ratio >= TRINO_DEGRADED_FAILURE_RATIO) {
      h.degraded += 1;
    }
    h.worstFailureRatio = Math.max(h.worstFailureRatio, ratio);
  }
  return h;
}

// Why the Trino views cannot show live data. These are three genuinely
// different situations with three different fixes, and collapsing them into
// one "error" is what makes an operator debug the wrong system.
export type TrinoUnavailableReason =
  | "no_cell" // this deployment has no Trino cell at all
  | "unauthorized" // the observer principal is not authorized by the OPA bundle
  | "unreachable" // the coordinator did not answer
  | null; // fine

export function trinoUnavailableReason(status: TrinoStatus | undefined): TrinoUnavailableReason {
  if (!status) return null;
  if (status.cell.id === "") return "no_cell";
  if (status.available) return null;
  // The server surfaces a distinctive message for a 403, because that means
  // the bundle has not rolled out (or the grant is missing) rather than the
  // cell being down — a fix in the control plane, not in the cluster.
  if ((status.error ?? "").includes("403")) return "unauthorized";
  return "unreachable";
}

export function trinoUnavailableMessage(reason: TrinoUnavailableReason): string {
  switch (reason) {
    case "no_cell":
      return "This control plane has no Trino shared pool configured. Check DUCKGRES_TRINO_CELLS_FILE.";
    case "unauthorized":
      return "The coordinator rejected the control plane's observer credential. The cell's OPA bundle may not have rolled out yet.";
    case "unreachable":
      return "The Trino coordinator did not answer. Provisioning state below still reflects the config store.";
    default:
      return "";
  }
}

// Orgs whose Trino provisioning needs attention: failed outright, or
// pending/provisioning without ever having reached ready. An org that is
// re-reconciling after a successful provision (ready_at set) is not
// trouble — it is a tick in flight.
export function trinoOrgsNeedingAttention(orgs: TrinoOrgStatus[]): TrinoOrgStatus[] {
  return orgs.filter((o) => {
    if (o.state === "failed") return true;
    if (o.state === "ready") return false;
    return !o.ready_at;
  });
}

// Bytes scanned per row returned is the DuckLake pruning signal: a query
// reading gigabytes to return a handful of rows is one whose predicates are
// not pruning files. Returns null when there is nothing to compare against
// yet (no rows processed), which is the common case for a query that has
// only just started.
export function trinoScanEfficiency(q: TrinoQuery): number | null {
  if (!q.processed_input_rows || q.processed_input_rows <= 0) return null;
  return q.physical_input_bytes / q.processed_input_rows;
}

// ---- Making a list of hundreds of SQLMesh statements readable ----

export interface TrinoSqlSummary {
  // verb is the leading statement keyword(s): SELECT, INSERT INTO, CREATE
  // TABLE, MERGE INTO, DROP TABLE, ... ("" when the text is empty).
  verb: string;
  // target is the relation the statement writes (or, for a SELECT, the first
  // relation it reads). "" when none could be found.
  target: string;
  // tag is the client's leading comment when it is a short label, e.g. the
  // SQLMesh `/* SQLMESH_PLAN: ... */` marker. "" when there was none.
  tag: string;
  // text is the statement with leading comments and whitespace removed.
  text: string;
}

const LEADING_COMMENT = /^\s*(?:\/\*([\s\S]*?)\*\/|--([^\n]*)(?:\n|$))/;
const IDENT = String.raw`(?:"(?:[^"]|"")+"|[A-Za-z_][\w$]*)`;
const QUALIFIED = String.raw`${IDENT}(?:\s*\.\s*${IDENT}){0,2}`;
const WRITE_PATTERNS: Array<[RegExp, string]> = [
  [new RegExp(String.raw`^insert\s+(?:overwrite\s+)?into\s+(${QUALIFIED})`, "i"), "INSERT INTO"],
  [new RegExp(String.raw`^merge\s+into\s+(${QUALIFIED})`, "i"), "MERGE INTO"],
  [new RegExp(String.raw`^delete\s+from\s+(${QUALIFIED})`, "i"), "DELETE FROM"],
  [new RegExp(String.raw`^update\s+(${QUALIFIED})`, "i"), "UPDATE"],
  [new RegExp(String.raw`^create\s+(?:or\s+replace\s+)?(materialized\s+view|view|table|schema)\s+(?:if\s+not\s+exists\s+)?(${QUALIFIED})`, "i"), "CREATE"],
  [new RegExp(String.raw`^drop\s+(materialized\s+view|view|table|schema)\s+(?:if\s+exists\s+)?(${QUALIFIED})`, "i"), "DROP"],
  [new RegExp(String.raw`^alter\s+(table|view|schema)\s+(?:if\s+exists\s+)?(${QUALIFIED})`, "i"), "ALTER"],
];
const FIRST_FROM = new RegExp(String.raw`\bfrom\s+(${QUALIFIED})`, "i");

function unquote(name: string): string {
  return (name.match(new RegExp(IDENT, "g")) ?? [])
    .map((part) => (part.startsWith('"') ? part.slice(1, -1).replace(/""/g, '"') : part))
    .join(".");
}

export function trinoSqlSummary(sql: string): TrinoSqlSummary {
  let text = sql ?? "";
  let tag = "";
  for (let m = LEADING_COMMENT.exec(text); m; m = LEADING_COMMENT.exec(text)) {
    const body = (m[1] ?? m[2] ?? "").trim().replace(/\s+/g, " ");
    if (!tag && body && body.length <= 120) tag = body;
    text = text.slice(m[0].length);
  }
  text = text.trim();
  for (const [pattern, verb] of WRITE_PATTERNS) {
    const m = pattern.exec(text);
    if (!m) continue;
    if (verb === "CREATE" || verb === "DROP" || verb === "ALTER") {
      return { verb: `${verb} ${m[1].toUpperCase().replace(/\s+/g, " ")}`, target: unquote(m[2]), tag, text };
    }
    return { verb, target: unquote(m[1]), tag, text };
  }
  const word = /^[A-Za-z]+/.exec(text)?.[0]?.toUpperCase() ?? "";
  // A WITH ... SELECT is a read; report it as one rather than as "WITH".
  const verb = word === "WITH" ? "SELECT" : word;
  const from = verb === "SELECT" ? FIRST_FROM.exec(text) : null;
  return { verb, target: from ? unquote(from[1]) : "", tag, text };
}

export interface TrinoQueryGroup {
  key: string;
  total: number;
  running: number;
  queued: number;
  blocked: number;
  cpuMs: number;
  scannedBytes: number;
  peakMemoryBytes: number;
  longestMs: number;
}

// Group the listed queries by a key (org, instance, resource group), largest
// first. This is what answers "who is using the cell right now" without
// reading every row.
export function groupTrinoQueries(
  queries: TrinoQuery[],
  keyOf: (q: TrinoQuery) => string,
): TrinoQueryGroup[] {
  const groups = new Map<string, TrinoQueryGroup>();
  for (const q of queries) {
    const key = keyOf(q);
    let g = groups.get(key);
    if (!g) {
      g = { key, total: 0, running: 0, queued: 0, blocked: 0, cpuMs: 0, scannedBytes: 0, peakMemoryBytes: 0, longestMs: 0 };
      groups.set(key, g);
    }
    g.total += 1;
    if (q.state === "RUNNING" || q.state === "FINISHING") g.running += 1;
    if (isWaitingTrinoQuery(q)) g.queued += 1;
    if (q.state === "RUNNING" && q.fully_blocked) g.blocked += 1;
    g.cpuMs += q.cpu_ms ?? 0;
    g.scannedBytes += q.physical_input_bytes ?? 0;
    g.peakMemoryBytes += q.peak_memory_bytes ?? 0;
    if (isActiveTrinoQuery(q)) g.longestMs = Math.max(g.longestMs, q.elapsed_ms ?? 0);
  }
  return [...groups.values()].sort(
    (a, b) => b.running + b.queued - (a.running + a.queued) || b.total - a.total || a.key.localeCompare(b.key),
  );
}

// Coarse lifecycle bucket for the state filter: what an operator asks for is
// "show me what is waiting", not one of Trino's nine states.
export type TrinoStateBucket = "running" | "waiting" | "finished" | "failed";

export function trinoStateBucket(q: Pick<TrinoQuery, "state">): TrinoStateBucket {
  const state = q.state.toUpperCase();
  if (state === "FAILED") return "failed";
  if (state === "FINISHED") return "finished";
  if (isWaitingTrinoQuery(q)) return "waiting";
  return "running";
}

export type TrinoQuerySortKey =
  | "elapsed"
  | "cpu"
  | "scanned"
  | "memory"
  | "progress"
  | "state"
  | "org";

// Running before waiting before finished, so the default "state" order puts
// live work on top.
const BUCKET_ORDER: Record<TrinoStateBucket, number> = { running: 0, waiting: 1, failed: 2, finished: 3 };

function sortValue(q: TrinoQuery, key: TrinoQuerySortKey): number | string {
  switch (key) {
    case "elapsed":
      return q.elapsed_ms ?? 0;
    case "cpu":
      return q.cpu_ms ?? 0;
    case "scanned":
      return q.physical_input_bytes ?? 0;
    case "memory":
      return q.peak_memory_bytes ?? 0;
    case "progress":
      // Unknown progress sorts below 0%.
      return q.progress_percentage ?? -1;
    case "state":
      return BUCKET_ORDER[trinoStateBucket(q)];
    case "org":
      return q.org || q.principal || "";
  }
}

// Stable sort; ties fall back to elapsed time, longest first, then query id
// so rows do not jump around between 2s polls.
export function sortTrinoQueries(
  queries: TrinoQuery[],
  key: TrinoQuerySortKey,
  direction: "asc" | "desc",
): TrinoQuery[] {
  const sign = direction === "asc" ? 1 : -1;
  return [...queries].sort((a, b) => {
    const va = sortValue(a, key);
    const vb = sortValue(b, key);
    const primary =
      typeof va === "string" || typeof vb === "string"
        ? String(va).localeCompare(String(vb))
        : (va as number) - (vb as number);
    if (primary !== 0) return sign * primary;
    return (b.elapsed_ms ?? 0) - (a.elapsed_ms ?? 0) || a.query_id.localeCompare(b.query_id);
  });
}
