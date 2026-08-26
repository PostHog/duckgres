// Pure derivations for the Trino cell views.
//
// This math lives here, not inline in the pages, because the console's
// derived numbers have shipped wrong before (worker hot/idle counts, a leak
// warning firing while every worker was busy). Everything below is a pure
// function with a test in trino.test.ts.

import type { BadgeProps } from "@/components/ui/badge";
import type { TrinoNode, TrinoOrgStatus, TrinoQuery, TrinoStatus } from "@/types/api";

// Trino's query states, in the order an operator cares about them.
export const TRINO_ACTIVE_STATES = ["RUNNING", "QUEUED", "PLANNING", "STARTING"] as const;

// A query is "actionable" when killing it would still do something.
export function isActiveTrinoQuery(q: TrinoQuery): boolean {
  return (TRINO_ACTIVE_STATES as readonly string[]).includes(q.state);
}

export function trinoStateVariant(state: string): BadgeProps["variant"] {
  switch (state.toUpperCase()) {
    case "RUNNING":
      return "success";
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
  if (q.state === "QUEUED") return "queued";
  if (isActiveTrinoQuery(q) && q.elapsed_ms >= TRINO_LONG_RUNNING_MS) return "long_running";
  return null;
}

export interface TrinoQuerySummary {
  total: number;
  running: number;
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
    if (q.state === "RUNNING") s.running += 1;
    if (q.state === "QUEUED") s.queued += 1;
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

export interface TrinoNodeHealth {
  total: number;
  failed: number;
  // degraded counts nodes the coordinator still schedules onto but which
  // are losing heartbeats. They are the early warning the `failed` count
  // gives only after the fact.
  degraded: number;
  worstFailureRatio: number;
}

// A node that is still scheduled but failing this share of its heartbeats
// is degraded. Below the failure detector's own threshold on purpose: the
// point is to see trouble before the coordinator evicts the node.
export const TRINO_DEGRADED_FAILURE_RATIO = 0.1;

export function summarizeTrinoNodes(nodes: TrinoNode[]): TrinoNodeHealth {
  const h: TrinoNodeHealth = { total: nodes.length, failed: 0, degraded: 0, worstFailureRatio: 0 };
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
      return "This control plane has no Trino cell configured (DUCKGRES_TRINO_COORDINATOR_URL is unset).";
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
