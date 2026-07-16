// Small presentation helpers shared across pages.

import type { MetricSeries, PromRangeResponse } from "@/types/api";

// promToSeries flattens a raw Prometheus/VictoriaMetrics `matrix` range response
// into chart-ready named series. Values arrive as [unixSeconds, "stringValue"];
// we convert to { t: ms, v: number }. A series is named by its most meaningful
// label (outcome/state/quantile) or the full label set.
export function promToSeries(resp: PromRangeResponse | undefined): MetricSeries[] {
  const result = resp?.data?.result;
  if (!result || !Array.isArray(result)) return [];
  return result.map((r, i) => {
    const labels = r.metric ?? {};
    const name =
      labels.outcome ??
      labels.state ??
      labels.quantile ??
      labels.le ??
      (Object.keys(labels).length
        ? Object.entries(labels)
            .map(([k, v]) => `${k}=${v}`)
            .join(",")
        : `series ${i + 1}`);
    return {
      name,
      labels,
      points: (r.values ?? []).map(([t, v]) => ({ t: t * 1000, v: Number(v) })),
    };
  });
}

export function fmtInt(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  return n.toLocaleString("en-US");
}

export function fmtPercent(n: number | null | undefined, digits = 1): string {
  if (n == null || Number.isNaN(n)) return "—";
  return `${n.toFixed(digits)}%`;
}

export function fmtBytes(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  if (n === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(Math.abs(n)) / Math.log(1024)));
  const v = n / Math.pow(1024, i);
  return `${v.toFixed(v >= 100 || i === 0 ? 0 : 1)} ${units[i]}`;
}

// fmtCompact renders a number in compact SI notation (1.5K, 20M, 3.2B). Keeps a
// wide-ranging metric axis readable and NARROW — a raw 20000000 would be clipped
// to "00000" by a fixed-width axis.
export function fmtCompact(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(n);
}

// fmtMetricAxis compacts a metric value for a chart Y-axis tick. Byte units use
// binary prefixes (19 MB); everything else uses compact SI (1.5K, 20M). Returns
// "" for nullish so recharts skips the tick rather than drawing a dash.
export function fmtMetricAxis(v: number | null | undefined, unit?: string): string {
  if (v == null || Number.isNaN(v)) return "";
  if (unit === "B/s" || unit === "B") return fmtBytes(v);
  return fmtCompact(v);
}

// fmtMetricValue is the full "value + unit" string for a metric tooltip: byte
// units render as bytes (with a /s rate suffix when applicable), everything else
// as a compact number followed by its unit label.
export function fmtMetricValue(v: number | null | undefined, unit?: string): string {
  if (v == null || Number.isNaN(v)) return "—";
  if (unit === "B/s") return `${fmtBytes(v)}/s`;
  if (unit === "B") return fmtBytes(v);
  return `${fmtCompact(v)}${unit ? ` ${unit}` : ""}`;
}

// fmtDurationMs renders a MILLISECOND duration as a compact human string
// (250ms, 3.4s, 2m 5s, 1h 3m). <=0 / nullish → "—". Used for running-query
// elapsed time in the Live view + detail dialog. (fmtDuration below takes
// SECONDS — different unit, used for worker TTLs/ages.)
export function fmtDurationMs(ms: number | null | undefined): string {
  if (ms == null || Number.isNaN(ms) || ms <= 0) return "—";
  const s = ms / 1000;
  if (s < 1) return `${Math.round(ms)}ms`;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${Math.round(s % 60)}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

export function fmtTime(ts: string | number | Date | null | undefined): string {
  if (ts == null) return "—";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

const ZERO_TIME = "0001-01-01";

// Relative "age" string from a past timestamp, e.g. "3m 12s", "2h", "5d".
export function fmtAge(ts: string | number | Date | null | undefined): string {
  if (ts == null) return "—";
  if (typeof ts === "string" && ts.startsWith(ZERO_TIME)) return "—";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "—";
  let s = Math.floor((Date.now() - d.getTime()) / 1000);
  if (s < 0) s = 0;
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  const days = Math.floor(h / 24);
  return `${days}d ${h % 24}h`;
}

// Humanize a duration in seconds, e.g. 1800 → "30m", 3600 → "1h", 5400 → "1h 30m".
// Returns "—" for 0/unset (the backend uses 0 to mean "default").
export function fmtDuration(seconds: number | null | undefined): string {
  if (seconds == null || Number.isNaN(seconds) || seconds <= 0) return "—";
  const s = Math.floor(seconds);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) {
    const rem = m % 60;
    return rem ? `${h}h ${rem}m` : `${h}h`;
  }
  const days = Math.floor(h / 24);
  const remH = h % 24;
  return remH ? `${days}d ${remH}h` : `${days}d`;
}

export function isZeroTime(ts: string | null | undefined): boolean {
  return !ts || ts.startsWith(ZERO_TIME);
}

// A human-readable label for an org: its database name, then hostname alias,
// then the raw UUID as a last resort. Use this anywhere an org UUID would
// otherwise be shown on its own — the UUIDs are impossible to tell apart at a
// glance. Returns the UUID unchanged when no readable name is available.
export function orgLabel(org: {
  name: string;
  database_name?: string | null;
  hostname_alias?: string | null;
}): string {
  return org.database_name || org.hostname_alias || org.name;
}

// Resolve an org's entry in a Duckling-CR-name-keyed map by exact match only:
// the warehouse's stored duckling_name (authoritative, NOT NULL in the DB)
// wins, then the org name itself. No client-side derivation of CR names.
export function ducklingEntryFor<T>(
  entries: Record<string, T> | undefined,
  orgName: string,
  ducklingName: string | undefined,
): T | undefined {
  if (!entries) return undefined;
  if (ducklingName && entries[ducklingName] !== undefined) return entries[ducklingName];
  return entries[orgName];
}

// A Duckling is broken/unhealthy when its warehouse row exists but the overall
// state is failed or deleted.
export function ducklingBroken(state: string | undefined): boolean {
  const s = (state ?? "").toLowerCase();
  return s === "failed" || s === "deleted";
}

// A SQL statement is a "read" if it begins (ignoring leading comments/whitespace)
// with SELECT / EXPLAIN / SHOW, or a WITH ... that ultimately SELECTs. Anything
// else is treated as a write and requires explicit confirmation + allowWrite.
export function sqlLooksLikeRead(sqlRaw: string): boolean {
  let sql = sqlRaw.trim();
  // strip leading line + block comments
  for (;;) {
    if (sql.startsWith("--")) {
      const nl = sql.indexOf("\n");
      sql = nl === -1 ? "" : sql.slice(nl + 1).trim();
      continue;
    }
    if (sql.startsWith("/*")) {
      const end = sql.indexOf("*/");
      sql = end === -1 ? "" : sql.slice(end + 2).trim();
      continue;
    }
    break;
  }
  const head = sql.toLowerCase();
  if (head.startsWith("select") || head.startsWith("explain") || head.startsWith("show") || head.startsWith("(")) {
    return true;
  }
  if (head.startsWith("with")) {
    // Writable CTE (INSERT/UPDATE/DELETE/MERGE anywhere) → treat as write.
    return !/\b(insert|update|delete|merge|create|drop|alter|attach|copy|truncate)\b/i.test(sql);
  }
  return false;
}
