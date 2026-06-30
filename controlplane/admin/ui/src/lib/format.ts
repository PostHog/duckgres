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

// The Duckling (managed warehouse) CR name is the org ID lowercased, hyphens
// preserved.
export function ducklingName(orgName: string): string {
  return orgName.toLowerCase();
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
