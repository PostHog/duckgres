// Pure, testable helpers for the Errors page. Categories come from the PG
// server's error classifier (server/conn.go logQueryError): "user" errors are
// caller-attributable (bad SQL, missing table), "system" errors are ours to
// fix, and the two DuckLake variants sit in between. Keep the mapping here, not
// in JSX, so it can be unit-tested and reused by badges/filters.

export type ErrorCategory = "user" | "system" | "conflict" | "metadata_connection_lost" | string;

export type BadgeVariant = "default" | "secondary" | "destructive" | "outline" | "warning";

// categoryVariant maps an error category to a Badge variant. "system" is the
// one that demands attention (a genuine server-side failure) → destructive; the
// DuckLake conflict/metadata variants are transient-ish → warning; "user" and
// anything unknown are low-signal → secondary.
export function categoryVariant(category: ErrorCategory): BadgeVariant {
  switch (category) {
    case "system":
      return "destructive";
    case "conflict":
    case "metadata_connection_lost":
      return "warning";
    default:
      return "secondary";
  }
}

// categoryLabel is the short human label for a category (the raw wire values use
// snake_case and are terse).
export function categoryLabel(category: ErrorCategory): string {
  switch (category) {
    case "user":
      return "User";
    case "system":
      return "System";
    case "conflict":
      return "Conflict";
    case "metadata_connection_lost":
      return "Metadata lost";
    default:
      return category || "unknown";
  }
}

// isSystemError reports whether an error is server-attributable (the ones worth
// paging on) — used to surface a "system errors" count distinct from the noisy
// user-error stream.
export function isSystemError(category: ErrorCategory): boolean {
  return category === "system" || category === "metadata_connection_lost";
}

// sqlstateClass groups a Postgres SQLSTATE by its 2-char class for coarse
// bucketing (e.g. "42" = syntax/access, "53" = insufficient resources, "XX" =
// internal). Empty/short codes bucket as "??".
export function sqlstateClass(sqlstate: string | undefined): string {
  const s = (sqlstate ?? "").trim();
  return s.length >= 2 ? s.slice(0, 2) : "??";
}
