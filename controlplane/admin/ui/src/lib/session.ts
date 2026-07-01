// Pure, testable helpers for live-session state. A session that holds a worker
// but has no in-flight query ("idle"/"idle in transaction") is a smell when
// it persists — surfaced in the Live view. Keep this logic here, not in JSX.

// isIdleSession reports whether a session has NO in-flight query, i.e. its
// pg_stat_activity-style state is idle / idle in transaction / aborted. An
// "active" session (or an unknown/empty state) is NOT flagged — we only mark a
// session idle when the backend explicitly says so, to avoid false positives
// during the brief window before state is first reported.
export function isIdleSession(state: string | undefined): boolean {
  return (state ?? "").toLowerCase().startsWith("idle");
}

// idleInTransaction is the worse idle variant: the session holds an OPEN
// transaction (and its worker) while running nothing. Worth distinguishing in
// the UI from a plain idle connection.
export function idleInTransaction(state: string | undefined): boolean {
  return (state ?? "").toLowerCase().startsWith("idle in transaction");
}

// sessionStateLabel is the short human label for a session state, for tooltips.
export function sessionStateLabel(state: string | undefined): string {
  const s = (state ?? "").trim();
  return s === "" ? "unknown" : s;
}
