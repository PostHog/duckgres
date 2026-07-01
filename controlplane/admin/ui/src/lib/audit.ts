// Human-readable rendering of admin audit rows.
//
// The backend records a terse "<resource>.<verb>" action code (see
// admin/audit.go::auditActionFor) plus method/path/target/detail. Operators
// shouldn't have to reverse-engineer "config.create POST /operators" — this maps
// each known action code to a plain-English label and composes it with the
// target/detail into one sentence.
//
// Keep ACTION_LABELS in sync with the codes emitted by auditActionFor.
import type { AuditEntry } from "@/types/api";

// A friendly label per action code. The verb is phrased as a completed action
// ("Created org") so the row reads as a log of what happened.
const ACTION_LABELS: Record<string, string> = {
  "org.create": "Created org",
  "org.update": "Updated org config",
  "org.delete": "Deleted org",

  "warehouse.create": "Created warehouse config",
  "warehouse.update": "Updated warehouse config",
  "warehouse.delete": "Deleted warehouse config",

  "user.create": "Created user",
  "user.update": "Updated user",
  "user.delete": "Deleted user",
  "user.kill": "Killed user sessions",
  "user.disable": "Disabled user",
  "user.enable": "Enabled user",

  "secret.create": "Created user secret",
  "secret.delete": "Deleted user secret",

  "operators.create": "Set operator role",
  "operators.update": "Set operator role",
  "operators.delete": "Removed operator",

  "session.cancel": "Cancelled session",

  "impersonate.query": "Ran impersonated query",
};

// Fallback labels for the generic config.<verb> bucket and any unmapped verb.
const VERB_LABELS: Record<string, string> = {
  create: "Created",
  update: "Updated",
  delete: "Deleted",
  other: "Changed",
};

// actionLabel returns the friendly label for an action code, degrading
// gracefully: a known "config.update" → "Config change", an unknown
// "widget.frobnicate" → "Frobnicate widget", and a bare code → the code itself.
export function actionLabel(action: string): string {
  if (!action) return "—";
  const known = ACTION_LABELS[action];
  if (known) return known;
  const [resource, verb] = action.split(".");
  if (resource === "config") return `Config ${VERB_LABELS[verb] ? VERB_LABELS[verb].toLowerCase() : verb ?? ""}`.trim();
  const v = VERB_LABELS[verb] ?? (verb ? verb.charAt(0).toUpperCase() + verb.slice(1) : "");
  return `${v} ${resource}`.trim();
}

// auditSubject returns the "who/what this acted on" phrase — the target user or
// org — used to complete the sentence. Returns "" when neither is set (the
// action label alone is enough).
export function auditSubject(e: AuditEntry): string {
  if (e.target_user) return e.target_user;
  if (e.org) return e.org;
  return "";
}

// auditSummary composes the full human sentence: "<label> <subject>". The org
// and detail render in their own cells, so this stays short — e.g.
// "Set operator role bob@posthog.com".
export function auditSummary(e: AuditEntry): string {
  const label = actionLabel(e.action);
  const subject = e.target_user ?? "";
  return subject ? `${label} ${subject}` : label;
}
