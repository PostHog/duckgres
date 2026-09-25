import { ApiError } from "./api";
import type { TrinoRecoveryBody, TrinoRecoveryIdentity, TrinoRecoveryPreview } from "@/types/api";

export const RECOVERY_PREVIEW_MAX_AGE_MS = 30_000;

export function recoveryComplete(phase: string): boolean {
  return phase === "FAILURE_RETIRED" || phase === "RETIRED";
}

export function recoveryIdentity(identity: TrinoRecoveryIdentity): TrinoRecoveryIdentity {
  return {
    expected_generation: identity.expected_generation,
    incarnation: identity.incarnation,
    pod_uid: identity.pod_uid,
    boot_id: identity.boot_id,
    node_id: identity.node_id,
    coordinator_id: identity.coordinator_id,
  };
}

export function validRecoveryIdentity(identity: TrinoRecoveryIdentity): boolean {
  return invalidRecoveryIdentityFields(identity).length === 0;
}

function invalidRecoveryIdentityFields(identity: TrinoRecoveryIdentity): string[] {
  const fields: string[] = [];
  if (!Number.isSafeInteger(identity.expected_generation) || identity.expected_generation <= 0) fields.push("expected_generation");
  for (const field of ["incarnation", "pod_uid", "boot_id", "node_id", "coordinator_id"] as const) {
    if (typeof identity[field] !== "string" || !identity[field].trim()) fields.push(field);
  }
  return fields;
}

export function recoveryAllowed(preview: TrinoRecoveryPreview): boolean {
  return recoveryBlockers(preview).length === 0;
}

export function recoveryBlockers(preview: TrinoRecoveryPreview): string[] {
  const blockers: string[] = [];
  if (preview.request) blockers.push(`Recovery operation ${preview.request.operation_id} is already recorded. A new request is not allowed.`);
  if (preview.instance.phase !== "DRAINING") blockers.push(`Instance phase is ${preview.instance.phase}; recovery requires DRAINING.`);
  if (preview.capacity.frozen) blockers.push("The pool is frozen. Recovery cannot proceed while it is frozen.");
  const invalidIdentity = invalidRecoveryIdentityFields(preview.instance);
  if (invalidIdentity.length) blockers.push(`Missing or invalid admitted identity fields: ${invalidIdentity.join(", ")}.`);
  if (!(preview.capacity.stored_serving >= preview.capacity.min_serving)) {
    blockers.push(`Stored serving count is ${preview.capacity.stored_serving}; minimum required is ${preview.capacity.min_serving}. Recovery cannot proceed below this minimum.`);
  }
  return blockers;
}

export function recoveryReviewKey(preview: TrinoRecoveryPreview): string {
  return JSON.stringify({ identity: recoveryIdentity(preview.instance), phase: preview.instance.phase, capacity: preview.capacity });
}

export function validRecoveryReason(reason: string): boolean {
  return reason.trim().length > 0 && new TextEncoder().encode(reason).length <= 256;
}

export function recoveryAccessDenied(error: unknown): boolean {
  return error instanceof ApiError && [401, 403].includes(error.status);
}

export function recoveryPollingPaused(error: unknown): boolean {
  return recoveryAccessDenied(error) || error instanceof ApiError && [404, 503].includes(error.status);
}

export function recoveryError(error: unknown): string {
  if (error instanceof ApiError) {
    if (recoveryAccessDenied(error)) return "Admin access is required. Check your sign-in and permissions.";
    if (error.status === 404) return "Shared-pool recovery is unavailable for this cell or instance.";
    if (error.status === 409) return "The identity or recorded intent changed. Refresh the preview before continuing.";
    if (error.status === 503) return "Recovery is temporarily unavailable. Refresh to check again.";
  }
  return error instanceof Error ? error.message : "Recovery request failed.";
}

export function recoveryStorageKey(cell: string, instance: string, actor: string): string {
  return `trino-recovery:${encodeURIComponent(actor)}:${encodeURIComponent(cell)}:${encodeURIComponent(instance)}`;
}

export interface SavedRecoveryRequest {
  body: TrinoRecoveryBody;
  mayHaveBeenAccepted: boolean;
}

export function readRecoveryRequest(key: string): SavedRecoveryRequest | null {
  const raw = sessionStorage.getItem(key);
  if (!raw) return null;
  const saved = JSON.parse(raw) as SavedRecoveryRequest;
  const body = saved?.body;
  if (!body || typeof saved.mayHaveBeenAccepted !== "boolean" || !validRecoveryIdentity(body) || typeof body.operation_id !== "string" || !body.operation_id ||
      body.destructive_authorization !== true || typeof body.reason !== "string" || !validRecoveryReason(body.reason)) {
    throw new Error("Saved recovery request is invalid. Do not submit a new request until you inspect its recorded status.");
  }
  return { body: { ...recoveryIdentity(body), operation_id: body.operation_id, reason: body.reason, destructive_authorization: true }, mayHaveBeenAccepted: saved.mayHaveBeenAccepted };
}
