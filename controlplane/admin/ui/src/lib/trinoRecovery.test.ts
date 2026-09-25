import { describe, expect, it } from "vitest";
import { recoveryAllowed, recoveryComplete, recoveryIdentity, validRecoveryReason } from "./trinoRecovery";
import type { TrinoRecoveryPreview } from "@/types/api";

const snapshot: TrinoRecoveryPreview = {
  cell: "pool-a", request: null, live_work_verified: false,
  instance: {
    instance_id: "instance-a", phase: "DRAINING", gateway_state: "DRAINING", expected_generation: 7,
    incarnation: "incarnation-a", pod_uid: "pod-a", boot_id: "boot-a", node_id: "node-a",
    coordinator_id: "coordinator-a", phase_changed_at: "2026-01-01T00:00:00Z", last_error: "",
  },
  capacity: { stored_serving: 3, min_serving: 2, desired_instances: 3, frozen: false },
};

describe("recovery safety derivations", () => {
  it("offers new requests only for a draining instance with a complete identity", () => {
    expect(recoveryAllowed(snapshot)).toBe(true);
    for (const phase of ["SERVING", "SUSPECT", "LOST", "FAILED_PREPARING", "RETIRING", "RETIRED", "FAILURE_RETIRED", "unknown"]) {
      expect(recoveryAllowed({ ...snapshot, instance: { ...snapshot.instance, phase } })).toBe(false);
    }
    for (const field of ["incarnation", "pod_uid", "boot_id", "node_id", "coordinator_id"] as const) {
      expect(recoveryAllowed({ ...snapshot, instance: { ...snapshot.instance, [field]: "" } })).toBe(false);
    }
    expect(recoveryAllowed({ ...snapshot, instance: { ...snapshot.instance, expected_generation: 0 } })).toBe(false);
    expect(recoveryAllowed({ ...snapshot, instance: { ...snapshot.instance, expected_generation: Number.MAX_SAFE_INTEGER + 1 } })).toBe(false);
  });
  it("blocks frozen and below-floor pools without claiming stored capacity is live", () => {
    expect(recoveryAllowed({ ...snapshot, capacity: { ...snapshot.capacity, frozen: true } })).toBe(false);
    expect(recoveryAllowed({ ...snapshot, capacity: { ...snapshot.capacity, stored_serving: 1 } })).toBe(false);
  });
  it("uses the server's byte limit for the reason", () => {
    expect(validRecoveryReason(" ")).toBe(false);
    expect(validRecoveryReason("x".repeat(256))).toBe(true);
    expect(validRecoveryReason("x".repeat(257))).toBe(false);
    expect(validRecoveryReason("é".repeat(128))).toBe(true);
    expect(validRecoveryReason("é".repeat(129))).toBe(false);
  });
  it("recognizes both irreversible retirement outcomes", () => {
    expect(recoveryComplete("RETIRED")).toBe(true);
    expect(recoveryComplete("FAILURE_RETIRED")).toBe(true);
    expect(recoveryComplete("RETIRING")).toBe(false);
    expect(recoveryComplete("FAILED_PREPARING")).toBe(false);
  });
  it("copies only the six exact identity fields into the mutation body", () => {
    expect(Object.keys(recoveryIdentity(snapshot.instance))).toEqual([
      "expected_generation", "incarnation", "pod_uid", "boot_id", "node_id", "coordinator_id",
    ]);
  });
});
