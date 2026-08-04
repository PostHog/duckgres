import { describe, expect, it } from "vitest";
import { idleInTransaction, isIdleSession, sessionStateLabel } from "./session";

describe("isIdleSession", () => {
  it("flags idle states (no in-flight query)", () => {
    expect(isIdleSession("idle")).toBe(true);
    expect(isIdleSession("idle in transaction")).toBe(true);
    expect(isIdleSession("idle in transaction (aborted)")).toBe(true);
    expect(isIdleSession("IDLE")).toBe(true); // case-insensitive
  });

  it("does NOT flag active or unknown states", () => {
    expect(isIdleSession("active")).toBe(false);
    expect(isIdleSession("active (stuck)")).toBe(false);
    expect(isIdleSession("")).toBe(false); // unknown → not a false positive
    expect(isIdleSession(undefined)).toBe(false);
  });
});

describe("idleInTransaction", () => {
  it("distinguishes the open-transaction variant", () => {
    expect(idleInTransaction("idle in transaction")).toBe(true);
    expect(idleInTransaction("idle in transaction (aborted)")).toBe(true);
    expect(idleInTransaction("idle")).toBe(false);
    expect(idleInTransaction("active")).toBe(false);
    expect(idleInTransaction(undefined)).toBe(false);
  });
});

describe("sessionStateLabel", () => {
  it("returns the state, or 'unknown' for empty", () => {
    expect(sessionStateLabel("idle in transaction")).toBe("idle in transaction");
    expect(sessionStateLabel("")).toBe("unknown");
    expect(sessionStateLabel(undefined)).toBe("unknown");
  });
});
