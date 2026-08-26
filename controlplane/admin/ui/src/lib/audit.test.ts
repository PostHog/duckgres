import { describe, expect, it } from "vitest";
import { actionLabel, auditSummary } from "./audit";
import type { AuditEntry } from "@/types/api";

function entry(over: Partial<AuditEntry>): AuditEntry {
  return {
    id: 1,
    ts: "2026-06-30T16:46:37Z",
    actor: "benjamin.k@posthog.com",
    role: "admin",
    source: "sso",
    action: "",
    method: "POST",
    path: "/api/v1/operators",
    status: 200,
    ...over,
  };
}

describe("actionLabel", () => {
  it("maps known action codes to plain English", () => {
    expect(actionLabel("operators.create")).toBe("Set operator role");
    expect(actionLabel("operators.delete")).toBe("Removed operator");
    expect(actionLabel("org.update")).toBe("Updated org config");
    expect(actionLabel("warehouse.update")).toBe("Updated warehouse config");
    expect(actionLabel("warehouse.provision")).toBe("Provisioned warehouse");
    expect(actionLabel("warehouse.deprovision")).toBe("Deprovisioned warehouse");
    expect(actionLabel("warehouse.reset_password")).toBe("Reset warehouse root password");
    expect(actionLabel("credential.create")).toBe("Minted service credential");
    expect(actionLabel("credential.refresh")).toBe("Refreshed service credential");
    expect(actionLabel("user.kill")).toBe("Killed user sessions");
    expect(actionLabel("user.disable")).toBe("Disabled user");
    expect(actionLabel("secret.delete")).toBe("Deleted user secret");
    expect(actionLabel("session.cancel")).toBe("Cancelled session");
    expect(actionLabel("impersonate.query")).toBe("Ran impersonated query");
  });

  it("degrades gracefully for the generic config bucket and unknown codes", () => {
    expect(actionLabel("config.update")).toBe("Config updated");
    expect(actionLabel("widget.frobnicate")).toBe("Frobnicate widget");
    expect(actionLabel("")).toBe("—");
  });
});

describe("auditSummary", () => {
  it("appends the target user when present", () => {
    expect(auditSummary(entry({ action: "operators.create", target_user: "bob@posthog.com" }))).toBe(
      "Set operator role bob@posthog.com",
    );
  });

  it("falls back to the bare label when there is no target", () => {
    expect(auditSummary(entry({ action: "org.create", target_user: "" }))).toBe("Created org");
  });

  it("renders a pre-mint credential failure as failed", () => {
    expect(auditSummary(entry({ action: "credential.create", status: 500 }))).toBe(
      "Failed to mint service credential",
    );
  });

  it("renders a 2xx credential mint as successful", () => {
    expect(auditSummary(entry({ action: "credential.create", status: 201 }))).toBe("Minted service credential");
  });

  it("renders a durable mint as successful even when post-mint work returns 500", () => {
    const durableMint = entry({ action: "credential.create", status: 500 });
    Object.assign(durableMint, { outcome: "credential_minted" });

    expect(auditSummary(durableMint)).toBe("Minted service credential");
  });
});
