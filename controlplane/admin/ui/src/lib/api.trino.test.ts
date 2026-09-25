import { afterEach, describe, expect, it, vi } from "vitest";
import { api, ApiError } from "./api";

describe("Trino enablement API", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("uses same-origin authenticated routes and preserves the selected tier", async () => {
    const fetch = vi.fn().mockImplementation(async () => new Response('{"status":"queued"}', { status: 202 }));
    vi.stubGlobal("fetch", fetch);
    await api.enableTrino("org/a", "premium");
    expect(fetch).toHaveBeenCalledWith("/api/v1/orgs/org%2Fa/trino", expect.objectContaining({
      method: "POST", body: '{"enabled":true,"tier":"premium"}',
      headers: { Accept: "application/json", "Content-Type": "application/json" },
    }));
    await api.disableTrino("org/a");
    expect(fetch).toHaveBeenLastCalledWith("/api/v1/orgs/org%2Fa/trino", expect.objectContaining({
      method: "DELETE", headers: { Accept: "application/json" },
    }));
  });
  it("sends explicit backend selection without changing the separate disable route", async () => {
    const fetch = vi.fn().mockImplementation(async () => new Response('{"status":"queued"}', { status: 202 }));
    vi.stubGlobal("fetch", fetch);
    await api.enableTrino("tenant", "", "hoglake");
    expect(fetch).toHaveBeenCalledWith("/api/v1/orgs/tenant/trino", expect.objectContaining({
      method: "POST", body: '{"enabled":true,"tier":"","backend":"hoglake"}',
    }));
  });

});

describe("Trino recovery API", () => {
  const body = {
    operation_id: "operation-a", expected_generation: 7, incarnation: "incarnation-a", pod_uid: "pod-a",
    boot_id: "boot-a", node_id: "node-a", coordinator_id: "coordinator-a", reason: "Investigated drain",
    destructive_authorization: true as const,
  };
  afterEach(() => vi.unstubAllGlobals());

  it("uses same-origin authenticated requests with encoded exact cell and instance", async () => {
    const fetch = vi.fn().mockImplementation(async () => new Response(JSON.stringify({ request: { ...body, instance_id: "instance/a" } }), { status: 202 }));
    vi.stubGlobal("fetch", fetch);
    await api.trinoInstances("pool/a");
    expect(fetch).toHaveBeenLastCalledWith("/api/v1/trino/instances?cell=pool%2Fa", expect.objectContaining({ method: "GET" }));
    await api.trinoRecovery("instance/a", "pool/a");
    expect(fetch).toHaveBeenLastCalledWith("/api/v1/trino/instances/instance%2Fa/recovery?cell=pool%2Fa", expect.objectContaining({ method: "GET" }));
    await api.requestTrinoRecovery("instance/a", body, "pool/a");
    expect(fetch).toHaveBeenLastCalledWith("/api/v1/trino/instances/instance%2Fa/recovery?cell=pool%2Fa", expect.objectContaining({
      method: "POST", body: JSON.stringify(body), headers: { Accept: "application/json", "Content-Type": "application/json" },
    }));
  });

  it.each([{}, { request: { ...body, instance_id: "other-instance" } }, { request: { ...body, instance_id: "instance-a", operation_id: "other-operation" } }])(
    "treats an unexpected success response as unknown acceptance", async (result) => {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(result), { status: 200 })));
      await expect(api.requestTrinoRecovery("instance-a", body, "pool-a")).rejects.toBeInstanceOf(ApiError);
    },
  );
});
