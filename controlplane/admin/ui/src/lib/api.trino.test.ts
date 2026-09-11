import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";

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
});
