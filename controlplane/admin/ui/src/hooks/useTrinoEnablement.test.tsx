import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { api } from "@/lib/api";
import { useSetTrinoEnabled } from "./useApi";

vi.mock("@/lib/api", async (importOriginal) => {
  const mod = await importOriginal<typeof import("@/lib/api")>();
  return { ...mod, api: { ...mod.api, enableTrino: vi.fn(), disableTrino: vi.fn() } };
});

describe("Trino enablement mutation", () => {
  beforeEach(() => vi.clearAllMocks());

  it.each([true, false])("sends enabled=%s and refreshes Trino without discarding the org form", async (enabled) => {
    vi.mocked(api.enableTrino).mockResolvedValue({ status: "queued", org: "org-a" });
    vi.mocked(api.disableTrino).mockResolvedValue({ status: "queued", org: "org-a" });
    const client = new QueryClient();
    const invalidate = vi.spyOn(client, "invalidateQueries");
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result } = renderHook(() => useSetTrinoEnabled(), { wrapper });
    await act(async () => { await result.current.mutateAsync({ org: "org-a", enabled, tier: "premium" }); });
    if (enabled) expect(api.enableTrino).toHaveBeenCalledWith("org-a", "premium");
    else expect(api.disableTrino).toHaveBeenCalledWith("org-a");
    expect(invalidate.mock.calls).toEqual([[{ queryKey: ["trino"] }]]);
  });

  it("refreshes authoritative state after a failed request without automatic retry", async () => {
    vi.mocked(api.enableTrino).mockRejectedValue(new Error("Timed out"));
    const client = new QueryClient();
    const invalidate = vi.spyOn(client, "invalidateQueries");
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result } = renderHook(() => useSetTrinoEnabled(), { wrapper });
    await act(async () => {
      await expect(result.current.mutateAsync({ org: "org-a", enabled: true, tier: "" })).rejects.toThrow("Timed out");
    });
    expect(api.enableTrino).toHaveBeenCalledTimes(1);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["trino"] });
  });
});
