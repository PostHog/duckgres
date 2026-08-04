import { describe, expect, it, vi, beforeEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { RESHARD_LOG_CATCHUP_PAGE, useReshardLog } from "./useApi";
import { api } from "@/lib/api";
import type { ReshardLogEntry } from "@/types/api";

// Only getReshardLog is exercised here; keep the rest of the client intact so
// unrelated hooks in useApi.ts still type-check against the real module.
vi.mock("@/lib/api", async (importOriginal) => {
  const mod = await importOriginal<typeof import("@/lib/api")>();
  return { ...mod, api: { ...mod.api, getReshardLog: vi.fn() } };
});

const getLog = vi.mocked(api.getReshardLog);

function makeLines(n: number): ReshardLogEntry[] {
  return Array.from({ length: n }, (_, i) => ({
    id: i + 1,
    operation_id: 1,
    ts: "2026-01-01T00:00:00Z",
    level: "info",
    message: `line ${i + 1}`,
  }));
}

// Serve pages from `lines` the way the server does: id > after, capped at the
// requested limit (500 default, like the handler).
function serveFrom(lines: ReshardLogEntry[]) {
  getLog.mockImplementation(async (_opId, afterId, limit) =>
    lines.filter((e) => e.id > afterId).slice(0, limit ?? 500),
  );
}

function makeWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
}

describe("useReshardLog catch-up", () => {
  beforeEach(() => getLog.mockReset());

  it("drains a long backlog in a tight loop of max-size pages and reveals it instantly", async () => {
    const lines = makeLines(2 * RESHARD_LOG_CATCHUP_PAGE + 300);
    serveFrom(lines);

    const { result } = renderHook(() => useReshardLog(1, "running"), {
      wrapper: makeWrapper(),
    });

    // The whole backlog lands well within one poll interval and is revealed
    // at once — no line-by-line replay, no page-per-poll-tick pumping.
    await waitFor(() => expect(result.current).toHaveLength(lines.length));

    const catchup = getLog.mock.calls.filter(([, , limit]) => limit === RESHARD_LOG_CATCHUP_PAGE);
    expect(catchup).toEqual([
      [1, 0, RESHARD_LOG_CATCHUP_PAGE],
      [1, RESHARD_LOG_CATCHUP_PAGE, RESHARD_LOG_CATCHUP_PAGE],
      [1, 2 * RESHARD_LOG_CATCHUP_PAGE, RESHARD_LOG_CATCHUP_PAGE],
    ]);
  });

  it("stops after one short page when the backlog fits", async () => {
    const lines = makeLines(42);
    serveFrom(lines);

    const { result } = renderHook(() => useReshardLog(1, "succeeded"), {
      wrapper: makeWrapper(),
    });
    await waitFor(() => expect(result.current).toHaveLength(42));
    const catchup = getLog.mock.calls.filter(([, , limit]) => limit === RESHARD_LOG_CATCHUP_PAGE);
    expect(catchup).toEqual([[1, 0, RESHARD_LOG_CATCHUP_PAGE]]);
  });

  it("falls back to the incremental poll when catch-up fails", async () => {
    const lines = makeLines(3);
    getLog.mockRejectedValueOnce(new Error("boom"));
    serveFrom(lines); // mockImplementation serves calls after the one-shot rejection

    const { result } = renderHook(() => useReshardLog(1, "running"), {
      wrapper: makeWrapper(),
    });
    // The poll (default page size) picks the lines up and replays them live.
    await waitFor(() => expect(result.current).toHaveLength(3), { timeout: 3000 });
  });

  it("does nothing without an operation id", () => {
    const { result } = renderHook(() => useReshardLog(null, undefined), {
      wrapper: makeWrapper(),
    });
    expect(result.current).toEqual([]);
    expect(getLog).not.toHaveBeenCalled();
  });
});
