import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { TrinoQuery, TrinoStatus } from "@/types/api";

const hooks = vi.hoisted(() => ({
  useKillTrinoQuery: vi.fn(),
  useOrgLabels: vi.fn(),
  useTrinoQueries: vi.fn(),
  useTrinoStatus: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

import { TrinoQueries } from "./TrinoQueries";

const ok = <T,>(data: T) => ({
  data,
  isSuccess: true,
  isLoading: false,
  isError: false,
  refetch: vi.fn(),
});

function query(over: Partial<TrinoQuery> = {}): TrinoQuery {
  return {
    query_id: "q1",
    state: "RUNNING",
    org: "org-a-id",
    principal: "product_analytics",
    source: "trino-cli",
    resource_group: "global.tier_free",
    query: "SELECT * FROM events",
    elapsed_ms: 1_000,
    queued_ms: 0,
    cpu_ms: 0,
    physical_input_bytes: 0,
    internal_network_bytes: 0,
    peak_memory_bytes: 0,
    spilled_bytes: 0,
    processed_input_rows: 0,
    total_drivers: 0,
    queued_drivers: 0,
    running_drivers: 0,
    completed_drivers: 0,
    fully_blocked: false,
    progress_percentage: null,
    ...over,
  };
}

function status(over: Partial<TrinoStatus> = {}): TrinoStatus {
  return {
    cell: { id: "cell-001", coordinator_url: "https://coordinator" },
    available: true,
    queries_by_state: {},
    blocked_queries: 0,
    node_stats: true,
    nodes: 2,
    failed_nodes: 0,
    orgs_by_state: {},
    total_orgs: 1,
    ...over,
  };
}

function renderPage() {
  return render(
    <MemoryRouter>
      <TooltipProvider>
        <TrinoQueries />
      </TooltipProvider>
    </MemoryRouter>,
  );
}

// statValue reads the number out of a StatCard by its label.
function statValue(label: string): string {
  return within(screen.getByTestId(`stat-${label}`)).getAllByText(/./)[1].textContent ?? "";
}

describe("TrinoQueries page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({ isAdmin: true });
    hooks.useKillTrinoQuery.mockReturnValue({ mutate: vi.fn(), isPending: false, isError: false });
    hooks.useOrgLabels.mockReturnValue(new Map([["org-a-id", "product_analytics"]]));
    hooks.useTrinoStatus.mockReturnValue(ok(status()));
    hooks.useTrinoQueries.mockReturnValue(ok({ cell: status().cell, available: true, queries: [] }));
  });

  it("summarizes the cell's current load from the listed queries", () => {
    hooks.useTrinoQueries.mockReturnValue(
      ok({
        cell: status().cell,
        available: true,
        queries: [
          query({ query_id: "a", state: "RUNNING", elapsed_ms: 30_000, physical_input_bytes: 1024 }),
          query({ query_id: "b", state: "RUNNING", fully_blocked: true, elapsed_ms: 90_000 }),
          query({ query_id: "c", state: "QUEUED", elapsed_ms: 5_000 }),
        ],
      }),
    );
    renderPage();

    expect(statValue("Running")).toBe("2");
    expect(statValue("Queued")).toBe("1");
    // Blocked is counted separately from running: it means every driver is
    // waiting on the metadata store or S3, which is a cell problem.
    expect(statValue("Blocked")).toBe("1");
  });

  it("flags a blocked query rather than calling it merely slow", () => {
    hooks.useTrinoQueries.mockReturnValue(
      ok({
        cell: status().cell,
        available: true,
        queries: [query({ fully_blocked: true, elapsed_ms: 10 * 60_000 })],
      }),
    );
    renderPage();
    expect(screen.getByText("blocked")).toBeInTheDocument();
    expect(screen.queryByText("long running")).not.toBeInTheDocument();
  });

  it("shows the redacted SQL the server sent, never a raw statement", () => {
    // The control plane redacts at decode; the page renders whatever it is
    // given. This pins that the page does not, say, fall back to a raw
    // field if one were ever added.
    hooks.useTrinoQueries.mockReturnValue(
      ok({
        cell: status().cell,
        available: true,
        queries: [query({ query: "CREATE SECRET s (KEY_ID '<redacted>')" })],
      }),
    );
    renderPage();
    expect(screen.getByText("CREATE SECRET s (KEY_ID '<redacted>')")).toBeInTheDocument();
  });

  it("attributes a control-plane query to its source rather than to an org", () => {
    // A query with no org is the reconcile loop's DDL or this console's own
    // reads. Showing a blank org cell would read as "unknown tenant".
    hooks.useTrinoQueries.mockReturnValue(
      ok({
        cell: status().cell,
        available: true,
        queries: [query({ org: "", principal: "__admin_provisioner", source: "duckgres-provisioner" })],
      }),
    );
    renderPage();
    expect(screen.getByText("duckgres-provisioner")).toBeInTheDocument();
  });

  it("explains an unconfigured deployment differently from an outage", () => {
    hooks.useTrinoStatus.mockReturnValue(
      ok(status({ cell: { id: "", coordinator_url: "" }, available: false })),
    );
    renderPage();
    expect(screen.getByText(/DUCKGRES_TRINO_COORDINATOR_URL/)).toBeInTheDocument();

    hooks.useTrinoStatus.mockReturnValue(
      ok(status({ available: false, error: "dial tcp: connection refused" })),
    );
    renderPage();
    expect(screen.getAllByText(/did not answer/).length).toBeGreaterThan(0);
  });

  it("offers Kill only on queries that are still killable", () => {
    hooks.useTrinoQueries.mockReturnValue(
      ok({
        cell: status().cell,
        available: true,
        queries: [
          query({ query_id: "live", state: "RUNNING" }),
          query({ query_id: "done", state: "FINISHED" }),
        ],
      }),
    );
    renderPage();
    // One row is killable, the other has already finished.
    expect(screen.getAllByRole("button", { name: "Kill" })).toHaveLength(1);
  });

  it("disables Kill for a viewer instead of hiding it", () => {
    // The console's convention: viewers see the affordance disabled with a
    // reason, so the capability is discoverable and still enforced (403).
    identity.useIdentity.mockReturnValue({ isAdmin: false });
    hooks.useTrinoQueries.mockReturnValue(
      ok({ cell: status().cell, available: true, queries: [query()] }),
    );
    renderPage();
    expect(screen.getByRole("button", { name: "Kill" })).toBeDisabled();
  });
});
