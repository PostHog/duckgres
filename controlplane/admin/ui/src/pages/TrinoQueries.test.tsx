import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { TrinoQuery, TrinoStatus } from "@/types/api";

const hooks = vi.hoisted(() => ({
  useKillTrinoQuery: vi.fn(),
  useOrgLabels: vi.fn(),
  useTrinoQueries: vi.fn(),
  useTrinoStatus: vi.fn(),
  useTrinoCells: vi.fn(),
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
    cell: { id: "legacy" },
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

function renderPage(path = "/trino/queries") {
  return render(
    <MemoryRouter initialEntries={[path]}>
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
    hooks.useTrinoCells.mockReturnValue(ok({ cells: [{ id: "legacy" }] }));
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
          // Past the resource-group queue but not yet executing: this is the
          // state a saturated cell parks work in, and it used to count as 0.
          query({ query_id: "d", state: "WAITING_FOR_RESOURCES", elapsed_ms: 5_000 }),
        ],
      }),
    );
    renderPage();

    expect(statValue("Running")).toBe("2");
    expect(statValue("Waiting")).toBe("2");
    // Blocked is counted separately from running: it means every driver is
    // waiting on the metadata store or S3, which is a cell problem.
    expect(statValue("Blocked")).toBe("1");
  });

  it("keeps a selected cell in both query and status requests", () => {
    renderPage("/trino/queries?cell=cell-001");
    expect(hooks.useTrinoQueries).toHaveBeenCalledWith({ active: true, cell: "cell-001" });
    expect(hooks.useTrinoStatus).toHaveBeenCalledWith("cell-001");
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
    expect(within(screen.getByTestId("query-q1")).getByText("duckgres-provisioner")).toBeInTheDocument();
  });

  it("explains an unconfigured deployment differently from an outage", () => {
    hooks.useTrinoStatus.mockReturnValue(
      ok(status({ cell: { id: "" }, available: false })),
    );
    renderPage();
    expect(screen.getByText(/DUCKGRES_TRINO_CELLS_FILE/)).toBeInTheDocument();

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

  function withQueries(queries: TrinoQuery[]) {
    hooks.useTrinoQueries.mockReturnValue(ok({ cell: status().cell, available: true, queries }));
  }
  const rowIds = () =>
    screen.getAllByTestId(/^query-/).map((row) => row.getAttribute("data-testid")!.slice("query-".length));

  it("leads each row with the statement and its target, not the client's comment", () => {
    withQueries([
      query({ query: '/* SQLMESH_PLAN: abc */ INSERT INTO "org_p"."sqlmesh_trino"."events__1" SELECT 1' }),
    ]);
    renderPage();
    const row = screen.getByTestId("query-q1");
    expect(within(row).getByText("INSERT INTO")).toBeInTheDocument();
    expect(within(row).getByText("org_p.sqlmesh_trino.events__1")).toBeInTheDocument();
  });

  it("expands a row to the full SQL and its execution detail", () => {
    withQueries([query({ query: "SELECT * FROM events", total_drivers: 8, completed_drivers: 3 })]);
    renderPage();
    expect(screen.queryByText("Query id")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("query-q1"));
    expect(screen.getByText("Query id")).toBeInTheDocument();
    expect(screen.getByText(/3\/8 done/)).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("query-q1"));
    expect(screen.queryByText("Query id")).not.toBeInTheDocument();
  });

  it("sorts by elapsed by default and re-sorts by a clicked column", () => {
    withQueries([
      query({ query_id: "short", elapsed_ms: 1_000, cpu_ms: 900 }),
      query({ query_id: "long", elapsed_ms: 60_000, cpu_ms: 10 }),
    ]);
    renderPage();
    expect(rowIds()).toEqual(["long", "short"]);
    fireEvent.click(screen.getByRole("button", { name: "CPU" }));
    expect(rowIds()).toEqual(["short", "long"]);
    fireEvent.click(screen.getByRole("button", { name: "CPU" }));
    expect(rowIds()).toEqual(["long", "short"]);
  });

  it("filters by state bucket while the headline keeps describing the cell", () => {
    withQueries([
      query({ query_id: "run", state: "RUNNING" }),
      query({ query_id: "wait", state: "WAITING_FOR_RESOURCES" }),
      query({ query_id: "plan", state: "PLANNING" }),
    ]);
    renderPage();
    const chips = screen.getByRole("group", { name: "State filter" });
    fireEvent.click(within(chips).getByRole("button", { name: /Waiting/ }));
    expect(rowIds().sort()).toEqual(["plan", "wait"]);
    expect(statValue("Running")).toBe("1");
  });

  it("breaks load down by org and narrows the list to a clicked org", () => {
    hooks.useOrgLabels.mockReturnValue(new Map([["org-a-id", "alpha"], ["org-b-id", "beta"]]));
    withQueries([
      query({ query_id: "a1", org: "org-a-id" }),
      query({ query_id: "b1", org: "org-b-id" }),
      query({ query_id: "b2", org: "org-b-id", state: "QUEUED" }),
    ]);
    renderPage();
    // Busiest org first.
    const groups = screen.getAllByTestId(/^group-/).map((g) => g.getAttribute("data-testid"));
    expect(groups).toEqual(["group-org-b-id", "group-org-a-id"]);
    fireEvent.click(screen.getByTestId("group-org-b-id"));
    expect(rowIds().sort()).toEqual(["b1", "b2"]);
    fireEvent.click(screen.getByRole("button", { name: /Clear beta/ }));
    expect(rowIds()).toHaveLength(3);
  });

  it("shows which pool instance holds each query when the cell is pooled", () => {
    withQueries([
      query({ query_id: "x", instance: "cell-0a1b2c3d" }),
      query({ query_id: "y", instance: "cell-0a1b2c3d" }),
      query({ query_id: "z", instance: "cell-ffee0011" }),
    ]);
    renderPage();
    expect(screen.getByText("By pool instance")).toBeInTheDocument();
    expect(within(screen.getByTestId("group-cell-0a1b2c3d")).getByText("2")).toBeInTheDocument();
    expect(within(screen.getByTestId("query-z")).getByText("cell-ffee0011")).toBeInTheDocument();
  });

  it("omits the instance breakdown for a single-coordinator cell", () => {
    withQueries([query()]);
    renderPage();
    expect(screen.queryByText("By pool instance")).not.toBeInTheDocument();
  });

  it("searches SQL text and query ids, not only orgs", () => {
    withQueries([
      query({ query_id: "20261007_a", query: "SELECT * FROM persons" }),
      query({ query_id: "20261007_b", query: "SELECT * FROM events" }),
    ]);
    renderPage();
    const search = screen.getByPlaceholderText(/Search org/);
    fireEvent.change(search, { target: { value: "persons" } });
    expect(rowIds()).toEqual(["20261007_a"]);
    fireEvent.change(search, { target: { value: "_b" } });
    expect(rowIds()).toEqual(["20261007_b"]);
  });

  it("does not expand the row when Kill is clicked", () => {
    withQueries([query()]);
    renderPage();
    fireEvent.click(screen.getByRole("button", { name: "Kill" }));
    expect(screen.queryByText("Query id")).not.toBeInTheDocument();
  });
});
