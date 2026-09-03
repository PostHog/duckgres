import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { MonthlyUsageResponse } from "@/types/api";

// Mock the data hooks so we can render Usage with a controlled monthly
// response and assert the grouping/totals directly.
const hooks = vi.hoisted(() => ({
  useMonthlyUsage: vi.fn(),
  useOrgLabels: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

import { Usage } from "./Usage";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false, refetch: vi.fn() });
const AUGUST_HOURS = 744;
const gibSeconds = (gibMonths: number) => gibMonths * AUGUST_HOURS * 3600;

const RESPONSE: MonthlyUsageResponse = {
  from: "2026-06-01T00:00:00Z",
  months: 3,
  watermark_low: "2026-07-20T00:00:00Z",
  rows: [
    // Two historical team stamps for the same org must become one storage row.
    { month: "2026-08", org_id: "acme", team_id: 5, schema_name: "team_5", cpu_seconds: 1, memory_seconds: 1, gib_seconds: gibSeconds(200) },
    { month: "2026-08", org_id: "acme", team_id: 6, schema_name: "team_6", cpu_seconds: 1, memory_seconds: 1, gib_seconds: gibSeconds(400) },
    { month: "2026-08", org_id: "globex", team_id: 9, schema_name: "team_9", cpu_seconds: 1, memory_seconds: 1, gib_seconds: gibSeconds(50) },
    { month: "2026-07", org_id: "acme", team_id: 5, schema_name: "team_5", cpu_seconds: 600, memory_seconds: 600, gib_seconds: 0 },
  ],
};

function renderPage() {
  render(
    <MemoryRouter>
      <TooltipProvider delayDuration={0}>
        <Usage />
      </TooltipProvider>
    </MemoryRouter>,
  );
}

describe("Usage page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    hooks.useOrgLabels.mockReturnValue(new Map());
    identity.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "op@posthog.com", role: "admin", source: "sso" } });
  });

  it("shows an admin-only notice to viewers and never exposes the data", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok(RESPONSE));
    identity.useIdentity.mockReturnValue({ isAdmin: false, me: { email: "v@posthog.com", role: "viewer", source: "sso" } });
    renderPage();
    expect(screen.getByText(/admin only/i)).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Org" })).not.toBeInTheDocument();
    expect(screen.queryByText(/per-team/i)).not.toBeInTheDocument();
  });

  it("shows storage only and aggregates historical team rows into one org row", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok(RESPONSE));
    renderPage();

    expect(screen.getByText(/storage-time.*retained billing buffer/i)).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Team" })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: /CPU/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: /Memory/i })).not.toBeInTheDocument();
    expect(screen.queryByText("team_5")).not.toBeInTheDocument();
    expect(screen.queryByText("team_6")).not.toBeInTheDocument();

    expect(screen.getAllByRole("table")).toHaveLength(1);
    const usageTable = screen.getByRole("table");
    expect(within(usageTable).getByRole("columnheader", { name: /allocated aws cost/i })).toBeInTheDocument();
    const acmeRow = within(usageTable).getByText("acme").closest("tr")!;
    expect(within(acmeRow).getByText("446,400")).toBeInTheDocument();
  });

  it("shows total cost, customer price, and gross margin without compute cards", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok(RESPONSE));
    renderPage();

    expect(within(screen.getByTestId("stat-Total cost")).getByText("$14.95")).toBeInTheDocument();
    expect(within(screen.getByTestId("stat-Total price")).getByText("$19.50")).toBeInTheDocument();
    expect(within(screen.getByTestId("stat-Total gross margin")).getByText("23.3%")).toBeInTheDocument();
    expect(within(screen.getByTestId("stat-Total gross margin")).getByText("$4.55 gross profit")).toBeInTheDocument();
    expect(screen.queryByTestId("stat-S3 GiB·h")).not.toBeInTheDocument();
    expect(screen.queryByTestId("stat-CPU-min")).not.toBeInTheDocument();
    expect(screen.queryByTestId("stat-Memory GiB·min")).not.toBeInTheDocument();
  });

  it("renders the retention caveat when billing has acked a watermark", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok(RESPONSE));
    renderPage();
    expect(screen.getByText(/billed and removed/i)).toBeInTheDocument();
    expect(screen.queryByText(/garbage-collected/i)).not.toBeInTheDocument();
  });

  it("renders an empty state when there is no usage", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok({ from: "2026-06-01T00:00:00Z", months: 3, watermark_low: null, rows: [] }));
    renderPage();
    expect(screen.getByText(/no usage has been recorded/i)).toBeInTheDocument();
  });
});
