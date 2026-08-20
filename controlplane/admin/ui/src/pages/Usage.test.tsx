import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
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

const RESPONSE: MonthlyUsageResponse = {
  from: "2026-06-01T00:00:00Z",
  months: 3,
  watermark_low: "2026-07-20T00:00:00Z",
  rows: [
    // 7200 CPU-seconds = 120 CPU-minutes; 3600 GiB-seconds storage = 1 GiB-hour.
    { month: "2026-08", org_id: "acme", team_id: 5, schema_name: "team_5", cpu_seconds: 7200, memory_seconds: 3600, gib_seconds: 3600 },
    { month: "2026-08", org_id: "acme", team_id: 6, schema_name: "team_6", cpu_seconds: 60, memory_seconds: 120, gib_seconds: 0 },
    { month: "2026-07", org_id: "acme", team_id: 5, schema_name: "team_5", cpu_seconds: 600, memory_seconds: 600, gib_seconds: 0 },
  ],
};

function renderPage() {
  render(
    <MemoryRouter>
      <Usage />
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
    expect(screen.queryByText("team_5")).not.toBeInTheDocument();
  });

  it("defaults to the latest month and shows per-team rows with derived units", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok(RESPONSE));
    renderPage();

    // Latest month (2026-08) is selected by default: both acme teams render.
    expect(screen.getByText("team_5")).toBeInTheDocument();
    expect(screen.getByText("team_6")).toBeInTheDocument();
    // CPU minutes: 7200s -> 120.
    expect(screen.getByText("120")).toBeInTheDocument();
    // Storage: 3600 GiB-seconds -> 1 GiB-hour.
    expect(screen.getAllByText("1").length).toBeGreaterThan(0);
    // The July row is NOT in the default view.
    expect(screen.queryByText("10")).not.toBeInTheDocument(); // 600s = 10 min (July only)
  });

  it("shows month totals for the selected month", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok(RESPONSE));
    renderPage();

    // August total CPU-minutes = 120 + 1 = 121 (scoped to the stat card — the
    // pricing table shows the same org sum).
    expect(within(screen.getByTestId("stat-CPU-min")).getByText("121")).toBeInTheDocument();
  });

  it("renders the retention caveat when billing has acked a watermark", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok(RESPONSE));
    renderPage();
    expect(screen.getByText(/billed and removed/i)).toBeInTheDocument();
  });

  it("renders an empty state when there is no usage", () => {
    hooks.useMonthlyUsage.mockReturnValue(ok({ from: "2026-06-01T00:00:00Z", months: 3, watermark_low: null, rows: [] }));
    renderPage();
    expect(screen.getByText(/no usage has been recorded/i)).toBeInTheDocument();
  });
});
