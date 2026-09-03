import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { DailyUsageResponse } from "@/types/api";

// Mock data + identity hooks: render OrgUsageSection with a controlled daily
// response and assert the chart cards, window totals, period switch, caveat,
// and the viewer gate. (Recharts draws nothing in jsdom — the assertions are
// on the surrounding card chrome, which is where the totals live.)
const hooks = vi.hoisted(() => ({
  useOrgDailyUsage: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

import { OrgUsageSection } from "./OrgUsage";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false, refetch: vi.fn() });

const RESPONSE: DailyUsageResponse = {
  org_id: "acme",
  days: 14,
  from: "2026-08-01T00:00:00Z",
  watermark_low: null,
  rows: [
    // 7200 CPU-seconds = 120 CPU-min; 3600 mem-seconds = 60 GiB·min; 3600 gib-seconds = 1 GiB·h.
    { date: "2026-08-13", team_id: 5, schema_name: "team_5", cpu_seconds: 7200, memory_seconds: 3600, gib_seconds: 3600 },
    { date: "2026-08-13", team_id: 6, schema_name: "team_6", cpu_seconds: 60, memory_seconds: 60, gib_seconds: 0 },
    { date: "2026-08-14", team_id: 5, schema_name: "team_5", cpu_seconds: 600, memory_seconds: 600, gib_seconds: 7200 },
  ],
};

function renderSection() {
  return render(
    <MemoryRouter>
      <TooltipProvider delayDuration={0}>
        <OrgUsageSection orgId="acme" />
      </TooltipProvider>
    </MemoryRouter>,
  );
}

describe("OrgUsageSection", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "op@posthog.com", role: "admin", source: "sso" } });
    hooks.useOrgDailyUsage.mockReturnValue(ok(RESPONSE));
  });

  it("renders one org-level storage chart without compute or team series", () => {
    renderSection();
    expect(screen.getByText("S3 GiB·hours")).toBeInTheDocument();
    expect(screen.getByText(/3 total/)).toBeInTheDocument();
    expect(screen.queryByText("CPU-minutes")).not.toBeInTheDocument();
    expect(screen.queryByText("Memory GiB·minutes")).not.toBeInTheDocument();
    expect(screen.queryByText("team_5")).not.toBeInTheDocument();
    expect(screen.queryByText("team_6")).not.toBeInTheDocument();
  });

  it("queries with the selected period when a period button is clicked", () => {
    renderSection();
    expect(hooks.useOrgDailyUsage).toHaveBeenCalledWith("acme", 14);
    fireEvent.click(screen.getByRole("button", { name: "30d" }));
    expect(hooks.useOrgDailyUsage).toHaveBeenCalledWith("acme", 30);
  });

  it("offers UTC week-to-date and month-to-date presets", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-19T12:00:00Z")); // Wednesday, day 19.
    renderSection();

    fireEvent.click(screen.getByRole("button", { name: "WTD" }));
    expect(hooks.useOrgDailyUsage).toHaveBeenCalledWith("acme", 3);

    fireEvent.click(screen.getByRole("button", { name: "MTD" }));
    expect(hooks.useOrgDailyUsage).toHaveBeenCalledWith("acme", 19);
  });

  it("explains that GiB·h is storage over time", async () => {
    renderSection();
    fireEvent.focus(screen.getByRole("button", { name: "Explain S3 GiB·h" }));
    expect(
      (await screen.findAllByText(/storage over time, not current bucket size or a transfer rate/i)).length,
    ).toBeGreaterThan(0);
  });

  it("renders an empty state when the org has no usage in the window", () => {
    hooks.useOrgDailyUsage.mockReturnValue(ok({ ...RESPONSE, rows: [] }));
    renderSection();
    expect(screen.getByText(/no usage recorded/i)).toBeInTheDocument();
  });

  it("shows the retention caveat when billing has acked inside the window", () => {
    hooks.useOrgDailyUsage.mockReturnValue(ok({ ...RESPONSE, watermark_low: "2026-08-12T00:00:00Z" }));
    renderSection();
    expect(screen.getByText(/billed and removed/i)).toBeInTheDocument();
    expect(screen.queryByText(/garbage-collected/i)).not.toBeInTheDocument();
  });

  it("renders nothing for viewers (cost data is admin-only)", () => {
    identity.useIdentity.mockReturnValue({ isAdmin: false, me: { email: "v@posthog.com", role: "viewer", source: "sso" } });
    const { container } = renderSection();
    expect(container).toBeEmptyDOMElement();
  });
});
