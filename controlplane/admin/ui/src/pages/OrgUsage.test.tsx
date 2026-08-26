import { describe, expect, it, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
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
      <OrgUsageSection orgId="acme" />
    </MemoryRouter>,
  );
}

describe("OrgUsageSection", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "op@posthog.com", role: "admin", source: "sso" } });
    hooks.useOrgDailyUsage.mockReturnValue(ok(RESPONSE));
  });

  it("renders the three usage charts with window totals", () => {
    renderSection();
    expect(screen.getByText("CPU-minutes")).toBeInTheDocument();
    expect(screen.getByText("Memory GiB·minutes")).toBeInTheDocument();
    expect(screen.getByText("S3 GiB·hours")).toBeInTheDocument();
    // Window totals: CPU (7200+60+600)/60 = 131; mem (3600+60+600)/60 = 71; S3 (3600+7200)/3600 = 3.
    expect(screen.getByText(/131 total/)).toBeInTheDocument();
    expect(screen.getByText(/71 total/)).toBeInTheDocument();
    expect(screen.getByText(/3 total/)).toBeInTheDocument();
  });

  it("queries with the selected period when a period button is clicked", () => {
    renderSection();
    expect(hooks.useOrgDailyUsage).toHaveBeenCalledWith("acme", 14);
    fireEvent.click(screen.getByRole("button", { name: "30d" }));
    expect(hooks.useOrgDailyUsage).toHaveBeenCalledWith("acme", 30);
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
  });

  it("renders nothing for viewers (cost data is admin-only)", () => {
    identity.useIdentity.mockReturnValue({ isAdmin: false, me: { email: "v@posthog.com", role: "viewer", source: "sso" } });
    const { container } = renderSection();
    expect(container).toBeEmptyDOMElement();
  });
});
