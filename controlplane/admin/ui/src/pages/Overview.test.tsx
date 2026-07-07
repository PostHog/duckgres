import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, within } from "@testing-library/react";
import type { FleetStat } from "@/types/api";

// Mock the data hooks so we can render Overview with controlled fleet/status and
// assert the derived UI directly — this is the render-level guard for the
// worker-count regressions (the pure math is covered in lib/fleet.test.ts).
const hooks = vi.hoisted(() => ({
  useClusterStatus: vi.fn(),
  useFleet: vi.fn(),
  useModel: vi.fn(),
  useMetricRange: vi.fn(),
  useOrgs: vi.fn(),
}));

vi.mock("@/hooks/useApi", () => hooks);

import { Overview } from "./Overview";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false });

function setup(fleet: FleetStat[], opts?: { sessions?: number; orgs?: number; statusOrgs?: unknown[]; orgRows?: unknown[] }) {
  hooks.useFleet.mockReturnValue(ok(fleet));
  hooks.useClusterStatus.mockReturnValue(
    ok({
      total_orgs: opts?.orgs ?? 11,
      total_sessions: opts?.sessions ?? 0,
      total_workers: 0,
      orgs: opts?.statusOrgs ?? [],
    }),
  );
  hooks.useOrgs.mockReturnValue(ok(opts?.orgRows ?? []));
  hooks.useModel.mockReturnValue(ok({ rows: [] }));
  hooks.useMetricRange.mockReturnValue(ok(undefined));
}

const fs = (state: string, count: number): FleetStat => ({
  image: "img",
  state,
  binding: "org_bound",
  count,
  cpu_cores: 0,
  memory_bytes: 0,
});

// Find a StatCard by its stable data-testid (set in StatCard.tsx), so the test
// isn't coupled to the card's Tailwind classes.
function card(label: string): HTMLElement {
  return screen.getByTestId(`stat-${label}`);
}

describe("Overview Workers card", () => {
  beforeEach(() => vi.clearAllMocks());

  // Regression for the prod screenshot: 150 hot workers, 150 sessions, 0
  // hot_idle. Must read "150 hot · 0 idle" and NOT show the leak warning.
  it("shows busy/idle from distinct states and no false leak warning", () => {
    setup([fs("hot", 150)], { sessions: 150 });
    render(<Overview />);

    const workers = card("Workers");
    expect(within(workers).getByText("150")).toBeInTheDocument();
    expect(within(workers).getByText("150 hot · 0 idle")).toBeInTheDocument();

    // The "not holding a session" / hot-idle leak warning must be absent.
    expect(screen.queryByText(/parked/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/reserving vCPU/i)).not.toBeInTheDocument();
  });

  it("raises the leak warning only when hot_idle is genuinely high", () => {
    setup([fs("hot", 4), fs("hot_idle", 25)], { sessions: 4 });
    render(<Overview />);

    expect(within(card("Workers")).getByText("4 hot · 25 idle")).toBeInTheDocument();
    expect(screen.getByText(/reserving vCPU/i)).toBeInTheDocument();
  });
});

describe("Overview per-org load", () => {
  beforeEach(() => vi.clearAllMocks());

  it("shows the readable org database name when org metadata is available", () => {
    setup([], {
      statusOrgs: [
        {
          name: "019740a8-ac01-0000-cad1-26dbbe0cde55",
          workers: 7,
          active_sessions: 3,
          max_workers: 10,
        },
      ],
      orgRows: [
        {
          name: "019740a8-ac01-0000-cad1-26dbbe0cde55",
          database_name: "product_analytics",
          hostname_alias: null,
        },
      ],
    });

    render(<Overview />);

    expect(screen.getByText("product_analytics")).toBeInTheDocument();
    expect(screen.getByText("019740a8-ac01-0000-cad1-26dbbe0cde55")).toBeInTheDocument();
  });
});
