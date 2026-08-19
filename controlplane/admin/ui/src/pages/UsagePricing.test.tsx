import { describe, expect, it, beforeEach } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { UsagePricing } from "./UsagePricing";
import type { MonthlyUsageRow } from "@/types/api";

// UsagePricing is a pure-props component (rows come from the Usage page's
// monthly query) — no hook mocks needed. localStorage is cleared per test so
// scenario persistence doesn't leak between cases.
const ROWS: MonthlyUsageRow[] = [
  // acme: cpu 130 min, mem 70 GiB·min, storage 3 GiB·h (two teams summed).
  { month: "2026-08", org_id: "acme", team_id: 5, schema_name: "team_5", cpu_seconds: 7200, memory_seconds: 3600, gib_seconds: 3600 },
  { month: "2026-08", org_id: "acme", team_id: 6, schema_name: "team_6", cpu_seconds: 600, memory_seconds: 600, gib_seconds: 7200 },
  // globex: cpu 20 min.
  { month: "2026-08", org_id: "globex", team_id: 9, schema_name: "team_9", cpu_seconds: 1200, memory_seconds: 0, gib_seconds: 0 },
];

function renderPricing(rows = ROWS) {
  return render(
    <MemoryRouter>
      <UsagePricing rows={rows} />
    </MemoryRouter>,
  );
}

describe("UsagePricing", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("starts with one baseline scenario and zero costs", () => {
    renderPricing();
    // One scenario editor.
    expect(screen.getByDisplayValue("Baseline")).toBeInTheDocument();
    // acme + globex rows and the all-orgs total, each $0.00.
    expect(screen.getByText("acme")).toBeInTheDocument();
    expect(screen.getByText("globex")).toBeInTheDocument();
    expect(screen.getAllByText("$0.00")).toHaveLength(3);
  });

  it("prices each org and the grand total from the entered prices", () => {
    renderPricing();
    fireEvent.change(screen.getByLabelText("Baseline $/CPU-min"), { target: { value: "0.1" } });
    fireEvent.change(screen.getByLabelText("Baseline $/GiB·min"), { target: { value: "0.01" } });
    fireEvent.change(screen.getByLabelText("Baseline $/GiB·h"), { target: { value: "2" } });
    // acme: 130×0.1 + 70×0.01 + 3×2 = $19.70; globex: 20×0.1 = $2.00; total $21.70.
    expect(screen.getByText("$19.70")).toBeInTheDocument();
    expect(screen.getByText("$2.00")).toBeInTheDocument();
    expect(screen.getByText("$21.70")).toBeInTheDocument();
  });

  it("adds a scenario column for side-by-side sensitivity", () => {
    renderPricing();
    fireEvent.click(screen.getByRole("button", { name: /add scenario/i }));
    // Two scenario editors now: Baseline + Scenario B.
    expect(screen.getByDisplayValue("Baseline")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Scenario B")).toBeInTheDocument();
    // Price only B: acme = 130 × 0.5 = $65.00 under B, still $0.00 under Baseline.
    fireEvent.change(screen.getByLabelText("Scenario B $/CPU-min"), { target: { value: "0.5" } });
    expect(screen.getByText("$65.00")).toBeInTheDocument();
    // acme's Baseline cell stays $0.00 while B prices at $65.00.
    const acmeRow = screen.getByText("acme").closest("tr")!;
    expect(within(acmeRow).getByText("$0.00")).toBeInTheDocument();
  });

  it("persists scenarios to localStorage across remounts", () => {
    const { unmount } = renderPricing();
    fireEvent.change(screen.getByLabelText("Baseline $/CPU-min"), { target: { value: "0.1" } });
    unmount();
    renderPricing();
    expect(screen.getByText("$13.00")).toBeInTheDocument(); // acme 130 × 0.1
  });

  it("renders a friendly empty state without usage rows", () => {
    renderPricing([]);
    expect(screen.getByText(/no usage rows/i)).toBeInTheDocument();
  });
});
