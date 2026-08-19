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

const LABELS = new Map([
  ["acme", "Acme Inc"],
  ["globex", "Globex Corp"],
]);

function renderPricing(rows = ROWS) {
  return render(
    <MemoryRouter>
      <UsagePricing rows={rows} labels={LABELS} />
    </MemoryRouter>,
  );
}

describe("UsagePricing", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("starts with a baseline scenario prefilled with grounded default prices", () => {
    renderPricing();
    expect(screen.getByDisplayValue("Baseline")).toBeInTheDocument();
    // Defaults ≈ EC2 on-demand m6i-class economics + S3 standard storage:
    // $0.0004/CPU-min ($0.024/vCPU·h), $0.0001/GiB·min ($0.006/GiB·h RAM),
    // $0.00003/GiB·h (~$0.022/GiB·mo S3).
    expect(screen.getByLabelText("Baseline $/CPU-min")).toHaveValue(0.0004);
    expect(screen.getByLabelText("Baseline $/GiB·min")).toHaveValue(0.0001);
    expect(screen.getByLabelText("Baseline $/GiB·h")).toHaveValue(0.00003);
    // The defaults already price the orgs — the calculator is useful on open:
    // acme: 130×0.0004 + 70×0.0001 + 3×0.00003 = $0.0590 → $0.06;
    // globex: 20×0.0004 = $0.008 → $0.01; total $0.067 → $0.07.
    expect(screen.getByText("$0.06")).toBeInTheDocument();
    expect(screen.getByText("$0.01")).toBeInTheDocument();
    expect(screen.getByText("$0.07")).toBeInTheDocument();
  });

  it("shows the org's friendly name with its ID, linked to the org page", () => {
    renderPricing();
    expect(screen.getByText("Acme Inc")).toBeInTheDocument();
    expect(screen.getByText("Globex Corp")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /acme inc/i })).toHaveAttribute("href", "/orgs/acme");
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
    // Two scenario editors now: Baseline + Scenario B (also prefilled).
    expect(screen.getByDisplayValue("Baseline")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Scenario B")).toBeInTheDocument();
    // Price only B's CPU: acme = 130×0.5 + 70×0.0001 + 3×0.00003 = $65.00709
    // → $65.01 under B; Baseline keeps its default-priced $0.06.
    fireEvent.change(screen.getByLabelText("Scenario B $/CPU-min"), { target: { value: "0.5" } });
    expect(screen.getByText("$65.01")).toBeInTheDocument();
    const acmeRow = screen.getByText("Acme Inc").closest("tr")!;
    expect(within(acmeRow).getByText("$0.06")).toBeInTheDocument();
  });

  it("persists scenarios to localStorage across remounts", () => {
    const { unmount } = renderPricing();
    fireEvent.change(screen.getByLabelText("Baseline $/CPU-min"), { target: { value: "0.1" } });
    unmount();
    renderPricing();
    // acme 130×0.1 + 70×0.0001 + 3×0.00003 = $13.00709 → $13.01
    expect(screen.getByText("$13.01")).toBeInTheDocument();
  });

  it("renders a friendly empty state without usage rows", () => {
    renderPricing([]);
    expect(screen.getByText(/no usage rows/i)).toBeInTheDocument();
  });
});
