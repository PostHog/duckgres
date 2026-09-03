import { describe, expect, it } from "vitest";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import { UsagePricing } from "./UsagePricing";
import type { MonthlyUsageRow } from "@/types/api";
import type { PricingRegion } from "@/lib/pricing";

const AUGUST_HOURS = 744;
const gibSeconds = (gibMonths: number) => gibMonths * AUGUST_HOURS * 3600;
const ROWS: MonthlyUsageRow[] = [
  { month: "2026-08", org_id: "acme", team_id: 5, schema_name: "team_5", cpu_seconds: 1, memory_seconds: 1, gib_seconds: gibSeconds(200) },
  { month: "2026-08", org_id: "acme", team_id: 6, schema_name: "team_6", cpu_seconds: 1, memory_seconds: 1, gib_seconds: gibSeconds(400) },
  { month: "2026-08", org_id: "globex", team_id: 9, schema_name: "team_9", cpu_seconds: 1, memory_seconds: 1, gib_seconds: gibSeconds(50) },
  { month: "2026-08", org_id: "initech", team_id: 10, schema_name: "team_10", cpu_seconds: 1, memory_seconds: 1, gib_seconds: gibSeconds(200) },
];

const LABELS = new Map([
  ["acme", "Acme Inc"],
  ["globex", "Globex Corp"],
  ["initech", "Aardvark Systems"],
]);

function renderPricing(region: PricingRegion = "US") {
  return render(
    <MemoryRouter>
      <TooltipProvider delayDuration={0}>
        <UsagePricing rows={ROWS} labels={LABELS} month="2026-08" region={region} />
      </TooltipProvider>
    </MemoryRouter>,
  );
}

function visibleOrgOrder(): string[] {
  return screen
    .getAllByRole("link")
    .map((link) => link.querySelector("span > span")?.textContent?.trim() ?? "")
    .filter(Boolean);
}

describe("UsagePricing", () => {
  it("separates AWS cost, customer price, and gross margin per org", () => {
    renderPricing();
    const acme = screen.getByText("Acme Inc").closest("tr")!;
    expect(within(acme).getByText("$13.80")).toBeInTheDocument();
    expect(within(acme).getByText("$19.50")).toBeInTheDocument();
    expect(within(acme).getByText("29.2% ($5.70 profit)")).toBeInTheDocument();

    const globex = screen.getByText("Globex Corp").closest("tr")!;
    expect(within(globex).getByText("$1.15")).toBeInTheDocument();
    expect(within(globex).getByText("$0.00")).toBeInTheDocument();
    expect(within(globex).getByText("N/A (-$1.15 profit)")).toBeInTheDocument();

    const acmeCells = within(acme).getAllByRole("cell");
    expect(acmeCells[4]).toHaveTextContent("$69.00");
  });

  it("shows the binary-unit note on the pricing page", () => {
    renderPricing();
    expect(screen.getByText(/1 GB = 2\^30 bytes.*1 TB = 2\^40 bytes.*1024 GB/i)).toBeInTheDocument();
  });

  it("shows the deployment-selected customer schedule without a region switch", () => {
    renderPricing("EU");
    expect(screen.getByRole("columnheader", { name: /customer price.*eu/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /us pricing|eu pricing/i })).not.toBeInTheDocument();
  });

  it("adds accessible explanations for usage, cost, price, and margin", async () => {
    renderPricing();
    const expected = [
      ["Explain S3 GiB·h", /storage over time, not current bucket size or a transfer rate/i],
      ["Explain AWS storage cost", /S3 Standard storage cost at public us-east-1 rates.*storage capacity only/i],
      ["Explain customer price", /progressive US monthly tiers.*first 100 GiB-month/i],
      ["Explain price at 80% margin", /80% storage gross margin.*5 × allocated cost/i],
      ["Explain gross margin", /gross margin is gross profit divided by customer price/i],
    ] as const;

    for (const [label, text] of expected) {
      fireEvent.focus(screen.getByRole("button", { name: label }));
      expect((await screen.findAllByText(text)).length).toBeGreaterThan(0);
      fireEvent.blur(screen.getByRole("button", { name: label }));
    }
  });

  it("sorts initially by displayed org label and toggles organization order", () => {
    renderPricing();
    expect(visibleOrgOrder()).toEqual(["Aardvark Systems", "Acme Inc", "Globex Corp"]);
    expect(screen.getByRole("columnheader", { name: /org/i })).toHaveAttribute("aria-sort", "ascending");

    fireEvent.click(screen.getByRole("button", { name: "Sort by Org" }));
    expect(visibleOrgOrder()).toEqual(["Globex Corp", "Acme Inc", "Aardvark Systems"]);
    expect(screen.getByRole("columnheader", { name: /org/i })).toHaveAttribute("aria-sort", "descending");
  });

  it.each([
    ["S3 GiB·h", ["Acme Inc", "Aardvark Systems", "Globex Corp"], ["Globex Corp", "Aardvark Systems", "Acme Inc"]],
    ["Allocated AWS cost", ["Acme Inc", "Aardvark Systems", "Globex Corp"], ["Globex Corp", "Aardvark Systems", "Acme Inc"]],
    ["Customer price (US)", ["Acme Inc", "Aardvark Systems", "Globex Corp"], ["Globex Corp", "Aardvark Systems", "Acme Inc"]],
    ["Price at 80% margin", ["Acme Inc", "Aardvark Systems", "Globex Corp"], ["Globex Corp", "Aardvark Systems", "Acme Inc"]],
    ["Gross margin", ["Acme Inc", "Aardvark Systems", "Globex Corp"], ["Aardvark Systems", "Acme Inc", "Globex Corp"]],
  ])("sorts %s numerically in both directions with unavailable margins last", (heading, descending, ascending) => {
    renderPricing();
    const sort = screen.getByRole("button", { name: `Sort by ${heading}` });
    fireEvent.click(sort);
    expect(visibleOrgOrder()).toEqual(descending);
    fireEvent.click(sort);
    expect(visibleOrgOrder()).toEqual(ascending);
    expect(screen.getByText("All orgs").closest("tr")).toBe(screen.getAllByRole("row").at(-1));
  });

  it("renders a friendly empty state without usage rows", () => {
    render(
      <MemoryRouter>
        <UsagePricing rows={[]} labels={LABELS} month="2026-08" region="US" />
      </MemoryRouter>,
    );
    expect(screen.getByText(/no usage rows/i)).toBeInTheDocument();
  });
});
