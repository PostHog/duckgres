import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type { TrinoOrgDetail, TrinoOrgStatus } from "@/types/api";

const hooks = vi.hoisted(() => ({ useOrgTrino: vi.fn() }));
vi.mock("@/hooks/useApi", () => hooks);

import { OrgTrinoCard } from "./OrgTrinoCard";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false });

function orgStatus(over: Partial<TrinoOrgStatus> = {}): TrinoOrgStatus {
  return {
    org: "org-a",
    principal: "product_analytics",
    catalog: "org_product_analytics",
    tier: "free",
    cell: "cell-001",
    state: "ready",
    ready_at: "2026-08-01T10:00:00Z",
    running_queries: 2,
    queued_queries: 1,
    ...over,
  };
}

function detail(over: Partial<TrinoOrgDetail> = {}): TrinoOrgDetail {
  return {
    cell: { id: "cell-001", coordinator_url: "https://coordinator" },
    enabled: true,
    available: true,
    status: orgStatus(),
    ...over,
  };
}

function renderCard() {
  return render(
    <MemoryRouter>
      <OrgTrinoCard orgId="org-a" />
    </MemoryRouter>,
  );
}

describe("OrgTrinoCard", () => {
  beforeEach(() => vi.clearAllMocks());

  it("renders nothing for an org that is not Trino-enabled", () => {
    // Most orgs have no Trino row, and a control plane with no cell 404s
    // the endpoint into the same shape. An empty card on every org page
    // would be noise.
    hooks.useOrgTrino.mockReturnValue(ok(detail({ enabled: false, status: undefined })));
    const { container } = renderCard();
    expect(container).toBeEmptyDOMElement();
  });

  it("shows the principal, catalog and live counts for a ready org", () => {
    hooks.useOrgTrino.mockReturnValue(ok(detail()));
    renderCard();
    expect(screen.getByText("product_analytics")).toBeInTheDocument();
    expect(screen.getByText("org_product_analytics")).toBeInTheDocument();
    expect(screen.getByText("ready")).toBeInTheDocument();
  });

  it("surfaces the reconcile failure message, which is the actionable part", () => {
    // This is the gap the card closes: state/status_message live on
    // duckgres_managed_warehouse_trino and were rendered nowhere, so a
    // failed provision was silent.
    hooks.useOrgTrino.mockReturnValue(
      ok(
        detail({
          status: orgStatus({
            state: "failed",
            status_message: "catalog reconcile failed: duckling has published no credential",
            ready_at: undefined,
            failed_at: "2026-08-26T09:00:00Z",
          }),
        }),
      ),
    );
    renderCard();
    expect(screen.getByText(/duckling has published no credential/)).toBeInTheDocument();
    expect(screen.getByText("failed")).toBeInTheDocument();
  });

  it("blanks the live counts when the coordinator is unreachable", () => {
    // Zero would read as "this org is idle" during an outage.
    hooks.useOrgTrino.mockReturnValue(
      ok(detail({ available: false, status: orgStatus({ running_queries: 0, queued_queries: 0 }) })),
    );
    renderCard();
    const dashes = screen.getAllByText("—");
    expect(dashes.length).toBeGreaterThanOrEqual(2);
  });
});
