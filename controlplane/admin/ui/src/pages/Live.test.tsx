import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { TooltipProvider } from "@/components/ui/tooltip";

const hooks = vi.hoisted(() => ({
  useCancelSession: vi.fn(),
  useKillUserSessions: vi.fn(),
  useOrgs: vi.fn(),
  useQueries: vi.fn(),
  useQueryDetail: vi.fn(),
  useSessions: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

import { Live } from "./Live";

const ok = <T,>(data: T) => ({
  data,
  isSuccess: true,
  isLoading: false,
  isError: false,
  refetch: vi.fn(),
});
const mutation = () => ({ mutate: vi.fn(), mutateAsync: vi.fn(), isPending: false });

describe("Live page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({ isAdmin: true });
    hooks.useCancelSession.mockReturnValue(mutation());
    hooks.useKillUserSessions.mockReturnValue(mutation());
    hooks.useQueryDetail.mockReturnValue({ data: undefined, isLoading: false, isError: false });
    hooks.useOrgs.mockReturnValue(
      ok([
        {
          name: "org-a-id",
          database_name: "product_analytics",
          hostname_alias: null,
        },
      ]),
    );
    hooks.useQueries.mockReturnValue(
      ok([
        {
          org: "org-a-id",
          user: "query-user",
          pid: 101,
          worker_id: 11,
          protocol: "pg",
          percentage: 25,
          rows: 10,
          total_rows: 40,
          stalled: false,
          started_at: "2026-08-04T12:00:00Z",
          elapsed_ms: 500,
          state: "active",
        },
      ]),
    );
    hooks.useSessions.mockReturnValue(
      ok([
        {
          org: "org-a-id",
          user: "session-user",
          pid: 202,
          worker_id: 22,
          protocol: "flight",
        },
      ]),
    );
  });

  it("shows the readable org name next to the org ID in both live tables", () => {
    render(
      <TooltipProvider>
        <Live />
      </TooltipProvider>,
    );

    expect(screen.getAllByText("product_analytics")).toHaveLength(2);
    expect(screen.getAllByText("org-a-id")).toHaveLength(2);
  });
});
