import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import type { OrgTeam } from "@/types/api";

// Mock the data + identity hooks so the page renders with a controlled team
// list and role, asserting the table columns and the delete-dialog rules.
const hooks = vi.hoisted(() => ({
  useAllOrgTeams: vi.fn(),
  useOrgs: vi.fn(),
  useCreateOrgTeam: vi.fn(),
  useUpdateOrgTeam: vi.fn(),
  useDeleteOrgTeam: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

import { OrgTeams } from "./OrgTeams";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false, refetch: vi.fn() });
const mut = () => ({ mutateAsync: vi.fn(), isPending: false });

const TEAMS: OrgTeam[] = [
  {
    org_id: "acme",
    team_id: 1,
    schema_name: "team_1",
    enabled: true,
    is_billing_team: true,
    backfill_enabled: null,
    created_at: "2026-07-01T00:00:00Z",
    updated_at: "2026-07-01T00:00:00Z",
  },
  {
    org_id: "acme",
    team_id: 2,
    schema_name: "team_2",
    enabled: false,
    is_billing_team: null,
    backfill_enabled: true,
    created_at: "2026-07-02T00:00:00Z",
    updated_at: "2026-07-02T00:00:00Z",
  },
  {
    org_id: "solo",
    team_id: 7,
    schema_name: "custom_schema",
    enabled: true,
    is_billing_team: true,
    backfill_enabled: false,
    created_at: "2026-07-03T00:00:00Z",
    updated_at: "2026-07-03T00:00:00Z",
  },
];

function renderPage() {
  render(
    <MemoryRouter>
      <OrgTeams />
    </MemoryRouter>,
  );
}

describe("Org teams page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "a@posthog.com", role: "admin", source: "sso" } });
    hooks.useAllOrgTeams.mockReturnValue(ok(TEAMS));
    hooks.useOrgs.mockReturnValue(ok([{ name: "acme" }, { name: "solo" }]));
    hooks.useCreateOrgTeam.mockReturnValue(mut());
    hooks.useUpdateOrgTeam.mockReturnValue(mut());
    hooks.useDeleteOrgTeam.mockReturnValue(mut());
  });

  it("lists every team with schema, enabled, billing and backfill state", () => {
    renderPage();

    expect(screen.getAllByText("acme")).toHaveLength(2);
    expect(screen.getByText("solo")).toBeInTheDocument();
    expect(screen.getByText("team_1")).toBeInTheDocument();
    expect(screen.getByText("team_2")).toBeInTheDocument();
    expect(screen.getByText("custom_schema")).toBeInTheDocument();
    // Both billing rows carry the badge; the disabled team is flagged.
    expect(screen.getAllByText("billing")).toHaveLength(2);
    expect(screen.getByText("disabled")).toBeInTheDocument();
    // Admin affordance present.
    expect(screen.getByRole("button", { name: /add team/i })).toBeInTheDocument();
  });

  it("refuses deleting an org's last team up front", async () => {
    renderPage();

    // "solo" has exactly one team → the delete dialog must refuse.
    const deleteButtons = screen.getAllByTitle("Delete");
    await userEvent.click(deleteButtons[2]);

    expect(screen.getByText(/last team and cannot be deleted/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /delete team/i })).toBeDisabled();
  });

  it("warns that deleting a billing team hands billing to the oldest remaining team", async () => {
    renderPage();

    // acme team 1 is billing and acme has two teams → warning, not refusal.
    const deleteButtons = screen.getAllByTitle("Delete");
    await userEvent.click(deleteButtons[0]);

    expect(screen.getByText(/oldest remaining team automatically becomes billing/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /delete team/i })).toBeEnabled();
  });
});
