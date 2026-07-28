import { describe, expect, it, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { Org } from "@/types/api";

// Mock the data hooks so the form renders against a controlled org list and a
// spy-able provision mutation — the point of these tests is the REQUEST the
// page builds, since that request is what has to match the PostHog backend's.
const hooks = vi.hoisted(() => ({
  useOrgs: vi.fn(),
  useProvisionWarehouse: vi.fn(),
  useDatabaseNameAvailable: vi.fn(),
  useWarehouseStatus: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

import { ProvisionWarehouse } from "./ProvisionWarehouse";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false, refetch: vi.fn() });

const org = (name: string, warehouseState?: string): Org =>
  ({
    name,
    database_name: name,
    warehouse: warehouseState ? { state: warehouseState } : null,
  }) as unknown as Org;

function renderPage() {
  render(
    // AdminGate wraps a viewer's disabled control in a Tooltip, which needs the
    // provider the real app mounts in main.tsx.
    <TooltipProvider>
      <MemoryRouter>
        <ProvisionWarehouse />
      </MemoryRouter>
    </TooltipProvider>,
  );
}

function type(label: RegExp, value: string) {
  fireEvent.change(screen.getByLabelText(label), { target: { value } });
}

describe("ProvisionWarehouse page", () => {
  let mutateAsync: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "a@posthog.com", role: "admin", source: "sso" } });
    mutateAsync = vi.fn().mockResolvedValue({
      status: "provisioning started",
      org: "acme",
      username: "root",
      password: "s3cr3t",
      bucket: "posthog-duckling-acme-mw-dev-us",
    });
    hooks.useProvisionWarehouse.mockReturnValue({ mutateAsync, isPending: false });
    hooks.useOrgs.mockReturnValue(ok<Org[]>([]));
    hooks.useDatabaseNameAvailable.mockReturnValue(ok(null));
    hooks.useWarehouseStatus.mockReturnValue(ok(null));
  });

  it("posts the standard PostHog-backend body to the shared provisioning endpoint", async () => {
    renderPage();
    type(/org id/i, "acme");
    type(/database name/i, "acme_db");
    type(/team id/i, "42");

    fireEvent.click(screen.getByRole("button", { name: /provision warehouse/i }));
    // Destructive-action confirmation: the org id must be typed back.
    type(/type the org id to confirm/i, "acme");
    fireEvent.click(screen.getByRole("button", { name: /^provision$/i }));

    expect(mutateAsync).toHaveBeenCalledWith({
      org: "acme",
      body: {
        database_name: "acme_db",
        team_id: 42,
        metadata_store: { type: "cnpg-shard" },
        data_store: { type: "s3bucket" },
        ducklake: { enabled: true },
      },
    });
  });

  it("shows the literal request so the shared-endpoint claim is inspectable", () => {
    renderPage();
    type(/org id/i, "acme");
    type(/database name/i, "acme_db");
    type(/team id/i, "42");

    expect(screen.getByText(/POST \/api\/v1\/orgs\/acme\/provision/)).toBeInTheDocument();
  });

  it("blocks submit until the form matches the server's rules", () => {
    renderPage();
    // An upper-case org id is not a DNS-1123 label, and team_id is required for
    // a new org — both are server 400s, surfaced before submit.
    type(/org id/i, "ACME");
    type(/database name/i, "acme_db");

    expect(screen.getByText(/DNS-1123/)).toBeInTheDocument();
    expect(screen.getByText(/team_id is required/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /provision warehouse/i })).toBeDisabled();
  });

  it("warns that an org with a live warehouse would be refused", () => {
    hooks.useOrgs.mockReturnValue(ok<Org[]>([org("acme", "ready")]));
    renderPage();
    type(/org id/i, "acme");

    expect(screen.getByText(/deprovision it first/i)).toBeInTheDocument();
  });

  it("flags a database name already taken by another org", () => {
    hooks.useDatabaseNameAvailable.mockReturnValue(ok({ name: "taken_db", available: false }));
    renderPage();
    type(/database name/i, "taken_db");

    expect(screen.getByText(/already in use by another org/i)).toBeInTheDocument();
  });

  it("surfaces the once-only root password after a successful provision", async () => {
    renderPage();
    type(/org id/i, "acme");
    type(/database name/i, "acme_db");
    type(/team id/i, "42");
    fireEvent.click(screen.getByRole("button", { name: /provision warehouse/i }));
    type(/type the org id to confirm/i, "acme");
    fireEvent.click(screen.getByRole("button", { name: /^provision$/i }));

    expect(await screen.findByText("s3cr3t")).toBeInTheDocument();
    expect(screen.getByText(/shown/i)).toBeInTheDocument();
    expect(screen.getByText("posthog-duckling-acme-mw-dev-us")).toBeInTheDocument();
  });

  it("hides the provision affordance from viewers", () => {
    identity.useIdentity.mockReturnValue({ isAdmin: false, me: { email: "v@posthog.com", role: "viewer", source: "sso" } });
    renderPage();
    type(/org id/i, "acme");
    type(/database name/i, "acme_db");
    type(/team id/i, "42");

    expect(screen.getByRole("button", { name: /provision warehouse/i })).toBeDisabled();
  });
});
