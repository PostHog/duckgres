import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type { Operator } from "@/types/api";

// Mock the data + identity hooks so we can render Operators with a controlled
// role and operator list, and assert the admin-gate + list rendering directly.
const hooks = vi.hoisted(() => ({
  useOperators: vi.fn(),
  useUpsertOperator: vi.fn(),
  useDeleteOperator: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

import { Operators } from "./Operators";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false, refetch: vi.fn() });
const mut = () => ({ mutateAsync: vi.fn(), isPending: false });

const OPS: Operator[] = [
  { email: "alice@posthog.com", role: "admin", added_by: "bootstrap", created_at: "2026-07-08T00:00:00Z", updated_at: "2026-07-08T00:00:00Z" },
  { email: "james.g@posthog.com", role: "viewer", added_by: "bootstrap", created_at: "2026-07-09T00:00:00Z", updated_at: "2026-07-09T00:00:00Z" },
];

function renderPage() {
  render(
    <MemoryRouter>
      <Operators />
    </MemoryRouter>,
  );
}

describe("Operators page", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    hooks.useUpsertOperator.mockReturnValue(mut());
    hooks.useDeleteOperator.mockReturnValue(mut());
  });

  it("lists operators with roles and marks the current user for an admin", () => {
    identity.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "james.g@posthog.com", role: "admin", source: "sso" } });
    hooks.useOperators.mockReturnValue(ok(OPS));

    renderPage();

    // Both operators and their console roles render.
    expect(screen.getByText("alice@posthog.com")).toBeInTheDocument();
    expect(screen.getByText("james.g@posthog.com")).toBeInTheDocument();
    expect(screen.getByText("admin")).toBeInTheDocument();
    expect(screen.getByText("viewer")).toBeInTheDocument();

    // The signed-in operator's own row is tagged "you".
    expect(screen.getByText("you")).toBeInTheDocument();

    // Admin affordance + the disambiguating cross-link to Org Users are present.
    expect(screen.getByRole("button", { name: /add operator/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /org users/i })).toHaveAttribute("href", "/users");
  });

  it("shows an admin-only notice (no list, no add) for a viewer", () => {
    identity.useIdentity.mockReturnValue({ isAdmin: false, me: { email: "viewer@posthog.com", role: "viewer", source: "sso" } });
    hooks.useOperators.mockReturnValue(ok([]));

    renderPage();

    expect(screen.getByText("Admin only")).toBeInTheDocument();
    // The operator list and the mutating affordance must not render for viewers.
    expect(screen.queryByText("alice@posthog.com")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /add operator/i })).not.toBeInTheDocument();
  });
});
