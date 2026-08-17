import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

// Mock the API client: the dialog must build the EXACT provision body the
// PostHog backend's onboarding flow sends, so the tests assert on the payload
// handed to api.provisionWarehouse.
const client = vi.hoisted(() => ({
  provisionWarehouse: vi.fn(),
  warehouseStatus: vi.fn(),
  checkDatabaseName: vi.fn(),
}));
vi.mock("@/lib/api", () => ({
  api: client,
  ApiError: class ApiError extends Error {
    status: number;
    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  },
}));

import { AddOrgDialog } from "./AddOrgDialog";

function renderDialog() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <AddOrgDialog open onClose={() => {}} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const OK_UUID = "0123abcd-4567-4890-abcd-ef0123456789";

async function fillMinimal(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText("Org id"), OK_UUID);
  // Database name prefills from the org id (untouched), so only team id is
  // still missing.
  await user.type(screen.getByLabelText("Team id"), "12345");
}

beforeEach(() => {
  vi.clearAllMocks();
  client.checkDatabaseName.mockResolvedValue({ name: "", available: true });
  client.warehouseStatus.mockResolvedValue({
    org_id: OK_UUID,
    state: "provisioning",
    status_message: "",
  });
});

describe("AddOrgDialog", () => {
  it("keeps the submit disabled until org id, database name and team id are valid", async () => {
    const user = userEvent.setup();
    renderDialog();

    const submit = screen.getByRole("button", { name: /provision organization/i });
    expect(submit).toBeDisabled();

    // A new org REQUIRES team_id (a warehouse cannot exist without a team).
    await user.type(screen.getByLabelText("Org id"), OK_UUID);
    expect(submit).toBeDisabled();

    await user.type(screen.getByLabelText("Team id"), "12345");
    expect(submit).toBeEnabled();
  });

  it("rejects an org id that violates the DNS-1123 / length rule client-side", async () => {
    const user = userEvent.setup();
    renderDialog();

    await user.type(screen.getByLabelText("Org id"), "Not_A_Valid_Org");
    await user.type(screen.getByLabelText("Team id"), "1");
    expect(screen.getByText(/DNS-1123 label/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /provision organization/i })).toBeDisabled();
  });

  it("rejects a database name that is not a valid DNS label client-side", async () => {
    const user = userEvent.setup();
    renderDialog();

    await user.type(screen.getByLabelText("Org id"), OK_UUID);
    const dbInput = screen.getByLabelText("Database name");
    await user.clear(dbInput);
    await user.type(dbInput, "ACME INC");
    await user.type(screen.getByLabelText("Team id"), "12345");

    expect(screen.getByText(/single DNS label/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /provision organization/i })).toBeDisabled();
  });

  it("blocks a database name that is already taken", async () => {
    const user = userEvent.setup();
    client.checkDatabaseName.mockResolvedValue({ name: OK_UUID, available: false });
    renderDialog();

    await fillMinimal(user);
    expect(
      await screen.findByText(/already in use by another org/i),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /provision organization/i })).toBeDisabled();
  });

  it("submits the exact onboarding body the PostHog backend sends and shows the one-time password", async () => {
    const user = userEvent.setup();
    client.provisionWarehouse.mockResolvedValue({
      status: "provisioning started",
      org: OK_UUID,
      username: "root",
      password: "s3cret-once",
      bucket: "posthog-duckling-0123abcd45674890abcdef0123456789-mw-prod-us",
    });
    renderDialog();

    await fillMinimal(user);
    await user.click(screen.getByRole("button", { name: /provision organization/i }));

    await waitFor(() => expect(client.provisionWarehouse).toHaveBeenCalledTimes(1));
    // The django payload shape, verbatim — and NOTHING else: no schema
    // override (the team's schema defaults to team_<id>), no enabled/backfill
    // override (the team defaults to enabled, backing the "enabled
    // immediately" promise), no external stores.
    expect(client.provisionWarehouse).toHaveBeenCalledWith(OK_UUID, {
      database_name: OK_UUID,
      team_id: 12345,
      metadata_store: { type: "cnpg-shard" },
      data_store: { type: "s3bucket" },
      ducklake: { enabled: true },
    });

    // Success panel: the root password is shown once, with a copy affordance.
    expect(await screen.findByDisplayValue("s3cret-once")).toBeInTheDocument();
    expect(screen.getByDisplayValue("root")).toBeInTheDocument();
    expect(screen.getByText(/never shown or retrievable again/i)).toBeInTheDocument();
    // The in-flight warehouse status is watched until ready. Failed remains
    // observable because external dependency repair can recover it.
    await waitFor(() => expect(client.warehouseStatus).toHaveBeenCalledWith(OK_UUID));
  });

  it("tells the operator the team lands at team_<id>, enabled immediately like django onboarding", async () => {
    const user = userEvent.setup();
    renderDialog();

    // The form promises the django-equivalent outcome up front (schema
    // team_<id> + immediate enablement) instead of exposing either as a knob.
    expect(screen.getAllByText(/enabled immediately/i).length).toBeGreaterThan(0);

    await fillMinimal(user);
    // With a team id typed, the note shows the concrete schema it will get.
    expect(screen.getByText(/team_12345/)).toBeInTheDocument();
  });

  it("surfaces the API error (e.g. 400 team_id required / 409 conflict) inline", async () => {
    const user = userEvent.setup();
    client.provisionWarehouse.mockRejectedValue(new Error("provision conflicts with existing state"));
    renderDialog();

    await fillMinimal(user);
    await user.click(screen.getByRole("button", { name: /provision organization/i }));

    expect(await screen.findByText(/conflicts with existing state/i)).toBeInTheDocument();
    // Still on the form — no credentials panel was rendered.
    expect(screen.getByRole("button", { name: /provision organization/i })).toBeInTheDocument();
  });
});
