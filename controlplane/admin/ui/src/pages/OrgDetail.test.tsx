import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { ManagedWarehouse, Org } from "@/types/api";

const hooks = vi.hoisted(() => ({
  useDatabaseNameAvailable: vi.fn(),
  useDeleteOrg: vi.fn(),
  useDeprovisionWarehouse: vi.fn(),
  useDucklingsMetadata: vi.fn(),
  useOrg: vi.fn(),
  useOrgReshards: vi.fn(),
  useOrgTeams: vi.fn(),
  useUpdateOrg: vi.fn(),
  useUpdateWarehouse: vi.fn(),
  useWarehouse: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);

vi.mock("@/components/OrgTeamDialogs", () => ({
  BackfillBadge: () => null,
  CreateTeamDialog: () => null,
  DeleteTeamDialog: () => null,
  EarliestEventDateCell: () => null,
  EditTeamDialog: () => null,
  LegacyNamesBadge: () => null,
}));

import { OrgDetail } from "./OrgDetail";

const warehouseUpdate = vi.fn();
const orgUpdate = vi.fn();
const ok = <T,>(data: T) => ({
  data,
  isSuccess: true,
  isLoading: false,
  isError: false,
  error: null,
  refetch: vi.fn(),
});
const mut = (mutateAsync = vi.fn().mockResolvedValue(undefined)) => ({
  mutateAsync,
  isPending: false,
});

const ORG: Org = {
  name: "acme",
  database_name: "acme",
  hostname_alias: null,
  max_workers: 1,
  max_vcpus: 2,
  max_memory: "120Gi",
  default_worker_cpu: "2",
  default_worker_memory: "8Gi",
  default_worker_ttl: "75m",
  default_worker_min_hot_idle: 0,
  data_imports_table_naming_version: "legacy_batch_v1",
  created_at: "2026-07-01T00:00:00Z",
  updated_at: "2026-07-01T00:00:00Z",
};

function warehouse(metadataProxyEnabled: boolean): ManagedWarehouse {
  return {
    org_id: "acme",
    duckling_name: "duckling-acme",
    image: "duckgres:latest",
    ducklake_version: "0.4",
    metadata_proxy_enabled: metadataProxyEnabled,
    warehouse_database: { endpoint: "", port: 5432 },
    metadata_store: {
      kind: "cnpg-shard",
      endpoint: "",
      port: 5432,
      database_name: "acme",
      username: "acme",
    },
    pgbouncer: { enabled: true },
    s3: {
      provider: "aws",
      region: "us-east-1",
      bucket: "test",
      path_prefix: "",
      endpoint: "",
      use_ssl: true,
      url_style: "vhost",
      delta_catalog_enabled: false,
      delta_catalog_path: "",
    },
    worker_identity: { namespace: "ducklings", iam_role_arn: "" },
    warehouse_database_credentials: { namespace: "ducklings", name: "warehouse", key: "url" },
    metadata_store_credentials: { namespace: "ducklings", name: "metadata", key: "url" },
    s3_credentials: { namespace: "ducklings", name: "s3", key: "credentials" },
    runtime_config: { namespace: "ducklings", name: "runtime", key: "config" },
    state: "ready",
    status_message: "",
    metadata_store_state: "ready",
    s3_state: "ready",
    identity_state: "ready",
    secrets_state: "ready",
    created_at: "2026-07-01T00:00:00Z",
    updated_at: "2026-07-01T00:00:00Z",
  } as ManagedWarehouse;
}

function renderPage(metadataProxyEnabled: boolean, warehouseOverrides: Partial<ManagedWarehouse> = {}) {
  hooks.useWarehouse.mockReturnValue(ok({ ...warehouse(metadataProxyEnabled), ...warehouseOverrides }));
  render(
    <MemoryRouter initialEntries={["/orgs/acme"]}>
      <TooltipProvider>
        <Routes>
          <Route path="/orgs/:id" element={<OrgDetail />} />
        </Routes>
      </TooltipProvider>
    </MemoryRouter>,
  );
}

describe("Org detail", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({
      isAdmin: true,
      me: { email: "admin@example.com", role: "admin", source: "sso" },
    });
    hooks.useOrg.mockReturnValue(ok(ORG));
    hooks.useDatabaseNameAvailable.mockReturnValue({ data: null, isLoading: false });
    hooks.useUpdateOrg.mockReturnValue(mut(orgUpdate));
    hooks.useDeleteOrg.mockReturnValue(mut());
    hooks.useUpdateWarehouse.mockReturnValue(mut(warehouseUpdate));
    hooks.useDeprovisionWarehouse.mockReturnValue(mut());
    hooks.useDucklingsMetadata.mockReturnValue(ok({ available: true, entries: [] }));
    hooks.useOrgReshards.mockReturnValue(ok([]));
    hooks.useOrgTeams.mockReturnValue(ok([]));
  });

  it("headline leads with the database name; the org id is a subline with a copy button", () => {
    // The URL org id ("acme") is the subline; the readable database_name is
    // the headline. (The headline falls back to the raw id while the org is
    // still loading, covered by the loading branch of Header.)
    hooks.useOrg.mockReturnValue(ok({ ...ORG, database_name: "posthog" }));
    renderPage(false);

    const headline = screen.getByRole("heading", { level: 1 });
    expect(within(headline).getByText("posthog")).toBeInTheDocument();
    expect(within(headline).getByText("acme")).toBeInTheDocument();
    expect(within(headline).getByRole("button", { name: /copy acme/i })).toBeInTheDocument();
  });

  it("distinguishes ready infrastructure components from an operational readiness blocker", () => {
    const blocker =
      "Infrastructure is ready, but metadata-store authentication failed; the PostgreSQL role password may not match its credential Secret. Waiting for recovery.";
    renderPage(false, {
      state: "failed",
      metadata_store_state: "ready",
      s3_state: "ready",
      identity_state: "ready",
      secrets_state: "ready",
      status_message: blocker,
      failed_at: "2026-08-14T10:18:09Z",
    });

    const alert = screen.getByRole("alert");
    expect(within(alert).getByText("Warehouse is not operationally ready")).toBeInTheDocument();
    expect(within(alert).getByText("Current blocker")).toBeInTheDocument();
    expect(within(alert).getByText(blocker)).toBeInTheDocument();
    expect(within(alert).getByText(/component badges reflect Duckling infrastructure/i)).toBeInTheDocument();
    expect(within(alert).getByText(/recovery is checked automatically/i)).toBeInTheDocument();
    expect(screen.getByText("Metadata store").parentElement).toHaveTextContent("ready");
    expect(screen.getByText("S3").parentElement).toHaveTextContent("ready");
    expect(screen.getByText("Identity").parentElement).toHaveTextContent("ready");
    expect(screen.getByText("Secrets").parentElement).toHaveTextContent("ready");
    expect(screen.getByText("Last ready")).toBeInTheDocument();
    expect(screen.getByText("Last failed")).toBeInTheDocument();
  });

  it.each([
    { current: false, next: true },
    { current: true, next: false },
  ])("saves an explicit $next when the current value is $current", async ({ current, next }) => {
    const user = userEvent.setup();
    renderPage(current);

    const toggle = screen.getByRole("switch", { name: /public metadata postgres/i });
    expect(toggle).toHaveAttribute("aria-checked", String(current));

    await user.click(toggle);
    expect(toggle).toHaveAttribute("aria-checked", String(next));
    await user.click(screen.getByRole("button", { name: /save warehouse/i }));

    expect(warehouseUpdate).toHaveBeenCalledTimes(1);
    expect(warehouseUpdate).toHaveBeenCalledWith({ metadata_proxy_enabled: next });
  });

  it("sends database_name only when it changed", async () => {
    const user = userEvent.setup();
    renderPage(false);

    // Untouched: save carries no database_name (no spurious rename).
    await user.click(screen.getByText("Save changes"));
    expect(orgUpdate).toHaveBeenCalledTimes(1);
    expect(orgUpdate).toHaveBeenCalledWith(expect.not.objectContaining({ database_name: expect.anything() }));

    orgUpdate.mockClear();

    // Fixed a broken stored name: the rename rides the update.
    const dbInput = screen.getByLabelText(/database name/i);
    await user.clear(dbInput);
    await user.type(dbInput, "acme-inc");
    await user.click(screen.getByText("Save changes"));
    expect(orgUpdate).toHaveBeenCalledTimes(1);
    expect(orgUpdate).toHaveBeenCalledWith(expect.objectContaining({ database_name: "acme-inc" }));
  });

  it("shows and saves the org memory limit", async () => {
    const user = userEvent.setup();
    renderPage(false);

    const input = screen.getByLabelText(/max memory/i);
    expect(input).toHaveValue("120Gi");
    await user.clear(input);
    await user.type(input, "240Gi");
    await user.click(screen.getByText("Save changes"));

    expect(orgUpdate).toHaveBeenCalledWith(expect.objectContaining({ max_memory: "240Gi" }));
  });

  it("keeps every other setting editable for an org whose stored name predates the rule", async () => {
    // The premise of the break-glass surface: grandfathered rows like
    // "ACME INC" must NOT wedge the whole org-config card behind a forced
    // rename. The server preserves database_name when the key is absent
    // (Go test TestUpdateOrgWithoutDatabaseNameKeyPreservesIt).
    const user = userEvent.setup();
    hooks.useOrg.mockReturnValue(ok({ ...ORG, database_name: "ACME INC" }));
    renderPage(false);

    const save = screen.getByRole("button", { name: /save changes/i });
    expect(save).toBeEnabled();
    expect(screen.getByLabelText(/database name/i)).toHaveValue("ACME INC");

    await user.click(save);
    expect(orgUpdate).toHaveBeenCalledTimes(1);
    expect(orgUpdate).toHaveBeenCalledWith(expect.not.objectContaining({ database_name: expect.anything() }));

    // Fixing the name unlocks the rename path.
    const dbInput = screen.getByLabelText(/database name/i);
    await user.clear(dbInput);
    await user.type(dbInput, "acme-inc");
    await user.click(screen.getByRole("button", { name: /save changes/i }));
    expect(orgUpdate).toHaveBeenLastCalledWith(expect.objectContaining({ database_name: "acme-inc" }));
  });

  it("rejects a database name that is not a valid DNS label client-side", async () => {
    const user = userEvent.setup();
    renderPage(false);

    const dbInput = screen.getByLabelText(/database name/i);
    await user.clear(dbInput);
    await user.type(dbInput, "acme inc");

    expect(screen.getByText(/single DNS label/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /save changes/i })).toBeDisabled();
  });

  it("disables save with an explanation when the database name is cleared", async () => {
    const user = userEvent.setup();
    renderPage(false);

    const dbInput = screen.getByLabelText(/database name/i);
    await user.clear(dbInput);

    expect(screen.getByText(/database name is required/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /save changes/i })).toBeDisabled();
  });

  it("blocks a rename onto a database name another org already owns", async () => {
    const user = userEvent.setup();
    hooks.useDatabaseNameAvailable.mockReturnValue(ok({ name: "taken-name", available: false }));
    renderPage(false);

    const dbInput = screen.getByLabelText(/database name/i);
    await user.clear(dbInput);
    await user.type(dbInput, "taken-name");

    expect(await screen.findByText(/already in use by another org/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /save changes/i })).toBeDisabled();
  });

  it("saves a changed data import table naming version", async () => {
    const user = userEvent.setup();
    HTMLElement.prototype.scrollIntoView = vi.fn();
    renderPage(false);

    const namingSelect = screen.getByLabelText("Data import table naming");
    expect(namingSelect).toHaveTextContent("Legacy batch (legacy_batch_v1)");

    namingSelect.focus();
    await user.keyboard("{Enter}{ArrowDown}{Enter}");
    await user.click(screen.getByText("Save changes"));

    expect(orgUpdate).toHaveBeenCalledTimes(1);
    expect(orgUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ data_imports_table_naming_version: "copy_v1" }),
    );
  });
});
