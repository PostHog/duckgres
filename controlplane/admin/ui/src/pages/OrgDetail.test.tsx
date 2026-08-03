import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { ManagedWarehouse, Org } from "@/types/api";

const hooks = vi.hoisted(() => ({
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
  default_worker_cpu: "2",
  default_worker_memory: "8Gi",
  default_worker_ttl: "75m",
  default_worker_min_hot_idle: 0,
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

function renderPage(metadataProxyEnabled: boolean) {
  hooks.useWarehouse.mockReturnValue(ok(warehouse(metadataProxyEnabled)));
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

describe("Org warehouse metadata proxy setting", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    identity.useIdentity.mockReturnValue({
      isAdmin: true,
      me: { email: "admin@example.com", role: "admin", source: "sso" },
    });
    hooks.useOrg.mockReturnValue(ok(ORG));
    hooks.useUpdateOrg.mockReturnValue(mut());
    hooks.useDeleteOrg.mockReturnValue(mut());
    hooks.useUpdateWarehouse.mockReturnValue(mut(warehouseUpdate));
    hooks.useDeprovisionWarehouse.mockReturnValue(mut());
    hooks.useDucklingsMetadata.mockReturnValue(ok({ available: true, entries: [] }));
    hooks.useOrgReshards.mockReturnValue(ok([]));
    hooks.useOrgTeams.mockReturnValue(ok([]));
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
});
