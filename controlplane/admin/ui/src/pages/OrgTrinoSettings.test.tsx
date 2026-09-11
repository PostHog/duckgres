import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";

const hooks = vi.hoisted(() => ({ useOrgTrino: vi.fn(), useTrinoCells: vi.fn(), useSelectTrinoCell: vi.fn(), useSetTrinoEnabled: vi.fn() }));
vi.mock("@/hooks/useApi", () => hooks);
const identity = vi.hoisted(() => ({ useIdentity: vi.fn() }));
vi.mock("@/components/IdentityProvider", () => identity);
import { OrgTrinoSettings } from "./OrgTrinoSettings";

const select = vi.fn();
const enable = vi.fn();
const refetch = vi.fn();
const ok = <T,>(data: T) => ({ data, isLoading: false, error: null, refetch });
const details = (enabled = false, assigned = false) => ok({ enabled, assigned, cell: { id: "legacy" } });
function renderSettings(hasWarehouse = true, tier = "premium") {
  return render(<OrgTrinoSettings orgId="org-a" hasWarehouse={hasWarehouse} tier={tier} />);
}

describe("Org Trino configuration", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    select.mockResolvedValue({ assigned: true });
    enable.mockResolvedValue({ status: "queued" });
    refetch.mockResolvedValue(undefined);
    identity.useIdentity.mockReturnValue({ isAdmin: true });
    hooks.useOrgTrino.mockReturnValue(details());
    hooks.useTrinoCells.mockReturnValue(ok({ cells: [{ id: "legacy" }, { id: "cell-001" }] }));
    hooks.useSelectTrinoCell.mockReturnValue({ mutateAsync: select, isPending: false });
    hooks.useSetTrinoEnabled.mockReturnValue({ mutateAsync: enable, isPending: false });
  });

  it("requires an explicit saved selection before enabling and never enables on selection", async () => {
    renderSettings();
    expect(screen.getByRole("button", { name: "Enable Trino" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Select cell" })).toBeDisabled();
    fireEvent.change(screen.getByLabelText("Initial Trino cell"), { target: { value: "cell-001" } });
    fireEvent.click(screen.getByRole("button", { name: "Select cell" }));
    await waitFor(() => expect(select).toHaveBeenCalledWith({ org: "org-a", cell: "cell-001" }));
    expect(enable).not.toHaveBeenCalled();
    expect(await screen.findByRole("status")).toHaveTextContent("Trino remains disabled");
    expect(screen.getByRole("button", { name: "Enable Trino" })).toBeDisabled();
  });

  it("re-enables an assigned legacy warehouse without changing its tier or cell", async () => {
    hooks.useOrgTrino.mockReturnValue(details(false, true));
    renderSettings();
    expect(screen.getByText("Cell: legacy")).toBeInTheDocument();
    expect(screen.queryByLabelText("Initial Trino cell")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Enable Trino" }));
    await waitFor(() => expect(enable).toHaveBeenCalledWith({ org: "org-a", enabled: true, tier: "premium" }));
    expect(select).not.toHaveBeenCalled();
    expect(await screen.findByRole("status")).toHaveTextContent("Provisioning may take a moment");
  });

  it("offers explicit initial selection for a registry-only blank assignment", () => {
    hooks.useOrgTrino.mockReturnValue(ok({ enabled: false, assigned: false, available: false, cell: { id: "", coordinator_url: "" } }));
    hooks.useTrinoCells.mockReturnValue(ok({ cells: [{ id: "cell-test" }] }));
    renderSettings();
    expect(screen.getByText("Cell: Not selected")).toBeInTheDocument();
    expect(screen.getByLabelText("Initial Trino cell")).toHaveValue("");
    expect(screen.getByRole("button", { name: "Enable Trino" })).toBeDisabled();
    expect(screen.queryByRole("option", { name: "legacy" })).not.toBeInTheDocument();
  });

  it("lets an enabled unassigned warehouse disable before selecting its initial cell", async () => {
    hooks.useOrgTrino.mockReturnValue(ok({ enabled: true, assigned: false, available: false, cell: { id: "", coordinator_url: "" } }));
    hooks.useTrinoCells.mockReturnValue(ok({ cells: [{ id: "cell-test" }] }));
    renderSettings();
    expect(screen.queryByLabelText("Initial Trino cell")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Disable Trino" }));
    await waitFor(() => expect(enable).toHaveBeenCalledWith({ org: "org-a", enabled: false, tier: "premium" }));
    expect(select).not.toHaveBeenCalled();
  });

  it("disables independently and does not offer a move", async () => {
    hooks.useOrgTrino.mockReturnValue(details(true, true));
    renderSettings();
    fireEvent.click(screen.getByRole("button", { name: "Disable Trino" }));
    await waitFor(() => expect(enable).toHaveBeenCalledWith({ org: "org-a", enabled: false, tier: "premium" }));
    expect(select).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("Initial Trino cell")).not.toBeInTheDocument();
    expect(await screen.findByRole("status")).toHaveTextContent("Access is removed during reconciliation");
  });

  it("retains selection errors and refreshes after an ambiguous write", async () => {
    select.mockRejectedValue(new Error("Assignment is immutable"));
    renderSettings();
    fireEvent.change(screen.getByLabelText("Initial Trino cell"), { target: { value: "cell-001" } });
    fireEvent.click(screen.getByRole("button", { name: "Select cell" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Assignment is immutable");
    expect(enable).not.toHaveBeenCalled();
    expect(refetch).toHaveBeenCalled();
  });

  it("shows enable errors without pretending Trino is enabled", async () => {
    hooks.useOrgTrino.mockReturnValue(details(false, true));
    enable.mockRejectedValue(new Error("Root user is missing"));
    renderSettings();
    fireEvent.click(screen.getByRole("button", { name: "Enable Trino" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Root user is missing");
    expect(screen.getByText("Disabled")).toBeInTheDocument();
  });

  it.each(["selection", "enablement"])("blocks changes while %s is pending", (pending) => {
    if (pending === "selection") hooks.useSelectTrinoCell.mockReturnValue({ mutateAsync: select, isPending: true });
    else hooks.useSetTrinoEnabled.mockReturnValue({ mutateAsync: enable, isPending: true });
    renderSettings();
    for (const button of screen.getAllByRole("button")) expect(button).toBeDisabled();
    expect(screen.getByLabelText("Initial Trino cell")).toBeDisabled();
  });

  it("renders read-only configuration for viewers", () => {
    identity.useIdentity.mockReturnValue({ isAdmin: false });
    hooks.useOrgTrino.mockReturnValue(details(true, true));
    renderSettings();
    expect(screen.getByText("Cell: legacy")).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
  });

  it.each(["assignment", "cells"])("fails closed when %s cannot be read", (failed) => {
    (failed === "assignment" ? hooks.useOrgTrino : hooks.useTrinoCells).mockReturnValue({ error: new Error("Unavailable") });
    renderSettings();
    expect(screen.getByRole("alert")).toHaveTextContent("Unavailable");
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("reports an environment without Trino and makes no writes", () => {
    hooks.useTrinoCells.mockReturnValue(ok({ cells: [] }));
    renderSettings();
    expect(screen.getByText("Trino is not configured in this environment.")).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("requires a warehouse before selection or enablement", () => {
    renderSettings(false);
    expect(screen.getByLabelText("Initial Trino cell")).toBeDisabled();
    for (const button of screen.getAllByRole("button")) expect(button).toBeDisabled();
  });

  it("shows loading before offering controls", () => {
    hooks.useOrgTrino.mockReturnValue({ isLoading: true });
    renderSettings();
    expect(screen.getByText("Loading Trino configuration…")).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
});
