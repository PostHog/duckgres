import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { focusManager, QueryClient, QueryClientProvider } from "@tanstack/react-query";

const mocks = vi.hoisted(() => ({
  trinoInstances: vi.fn(), trinoRecovery: vi.fn(), requestTrinoRecovery: vi.fn(),
  useIdentity: vi.fn(),
}));
vi.mock("@/components/IdentityProvider", () => ({ useIdentity: mocks.useIdentity }));
vi.mock("@/lib/api", async (original) => ({
  ...await original<typeof import("@/lib/api")>(),
  api: mocks,
}));
import { ApiError } from "@/lib/api";
import { TrinoRecovery } from "./TrinoRecovery";

function preview() {
  return {
    cell: "pool-a",
    instance: {
      instance_id: "instance-a", phase: "DRAINING", gateway_state: "DRAINING",
      expected_generation: 7, incarnation: "incarnation-a", pod_uid: "pod-a",
      boot_id: "boot-a", node_id: "node-a", coordinator_id: "coordinator-a",
      phase_changed_at: "2026-01-01T00:00:00Z", last_error: "",
    },
    capacity: { stored_serving: 3, min_serving: 2, desired_instances: 3, frozen: false },
    live_work_verified: false,
    request: null as unknown,
  };
}

function mount(cell = "pool-a") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return { ...render(<QueryClientProvider client={client}><TrinoRecovery cell={cell} /></QueryClientProvider>), client };
}

async function selectInstance() {
  const picker = await screen.findByRole("combobox", { name: "Trino instance" });
  await waitFor(() => expect(screen.getByRole("option", { name: /instance-a/ })).toBeInTheDocument());
  await userEvent.selectOptions(picker, "instance-a");
  await screen.findByText("coordinator-a");
}

async function confirmRecovery() {
  await userEvent.type(screen.getByLabelText("Reason"), "Retire a blocked instance");
  await userEvent.type(screen.getByLabelText("Type the instance ID"), "instance-a");
  await userEvent.click(screen.getByRole("checkbox", { name: /retained results/ }));
}

describe("Trino recovery", () => {
  afterEach(() => {
    cleanup();
    focusManager.setFocused(undefined);
    vi.useRealTimers();
    vi.restoreAllMocks();
  });
  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage.clear();
    mocks.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "operator@example.com" } });
    mocks.trinoInstances.mockResolvedValue({ cell: "pool-a", instances: [{ instance_id: "instance-a", phase: "DRAINING" }] });
    mocks.trinoRecovery.mockResolvedValue(preview());
    mocks.requestTrinoRecovery.mockImplementation(async (_instance, body) => ({ request: { ...body, instance_id: "instance-a" } }));
  });

  it("does not request sensitive inventory or recovery data for viewers", () => {
    mocks.useIdentity.mockReturnValue({ isAdmin: false });
    mount();
    expect(screen.getByText(/admin role required/i)).toBeInTheDocument();
    expect(mocks.trinoInstances).not.toHaveBeenCalled();
    expect(mocks.trinoRecovery).not.toHaveBeenCalled();
  });

  it("shows exact identity and labels stored capacity as not live-verified", async () => {
    mount();
    await selectInstance();
    expect(mocks.trinoRecovery).toHaveBeenCalledWith("instance-a", "pool-a");
    expect(screen.getByText(/does not verify live workload or capacity/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
  });

  it("requires reason, exact typed identity and destructive acknowledgement", async () => {
    mount();
    await selectInstance();
    await confirmRecovery();
    fireEvent.change(screen.getByLabelText("Type the instance ID"), { target: { value: "wrong" } });
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    fireEvent.change(screen.getByLabelText("Type the instance ID"), { target: { value: "instance-a" } });
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await waitFor(() => expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1));
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledWith("instance-a", expect.objectContaining({
      operation_id: expect.any(String), expected_generation: 7, incarnation: "incarnation-a",
      pod_uid: "pod-a", boot_id: "boot-a", node_id: "node-a", coordinator_id: "coordinator-a",
      reason: "Retire a blocked instance", destructive_authorization: true,
    }), "pool-a");
  });

  it("preserves the identical request after a lost reply and never retries automatically", async () => {
    mocks.requestTrinoRecovery.mockRejectedValue(new ApiError(0, "connection lost"));
    mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await screen.findByText(/acceptance is unknown/i);
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1);
    const first = mocks.requestTrinoRecovery.mock.calls[0];
    await userEvent.click(screen.getByRole("button", { name: "Retry identical request" }));
    await waitFor(() => expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(2));
    expect(mocks.requestTrinoRecovery.mock.calls[1]).toEqual(first);
  });

  it("refreshes the preview on submit and requires confirmation again if identity changed", async () => {
    mount();
    await selectInstance();
    await confirmRecovery();
    const changed = preview();
    changed.instance.boot_id = "replacement-boot";
    mocks.trinoRecovery.mockResolvedValue(changed);
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await screen.findByText("replacement-boot");
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Type the instance ID")).toHaveValue("");
  });

  it("sends only one request when the button is clicked twice before completion", async () => {
    mocks.requestTrinoRecovery.mockReturnValue(new Promise(() => {}));
    mount();
    await selectInstance();
    await confirmRecovery();
    const button = screen.getByRole("button", { name: "Request destructive recovery" });
    fireEvent.click(button);
    fireEvent.click(button);
    await waitFor(() => expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1));
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
  });

  it("restores an unknown request after remount with the identical operation ID and body", async () => {
    mocks.requestTrinoRecovery.mockRejectedValue(new ApiError(0, "connection lost"));
    const firstPage = mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await screen.findByText(/acceptance is unknown/i);
    const first = mocks.requestTrinoRecovery.mock.calls[0];
    firstPage.unmount();
    mount();
    await selectInstance();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    await userEvent.click(screen.getByRole("button", { name: "Retry identical request" }));
    await waitFor(() => expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(2));
    expect(mocks.requestTrinoRecovery.mock.calls[1]).toEqual(first);
  });

  it("never discards an ambiguous request when a later retry gets 409, including after reload", async () => {
    mocks.requestTrinoRecovery.mockRejectedValueOnce(new ApiError(0, "connection lost")).mockRejectedValue(new ApiError(409, "changed"));
    const firstPage = mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await screen.findByText(/acceptance is unknown/i);
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry identical request" })).toBeEnabled());
    await userEvent.click(screen.getByRole("button", { name: "Retry identical request" }));
    await screen.findByText(/identity or recorded intent changed/i);
    expect(screen.queryByRole("button", { name: "Review a new preview" })).not.toBeInTheDocument();
    firstPage.unmount();
    mount();
    await selectInstance();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "Review a new preview" })).not.toBeInTheDocument();
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(2);
  });

  it("does not retry a lost reply until the subsequent GET finishes", async () => {
    let resolveRead: ((value: unknown) => void) | undefined;
    mocks.requestTrinoRecovery.mockImplementation(async () => {
      mocks.trinoRecovery.mockReturnValue(new Promise((resolve) => { resolveRead = resolve; }));
      throw new ApiError(0, "connection lost");
    });
    mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await waitFor(() => expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1));
    expect(screen.getByRole("button", { name: "Retry identical request" })).toBeDisabled();
    await act(async () => { resolveRead?.(preview()); });
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry identical request" })).toBeEnabled());
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1);
  });

  it("shows an existing immutable request and prevents any new mutation", async () => {
    const existing = preview();
    existing.request = { operation_id: "operation-existing", requested_by: "operator@example.com", reason: "Investigated drain", created_at: "2026-01-01T00:00:00Z" };
    mocks.trinoRecovery.mockResolvedValue(existing);
    mount();
    await selectInstance();
    expect(screen.getByText(/cannot be cancelled or amended/)).toBeInTheDocument();
    expect(screen.getByText("operation-existing")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it.each([401, 403])("blocks preview and writes when access is denied (%s)", async (status) => {
    mocks.trinoRecovery.mockRejectedValue(new ApiError(status, "denied"));
    mount();
    await waitFor(() => expect(screen.getByRole("option", { name: /instance-a/ })).toBeInTheDocument());
    await userEvent.selectOptions(screen.getByRole("combobox", { name: "Trino instance" }), "instance-a");
    await screen.findByText(/Check your sign-in and permissions/);
    expect(screen.getByRole("button", { name: "Refresh preview" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
  });

  it("blocks writes when identity refresh failed even if the cached role is admin", () => {
    mocks.useIdentity.mockReturnValue({ isAdmin: true, me: { email: "operator@example.com" }, error: new Error("offline") });
    mount();
    expect(mocks.trinoInstances).not.toHaveBeenCalled();
    expect(screen.getByText(/admin role required/i)).toBeInTheDocument();
  });

  it("requires explicit review after a 409 without automatically changing the operation", async () => {
    mocks.requestTrinoRecovery.mockRejectedValue(new ApiError(409, "changed"));
    mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await screen.findByText(/identity or recorded intent changed/i);
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("button", { name: "Retry identical request" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    await waitFor(() => expect(screen.getByRole("button", { name: "Review a new preview" })).toBeEnabled());
    await userEvent.click(screen.getByRole("button", { name: "Review a new preview" }));
    expect(await screen.findByLabelText("Type the instance ID")).toHaveValue("");
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1);
  });

  it("does not use a cached preview after a refresh fails with 503", async () => {
    mount();
    await selectInstance();
    await confirmRecovery();
    mocks.trinoRecovery.mockRejectedValue(new ApiError(503, "unavailable"));
    await userEvent.click(screen.getByRole("button", { name: "Refresh preview" }));
    await screen.findByText(/previous preview cannot authorize/i);
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it.each(["FAILURE_RETIRED", "RETIRED"])("shows terminal %s after the instance leaves active inventory", async (phase) => {
    mount();
    await selectInstance();
    const completed = preview();
    completed.instance.phase = phase;
    mocks.trinoRecovery.mockResolvedValue(completed);
    mocks.trinoInstances.mockResolvedValue({ cell: "pool-a", instances: [] });
    await userEvent.click(screen.getByRole("button", { name: "Refresh instances" }));
    await userEvent.click(screen.getByRole("button", { name: "Refresh preview" }));
    await screen.findByText(/retirement completed|recovery completed/i);
    expect(screen.getByRole("combobox", { name: "Trino instance" })).toHaveValue("instance-a");
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
  });

  it("isolates a cell switch from an old in-flight response", async () => {
    let resolve: ((value: unknown) => void) | undefined;
    mocks.trinoRecovery.mockReturnValue(new Promise((done) => { resolve = done; }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const page = render(<QueryClientProvider client={client}><TrinoRecovery cell="pool-a" /></QueryClientProvider>);
    await waitFor(() => expect(screen.getByRole("option", { name: /instance-a/ })).toBeInTheDocument());
    await userEvent.selectOptions(screen.getByRole("combobox", { name: "Trino instance" }), "instance-a");
    mocks.trinoInstances.mockResolvedValue({ cell: "pool-b", instances: [] });
    page.rerender(<QueryClientProvider client={client}><TrinoRecovery cell="pool-b" /></QueryClientProvider>);
    await act(async () => { resolve?.(preview()); });
    expect(screen.queryByText("coordinator-a")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Request destructive recovery" })).not.toBeInTheDocument();
  });

  it("does not submit after navigating away while the preflight GET is pending", async () => {
    const page = mount();
    await selectInstance();
    await confirmRecovery();
    let resolveRead: ((value: unknown) => void) | undefined;
    mocks.trinoRecovery.mockReturnValue(new Promise((resolve) => { resolveRead = resolve; }));
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    page.unmount();
    await act(async () => { resolveRead?.(preview()); });
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it("does not let an old unmounted rejection clear a newer ambiguous attempt", async () => {
    let rejectOld: ((error: unknown) => void) | undefined;
    mocks.requestTrinoRecovery.mockReturnValueOnce(new Promise((_resolve, reject) => { rejectOld = reject; }))
      .mockReturnValue(new Promise(() => {}));
    const first = mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await waitFor(() => expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1));
    first.unmount();
    mount();
    await selectInstance();
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry identical request" })).toBeEnabled());
    await userEvent.click(screen.getByRole("button", { name: "Retry identical request" }));
    await waitFor(() => expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(2));
    await act(async () => { rejectOld?.(new ApiError(409, "changed")); });
    const key = Object.keys(sessionStorage).find((entry) => entry.startsWith("trino-recovery:"));
    expect(JSON.parse(sessionStorage.getItem(key!)!).mayHaveBeenAccepted).toBe(true);
  });

  it("keeps the focused form editable during a background preview refresh while blocking submission", async () => {
    const page = mount();
    await selectInstance();
    await confirmRecovery();
    const reason = screen.getByLabelText("Reason");
    await userEvent.click(reason);
    let resolveRead: ((value: unknown) => void) | undefined;
    mocks.trinoRecovery.mockReturnValue(new Promise((resolve) => { resolveRead = resolve; }));
    await act(async () => { void page.client.refetchQueries({ queryKey: ["trino-recovery"] }); });
    await waitFor(() => expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled());
    expect(reason).toBeEnabled();
    expect(reason).toHaveFocus();
    expect(screen.getByLabelText("Type the instance ID")).toBeEnabled();
    expect(screen.getByRole("checkbox", { name: /retained results/ })).toBeEnabled();
    await userEvent.type(reason, " after review");
    await act(async () => { resolveRead?.(preview()); });
    await waitFor(() => expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeEnabled());
    expect(reason).toHaveValue("Retire a blocked instance after review");
    expect(reason).toHaveFocus();
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it("explains a stale preview when a previously enabled submit button is clicked", async () => {
    mount();
    await selectInstance();
    await confirmRecovery();
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now + 31_000);
    fireEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    expect(await screen.findByText(/preview is stale.*refresh preview/i)).toBeInTheDocument();
    expect(screen.getByLabelText("Reason")).toHaveValue("Retire a blocked instance");
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it("explains why background capacity changes reset the confirmation", async () => {
    const page = mount();
    await selectInstance();
    expect(screen.queryByText(/preview changed/i)).not.toBeInTheDocument();
    await confirmRecovery();
    const changed = preview();
    changed.capacity.desired_instances = 4;
    mocks.trinoRecovery.mockResolvedValue(changed);
    await act(async () => { await page.client.refetchQueries({ queryKey: ["trino-recovery"] }); });
    await waitFor(() => expect(screen.getByLabelText("Type the instance ID")).toHaveValue(""));
    expect(await screen.findByText(/preview changed.*confirm again/i)).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: /retained results/ })).not.toBeChecked();
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it("gives actionable investigation guidance for an ambiguous 409 without clearing the original operation", async () => {
    mocks.requestTrinoRecovery.mockRejectedValueOnce(new ApiError(0, "connection lost")).mockRejectedValue(new ApiError(409, "changed"));
    mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await screen.findByText(/acceptance is unknown/i);
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry identical request" })).toBeEnabled());
    await userEvent.click(screen.getByRole("button", { name: "Retry identical request" }));
    await screen.findByText(/identity or recorded intent changed/i);
    expect(await screen.findByText(/stop retrying.*original operation/i)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Open recorded recovery status" })).toHaveAttribute("href", "/api/v1/trino/instances/instance-a/recovery?cell=pool-a");
    expect(screen.getByRole("link", { name: "Ambiguous-conflict runbook" })).toHaveAttribute("href", "https://github.com/PostHog/duckgres/blob/main/docs/runbooks/trino-pool-admin-recovery.md#resolve-an-ambiguous-conflict");
    expect(screen.queryByRole("button", { name: "Review a new preview" })).not.toBeInTheDocument();
    const key = Object.keys(sessionStorage).find((entry) => entry.startsWith("trino-recovery:"));
    const saved = JSON.parse(sessionStorage.getItem(key!)!);
    expect(saved.mayHaveBeenAccepted).toBe(true);
    expect(saved.body).toEqual(mocks.requestTrinoRecovery.mock.calls[0][1]);
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(2);
  });

  it("hides the retry action after acceptance while waiting for the recorded request", async () => {
    mount();
    await selectInstance();
    await confirmRecovery();
    await userEvent.click(screen.getByRole("button", { name: "Request destructive recovery" }));
    await screen.findByText("Recovery accepted. Waiting for its recorded status.");
    expect(screen.queryByRole("button", { name: "Retry identical request" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Refresh preview" })).toBeEnabled();
    expect(mocks.requestTrinoRecovery).toHaveBeenCalledTimes(1);
  });

  it.each([404, 503])("stops unavailable inventory polling (%s), permits manual refresh, and resumes after success", async (status) => {
    vi.useFakeTimers();
    focusManager.setFocused(true);
    mocks.trinoInstances.mockRejectedValue(new ApiError(status, "unavailable"));
    mount();
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.trinoInstances).toHaveBeenCalledTimes(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
    expect(mocks.trinoInstances).toHaveBeenCalledTimes(1);
    const refresh = screen.getByRole("button", { name: "Refresh instances" });
    expect(refresh).toBeEnabled();
    mocks.trinoInstances.mockResolvedValue({ cell: "pool-a", instances: [] });
    fireEvent.click(refresh);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.trinoInstances).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(15_000); });
    expect(mocks.trinoInstances).toHaveBeenCalledTimes(3);
  });

  it.each([404, 503])("stops unavailable preview polling (%s) without disabling manual refresh", async (status) => {
    vi.useFakeTimers();
    focusManager.setFocused(true);
    mocks.trinoRecovery.mockRejectedValue(new ApiError(status, "unavailable"));
    mount();
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    fireEvent.change(screen.getByRole("combobox", { name: "Trino instance" }), { target: { value: "instance-a" } });
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.trinoRecovery).toHaveBeenCalledTimes(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(20_000); });
    expect(mocks.trinoRecovery).toHaveBeenCalledTimes(1);
    const refresh = screen.getByRole("button", { name: "Refresh preview" });
    expect(refresh).toBeEnabled();
    mocks.trinoRecovery.mockResolvedValue(preview());
    fireEvent.click(refresh);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.trinoRecovery).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });
    expect(mocks.trinoRecovery).toHaveBeenCalledTimes(3);
  });

  it("loads a newly selected cell after unavailable inventory stopped polling", async () => {
    vi.useFakeTimers();
    focusManager.setFocused(true);
    mocks.trinoInstances.mockRejectedValue(new ApiError(404, "not a shared pool"));
    const page = mount();
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.trinoInstances).toHaveBeenCalledTimes(1);
    mocks.trinoInstances.mockResolvedValue({ cell: "pool-b", instances: [] });
    page.rerender(<QueryClientProvider client={page.client}><TrinoRecovery cell="pool-b" /></QueryClientProvider>);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(mocks.trinoInstances).toHaveBeenLastCalledWith("pool-b");
    expect(mocks.trinoInstances).toHaveBeenCalledTimes(2);
    expect(screen.getByText("No active shared-pool instances.")).toBeInTheDocument();
  });

  it("shows why a draining instance cannot recover when stored capacity is zero", async () => {
    const blocked = preview();
    blocked.capacity.stored_serving = 0;
    blocked.capacity.min_serving = 3;
    mocks.trinoRecovery.mockResolvedValue(blocked);
    mount();
    await selectInstance();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    expect(screen.getByText("Stored serving count is 0; minimum required is 3. Recovery cannot proceed below this minimum.")).toBeInTheDocument();
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it("lists every failed eligibility condition instead of a generic missing button", async () => {
    const blocked = preview();
    blocked.instance.phase = "CREATING";
    blocked.instance.boot_id = "";
    blocked.instance.pod_uid = "";
    blocked.instance.expected_generation = 0;
    blocked.capacity.frozen = true;
    blocked.capacity.stored_serving = 1;
    mocks.trinoRecovery.mockResolvedValue(blocked);
    mount();
    await selectInstance();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    expect(screen.getByText("Instance phase is CREATING; recovery requires DRAINING.")).toBeInTheDocument();
    expect(screen.getByText("The pool is frozen. Recovery cannot proceed while it is frozen.")).toBeInTheDocument();
    expect(screen.getByText("Missing or invalid admitted identity fields: expected_generation, pod_uid, boot_id.")).toBeInTheDocument();
    expect(screen.getByText("Stored serving count is 1; minimum required is 2. Recovery cannot proceed below this minimum.")).toBeInTheDocument();
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });

  it("requires confirmation again after a capacity blocker clears", async () => {
    mount();
    await selectInstance();
    await confirmRecovery();
    const blocked = preview();
    blocked.capacity.stored_serving = 0;
    mocks.trinoRecovery.mockResolvedValue(blocked);
    await userEvent.click(screen.getByRole("button", { name: "Refresh preview" }));
    expect(await screen.findByText(/Stored serving count is 0/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    mocks.trinoRecovery.mockResolvedValue(preview());
    await userEvent.click(screen.getByRole("button", { name: "Refresh preview" }));
    await waitFor(() => expect(screen.getByLabelText("Type the instance ID")).toHaveValue(""));
    expect(screen.getByRole("button", { name: "Request destructive recovery" })).toBeDisabled();
    expect(screen.getByRole("checkbox", { name: /retained results/ })).not.toBeChecked();
    expect(mocks.requestTrinoRecovery).not.toHaveBeenCalled();
  });
});
