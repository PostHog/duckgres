import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type { HotIdleOrg } from "@/types/api";

const hooks = vi.hoisted(() => ({
  useFleet: vi.fn(),
  useWorkers: vi.fn(),
  useHotIdle: vi.fn(),
  useOrgLabels: vi.fn(),
}));
vi.mock("@/hooks/useApi", () => hooks);

import { Workers } from "./Workers";

const ok = <T,>(data: T) => ({ data, isSuccess: true, isLoading: false, isError: false, refetch: vi.fn() });

const HOT_IDLE: HotIdleOrg[] = [
  {
    org_id: "acme",
    count: 3,
    cpu_cores: 8,
    memory_bytes: 32 * 1024 ** 3,
    oldest_hot_idle_since: "2026-08-19T10:00:00Z",
    cap_workers: 5,
    cap_cpu: "16",
    cap_memory: "64Gi",
  },
  {
    org_id: "globex",
    count: 1,
    cpu_cores: 2,
    memory_bytes: 8 * 1024 ** 3,
    oldest_hot_idle_since: null,
    cap_workers: 0,
    cap_cpu: "",
    cap_memory: "",
  },
];

function renderPage() {
  render(
    <MemoryRouter>
      <Workers />
    </MemoryRouter>,
  );
}

describe("Workers page hot-idle card", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    hooks.useFleet.mockReturnValue(ok([]));
    hooks.useWorkers.mockReturnValue(ok([]));
    hooks.useOrgLabels.mockReturnValue(new Map([["acme", "Acme Inc"]]));
    hooks.useHotIdle.mockReturnValue(ok(HOT_IDLE));
  });

  it("lists each org's parked pool with its caps and links to the org page", () => {
    renderPage();
    expect(screen.getByText("Hot idle by org")).toBeInTheDocument();
    // Org label resolves; the row links through to the org detail page.
    const link = screen.getByRole("link", { name: /acme inc/i });
    expect(link).toHaveAttribute("href", "/orgs/acme");
    // acme: 3 parked against a 5-worker cap, with the cpu/memory caps shown.
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.getByText(/\/ 5/)).toBeInTheDocument();
    expect(screen.getByText("5 workers · 16 · 64Gi")).toBeInTheDocument();
    // globex has no cap configured.
    expect(screen.getByText("globex")).toBeInTheDocument();
  });

  it("renders an empty state when nothing is parked", () => {
    hooks.useHotIdle.mockReturnValue(ok([]));
    renderPage();
    expect(screen.getByText(/no workers are parked hot-idle/i)).toBeInTheDocument();
  });
});
