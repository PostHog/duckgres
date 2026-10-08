import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

// AppShell renders Sidebar/Topbar, which pull live data; stub them so the test
// only exercises the shell layout and the banner.
vi.mock("@/components/Sidebar", () => ({ Sidebar: () => null }));
vi.mock("@/components/Topbar", () => ({ Topbar: () => null }));

import { AppShell } from "./AppShell";

describe("DeprecationBanner", () => {
  it("is rendered by AppShell above the page content and cannot be dismissed", () => {
    render(
      <AppShell>
        <p>page body</p>
      </AppShell>,
    );
    const banner = screen.getByRole("alert");
    expect(banner).toHaveTextContent(/Deprecated:/);
    expect(banner).toHaveTextContent(/warehouse management has moved to hogtower/);
    expect(banner).toHaveTextContent(/Use hogtower to manage warehouses/);
    expect(screen.queryByRole("button")).toBeNull();
    expect(screen.getByText("page body")).toBeInTheDocument();
  });
});
