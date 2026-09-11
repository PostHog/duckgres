import { beforeEach, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";

const hooks = vi.hoisted(() => ({ useTrinoCells: vi.fn() }));
vi.mock("@/hooks/useApi", () => hooks);
import { TrinoCellPicker } from "./TrinoCellPicker";

function Location() { return <output aria-label="Location">{useLocation().search}</output>; }
function mount(path = "/trino/queries?active=true") {
  render(<MemoryRouter initialEntries={[path]}><TrinoCellPicker /><Location /></MemoryRouter>);
}
beforeEach(() => hooks.useTrinoCells.mockReturnValue({ data: { cells: [{ id: "cell-test" }] } }));
it("requires explicit selection even when only one registered cell exists", () => {
  mount();
  expect(screen.getByLabelText("Trino cell")).toHaveValue("");
  expect(screen.getByLabelText("Location")).toHaveTextContent("?active=true");
  fireEvent.change(screen.getByLabelText("Trino cell"), { target: { value: "cell-test" } });
  expect(screen.getByLabelText("Location")).toHaveTextContent("active=true&cell=cell-test");
});
it("preserves the legacy operational default only when configured", () => {
  hooks.useTrinoCells.mockReturnValue({ data: { cells: [{ id: "legacy" }, { id: "cell-test" }] } });
  mount();
  expect(screen.getByLabelText("Trino cell")).toHaveValue("legacy");
});
it("does not silently replace an unknown explicit cell", () => {
  mount("/trino/queries?cell=removed");
  expect(screen.getByLabelText("Location")).toHaveTextContent("cell=removed");
});
