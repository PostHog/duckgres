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
it("defaults to the only registered cell when the URL names none", () => {
  mount();
  expect(screen.getByLabelText("Trino cell")).toHaveValue("cell-test");
  expect(screen.getByLabelText("Location")).toHaveTextContent("active=true&cell=cell-test");
});
it("defaults to the first registered cell when several exist", () => {
  hooks.useTrinoCells.mockReturnValue({ data: { cells: [{ id: "cell-001" }, { id: "cell-002" }] } });
  mount();
  expect(screen.getByLabelText("Location")).toHaveTextContent("cell=cell-001");
  fireEvent.change(screen.getByLabelText("Trino cell"), { target: { value: "cell-002" } });
  expect(screen.getByLabelText("Location")).toHaveTextContent("cell=cell-002");
});
it("keeps an explicit cell", () => {
  hooks.useTrinoCells.mockReturnValue({ data: { cells: [{ id: "cell-001" }, { id: "cell-002" }] } });
  mount("/trino/queries?cell=cell-002");
  expect(screen.getByLabelText("Trino cell")).toHaveValue("cell-002");
});
it("does not navigate before the cell list loads", () => {
  hooks.useTrinoCells.mockReturnValue({ data: undefined });
  mount();
  expect(screen.getByLabelText("Location")).toHaveTextContent("?active=true");
  expect(screen.getByLabelText("Location")).not.toHaveTextContent("cell=");
});
it("does not silently replace an unknown explicit cell", () => {
  mount("/trino/queries?cell=removed");
  expect(screen.getByLabelText("Location")).toHaveTextContent("cell=removed");
});
