import { describe, expect, it } from "vitest";
import { warehouseNeedsPolling } from "./warehouseLifecycle";

describe("warehouseNeedsPolling", () => {
  it("keeps failed warehouses live because external dependency recovery can change their state", () => {
    expect(warehouseNeedsPolling("failed")).toBe(true);
  });

  it("polls active transitions but not stable terminal states", () => {
    expect(warehouseNeedsPolling("pending")).toBe(true);
    expect(warehouseNeedsPolling("provisioning")).toBe(true);
    expect(warehouseNeedsPolling("deleting")).toBe(true);
    expect(warehouseNeedsPolling("ready")).toBe(false);
    expect(warehouseNeedsPolling("deleted")).toBe(false);
  });
});
