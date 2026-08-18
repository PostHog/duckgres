// Vitest global setup: extends `expect` with jest-dom matchers (toBeInThe
// Document, toHaveTextContent, …) for component tests. Pure-function tests don't
// need it but it's harmless to load once here.
import "@testing-library/jest-dom/vitest";

// jsdom has no ResizeObserver; recharts' ResponsiveContainer needs one. A
// no-op stub is enough — the charts simply render at zero size in tests.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
if (typeof globalThis.ResizeObserver === "undefined") {
  globalThis.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver;
}
