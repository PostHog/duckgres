import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { REVEAL_TICK_MS, REVEAL_TICKS_TARGET, revealStep, useRevealedCount } from "./logReplay";

describe("revealStep", () => {
  it("reveals at least one line per tick", () => {
    expect(revealStep(0)).toBe(1);
    expect(revealStep(1)).toBe(1);
    expect(revealStep(REVEAL_TICKS_TARGET)).toBe(1);
  });

  it("scales so any backlog finishes within the tick target", () => {
    for (const total of [41, 200, 999, 10_000]) {
      const step = revealStep(total);
      expect(Math.ceil(total / step)).toBeLessThanOrEqual(REVEAL_TICKS_TARGET);
    }
  });
});

describe("useRevealedCount", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const tick = () => act(() => vi.advanceTimersByTime(REVEAL_TICK_MS));

  it("replays a backlog progressively and finishes within the tick target", () => {
    const { result } = renderHook(({ total }) => useRevealedCount(total), {
      initialProps: { total: 200 },
    });
    // Starts hidden, reveals in steps — never a full dump on first render.
    expect(result.current).toBe(0);
    tick();
    expect(result.current).toBe(revealStep(200));
    expect(result.current).toBeLessThan(200);

    let ticks = 1;
    while (result.current < 200 && ticks < REVEAL_TICKS_TARGET + 5) {
      tick();
      ticks++;
    }
    expect(result.current).toBe(200);
    expect(ticks).toBeLessThanOrEqual(REVEAL_TICKS_TARGET);
  });

  it("switches to prompt live-append once caught up", () => {
    const { result, rerender } = renderHook(({ total }) => useRevealedCount(total), {
      initialProps: { total: 3 },
    });
    for (let i = 0; i < 5; i++) tick();
    expect(result.current).toBe(3);

    // A new live line surfaces within one tick.
    rerender({ total: 4 });
    expect(result.current).toBe(3);
    tick();
    expect(result.current).toBe(4);
  });

  it("never exceeds the total", () => {
    const { result } = renderHook(({ total }) => useRevealedCount(total), {
      initialProps: { total: 5 },
    });
    for (let i = 0; i < 20; i++) tick();
    expect(result.current).toBe(5);
  });

  it("reveals the instant floor immediately, with no replay", () => {
    const { result } = renderHook(
      ({ total, instant }) => useRevealedCount(total, instant),
      { initialProps: { total: 5000, instant: 5000 } },
    );
    // The whole backlog is visible on first render — no ticks needed.
    expect(result.current).toBe(5000);
  });

  it("replays only lines beyond the instant floor", () => {
    const { result, rerender } = renderHook(
      ({ total, instant }) => useRevealedCount(total, instant),
      { initialProps: { total: 100, instant: 100 } },
    );
    expect(result.current).toBe(100);

    // Live appends after the backlog still surface tick-by-tick.
    rerender({ total: 102, instant: 100 });
    expect(result.current).toBe(100);
    tick();
    expect(result.current).toBeGreaterThan(100);
    tick();
    expect(result.current).toBe(102);
  });

  it("a raised instant floor mid-flight surfaces at once", () => {
    const { result, rerender } = renderHook(
      ({ total, instant }) => useRevealedCount(total, instant),
      { initialProps: { total: 0, instant: 0 } },
    );
    expect(result.current).toBe(0);
    // Catch-up completes: entries and the floor land together.
    rerender({ total: 3000, instant: 3000 });
    expect(result.current).toBe(3000);
  });

  it("clamps the instant floor to the total", () => {
    const { result } = renderHook(
      ({ total, instant }) => useRevealedCount(total, instant),
      { initialProps: { total: 3, instant: 10 } },
    );
    expect(result.current).toBe(3);
  });
});
