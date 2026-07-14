import { useEffect, useState } from "react";

// Progressive log replay for LIVE appends: new lines surface tick-by-tick like
// a live terminal. The backlog that already exists when a page opens is NOT
// replayed — callers pass its size as `instant` so an operation with thousands
// of accumulated lines shows its tail immediately (operators open the page to
// see the newest lines, not an animation). The step is derived from the total
// so any burst finishes within REVEAL_TICKS_TARGET ticks (~3.2s); once caught
// up, live appends surface within a single tick.
export const REVEAL_TICK_MS = 80;
export const REVEAL_TICKS_TARGET = 40;

// revealStep is how many lines one tick reveals for a log of `total` lines:
// at least 1, and enough that total/step ≤ REVEAL_TICKS_TARGET.
export function revealStep(total: number): number {
  return Math.max(1, Math.ceil(total / REVEAL_TICKS_TARGET));
}

// useRevealedCount returns how many of `total` items should be visible.
// Items up to `instant` (a monotonic floor — e.g. the accumulated backlog at
// page open) are visible immediately, with no replay; anything beyond climbs
// by revealStep(total) per tick. Monotonic (never goes backwards) and never
// exceeds total.
export function useRevealedCount(total: number, instant = 0): number {
  const [revealed, setRevealed] = useState(() => Math.min(instant, total));
  useEffect(() => {
    // A raised floor (the backlog landing after catch-up) surfaces at once.
    setRevealed((r) => Math.max(r, Math.min(instant, total)));
  }, [instant, total]);
  useEffect(() => {
    if (revealed >= total) return;
    const t = setTimeout(() => {
      setRevealed((r) => Math.min(total, r + revealStep(total)));
    }, REVEAL_TICK_MS);
    return () => clearTimeout(t);
  }, [total, revealed]);
  return Math.min(revealed, total);
}
