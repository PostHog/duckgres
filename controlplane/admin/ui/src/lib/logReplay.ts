import { useEffect, useState } from "react";

// Progressive log replay: opening an operation page with an accumulated log
// reveals it line-by-line like a live terminal instead of dumping everything
// at once. The step is derived from the backlog size so ANY backlog finishes
// within REVEAL_TICKS_TARGET ticks (~3.2s); once caught up, live appends
// surface within a single tick.
export const REVEAL_TICK_MS = 80;
export const REVEAL_TICKS_TARGET = 40;

// revealStep is how many lines one tick reveals for a log of `total` lines:
// at least 1, and enough that total/step ≤ REVEAL_TICKS_TARGET.
export function revealStep(total: number): number {
  return Math.max(1, Math.ceil(total / REVEAL_TICKS_TARGET));
}

// useRevealedCount returns how many of `total` items should be visible:
// starts at 0 and climbs by revealStep(total) per tick until it catches up.
// Monotonic (never goes backwards) and never exceeds total.
export function useRevealedCount(total: number): number {
  const [revealed, setRevealed] = useState(0);
  useEffect(() => {
    if (revealed >= total) return;
    const t = setTimeout(() => {
      setRevealed((r) => Math.min(total, r + revealStep(total)));
    }, REVEAL_TICK_MS);
    return () => clearTimeout(t);
  }, [total, revealed]);
  return Math.min(revealed, total);
}
