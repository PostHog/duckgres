// Deterministic per-key colors, matching the nodes-overview scheme
// (pages/nodes/peepernetes.ts `appColor`): FNV-1a hash of the key → a hue that
// skips the 95–150° band (avoids reading as the success-green used by status
// badges), rendered at 75% saturation / 62% lightness. Same key → same color
// everywhere, across pages and reloads.
export function hashColor(key: string): string {
  let h = 2166136261;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  const t = (h >>> 0) % 265;
  const hue = t < 75 ? 20 + t : 150 + (t - 75);
  return `hsl(${hue} 75% 62%)`;
}

// Translucent fill for the same key, for badge backgrounds behind hashColor
// text (mirrors the Badge variants' bg-*/15 pattern).
export function hashColorBg(key: string): string {
  let h = 2166136261;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  const t = (h >>> 0) % 265;
  const hue = t < 75 ? 20 + t : 150 + (t - 75);
  return `hsl(${hue} 75% 62% / 0.15)`;
}
