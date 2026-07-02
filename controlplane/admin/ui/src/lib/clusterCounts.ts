// Tiny external store that bridges the imperative peepernetes "Nodes" view's
// live cluster counters into the shared React Topbar. The Nodes page owns an
// imperative DOM/polling module (pages/nodes/peepernetes.ts) that can't render
// into React directly, so on each repaint it pushes the current
// nodes/workers/placeholders/pending counts here; Topbar subscribes via
// useSyncExternalStore and shows them next to the CP-replica health chips.
// When the Nodes view unmounts it pushes null and the Topbar hides them.

export interface ClusterCounts {
  nodes: number;
  workers: number; // non-placeholder, non-succeeded pods (the view calls these "workers")
  placeholders: number;
  pending: number;
  workerReq: string; // "12 CPU · 34 Gi" totals, "" when zero — shown as a tooltip
  placeholderReq: string;
}

let current: ClusterCounts | null = null;
const subscribers = new Set<() => void>();

export function setClusterCounts(next: ClusterCounts | null): void {
  current = next;
  for (const fn of subscribers) fn();
}

export function getClusterCounts(): ClusterCounts | null {
  return current;
}

export function subscribeClusterCounts(fn: () => void): () => void {
  subscribers.add(fn);
  return () => {
    subscribers.delete(fn);
  };
}
