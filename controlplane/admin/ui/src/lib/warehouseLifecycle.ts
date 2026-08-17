// Warehouse states whose value can change without an admin-UI mutation. Failed
// is intentionally included: the provisioner keeps observing externally
// managed Duckling dependencies and returns the row to ready after recovery.
const POLLED_WAREHOUSE_STATES = new Set(["pending", "provisioning", "failed", "deleting"]);

export function warehouseNeedsPolling(state: string | null | undefined): boolean {
  return state !== undefined && state !== null && POLLED_WAREHOUSE_STATES.has(state.toLowerCase());
}
