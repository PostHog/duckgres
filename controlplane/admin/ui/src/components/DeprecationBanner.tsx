import { AlertTriangle } from "lucide-react";

// Persistent, non-dismissible notice rendered by AppShell above every route.
// Styled like the console's other warning banners (warning border + tint).
export function DeprecationBanner() {
  return (
    <div
      role="alert"
      className="flex shrink-0 items-start gap-3 border-b border-warning/40 bg-warning/10 px-6 py-3 text-sm"
    >
      <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-warning" />
      <p>
        <span className="font-semibold uppercase tracking-wide text-warning">Deprecated:</span>{" "}
        <span className="font-medium">warehouse management has moved to hogtower.</span> This console
        is read-only and will be removed. Use hogtower to manage warehouses.
      </p>
    </div>
  );
}
