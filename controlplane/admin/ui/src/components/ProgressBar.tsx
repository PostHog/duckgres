import { cn } from "@/lib/utils";

// Query progress bar. `percentage` 0..100; when undefined/indeterminate we show
// a sliding indeterminate animation. `stalled` paints amber.
export function ProgressBar({
  percentage,
  stalled,
  indeterminate,
  className,
}: {
  percentage?: number;
  stalled?: boolean;
  indeterminate?: boolean;
  className?: string;
}) {
  const pct = percentage == null ? undefined : Math.max(0, Math.min(100, percentage));
  return (
    <div className={cn("relative h-2 w-full overflow-hidden rounded-full bg-muted", className)}>
      {indeterminate || pct == null ? (
        <div className="absolute inset-y-0 left-0 w-1/4 animate-indeterminate rounded-full bg-primary/70" />
      ) : (
        <div
          className={cn(
            "h-full rounded-full transition-all",
            stalled ? "bg-warning" : "bg-primary",
          )}
          style={{ width: `${pct}%` }}
        />
      )}
    </div>
  );
}
