import { Badge, type BadgeProps } from "@/components/ui/badge";

// Maps worker lifecycle + provisioning states to a badge color.
const VARIANT: Record<string, BadgeProps["variant"]> = {
  hot: "success",
  ready: "success",
  active: "success",
  idle: "secondary",
  hot_idle: "default",
  spawning: "warning",
  activating: "warning",
  reserved: "warning",
  provisioning: "warning",
  pending: "muted",
  draining: "warning",
  deleting: "warning",
  retired: "muted",
  deleted: "muted",
  lost: "destructive",
  failed: "destructive",
  // reshard operations
  running: "warning",
  resharding: "warning",
  succeeded: "success",
  cancelled: "muted",
};

export function StateBadge({ state }: { state: string | undefined }) {
  if (!state) return <span className="text-muted-foreground">—</span>;
  const variant = VARIANT[state.toLowerCase()] ?? "outline";
  return <Badge variant={variant}>{state}</Badge>;
}
