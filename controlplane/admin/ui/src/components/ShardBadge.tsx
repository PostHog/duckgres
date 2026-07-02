import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { hashColor, hashColorBg } from "@/lib/colors";
import type { DucklingMetadataEntry } from "@/types/api";

// ShardBadge renders a Duckling's live metadata-store assignment: a
// color-coded pill for the cnpg shard (same deterministic per-key palette as
// the nodes overview, so one shard is one color everywhere), a muted
// "external" pill for RDS-backed tenants, and "—" when unknown (CR missing /
// endpoint not yet populated / pre-rollout backend).
export function ShardBadge({ meta }: { meta: DucklingMetadataEntry | undefined }) {
  if (!meta) return <span className="text-muted-foreground">—</span>;
  const label = meta.cnpg_shard || (meta.kind === "external" ? "external" : meta.kind || "—");
  const colored = Boolean(meta.cnpg_shard);
  const badge = (
    <Badge
      variant={colored ? "outline" : "muted"}
      className="font-mono"
      style={colored ? { color: hashColor(label), backgroundColor: hashColorBg(label), borderColor: "transparent" } : undefined}
    >
      {label}
    </Badge>
  );
  return meta.endpoint ? (
    <Tooltip>
      <TooltipTrigger asChild>{badge}</TooltipTrigger>
      <TooltipContent className="font-mono text-xs">{meta.endpoint}</TooltipContent>
    </Tooltip>
  ) : (
    badge
  );
}
