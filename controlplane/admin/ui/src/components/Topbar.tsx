import { useSyncExternalStore } from "react";
import { Circle, ShieldCheck, Eye } from "lucide-react";
import { useClusterStatus, useModel } from "@/hooks/useApi";
import { useIdentity } from "@/components/IdentityProvider";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { getClusterCounts, subscribeClusterCounts } from "@/lib/clusterCounts";

function HealthDot({ ok, label, detail }: { ok: boolean; label: string; detail?: string }) {
  return (
    <div className="flex items-center gap-1.5" title={detail}>
      <Circle
        className={cn("h-2.5 w-2.5", ok ? "fill-success text-success" : "fill-destructive text-destructive")}
      />
      <span className="text-xs text-muted-foreground">{label}</span>
    </div>
  );
}

// One node/worker/placeholder/pending counter, shown only on the Nodes view
// (the peepernetes view pushes live counts into the clusterCounts store).
function CountStat({ n, label, detail, muted }: { n: number; label: string; detail?: string; muted?: boolean }) {
  return (
    <div className="flex items-baseline gap-1" title={detail}>
      <span className={cn("font-mono text-sm tabular-nums", muted && n === 0 ? "text-muted-foreground" : "text-foreground")}>
        {n}
      </span>
      <span className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</span>
    </div>
  );
}

function ClusterCounters() {
  const counts = useSyncExternalStore(subscribeClusterCounts, getClusterCounts);
  if (!counts) return null;
  return (
    <div className="flex items-center gap-4 border-l border-border pl-4">
      <CountStat n={counts.nodes} label="nodes" />
      <CountStat n={counts.workers} label="workers" detail={counts.workerReq || undefined} />
      <CountStat n={counts.placeholders} label="placeholders" detail={counts.placeholderReq || undefined} muted />
      <CountStat n={counts.pending} label="pending" muted />
    </div>
  );
}

export function Topbar() {
  const { me, role, isAdmin, loading } = useIdentity();
  const status = useClusterStatus();
  // cp-instances is a runtime model in the config-store explorer; active rows
  // = live control-plane replicas. Tolerant of the endpoint being absent.
  const cps = useModel("cp-instances");

  const cpRows = (cps.data?.rows ?? []) as { state?: string }[];
  const activeCPs = cpRows.filter((r) => (r.state ?? "").toLowerCase() === "active").length;
  const clusterOk = status.isSuccess && !status.isError;

  return (
    <header className="flex h-14 shrink-0 items-center justify-between border-b border-border bg-card/40 px-5">
      <div className="flex items-center gap-5">
        <HealthDot
          ok={clusterOk}
          label={clusterOk ? "Cluster healthy" : "Cluster unreachable"}
          detail={`${status.data?.total_orgs ?? 0} orgs · ${status.data?.total_workers ?? 0} workers`}
        />
        {cps.isSuccess && (
          <HealthDot
            ok={activeCPs > 0}
            label={`${activeCPs || cpRows.length} CP replica${(activeCPs || cpRows.length) === 1 ? "" : "s"}`}
            detail="control-plane instances (cp_instances)"
          />
        )}
        <ClusterCounters />
      </div>

      <div className="flex items-center gap-3">
        {loading ? (
          <span className="text-xs text-muted-foreground">authenticating…</span>
        ) : (
          <>
            <span className="font-mono text-xs text-muted-foreground">{me?.email ?? "unknown@—"}</span>
            <Badge variant={isAdmin ? "default" : "muted"} className="gap-1">
              {isAdmin ? <ShieldCheck className="h-3 w-3" /> : <Eye className="h-3 w-3" />}
              {role}
            </Badge>
          </>
        )}
      </div>
    </header>
  );
}
