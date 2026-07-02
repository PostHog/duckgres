import { useSyncExternalStore } from "react";
import { Circle, ShieldCheck, Eye } from "lucide-react";
import { useClusterStatus, useModel } from "@/hooks/useApi";
import { useIdentity } from "@/components/IdentityProvider";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { getClusterCounts, subscribeClusterCounts } from "@/lib/clusterCounts";

// One counter in the unified header stat row: a value + a small label, styled
// identically for every metric (CP replicas, nodes, workers, …) so they read as
// one design instead of a mix of health pills and counters. `muted` dims a zero
// value for metrics where 0 is the boring/expected state (placeholders/pending).
function Stat({ n, label, detail, muted }: { n: number; label: string; detail?: string; muted?: boolean }) {
  return (
    <div className="flex items-baseline gap-1" title={detail}>
      <span className={cn("font-mono text-sm tabular-nums", muted && n === 0 ? "text-muted-foreground" : "text-foreground")}>
        {n}
      </span>
      <span className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</span>
    </div>
  );
}

// Middot divider between stats.
function Dot() {
  return <span className="text-xs text-muted-foreground/40">·</span>;
}

// Count of live CP replicas: prefer rows explicitly marked "active", falling
// back to the total row count if none carry a state (older cp_instances rows).
function activeOrTotal(rows: { state?: string }[]): number {
  const active = rows.filter((r) => (r.state ?? "").toLowerCase() === "active").length;
  return active || rows.length;
}

export function Topbar() {
  const { me, role, isAdmin, loading } = useIdentity();
  const status = useClusterStatus();
  // cp-instances is a runtime model in the config-store explorer; active rows
  // = live control-plane replicas. Tolerant of the endpoint being absent.
  const cps = useModel("cp-instances");
  // Live node/worker/placeholder/pending counts, pushed by the Nodes view while
  // it's mounted (null elsewhere).
  const counts = useSyncExternalStore(subscribeClusterCounts, getClusterCounts);

  const cpRows = (cps.data?.rows ?? []) as { state?: string }[];
  const cpCount = cps.isSuccess ? activeOrTotal(cpRows) : null;
  // "Reachable" reflects only that the admin API answered GET /status — it is NOT
  // a real cluster-health signal (the query even tolerates a 404 as success). So
  // the dot means "the admin API is responding", nothing more; the tooltip spells
  // out the totals it returned.
  const reachable = status.isSuccess && !status.isError;
  const orgs = status.data?.total_orgs ?? 0;
  const workers = status.data?.total_workers ?? 0;
  const sessions = status.data?.total_sessions ?? 0;

  return (
    <header className="flex h-14 shrink-0 items-center justify-between border-b border-border bg-card/40 px-5">
      <div className="flex items-center gap-3">
        <div
          className="flex items-center gap-1.5"
          title={
            reachable
              ? `admin API reachable — ${orgs} orgs · ${workers} workers · ${sessions} sessions`
              : "admin API not reachable (GET /status failed)"
          }
        >
          <Circle
            className={cn("h-2.5 w-2.5", reachable ? "fill-success text-success" : "fill-destructive text-destructive")}
          />
          <span className="text-xs text-muted-foreground">{reachable ? "Connected" : "Unreachable"}</span>
        </div>

        {(cpCount !== null || counts) && (
          <div className="flex items-center gap-2.5 border-l border-border pl-3">
            {cpCount !== null && <Stat n={cpCount} label="CP" detail="live control-plane replicas (cp_instances)" />}
            {counts && (
              <>
                {cpCount !== null && <Dot />}
                <Stat n={counts.nodes} label="nodes" />
                <Dot />
                <Stat n={counts.workers} label="workers" detail={counts.workerReq || undefined} />
                <Dot />
                <Stat n={counts.placeholders} label="placeholders" detail={counts.placeholderReq || undefined} muted />
                <Dot />
                <Stat n={counts.pending} label="pending" muted />
              </>
            )}
          </div>
        )}
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
