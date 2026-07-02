import { useSyncExternalStore } from "react";
import { Circle, ShieldCheck, Eye } from "lucide-react";
import { useClusterStatus, useModel } from "@/hooks/useApi";
import { useIdentity } from "@/components/IdentityProvider";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { getClusterCounts, subscribeClusterCounts } from "@/lib/clusterCounts";

// One counter in the unified header stat row: a value + a small label on the top
// line, with an optional smaller sub-line beneath (e.g. the workers' vCPU/GiB
// totals — like the old peepernetes stat blocks). Styled identically for every
// metric so they read as one design. `muted` dims a zero value for metrics where
// 0 is the boring/expected state (placeholders/pending).
function Stat({ n, label, sub, detail, muted }: { n: number; label: string; sub?: string; detail?: string; muted?: boolean }) {
  return (
    <div className="flex flex-col justify-center leading-none" title={detail}>
      <div className="flex items-baseline gap-1">
        <span className={cn("font-mono text-sm tabular-nums", muted && n === 0 ? "text-muted-foreground" : "text-foreground")}>
          {n}
        </span>
        <span className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</span>
      </div>
      {sub !== undefined && <span className="mt-0.5 text-[10px] tabular-nums text-muted-foreground/70">{sub}</span>}
    </div>
  );
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
            className={cn(
              "h-2.5 w-2.5",
              // Connected is the live pulse now (moved off the Nodes view's old
              // in-view "LIVE" indicator): green, pulsing, with a soft glow.
              reachable
                ? "fill-success text-success animate-pulse [filter:drop-shadow(0_0_5px_currentColor)]"
                : "fill-destructive text-destructive",
            )}
          />
          <span className="text-xs text-muted-foreground">{reachable ? "Connected" : "Unreachable"}</span>
        </div>

        {(cpCount !== null || counts) && (
          <div className="flex items-start gap-4 border-l border-border pl-4">
            {counts && <Stat n={counts.nodes} label="nodes" />}
            {cpCount !== null && <Stat n={cpCount} label="CP" detail="live control-plane replicas (cp_instances)" />}
            {counts && (
              <>
                {/* workers carries the vCPU/GiB totals as a small sub-line — shown
                    even at zero, like the old peepernetes stat block. */}
                <Stat
                  n={counts.workers}
                  label="workers"
                  sub={`${counts.cpuCores} vCPU · ${counts.memGi} GiB`}
                  detail={counts.workerDetail || "no worker pods running"}
                />
                <Stat n={counts.placeholders} label="placeholders" detail={counts.placeholderDetail || undefined} muted />
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
