import { Link } from "react-router-dom";
import { AlertTriangle, Sparkles } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { StateBadge } from "@/components/StateBadge";
import { LoadingState } from "@/components/states";
import { CopyButton } from "@/components/CopyButton";
import { useOrgTrino } from "@/hooks/useApi";
import { fmtInt, fmtTime } from "@/lib/format";

function Field({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className={mono ? "font-mono text-xs break-all" : "text-xs"}>{value ?? "—"}</span>
    </div>
  );
}

// OrgTrinoCard surfaces the org's row on duckgres_managed_warehouse_trino.
// Those columns — state, status_message, ready_at, failed_at — have always
// existed and were rendered nowhere, so a failed Trino provision was silent
// until somebody read the table by hand.
export function OrgTrinoCard({ orgId }: { orgId: string }) {
  const trino = useOrgTrino(orgId);

  // A control plane with no Trino cell 404s the endpoint, which the hook
  // turns into enabled:false. Rendering nothing is right in both cases:
  // there is no Trino story to tell for this org.
  if (trino.isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-4 w-4" /> Trino
          </CardTitle>
        </CardHeader>
        <CardContent>
          <LoadingState />
        </CardContent>
      </Card>
    );
  }
  if (!trino.data?.enabled || !trino.data.status) {
    return null;
  }

  const s = trino.data.status;
  const failed = s.state === "failed";

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Sparkles className="h-4 w-4" /> Trino
        </CardTitle>
        <div className="flex items-center gap-2">
          <Badge variant="outline">{s.cell || "unassigned cell"}</Badge>
          <StateBadge state={s.state} />
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {failed && s.status_message && (
          <div className="flex items-start gap-2 rounded border border-destructive/40 bg-destructive/5 p-2 text-xs">
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-destructive" />
            {/* The reconcile detail names the step that broke — the whole
                actionable part of a failed provision. */}
            <span className="break-all">{s.status_message}</span>
          </div>
        )}

        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <Field
            label="Principal"
            mono
            value={
              <span className="inline-flex items-center gap-1">
                {s.principal || "—"}
                {s.principal && <CopyButton value={s.principal} />}
              </span>
            }
          />
          <Field
            label="Catalog"
            mono
            value={
              <span className="inline-flex items-center gap-1">
                {s.catalog || "—"}
                {s.catalog && <CopyButton value={s.catalog} />}
              </span>
            }
          />
          <Field label="Tier" value={<Badge variant="outline">{s.tier || "default"}</Badge>} />
          <Field
            label="Running"
            value={trino.data.available ? fmtInt(s.running_queries) : "—"}
          />
          <Field
            label="Queued"
            value={trino.data.available ? fmtInt(s.queued_queries) : "—"}
          />
          <Field
            label={failed ? "Failed at" : "Ready at"}
            value={failed ? fmtTime(s.failed_at) : s.ready_at ? fmtTime(s.ready_at) : "—"}
          />
        </div>

        <Link
          to={`/trino/queries`}
          className="inline-block text-xs text-primary hover:underline"
        >
          View this cell&apos;s live queries →
        </Link>
      </CardContent>
    </Card>
  );
}
