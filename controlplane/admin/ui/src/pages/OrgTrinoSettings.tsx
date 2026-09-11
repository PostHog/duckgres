import { useState } from "react";
import { useIdentity } from "@/components/IdentityProvider";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useOrgTrino, useSelectTrinoCell, useSetTrinoEnabled, useTrinoCells } from "@/hooks/useApi";

export function OrgTrinoSettings({ orgId, hasWarehouse, tier }: {
  orgId: string;
  hasWarehouse: boolean;
  tier: string;
}) {
  const { isAdmin } = useIdentity();
  const trino = useOrgTrino(orgId);
  const cells = useTrinoCells();
  const selection = useSelectTrinoCell();
  const enablement = useSetTrinoEnabled();
  const [chosen, setChosen] = useState("");
  const [message, setMessage] = useState<{ error: boolean; text: string } | null>(null);
  const busy = selection.isPending || enablement.isPending;
  const assigned = trino.data?.assigned === true;
  const enabled = trino.data?.enabled === true;

  async function selectCell() {
    setMessage(null);
    try {
      await selection.mutateAsync({ org: orgId, cell: chosen });
      setMessage({ error: false, text: "Cell selected. Trino remains disabled; enable it separately when ready." });
    } catch (error) {
      setMessage({ error: true, text: error instanceof Error ? error.message : "Cell selection failed." });
      await trino.refetch();
    }
  }

  async function setEnabled() {
    setMessage(null);
    try {
      await enablement.mutateAsync({ org: orgId, enabled: !enabled, tier });
      setMessage({ error: false, text: enabled ? "Disable requested. Access is removed during reconciliation." : "Enable requested. Provisioning may take a moment." });
    } catch (error) {
      setMessage({ error: true, text: error instanceof Error ? error.message : "Trino update failed." });
    }
  }

  const error = trino.error || cells.error;
  let content;
  if (trino.isLoading || cells.isLoading) {
    content = <p className="text-xs text-muted-foreground">Loading Trino configuration…</p>;
  } else if (error) {
    content = <p role="alert" className="text-xs text-destructive">Cannot read Trino configuration. {error.message}</p>;
  } else if (!cells.data?.cells.length) {
    content = <p className="text-xs text-muted-foreground">Trino is not configured in this environment.</p>;
  } else {
    content = <>
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <Badge variant="outline">{enabled ? "Enabled" : "Disabled"}</Badge>
        <span>Cell: {assigned ? trino.data?.cell.id : "Not selected"}</span>
      </div>
      {assigned ? (
        <p className="text-xs text-muted-foreground">The assigned cell cannot change here, including while Trino is disabled.</p>
      ) : (
        <p className="text-xs text-muted-foreground">Select an initial cell before enabling Trino. Selection is permanent here and does not enable Trino.</p>
      )}
      {!hasWarehouse && <p className="text-xs text-muted-foreground">Provision a warehouse before selecting a cell or enabling Trino.</p>}
      {isAdmin && <div className="flex flex-wrap items-center gap-2">
        {!assigned && !enabled && <>
          <select aria-label="Initial Trino cell" value={chosen} disabled={busy || !hasWarehouse}
            onChange={(event) => setChosen(event.target.value)} className="rounded border bg-background p-2 text-sm">
            <option value="">Choose a cell</option>
            {cells.data.cells.map((cell) => <option key={cell.id} value={cell.id}>{cell.id}</option>)}
          </select>
          <Button size="sm" disabled={busy || !hasWarehouse || !cells.data.cells.some((cell) => cell.id === chosen)} onClick={selectCell}>
            {selection.isPending ? "Selecting…" : "Select cell"}
          </Button>
        </>}
        <Button size="sm" variant={enabled ? "outline" : "default"}
          disabled={busy || (!enabled && (!assigned || !hasWarehouse))} onClick={setEnabled}>
          {enablement.isPending ? "Updating Trino…" : enabled ? "Disable Trino" : "Enable Trino"}
        </Button>
      </div>}
      <p className="text-xs text-muted-foreground">Trino actions apply separately from Save changes. Disabling does not move the warehouse or establish a maintenance barrier.</p>
    </>;
  }

  return <section aria-label="Trino configuration" className="space-y-2 border-b pb-4">
    <h3 className="text-sm font-medium">Trino configuration</h3>
    {content}
    {message && <p role={message.error ? "alert" : "status"} className={`text-xs ${message.error ? "text-destructive" : "text-muted-foreground"}`}>{message.text}</p>}
  </section>;
}
