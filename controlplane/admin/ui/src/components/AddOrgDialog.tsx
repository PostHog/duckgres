// "Add organization" dialog — drives the EXACT warehouse-onboarding API the
// PostHog backend (django) calls: POST /api/v1/orgs/:id/provision
// (controlplane/provisioning/api.go::provisionWarehouse). Same body shape,
// same 202 response, same outcome: it creates the org row, its first team row
// (schema team_<id>, enabled immediately — exactly as PostHog-side onboarding
// lands them), the root login, and kicks off the async warehouse provisioning
// (cnpg-shard metadata + fresh per-org bucket + DuckLake catalog).
//
// The form is deliberately just the two ids: every other field the API accepts
// (schema override, external metadata store, existing bucket) is what the
// normal onboarding flow itself does NOT set, so the dialog sends the defaults
// verbatim — that is what makes the result identical to a django-provisioned
// org. Special-case setups still go through the API directly.
//
// The response carries the generated root password in cleartext — that is the
// ONLY time it is ever served (only the bcrypt hash is persisted), so the
// success panel shows it once with a copy button.
import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, Copy, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { StateBadge } from "@/components/StateBadge";
import { api } from "@/lib/api";
import { databaseNameProblem } from "@/lib/databaseName";
import { warehouseNeedsPolling } from "@/lib/warehouseLifecycle";
import { useDatabaseNameAvailable, useWarehouseStatus } from "@/hooks/useApi";
import type { ProvisionWarehouseBody, ProvisionWarehouseResult } from "@/types/api";

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function FieldRow({
  label,
  id,
  children,
}: {
  label: string;
  // Associates the label with the wrapped control (getByLabelText); optional
  // because the Label renders fine standalone for read-only rows.
  id?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1">
      <Label htmlFor={id}>{label}</Label>
      {children}
    </div>
  );
}

// Mirrors validateDucklingOrgID in controlplane/provisioning/api.go: a single
// DNS-1123 label, and either a canonical UUID or a slug of at most 35 chars
// (bounded by the derived S3 bucket name). Client-side symmetry only — the
// server is authoritative.
const ORG_ID_PATTERN = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MAX_SLUG_LEN = 35;

function orgIdProblem(id: string): string | null {
  if (!ORG_ID_PATTERN.test(id)) {
    return "Lowercase letters, digits and hyphens; must start and end alphanumeric (DNS-1123 label).";
  }
  if (!UUID_PATTERN.test(id) && id.length > MAX_SLUG_LEN) {
    return `Must be a canonical UUID or a slug of at most ${MAX_SLUG_LEN} characters.`;
  }
  return null;
}

export function AddOrgDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const navigate = useNavigate();
  const qc = useQueryClient();
  const [orgId, setOrgId] = useState("");
  const [databaseName, setDatabaseName] = useState("");
  const [teamId, setTeamId] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [result, setResult] = useState<ProvisionWarehouseResult | null>(null);
  const [copied, setCopied] = useState(false);
  const [watch, setWatch] = useState(false);

  const trimmedOrg = orgId.trim();
  const trimmedDb = databaseName.trim();
  const orgProblem = trimmedOrg === "" ? null : orgIdProblem(trimmedOrg);
  const dbProblem = trimmedDb === "" ? null : databaseNameProblem(trimmedDb);
  const teamIdOk = /^\d+$/.test(teamId.trim()) && Number(teamId.trim()) > 0;

  // Prefill the database name with the org id (the django flow does the same)
  // until the operator edits it by hand.
  const [dbTouched, setDbTouched] = useState(false);
  useEffect(() => {
    if (!dbTouched) setDatabaseName(orgId);
  }, [orgId, dbTouched]);

  // Live database-name availability probe (the same endpoint the onboarding
  // flow offers for pre-validation).
  const dbLookupEnabled = trimmedDb !== "" && result === null;
  const dbCheck = useDatabaseNameAvailable(trimmedDb, dbLookupEnabled);
  const dbTaken = dbCheck.data && !dbCheck.data.available;

  // Optional post-submit watch of asynchronous provisioning. Failed is an
  // observed state, not terminal: the backend can return to Ready after an
  // externally repaired Duckling recovers, so keep polling it.
  const status = useWarehouseStatus(result?.org, {
    refetchInterval: watch ? 5_000 : false,
  });
  useEffect(() => {
    const s = status.data?.state;
    if (s && !warehouseNeedsPolling(s)) setWatch(false);
  }, [status.data?.state]);

  const reset = () => {
    setOrgId("");
    setDatabaseName("");
    setDbTouched(false);
    setTeamId("");
    setError(null);
    setPending(false);
    setResult(null);
    setCopied(false);
    setWatch(false);
  };

  const close = () => {
    reset();
    onClose();
  };

  const canSubmit = useMemo(() => {
    if (pending || result) return false;
    if (trimmedOrg === "" || orgProblem) return false;
    if (trimmedDb === "" || dbProblem || dbTaken) return false;
    return teamIdOk;
  }, [pending, result, trimmedOrg, orgProblem, trimmedDb, dbProblem, dbTaken, teamIdOk]);

  const submit = async () => {
    setError(null);
    // Built EXACTLY as the PostHog backend's provision call builds it: only
    // the fields the normal flow sets — no schema override, no external
    // stores. Everything else takes the server default, which is the whole
    // point: the resulting org is indistinguishable from a django-onboarded
    // one (team schema team_<id>, team enabled, cnpg-shard, fresh bucket).
    const body: ProvisionWarehouseBody = {
      database_name: trimmedDb,
      team_id: Number(teamId.trim()),
      metadata_store: { type: "cnpg-shard" },
      data_store: { type: "s3bucket" },
      ducklake: { enabled: true },
    };
    setPending(true);
    try {
      const resp = await api.provisionWarehouse(trimmedOrg, body);
      setResult(resp);
      setWatch(true);
      // Refresh the tenant views imperatively: this component deliberately
      // avoids the mutation hook so the one-time password payload never sits
      // in the query cache.
      qc.invalidateQueries({ queryKey: ["orgs"] });
      qc.invalidateQueries({ queryKey: ["warehouse-status", trimmedOrg] });
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setPending(false);
    }
  };

  const copyPassword = async () => {
    if (!result) return;
    try {
      await navigator.clipboard.writeText(result.password);
      setCopied(true);
      setTimeout(() => setCopied(false), 2_000);
    } catch {
      // Clipboard unavailable (non-secure context) — the password stays
      // selectable in the field for manual copying.
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => !o && close()}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>Add organization</DialogTitle>
          <DialogDescription>
            Provisions an org through the same onboarding API the PostHog backend uses (
            <span className="font-mono text-xs">POST /api/v1/orgs/:id/provision</span>), with the
            same defaults: cnpg-shard metadata, a fresh S3 bucket, and the team's warehouse schema
            at <span className="font-mono text-xs">team_&lt;id&gt;</span>. The team lands enabled
            immediately — exactly as PostHog-side onboarding enables them.
          </DialogDescription>
        </DialogHeader>

        {result ? (
          <div className="space-y-3">
            <p className="flex items-start gap-2 text-xs text-warning">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>
                Provisioning started for <span className="font-mono font-medium">{result.org}</span>.
                Save the root credentials now — the password is never shown or retrievable again.
              </span>
            </p>
            <FieldRow label="Username">
              <Input readOnly value={result.username} className="font-mono text-xs" />
            </FieldRow>
            <FieldRow label="Password (shown once)">
              <div className="flex gap-2">
                <Input readOnly value={result.password} className="font-mono text-xs" />
                <Button type="button" variant="outline" size="sm" onClick={copyPassword}>
                  {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
                </Button>
              </div>
            </FieldRow>
            {result.bucket && (
              <FieldRow label="S3 bucket">
                <Input readOnly value={result.bucket} className="font-mono text-xs" />
              </FieldRow>
            )}
            <div className="flex items-center gap-2 rounded-md border border-border/60 bg-background/40 px-3 py-2">
              <span className="text-xs text-muted-foreground">Warehouse status:</span>
              {status.data ? (
                <>
                  <StateBadge state={status.data.state} />
                  {status.data.state !== "ready" && status.data.state !== "failed" && watch && (
                    <RefreshCw className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                  )}
                  {status.data.status_message && (
                    <span className="truncate text-xs text-muted-foreground" title={status.data.status_message}>
                      {status.data.status_message}
                    </span>
                  )}
                </>
              ) : status.isError ? (
                <span className="text-xs text-muted-foreground">status unavailable ({errMsg(status.error)})</span>
              ) : (
                <span className="text-xs text-muted-foreground">provisioning…</span>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={reset}>
                Add another
              </Button>
              <Button
                size="sm"
                onClick={() => {
                  const dest = `/orgs/${encodeURIComponent(result.org)}`;
                  close();
                  navigate(dest);
                }}
              >
                Open organization
              </Button>
            </DialogFooter>
          </div>
        ) : (
          <div className="space-y-3">
            <FieldRow label="Org id" id="add-org-id">
              <Input
                id="add-org-id"
                value={orgId}
                onChange={(e) => setOrgId(e.target.value)}
                placeholder="PostHog organization UUID or slug"
                className="font-mono text-xs"
                autoFocus
              />
            </FieldRow>
            {orgProblem && <p className="text-xs text-destructive">{orgProblem}</p>}
            <FieldRow label="Database name" id="add-org-database-name">
              <Input
                id="add-org-database-name"
                value={databaseName}
                onChange={(e) => {
                  setDbTouched(true);
                  setDatabaseName(e.target.value);
                }}
                placeholder="Usually the org id"
                className="font-mono text-xs"
              />
            </FieldRow>
            {dbProblem && <p className="text-xs text-destructive">{dbProblem}</p>}
            {!dbProblem && dbTaken && (
              <p className="text-xs text-destructive">
                The database name "{trimmedDb}" is already in use by another org.
              </p>
            )}
            <FieldRow label="Team id" id="add-org-team-id">
              <Input
                id="add-org-team-id"
                value={teamId}
                onChange={(e) => setTeamId(e.target.value)}
                placeholder="PostHog team id, e.g. 12345"
                className="font-mono text-xs"
              />
            </FieldRow>
            <p className="text-xs text-muted-foreground">
              A warehouse cannot exist without a team: the id becomes the org's first team row at
              schema <span className="font-mono">team_{teamIdOk ? teamId.trim() : "<id>"}</span>,
              enabled immediately — the same state django's landing flow produces.
            </p>
            {error && <p className="text-xs text-destructive">{error}</p>}
            <DialogFooter>
              <Button variant="outline" size="sm" onClick={close}>
                Cancel
              </Button>
              <Button size="sm" onClick={submit} disabled={!canSubmit}>
                {pending ? "Provisioning…" : "Provision organization"}
              </Button>
            </DialogFooter>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
