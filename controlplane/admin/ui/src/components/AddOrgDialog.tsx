// "Add organization" dialog — drives the EXACT warehouse-onboarding API the
// PostHog backend (django) calls: POST /api/v1/orgs/:id/provision
// (controlplane/provisioning/api.go::provisionWarehouse). Same body shape,
// same 202 response. Use it to manually set up an org end to end: it creates
// the org row, its first team row, the root login, and kicks off the async
// warehouse provisioning (bucket + metadata store + DuckLake catalog).
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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
  const [schemaName, setSchemaName] = useState("");
  const [metadataType, setMetadataType] = useState<"cnpg-shard" | "external">("cnpg-shard");
  const [extEndpoint, setExtEndpoint] = useState("");
  const [extSecret, setExtSecret] = useState("");
  const [extUser, setExtUser] = useState("");
  const [extDatabase, setExtDatabase] = useState("");
  const [bucketName, setBucketName] = useState("");
  const [bucketRegion, setBucketRegion] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [result, setResult] = useState<ProvisionWarehouseResult | null>(null);
  const [copied, setCopied] = useState(false);
  const [watch, setWatch] = useState(false);

  const trimmedOrg = orgId.trim();
  const trimmedDb = databaseName.trim();
  const orgProblem = trimmedOrg === "" ? null : orgIdProblem(trimmedOrg);
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

  // Optional post-submit watch of the asynchronous provisioning. Enabled by
  // the "Watch progress" toggle; stops polling at a terminal state.
  const status = useWarehouseStatus(result?.org, {
    refetchInterval: watch ? 5_000 : false,
  });
  useEffect(() => {
    const s = status.data?.state;
    if (s === "ready" || s === "failed") setWatch(false);
  }, [status.data?.state]);

  const reset = () => {
    setOrgId("");
    setDatabaseName("");
    setDbTouched(false);
    setTeamId("");
    setSchemaName("");
    setMetadataType("cnpg-shard");
    setExtEndpoint("");
    setExtSecret("");
    setExtUser("");
    setExtDatabase("");
    setBucketName("");
    setBucketRegion("");
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
    if (trimmedDb === "" || dbTaken) return false;
    if (!teamIdOk) return false;
    if (metadataType === "external" && (extEndpoint.trim() === "" || extSecret.trim() === "")) {
      return false;
    }
    return true;
  }, [pending, result, trimmedOrg, orgProblem, trimmedDb, dbTaken, teamIdOk, metadataType, extEndpoint, extSecret]);

  const submit = async () => {
    setError(null);
    // Built EXACTLY as the PostHog backend's provision call builds it: fields
    // the flow doesn't set are omitted (the server applies its defaults),
    // ducklake is always enabled (a warehouse without a catalog is rejected).
    const body: ProvisionWarehouseBody = {
      database_name: trimmedDb,
      team_id: Number(teamId.trim()),
      metadata_store: { type: metadataType },
      data_store: { type: "s3bucket" },
      ducklake: { enabled: true },
    };
    const schema = schemaName.trim();
    if (schema !== "") body.schema_name = schema;
    if (metadataType === "external") {
      body.metadata_store.external = {
        endpoint: extEndpoint.trim(),
        password_aws_secret: extSecret.trim(),
      };
      if (extUser.trim() !== "") body.metadata_store.external.user = extUser.trim();
      if (extDatabase.trim() !== "") body.metadata_store.external.database = extDatabase.trim();
    }
    if (bucketName.trim() !== "") {
      body.data_store = { type: "external", bucket_name: bucketName.trim() };
      if (bucketRegion.trim() !== "") body.data_store.region = bucketRegion.trim();
    }
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
            Provisions a warehouse through the same onboarding API the PostHog backend uses (
            <span className="font-mono text-xs">POST /api/v1/orgs/:id/provision</span>). Creates the
            org, its first team and the root login, then starts asynchronous provisioning.
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
            {dbTaken && (
              <p className="text-xs text-destructive">
                The database name "{trimmedDb}" is already in use by another org.
              </p>
            )}
            <div className="grid grid-cols-2 gap-3">
              <FieldRow label="Team id" id="add-org-team-id">
                <Input
                  id="add-org-team-id"
                  value={teamId}
                  onChange={(e) => setTeamId(e.target.value)}
                  placeholder="PostHog team id, e.g. 12345"
                  className="font-mono text-xs"
                />
              </FieldRow>
              <FieldRow label="Schema name (optional)" id="add-org-schema-name">
                <Input
                  id="add-org-schema-name"
                  value={schemaName}
                  onChange={(e) => setSchemaName(e.target.value)}
                  placeholder={teamIdOk ? `team_${teamId.trim()} (default)` : "team_<id> (default)"}
                  className="font-mono text-xs"
                />
              </FieldRow>
            </div>
            <p className="text-xs text-muted-foreground">
              The team's warehouse schema. Required because a warehouse cannot exist without a
              team; the id becomes the org's first team row.
            </p>
            <FieldRow label="Metadata store">
              <Select value={metadataType} onValueChange={(v) => setMetadataType(v as typeof metadataType)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="cnpg-shard">cnpg-shard (composition picks the active shard)</SelectItem>
                  <SelectItem value="external">external (existing Postgres / RDS)</SelectItem>
                </SelectContent>
              </Select>
            </FieldRow>
            {metadataType === "external" && (
              <div className="space-y-3 rounded-md border border-border/60 p-3">
                <FieldRow label="Endpoint (host)" id="add-org-ext-endpoint">
                  <Input
                    id="add-org-ext-endpoint"
                    value={extEndpoint}
                    onChange={(e) => setExtEndpoint(e.target.value)}
                    placeholder="db.example.rds.amazonaws.com"
                    className="font-mono text-xs"
                  />
                </FieldRow>
                <FieldRow label="Password AWS secret name" id="add-org-ext-secret">
                  <Input
                    id="add-org-ext-secret"
                    value={extSecret}
                    onChange={(e) => setExtSecret(e.target.value)}
                    placeholder="Secrets Manager secret holding the password"
                    className="font-mono text-xs"
                  />
                </FieldRow>
                <div className="grid grid-cols-2 gap-3">
                  <FieldRow label="User (optional)" id="add-org-ext-user">
                    <Input
                      id="add-org-ext-user"
                      value={extUser}
                      onChange={(e) => setExtUser(e.target.value)}
                      placeholder="postgres (default)"
                      className="font-mono text-xs"
                    />
                  </FieldRow>
                  <FieldRow label="Database (optional)" id="add-org-ext-database">
                    <Input
                      id="add-org-ext-database"
                      value={extDatabase}
                      onChange={(e) => setExtDatabase(e.target.value)}
                      placeholder="postgres (default)"
                      className="font-mono text-xs"
                    />
                  </FieldRow>
                </div>
              </div>
            )}
            <FieldRow label="Existing S3 bucket (optional)" id="add-org-bucket-name">
              <div className="grid grid-cols-2 gap-3">
                <Input
                  id="add-org-bucket-name"
                  value={bucketName}
                  onChange={(e) => setBucketName(e.target.value)}
                  placeholder="Provision a fresh bucket (default)"
                  className="font-mono text-xs"
                />
                <Input
                  value={bucketRegion}
                  onChange={(e) => setBucketRegion(e.target.value)}
                  placeholder="Region (optional)"
                  className="font-mono text-xs"
                  disabled={bucketName.trim() === ""}
                />
              </div>
            </FieldRow>
            <p className="text-xs text-muted-foreground">
              DuckLake is always enabled — the API rejects a warehouse without a catalog.
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
