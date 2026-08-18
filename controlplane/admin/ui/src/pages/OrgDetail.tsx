import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { AlertTriangle, ArrowLeft, Database, Layers, Pencil, Plus, Save, Trash2, Warehouse } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { StateBadge } from "@/components/StateBadge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { AdminGate } from "@/components/AdminOnly";
import { JsonValue } from "@/components/JsonView";
import { ErrorState, LoadingState } from "@/components/states";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ApiError } from "@/lib/api";
import { databaseNameProblem } from "@/lib/databaseName";
import { ducklingBroken, ducklingEntryFor, fmtTime, orgLabel } from "@/lib/format";
import { CopyButton } from "@/components/CopyButton";
import { ShardBadge } from "@/components/ShardBadge";
import { OrgUsageSection } from "@/pages/OrgUsage";
import {
  useDatabaseNameAvailable,
  useDeleteOrg,
  useDeprovisionWarehouse,
  useDucklingsMetadata,
  useOrg,
  useOrgReshards,
  useOrgTeams,
  useUpdateOrg,
  useUpdateWarehouse,
  useWarehouse,
} from "@/hooks/useApi";
import {
  BackfillBadge,
  CreateTeamDialog,
  DeleteTeamDialog,
  EarliestEventDateCell,
  EditTeamDialog,
  LegacyNamesBadge,
} from "@/components/OrgTeamDialogs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import type { DataImportsTableNamingVersion, ManagedWarehouse, Org, OrgTeam, OrgUpdate } from "@/types/api";

interface FormState {
  database_name: string;
  max_workers: string;
  max_vcpus: string;
  default_worker_cpu: string;
  default_worker_memory: string;
  default_worker_ttl: string;
  default_worker_min_hot_idle: string;
  hostname_alias: string;
  data_imports_table_naming_version: DataImportsTableNamingVersion;
}

function orgToForm(o: {
  database_name: string;
  max_workers: number;
  max_vcpus: number;
  default_worker_cpu: string;
  default_worker_memory: string;
  default_worker_ttl: string;
  default_worker_min_hot_idle: number;
  hostname_alias: string | null;
  data_imports_table_naming_version: DataImportsTableNamingVersion;
}): FormState {
  return {
    database_name: o.database_name,
    max_workers: String(o.max_workers),
    max_vcpus: String(o.max_vcpus),
    default_worker_cpu: o.default_worker_cpu,
    default_worker_memory: o.default_worker_memory,
    default_worker_ttl: o.default_worker_ttl,
    default_worker_min_hot_idle: String(o.default_worker_min_hot_idle),
    hostname_alias: o.hostname_alias ?? "",
    data_imports_table_naming_version: o.data_imports_table_naming_version,
  };
}

export function OrgDetail() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const org = useOrg(id);
  const warehouse = useWarehouse(id);
  const update = useUpdateOrg(id);
  const del = useDeleteOrg();

  const [form, setForm] = useState<FormState | null>(null);
  const [msg, setMsg] = useState<{ kind: "ok" | "err"; text: string } | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");

  useEffect(() => {
    if (org.data) setForm(orgToForm(org.data));
  }, [org.data]);

  // Client-side mirror of the server's DNS-label rule (configstore
  // .ValidateDatabaseName) so a typo is a red border, not a round-trip 400.
  // Crucially the save gate only applies when the operator actually CHANGED
  // the name: orgs whose stored value predates the rule (the premise of this
  // break-glass surface) must keep every OTHER org setting editable without
  // forcing a rename, and database_name is only sent when it differs (save()
  // below), so an unchanged grandfathered value never blocks saving.
  // (Derived BEFORE the early returns below: the availability hook must run
  // on every render, unconditionally.)
  const trimmedDb = form ? form.database_name.trim() : "";
  const dbChanged = form !== null && trimmedDb !== org.data?.database_name;
  const dbProblem =
    form === null || !dbChanged ? null : trimmedDb === "" ? "Database name is required." : databaseNameProblem(trimmedDb);
  // Availability probe for the rename target: renaming onto another org's
  // name is the one failure an operator can hit at save time, so flag it
  // before the round-trip (the server still 409s authoritatively).
  const dbCheck = useDatabaseNameAvailable(trimmedDb, dbChanged && dbProblem === null);
  const dbTaken = Boolean(dbChanged && dbProblem === null && dbCheck.data && !dbCheck.data.available);

  if (org.isLoading || !form) {
    return (
      <>
        <Header id={id} org={org.data} />
        <PageBody>{org.isError ? <ErrorState error={org.error} /> : <LoadingState />}</PageBody>
      </>
    );
  }
  if (org.isError) {
    return (
      <>
        <Header id={id} org={org.data} />
        <PageBody>
          <ErrorState error={org.error} onRetry={() => org.refetch()} />
        </PageBody>
      </>
    );
  }

  const set = (k: keyof FormState, v: string) => setForm((f) => (f ? { ...f, [k]: v } : f));

  const save = async () => {
    setMsg(null);
    const trimmedDb = form.database_name.trim();
    const body: OrgUpdate = {
      max_workers: Number(form.max_workers) || 0,
      max_vcpus: Number(form.max_vcpus) || 0,
      default_worker_cpu: form.default_worker_cpu,
      default_worker_memory: form.default_worker_memory,
      default_worker_ttl: form.default_worker_ttl,
      default_worker_min_hot_idle: Number(form.default_worker_min_hot_idle) || 0,
      hostname_alias: form.hostname_alias === "" ? "" : form.hostname_alias,
      data_imports_table_naming_version: form.data_imports_table_naming_version,
    };
    // Only send database_name when it actually changed — renaming it also
    // renames the org's managed hostname, so an untouched form never risks a
    // spurious rename. The change reason: orgs whose stored name predates the
    // DNS-label rule are unroutable (<name>.<suffix> isn't a valid hostname);
    // this is the operator surface that fixes them without a SQL round-trip.
    if (trimmedDb !== org.data?.database_name) {
      body.database_name = trimmedDb;
    }
    try {
      await update.mutateAsync(body);
      setMsg({ kind: "ok", text: "Saved." });
    } catch (e) {
      setMsg({ kind: "err", text: e instanceof Error ? e.message : "Save failed" });
    }
  };

  // Org deletion is blocked while a managed warehouse is still LIVE: the
  // correct flow is deprovision → provisioner tears down the duckling → then
  // delete. Deprovisioning does not remove the warehouse row — it parks it in
  // the terminal "deleted" state (infra gone), which must NOT block deletion
  // or a fully deprovisioned org becomes undeletable. The backend applies the
  // same state<>deleted rule and sweeps the dead row; this is belt-and-suspenders.
  const liveWarehouse = (w?: { state?: string } | null) => Boolean(w) && w?.state !== "deleted";
  const orgHasWarehouse = liveWarehouse(org.data?.warehouse) || liveWarehouse(warehouse.data);

  const closeDelete = () => {
    setConfirmDelete(false);
    setDeleteConfirmText("");
  };

  const doDelete = async () => {
    try {
      await del.mutateAsync(id);
      navigate("/orgs");
    } catch (e) {
      setMsg({ kind: "err", text: e instanceof Error ? e.message : "Delete failed" });
      closeDelete();
    }
  };

  return (
    <>
      <Header
        id={id}
        org={org.data}
        actions={
          <AdminGate>
            {orgHasWarehouse ? (
              // Disabled buttons swallow pointer events, so the tooltip
              // triggers on a wrapping span. delayDuration 0 = immediate.
              <Tooltip delayDuration={0}>
                <TooltipTrigger asChild>
                  <span tabIndex={0}>
                    <Button variant="destructive" size="sm" disabled className="pointer-events-none">
                      <Trash2 className="h-4 w-4" /> Delete org
                    </Button>
                  </span>
                </TooltipTrigger>
                <TooltipContent>
                  Deprovision the warehouse first — org delete is blocked while it exists.
                </TooltipContent>
              </Tooltip>
            ) : (
              <Button variant="destructive" size="sm" onClick={() => setConfirmDelete(true)}>
                <Trash2 className="h-4 w-4" /> Delete org
              </Button>
            )}
          </AdminGate>
        }
      />
      <PageBody>
        <div className="grid gap-4 lg:grid-cols-2">
          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle>Org configuration</CardTitle>
              <span className="text-xs text-muted-foreground">
                updated {fmtTime(org.data?.updated_at)}
              </span>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <Field label="Max workers (0 = unbounded)">
                  <Input type="number" value={form.max_workers} onChange={(e) => set("max_workers", e.target.value)} />
                </Field>
                <Field label="Max vCPUs (0 = unbounded)">
                  <Input
                    type="number"
                    min={0}
                    value={form.max_vcpus}
                    onChange={(e) => set("max_vcpus", e.target.value)}
                  />
                </Field>
                <Field label="Default worker CPU">
                  <Input
                    value={form.default_worker_cpu}
                    placeholder='e.g. "2"'
                    onChange={(e) => set("default_worker_cpu", e.target.value)}
                  />
                </Field>
                <Field label="Default worker memory">
                  <Input
                    value={form.default_worker_memory}
                    placeholder='e.g. "8Gi"'
                    onChange={(e) => set("default_worker_memory", e.target.value)}
                  />
                </Field>
                <Field label="Default worker TTL">
                  <Input
                    value={form.default_worker_ttl}
                    placeholder='e.g. "75m"'
                    onChange={(e) => set("default_worker_ttl", e.target.value)}
                  />
                </Field>
                <Field label="Default min hot-idle">
                  <Input
                    type="number"
                    value={form.default_worker_min_hot_idle}
                    onChange={(e) => set("default_worker_min_hot_idle", e.target.value)}
                  />
                </Field>
              </div>
              <Field label="Database name (also the hostname label)">
                <Input
                  aria-label="Database name"
                  value={form.database_name}
                  placeholder="single DNS label, e.g. acme-prod"
                  className={`font-mono text-xs ${dbProblem ? "border-destructive" : ""}`}
                  onChange={(e) => set("database_name", e.target.value)}
                />
              </Field>
              {dbProblem ? (
                <p className="text-xs text-destructive">{dbProblem}</p>
              ) : dbTaken ? (
                <p className="text-xs text-destructive">
                  The database name "{trimmedDb}" is already in use by another org.
                </p>
              ) : (
                dbChanged && (
                  <p className="flex items-start gap-2 text-xs text-warning">
                    <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                    <span>
                      The database name is this org's hostname — clients reach it at
                      <span className="font-mono"> {trimmedDb || "<name>"}.&lt;managed-suffix&gt;</span> and
                      connect with dbname={trimmedDb || "<name>"}. Renaming moves the hostname and the
                      dbname immediately; fix invalid stored names here, and coordinate the rename with
                      the tenants' connection settings.
                    </span>
                  </p>
                )
              )}
              <Field label="Hostname alias (empty clears)">
                <Input
                  value={form.hostname_alias}
                  placeholder="single DNS label, e.g. acme"
                  onChange={(e) => set("hostname_alias", e.target.value)}
                />
              </Field>
              <Field label="Data import table naming">
                <Select
                  value={form.data_imports_table_naming_version}
                  onValueChange={(value) => {
                    if (value === "legacy_batch_v1" || value === "copy_v1") {
                      setForm((current) =>
                        current ? { ...current, data_imports_table_naming_version: value } : current,
                      );
                    }
                  }}
                >
                  <SelectTrigger aria-label="Data import table naming">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="legacy_batch_v1">Legacy batch (legacy_batch_v1)</SelectItem>
                    <SelectItem value="copy_v1">Copy workflow (copy_v1)</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              {form.data_imports_table_naming_version !== org.data?.data_imports_table_naming_version && (
                <p className="flex items-start gap-2 text-xs text-warning">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>
                    Changing the naming version does not rename or move existing data. Migrate the
                    existing tables before saving this change.
                  </span>
                </p>
              )}
              <div className="flex items-center gap-3 pt-1">
                <AdminGate>
                  <Button size="sm" onClick={save} disabled={update.isPending || dbProblem !== null || dbTaken}>
                    <Save className="h-4 w-4" /> {update.isPending ? "Saving…" : "Save changes"}
                  </Button>
                </AdminGate>
                {msg && (
                  <span className={msg.kind === "ok" ? "text-xs text-success" : "text-xs text-destructive"}>
                    {msg.text}
                  </span>
                )}
              </div>
            </CardContent>
          </Card>

          <WarehousePanel orgId={id} data={warehouse.data ?? null} loading={warehouse.isLoading} error={warehouse.error} />
        </div>
        <OrgUsageSection orgId={id} />
        <OrgTeamsCard orgId={id} />
      </PageBody>

      <Dialog open={confirmDelete} onOpenChange={(open) => (open ? setConfirmDelete(true) : closeDelete())}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete org "{id}"?</DialogTitle>
            <DialogDescription>
              This removes the org and all of its users from the config store. This cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            {orgHasWarehouse && (
              <p className="flex items-start gap-2 text-xs text-warning">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <span>
                  This org still has a managed warehouse. Deletion is blocked until the warehouse is
                  deprovisioned and fully gone — deprovision it from the warehouse panel first.
                </span>
              </p>
            )}
            <Field label="Type the org id to confirm">
              <Input
                value={deleteConfirmText}
                onChange={(e) => setDeleteConfirmText(e.target.value)}
                placeholder={id}
                className="font-mono text-xs"
              />
            </Field>
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={closeDelete}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              onClick={doDelete}
              disabled={del.isPending || orgHasWarehouse || deleteConfirmText.trim() !== id}
            >
              {del.isPending ? "Deleting…" : "Delete"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function Header({ id, org, actions }: { id: string; org?: Org; actions?: React.ReactNode }) {
  // Headline leads with the human-readable name (database_name, else alias)
  // — that is what an operator recognizes at a glance ("Posthog") — and drops
  // the opaque org id (a UUID for most tenants) to a subline with a copy
  // button for when it is actually needed (config store, API, k8s labels).
  const label = org ? orgLabel(org) : id;
  return (
    <PageHeader
      title={
        <span className="flex items-center gap-2">
          <Link to="/orgs" className="text-muted-foreground hover:text-foreground">
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <span className="flex min-w-0 flex-col">
            <span className="truncate font-medium">{label}</span>
            <span className="flex items-center gap-1.5">
              <span className="truncate font-mono text-xs font-normal text-muted-foreground" title={id}>
                {id}
              </span>
              <CopyButton value={id} />
            </span>
          </span>
        </span>
      }
      description="Per-org configuration and managed warehouse."
      actions={actions}
    />
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <Label>{label}</Label>
      {children}
    </div>
  );
}

const STATE_FIELDS: { key: keyof ManagedWarehouse; label: string }[] = [
  { key: "state", label: "Overall" },
  { key: "metadata_store_state", label: "Metadata store" },
  { key: "s3_state", label: "S3" },
  { key: "identity_state", label: "Identity" },
  { key: "secrets_state", label: "Secrets" },
];

function WarehousePanel({
  orgId,
  data,
  loading,
  error,
}: {
  orgId: string;
  data: ManagedWarehouse | null;
  loading: boolean;
  error: unknown;
}) {
  const update = useUpdateWarehouse(orgId);
  const deprovision = useDeprovisionWarehouse(orgId);
  const metadata = useDucklingsMetadata();
  const [image, setImage] = useState("");
  const [version, setVersion] = useState("");
  const [metadataProxyEnabled, setMetadataProxyEnabled] = useState(false);
  const [ducklingNameInput, setDucklingNameInput] = useState("");
  const [confirmDeprovision, setConfirmDeprovision] = useState(false);
  const [deprovisionConfirmText, setDeprovisionConfirmText] = useState("");
  const [msg, setMsg] = useState<{ kind: "ok" | "err"; text: string } | null>(null);

  useEffect(() => {
    if (data) {
      setImage(data.image ?? "");
      setVersion(data.ducklake_version ?? "");
      setMetadataProxyEnabled(data.metadata_proxy_enabled ?? false);
      setDucklingNameInput(data.duckling_name ?? "");
    }
  }, [data]);

  const notFound = error instanceof ApiError && error.status === 404;
  const missing = notFound || !data;
  const broken = !missing && ducklingBroken(data?.state);
  const ducklingWarning = loading
    ? null
    : missing
      ? "No duckling provisioned for this org"
      : broken
        ? `Managed warehouse not ready (state: ${data?.state})`
        : null;

  const ducklingNameEmpty = ducklingNameInput.trim() === "";

  const save = async () => {
    setMsg(null);
    if (ducklingNameEmpty) {
      setMsg({ kind: "err", text: "Duckling name is required." });
      return;
    }
    // Send only fields that actually changed: the PUT is a merge-patch and
    // the audit log records the body's keys as "changed", so carrying
    // untouched fields would log phantom changes.
    const body: Partial<ManagedWarehouse> = {};
    if (image !== (data?.image ?? "")) body.image = image;
    if (version !== (data?.ducklake_version ?? "")) body.ducklake_version = version;
    if (metadataProxyEnabled !== (data?.metadata_proxy_enabled ?? false)) {
      body.metadata_proxy_enabled = metadataProxyEnabled;
    }
    if (ducklingNameInput !== (data?.duckling_name ?? "")) {
      body.duckling_name = ducklingNameInput;
    }
    if (Object.keys(body).length === 0) {
      setMsg({ kind: "ok", text: "No changes." });
      return;
    }
    try {
      await update.mutateAsync(body);
      setMsg({ kind: "ok", text: "Saved." });
    } catch (e) {
      setMsg({ kind: "err", text: e instanceof Error ? e.message : "Save failed" });
    }
  };

  // Already deleting/deleted (or no warehouse at all) → nothing to deprovision.
  const canDeprovision = !missing && data != null && data.state !== "deleting" && data.state !== "deleted";

  const closeDeprovision = () => {
    setConfirmDeprovision(false);
    setDeprovisionConfirmText("");
  };

  const doDeprovision = async () => {
    setMsg(null);
    try {
      await deprovision.mutateAsync();
      setMsg({ kind: "ok", text: "Deprovisioning started." });
    } catch (e) {
      // A 409 (wrong warehouse state) surfaces its backend message as-is.
      setMsg({ kind: "err", text: e instanceof Error ? e.message : "Deprovision failed" });
    }
    closeDeprovision();
  };

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Warehouse className="h-4 w-4" /> Managed warehouse
        </CardTitle>
        {data && <StateBadge state={data.state} />}
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-center justify-between rounded-md border border-border bg-background/40 px-3 py-2">
          <div>
            <div className="text-[10px] uppercase text-muted-foreground">Duckling</div>
            <div className="font-mono text-xs">{data?.duckling_name ?? "—"}</div>
          </div>
          {/* Live metadata-store assignment from the Duckling CR status — the
              cnpg shard the tenant's metadata actually lives on (the config
              store doesn't hold this; the composition assigns it). */}
          <div className="text-right">
            <div className="text-[10px] uppercase text-muted-foreground">Metadata shard</div>
            <ShardBadge meta={ducklingEntryFor(metadata.data?.entries, orgId, data?.duckling_name)} />
          </div>
          {ducklingWarning && (
            <Tooltip>
              <TooltipTrigger asChild>
                <AlertTriangle className="h-4 w-4 text-warning" />
              </TooltipTrigger>
              <TooltipContent>{ducklingWarning}</TooltipContent>
            </Tooltip>
          )}
        </div>
        {loading ? (
          <LoadingState />
        ) : notFound || !data ? (
          <p className="py-4 text-center text-sm text-muted-foreground">
            No managed warehouse provisioned for this org.
          </p>
        ) : (
          <>
            {/* Read-only provisioning states */}
            <div>
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Provisioning state (read-only)
              </p>
              <div className="grid grid-cols-3 gap-2">
                {STATE_FIELDS.map((f) => (
                  <div key={String(f.key)} className="rounded-md border border-border bg-background/40 p-2">
                    <div className="mb-1 text-[10px] uppercase text-muted-foreground">{f.label}</div>
                    <StateBadge state={data[f.key] as string} />
                  </div>
                ))}
              </div>
              {data.state === "failed" ? (
                <div
                  role="alert"
                  className="mt-3 flex gap-3 rounded-md border border-warning/40 bg-warning/10 p-3 text-warning"
                >
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                  <div className="space-y-1 text-xs">
                    <p className="font-medium">Warehouse is not operationally ready</p>
                    <p>
                      <span className="font-medium">Current blocker</span>
                    </p>
                    <p className="font-mono text-foreground">
                      {data.status_message || "No current blocker detail is available yet."}
                    </p>
                    <p className="text-muted-foreground">
                      Component badges reflect Duckling infrastructure. Overall readiness also requires a successful
                      end-to-end metadata-store connection check. Recovery is checked automatically; this page updates
                      when dependencies recover.
                    </p>
                  </div>
                </div>
              ) : data.status_message ? (
                <p className="mt-2 font-mono text-xs text-muted-foreground">{data.status_message}</p>
              ) : null}
              <div className="mt-2 flex gap-4 text-[11px] text-muted-foreground">
                <span>
                  <span className="font-medium">Last ready</span>: {data.ready_at ? fmtTime(data.ready_at) : "—"}
                </span>
                <span>
                  <span className="font-medium">Last failed</span>: {data.failed_at ? fmtTime(data.failed_at) : "—"}
                </span>
              </div>
            </div>

            {/* Editable pinning */}
            <div className="space-y-3 border-t border-border pt-3">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Pinning</p>
              <Field label="Duckling name">
                <Input
                  value={ducklingNameInput}
                  onChange={(e) => setDucklingNameInput(e.target.value)}
                  className="font-mono text-xs"
                />
              </Field>
              <Field label="Worker image">
                <Input value={image} onChange={(e) => setImage(e.target.value)} className="font-mono text-xs" />
              </Field>
              <Field label="DuckLake spec version">
                <Input
                  value={version}
                  onChange={(e) => setVersion(e.target.value)}
                  placeholder='e.g. "0.4"'
                  className="font-mono text-xs"
                />
              </Field>
              <div className="flex items-center justify-between gap-4 rounded-md border border-border bg-background/40 px-3 py-2">
                <div className="space-y-1">
                  <Label htmlFor="metadata-proxy-enabled">Public metadata Postgres</Label>
                  <p className="text-xs text-muted-foreground">
                    Initial scope: dedicated, single-customer CNPG shards only. Do not enable this
                    for a shared shard until upstream CONNECT and role hardening lands. Clients use
                    the org&apos;s root credential and exact dbname=metadata, with full metadata
                    database access.
                  </p>
                </div>
                <Switch
                  id="metadata-proxy-enabled"
                  checked={metadataProxyEnabled}
                  onCheckedChange={setMetadataProxyEnabled}
                />
              </div>
              <div className="flex items-center gap-3">
                <AdminGate>
                  <Button size="sm" onClick={save} disabled={update.isPending || ducklingNameEmpty}>
                    <Save className="h-4 w-4" /> {update.isPending ? "Saving…" : "Save warehouse"}
                  </Button>
                </AdminGate>
                {msg && (
                  <span className={msg.kind === "ok" ? "text-xs text-success" : "text-xs text-destructive"}>
                    {msg.text}
                  </span>
                )}
              </div>
            </div>

            {/* Sub-config (read) */}
            <div className="space-y-2 border-t border-border pt-3">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Configuration</p>
              <ReadRow label="metadata_store" value={data.metadata_store} />
              <ReadRow label="s3" value={data.s3} />
              <ReadRow label="worker_identity" value={data.worker_identity} />
              <div className="flex flex-wrap gap-2 pt-1">
                <Badge variant={data.pgbouncer?.enabled ? "success" : "muted"}>
                  pgbouncer {data.pgbouncer?.enabled ? "on" : "off"}
                </Badge>
                <Badge variant={data.ducklake?.enabled ? "success" : "muted"}>
                  ducklake {data.ducklake?.enabled ? "on" : "off"}
                </Badge>
              </div>
            </div>

            {/* Teardown + reshard */}
            {canDeprovision && (
              <div className="space-y-2 border-t border-border pt-3">
                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Danger zone</p>
                <div className="flex items-center gap-3">
                  <AdminGate>
                    <Button variant="destructive" size="sm" onClick={() => setConfirmDeprovision(true)}>
                      <Trash2 className="h-4 w-4" /> Deprovision warehouse
                    </Button>
                  </AdminGate>
                  <span className="text-xs text-muted-foreground">
                    Required before the org can be deleted.
                  </span>
                </div>
                {data.state === "ready" && (
                  <div className="flex items-center gap-3">
                    <AdminGate>
                      <Button variant="outline" size="sm" asChild>
                        <Link to={`/orgs/${encodeURIComponent(orgId)}/reshard`}>
                          <Database className="h-4 w-4" /> Reshard metadata store…
                        </Link>
                      </Button>
                    </AdminGate>
                    <span className="text-xs text-muted-foreground">
                      Move the DuckLake catalog to another cnpg shard or an external Postgres.
                    </span>
                  </div>
                )}
              </div>
            )}

            <ReshardHistory orgId={orgId} />
          </>
        )}
      </CardContent>

      <Dialog open={confirmDeprovision} onOpenChange={(open) => (open ? setConfirmDeprovision(true) : closeDeprovision())}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Deprovision warehouse for "{orgId}"?</DialogTitle>
            <DialogDescription>
              This permanently tears down the org's duckling — the Duckling CR, the S3 data bucket, the
              metadata database, and the IAM role. Teardown runs asynchronously; the org itself is not
              deleted. This cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <Field label="Type the org id to confirm">
            <Input
              value={deprovisionConfirmText}
              onChange={(e) => setDeprovisionConfirmText(e.target.value)}
              placeholder={orgId}
              className="font-mono text-xs"
            />
          </Field>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={closeDeprovision}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              onClick={doDeprovision}
              disabled={deprovision.isPending || deprovisionConfirmText.trim() !== orgId}
            >
              {deprovision.isPending ? "Deprovisioning…" : "Deprovision"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}

// ReshardHistory lists the org's reshard operations with links to each
// operation's live overview/log page. Hidden while the org has none.
function ReshardHistory({ orgId }: { orgId: string }) {
  const reshards = useOrgReshards(orgId);
  const ops = reshards.data ?? [];
  if (ops.length === 0) return null;
  return (
    <div className="space-y-2 border-t border-border pt-3">
      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Reshard operations
      </p>
      <div className="space-y-1">
        {ops.slice(0, 5).map((op) => (
          <Link
            key={op.id}
            to={`/reshards/${op.id}`}
            className="flex items-center justify-between rounded-md border border-border bg-background/40 px-3 py-1.5 hover:bg-background"
          >
            <span className="font-mono text-xs">
              #{op.id}{" "}
              {op.source_kind === "cnpg-shard" ? op.from_shard || "cnpg" : "external"} →{" "}
              {op.target_kind === "cnpg-shard" ? op.to_shard : "external"}
            </span>
            <span className="flex items-center gap-2 text-xs text-muted-foreground">
              {fmtTime(op.created_at)}
              <StateBadge state={op.state} />
            </span>
          </Link>
        ))}
      </div>
    </div>
  );
}

// OrgTeamsCard lists the org's duckgres_org_teams rows with full CRUD.
function OrgTeamsCard({ orgId }: { orgId: string }) {
  const teams = useOrgTeams(orgId);
  const org = useOrg(orgId);
  const orgName = org.data ? orgLabel(org.data) : undefined;
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<OrgTeam | null>(null);
  const [deleting, setDeleting] = useState<OrgTeam | null>(null);
  const rows = teams.data ?? [];

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Layers className="h-4 w-4" /> Teams
        </CardTitle>
        <AdminGate>
          <Button size="sm" variant="outline" onClick={() => setCreating(true)}>
            <Plus className="h-4 w-4" /> Add team
          </Button>
        </AdminGate>
      </CardHeader>
      <CardContent>
        {teams.isLoading ? (
          <LoadingState />
        ) : teams.isError ? (
          <ErrorState error={teams.error} onRetry={() => teams.refetch()} />
        ) : rows.length === 0 ? (
          <p className="py-4 text-center text-sm text-muted-foreground">
            No PostHog teams are mapped to this org.
          </p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>Team id</TableHead>
                <TableHead>Schema</TableHead>
                <TableHead>Enabled</TableHead>
                <TableHead>Backfill</TableHead>
                <TableHead>Earliest event</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((t) => (
                <TableRow key={t.team_id} className="[&>td]:py-1.5">
                  <TableCell className="font-mono text-xs font-medium tabular-nums">{t.team_id}</TableCell>
                  <TableCell>
                    <span className="flex items-center gap-1.5">
                      <span className="font-mono text-xs">{t.schema_name}</span>
                      <LegacyNamesBadge team={t} />
                    </span>
                  </TableCell>
                  <TableCell>
                    {t.enabled ? (
                      <Badge variant="secondary">enabled</Badge>
                    ) : (
                      <Badge variant="destructive">disabled</Badge>
                    )}
                  </TableCell>
                  <TableCell>
                    <BackfillBadge value={t.backfill_enabled} />
                  </TableCell>
                  <TableCell>
                    <EarliestEventDateCell value={t.earliest_event_date} />
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">{fmtTime(t.created_at)}</TableCell>
                  <TableCell>
                    <div className="-my-1 flex justify-end gap-1">
                      <AdminGate>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6"
                          title="Edit"
                          onClick={() => setEditing(t)}
                        >
                          <Pencil className="h-3.5 w-3.5" />
                        </Button>
                      </AdminGate>
                      <AdminGate>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6"
                          title="Delete"
                          onClick={() => setDeleting(t)}
                        >
                          <Trash2 className="h-3.5 w-3.5 text-destructive" />
                        </Button>
                      </AdminGate>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>

      <CreateTeamDialog open={creating} onClose={() => setCreating(false)} org={orgId} orgLabel={orgName} />
      {editing && <EditTeamDialog team={editing} orgLabel={orgName} onClose={() => setEditing(null)} />}
      {deleting && (
        <DeleteTeamDialog team={deleting} teamCount={rows.length} orgLabel={orgName} onClose={() => setDeleting(null)} />
      )}
    </Card>
  );
}

function ReadRow({ label, value }: { label: string; value: unknown }) {
  return (
    <details className="rounded-md border border-border bg-background/40 px-2 py-1.5">
      <summary className="cursor-pointer font-mono text-xs text-muted-foreground">{label}</summary>
      <div className="mt-1.5">
        <JsonValue value={value} />
      </div>
    </details>
  );
}
