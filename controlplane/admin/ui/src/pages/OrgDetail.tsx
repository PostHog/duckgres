import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { AlertTriangle, ArrowLeft, Database, Layers, Pencil, Plus, Save, Trash2, Warehouse } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
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
import { ducklingBroken, ducklingEntryFor, fmtTime } from "@/lib/format";
import { ShardBadge } from "@/components/ShardBadge";
import {
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
  EditTeamDialog,
} from "@/components/OrgTeamDialogs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import type { ManagedWarehouse, OrgTeam, OrgUpdate } from "@/types/api";

interface FormState {
  max_workers: string;
  max_vcpus: string;
  default_worker_cpu: string;
  default_worker_memory: string;
  default_worker_ttl: string;
  default_worker_min_hot_idle: string;
  hostname_alias: string;
  default_team_id: string;
}

function orgToForm(o: {
  max_workers: number;
  max_vcpus: number;
  default_worker_cpu: string;
  default_worker_memory: string;
  default_worker_ttl: string;
  default_worker_min_hot_idle: number;
  hostname_alias: string | null;
  default_team_id?: number | null;
}): FormState {
  return {
    max_workers: String(o.max_workers),
    max_vcpus: String(o.max_vcpus),
    default_worker_cpu: o.default_worker_cpu,
    default_worker_memory: o.default_worker_memory,
    default_worker_ttl: o.default_worker_ttl,
    default_worker_min_hot_idle: String(o.default_worker_min_hot_idle),
    hostname_alias: o.hostname_alias ?? "",
    default_team_id: o.default_team_id == null ? "" : String(o.default_team_id),
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

  if (org.isLoading || !form) {
    return (
      <>
        <Header id={id} />
        <PageBody>{org.isError ? <ErrorState error={org.error} /> : <LoadingState />}</PageBody>
      </>
    );
  }
  if (org.isError) {
    return (
      <>
        <Header id={id} />
        <PageBody>
          <ErrorState error={org.error} onRetry={() => org.refetch()} />
        </PageBody>
      </>
    );
  }

  const set = (k: keyof FormState, v: string) => setForm((f) => (f ? { ...f, [k]: v } : f));

  const save = async () => {
    setMsg(null);
    // default_team_id is an integer on the wire (PostHog team id); the text
    // input needs a digits-only guard so junk can't reach the backend. It is
    // required (NOT NULL) server-side and cannot be cleared, so only a
    // positive value is ever sent.
    const teamIdText = form.default_team_id.trim();
    if (teamIdText !== "" && (!/^\d+$/.test(teamIdText) || Number(teamIdText) === 0)) {
      setMsg({ kind: "err", text: "Default team id must be a positive number." });
      return;
    }
    const body: OrgUpdate = {
      max_workers: Number(form.max_workers) || 0,
      max_vcpus: Number(form.max_vcpus) || 0,
      default_worker_cpu: form.default_worker_cpu,
      default_worker_memory: form.default_worker_memory,
      default_worker_ttl: form.default_worker_ttl,
      default_worker_min_hot_idle: Number(form.default_worker_min_hot_idle) || 0,
      hostname_alias: form.hostname_alias === "" ? "" : form.hostname_alias,
    };
    // Empty input = preserve the stored value (omit the field). Sending 0 or
    // null would be a clear, which the backend now rejects with a 400.
    if (teamIdText !== "") {
      body.default_team_id = Number(teamIdText);
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
              <Field label="Hostname alias (empty clears)">
                <Input
                  value={form.hostname_alias}
                  placeholder="single DNS label, e.g. acme"
                  onChange={(e) => set("hostname_alias", e.target.value)}
                />
              </Field>
              <Field label="Billing team id (wire field default_team_id)">
                <Input
                  value={form.default_team_id}
                  placeholder="PostHog team id, e.g. 12345"
                  onChange={(e) => set("default_team_id", e.target.value)}
                />
                <p className="text-[11px] text-muted-foreground">
                  Repoints the org's billing team. All of the org's teams are listed below.
                </p>
              </Field>

              <div className="flex items-center gap-3 pt-1">
                <AdminGate>
                  <Button size="sm" onClick={save} disabled={update.isPending}>
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

function Header({ id, actions }: { id: string; actions?: React.ReactNode }) {
  return (
    <PageHeader
      title={
        <span className="flex items-center gap-2">
          <Link to="/orgs" className="text-muted-foreground hover:text-foreground">
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <span className="font-mono">{id}</span>
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
  const [ducklingNameInput, setDucklingNameInput] = useState("");
  const [confirmDeprovision, setConfirmDeprovision] = useState(false);
  const [deprovisionConfirmText, setDeprovisionConfirmText] = useState("");
  const [msg, setMsg] = useState<{ kind: "ok" | "err"; text: string } | null>(null);

  useEffect(() => {
    if (data) {
      setImage(data.image ?? "");
      setVersion(data.ducklake_version ?? "");
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
        ? `Duckling unhealthy (state: ${data?.state})`
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
              {data.status_message && (
                <p className="mt-2 font-mono text-xs text-muted-foreground">{data.status_message}</p>
              )}
              <div className="mt-2 flex gap-4 text-[11px] text-muted-foreground">
                <span>ready: {data.ready_at ? fmtTime(data.ready_at) : "—"}</span>
                <span>failed: {data.failed_at ? fmtTime(data.failed_at) : "—"}</span>
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
// The billing team is also editable as "default_team_id" in the config form
// above (the legacy wire field) — both paths repoint the same row.
function OrgTeamsCard({ orgId }: { orgId: string }) {
  const teams = useOrgTeams(orgId);
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
                <TableHead>Billing</TableHead>
                <TableHead>Backfill</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((t) => (
                <TableRow key={t.team_id} className="[&>td]:py-1.5">
                  <TableCell className="font-mono text-xs font-medium tabular-nums">{t.team_id}</TableCell>
                  <TableCell className="font-mono text-xs">{t.schema_name}</TableCell>
                  <TableCell>
                    {t.enabled ? (
                      <Badge variant="secondary">enabled</Badge>
                    ) : (
                      <Badge variant="destructive">disabled</Badge>
                    )}
                  </TableCell>
                  <TableCell>
                    {t.is_billing_team ? (
                      <Badge variant="success">billing</Badge>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </TableCell>
                  <TableCell>
                    <BackfillBadge value={t.backfill_enabled} />
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

      <CreateTeamDialog open={creating} onClose={() => setCreating(false)} org={orgId} />
      {editing && <EditTeamDialog team={editing} onClose={() => setEditing(null)} />}
      {deleting && (
        <DeleteTeamDialog team={deleting} teamCount={rows.length} onClose={() => setDeleting(null)} />
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
