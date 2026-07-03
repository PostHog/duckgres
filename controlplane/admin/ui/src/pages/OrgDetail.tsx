import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { AlertTriangle, ArrowLeft, Save, Trash2, Warehouse } from "lucide-react";
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
  useDucklingsMetadata,
  useOrg,
  useUpdateOrg,
  useUpdateWarehouse,
  useWarehouse,
} from "@/hooks/useApi";
import type { ManagedWarehouse, OrgUpdate } from "@/types/api";

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
  default_team_id: string | null;
}): FormState {
  return {
    max_workers: String(o.max_workers),
    max_vcpus: String(o.max_vcpus),
    default_worker_cpu: o.default_worker_cpu,
    default_worker_memory: o.default_worker_memory,
    default_worker_ttl: o.default_worker_ttl,
    default_worker_min_hot_idle: String(o.default_worker_min_hot_idle),
    hostname_alias: o.hostname_alias ?? "",
    default_team_id: o.default_team_id ?? "",
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
    const body: OrgUpdate = {
      max_workers: Number(form.max_workers) || 0,
      max_vcpus: Number(form.max_vcpus) || 0,
      default_worker_cpu: form.default_worker_cpu,
      default_worker_memory: form.default_worker_memory,
      default_worker_ttl: form.default_worker_ttl,
      default_worker_min_hot_idle: Number(form.default_worker_min_hot_idle) || 0,
      hostname_alias: form.hostname_alias === "" ? "" : form.hostname_alias,
      default_team_id: form.default_team_id === "" ? "" : form.default_team_id,
    };
    try {
      await update.mutateAsync(body);
      setMsg({ kind: "ok", text: "Saved." });
    } catch (e) {
      setMsg({ kind: "err", text: e instanceof Error ? e.message : "Save failed" });
    }
  };

  const doDelete = async () => {
    try {
      await del.mutateAsync(id);
      navigate("/orgs");
    } catch (e) {
      setMsg({ kind: "err", text: e instanceof Error ? e.message : "Delete failed" });
      setConfirmDelete(false);
    }
  };

  return (
    <>
      <Header
        id={id}
        actions={
          <AdminGate>
            <Button variant="destructive" size="sm" onClick={() => setConfirmDelete(true)}>
              <Trash2 className="h-4 w-4" /> Delete org
            </Button>
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
              <Field label="Default team id (empty clears)">
                <Input
                  value={form.default_team_id}
                  placeholder="PostHog team id, e.g. 12345"
                  onChange={(e) => set("default_team_id", e.target.value)}
                />
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
      </PageBody>

      <Dialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete org "{id}"?</DialogTitle>
            <DialogDescription>
              This removes the org and all of its users from the config store. This cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setConfirmDelete(false)}>
              Cancel
            </Button>
            <Button variant="destructive" size="sm" onClick={doDelete} disabled={del.isPending}>
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
  const metadata = useDucklingsMetadata();
  const [image, setImage] = useState("");
  const [version, setVersion] = useState("");
  const [ducklingNameInput, setDucklingNameInput] = useState("");
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
          </>
        )}
      </CardContent>
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
