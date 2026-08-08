import { useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { AlertTriangle, ArrowLeft, Loader2, Rocket } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Copyable } from "@/components/Copyable";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { StateBadge } from "@/components/StateBadge";
import { AdminGate } from "@/components/AdminOnly";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  buildProvisionBody,
  DEFAULT_PROVISION_FORM,
  validateProvisionForm,
  type ProvisionForm,
} from "@/lib/provision";
import { useDatabaseNameAvailable, useOrgs, useProvisionWarehouse, useWarehouseStatus } from "@/hooks/useApi";
import type { ProvisionResult } from "@/types/api";

// Provision a managed warehouse from the console.
//
// This page is a FORM OVER THE PUBLIC PROVISIONING API — it POSTs to
// /api/v1/orgs/:id/provision, the exact endpoint (and handler, validation,
// transaction and analytics event) the PostHog backend calls when a customer
// provisions a warehouse. There is deliberately no operator-only provisioning
// path: a warehouse created here is indistinguishable from one a user created.
// The request preview below shows the literal call so that equivalence is
// visible, not just asserted.
export function ProvisionWarehouse() {
  const navigate = useNavigate();
  const orgs = useOrgs();
  const provision = useProvisionWarehouse();

  const [form, setForm] = useState<ProvisionForm>(DEFAULT_PROVISION_FORM);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [result, setResult] = useState<ProvisionResult | null>(null);

  const set = <K extends keyof ProvisionForm>(k: K, v: ProvisionForm[K]) =>
    setForm((f) => ({ ...f, [k]: v }));

  const orgId = form.orgId.trim();
  // Re-provisioning an EXISTING org is legal (the server keeps its teams and
  // only 409s while the warehouse row is non-terminal) and relaxes the team_id
  // requirement — so the check drives validation, not just the warning below.
  const existingOrg = useMemo(() => (orgs.data ?? []).find((o) => o.name === orgId), [orgs.data, orgId]);
  const orgExists = existingOrg != null;
  const liveWarehouse = existingOrg?.warehouse != null && existingOrg.warehouse.state !== "deleted";

  const errs = validateProvisionForm(form, orgExists);
  const valid = Object.keys(errs).length === 0;
  const body = buildProvisionBody(form);

  const dbCheck = useDatabaseNameAvailable(form.databaseName);
  const dbTaken = dbCheck.data != null && !dbCheck.data.available;

  const run = async () => {
    setErr(null);
    try {
      const res = await provision.mutateAsync({ org: orgId, body });
      setResult(res);
      setConfirmOpen(false);
      setConfirmText("");
    } catch (e) {
      setErr(e instanceof Error ? e.message : "provisioning failed");
      setConfirmOpen(false);
      setConfirmText("");
    }
  };

  if (result) {
    return (
      <ProvisionStarted
        org={orgId}
        result={result}
        onDone={() => navigate(`/orgs/${encodeURIComponent(orgId)}`)}
      />
    );
  }

  return (
    <>
      <PageHeader
        title="Provision warehouse"
        description="Creates a managed warehouse through the same API the PostHog backend uses."
        actions={
          <Button variant="outline" size="sm" asChild>
            <Link to="/orgs">
              <ArrowLeft className="h-4 w-4" /> Back to orgs
            </Link>
          </Button>
        }
      />
      <PageBody>
        <Card className="max-w-2xl">
          <CardHeader>
            <CardTitle>New managed warehouse</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="rounded-md border border-border bg-background/40 px-3 py-2 text-xs text-muted-foreground">
              This form posts to <span className="font-mono">POST /api/v1/orgs/:id/provision</span> — the same
              endpoint, handler and validation the PostHog backend calls. Provisioning runs asynchronously;
              the warehouse is not usable until it reports <span className="font-mono">ready</span>.
            </p>

            <Field id="org-id" label="Org id" error={errs.orgId}>
              <Input
                id="org-id"
                value={form.orgId}
                onChange={(e) => set("orgId", e.target.value)}
                placeholder="PostHog organization UUID, or a short slug"
                className="font-mono text-xs"
              />
            </Field>
            {orgExists && (
              <Note kind={liveWarehouse ? "warn" : "info"}>
                {liveWarehouse ? (
                  <>
                    Org <span className="font-mono">{orgId}</span> already has a warehouse in state{" "}
                    <span className="font-mono">{existingOrg?.warehouse?.state}</span>. Provisioning is
                    refused (409) unless it is <span className="font-mono">failed</span> or{" "}
                    <span className="font-mono">deleted</span> — deprovision it first.
                  </>
                ) : (
                  <>
                    Org <span className="font-mono">{orgId}</span> already exists; this re-provisions its
                    warehouse. Its existing teams are kept, and <span className="font-mono">team_id</span>{" "}
                    becomes optional.
                  </>
                )}
              </Note>
            )}

            <Field id="database-name" label="Database name" error={errs.databaseName}>
              <Input
                id="database-name"
                value={form.databaseName}
                onChange={(e) => set("databaseName", e.target.value)}
                placeholder="the dbname clients connect to"
                className="font-mono text-xs"
              />
            </Field>
            {form.databaseName.trim() !== "" && dbCheck.data != null && (
              <Note kind={dbTaken ? "warn" : "info"}>
                {dbTaken ? (
                  <>
                    Database name <span className="font-mono">{dbCheck.data.name}</span> is already in use by
                    another org — the provision would be rejected with 409.
                  </>
                ) : (
                  <>
                    Database name <span className="font-mono">{dbCheck.data.name}</span> is available.
                  </>
                )}
              </Note>
            )}

            <div className="grid grid-cols-2 gap-3">
              <Field id="team-id" label="Team id" error={errs.teamId}>
                <Input
                  id="team-id"
                  value={form.teamId}
                  onChange={(e) => set("teamId", e.target.value)}
                  placeholder="PostHog Team.id"
                  className="font-mono text-xs"
                />
              </Field>
              <Field id="schema-name" label="Schema name (optional)" error={errs.schemaName}>
                <Input
                  id="schema-name"
                  value={form.schemaName}
                  onChange={(e) => set("schemaName", e.target.value)}
                  placeholder="defaults to team_<id>"
                  className="font-mono text-xs"
                />
              </Field>
            </div>

            <div className="space-y-1">
              <Label>Metadata store</Label>
              <Select
                value={form.metadataType}
                onValueChange={(v) => set("metadataType", v as ProvisionForm["metadataType"])}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="cnpg-shard">cnpg shard (standard)</SelectItem>
                  <SelectItem value="external">external Postgres (RDS)</SelectItem>
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                Where the DuckLake catalog lives. The cnpg composition picks the active shard itself.
              </p>
            </div>

            {form.metadataType === "external" && (
              <div className="grid grid-cols-2 gap-3">
                <Field id="external-endpoint" label="Endpoint" error={errs.externalEndpoint}>
                  <Input
                    id="external-endpoint"
                    value={form.externalEndpoint}
                    onChange={(e) => set("externalEndpoint", e.target.value)}
                    placeholder="the RDS host"
                    className="font-mono text-xs"
                  />
                </Field>
                <Field id="external-secret" label="Password AWS secret" error={errs.externalSecret}>
                  <Input
                    id="external-secret"
                    value={form.externalSecret}
                    onChange={(e) => set("externalSecret", e.target.value)}
                    placeholder="Secrets Manager secret NAME"
                    className="font-mono text-xs"
                  />
                </Field>
                <Field id="external-user" label="User">
                  <Input
                    id="external-user"
                    value={form.externalUser}
                    onChange={(e) => set("externalUser", e.target.value)}
                    className="font-mono text-xs"
                  />
                </Field>
                <Field id="external-database" label="Database">
                  <Input
                    id="external-database"
                    value={form.externalDatabase}
                    onChange={(e) => set("externalDatabase", e.target.value)}
                    className="font-mono text-xs"
                  />
                </Field>
              </div>
            )}

            <div className="space-y-1">
              <Label>Data store</Label>
              <Select
                value={form.dataStoreType}
                onValueChange={(v) => set("dataStoreType", v as ProvisionForm["dataStoreType"])}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="s3bucket">new per-org S3 bucket (standard)</SelectItem>
                  <SelectItem value="external">existing bucket</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {form.dataStoreType === "external" && (
              <div className="grid grid-cols-2 gap-3">
                <Field id="bucket-name" label="Bucket name" error={errs.bucketName}>
                  <Input
                    id="bucket-name"
                    value={form.bucketName}
                    onChange={(e) => set("bucketName", e.target.value)}
                    className="font-mono text-xs"
                  />
                </Field>
                <Field id="region" label="Region (optional)">
                  <Input
                    id="region"
                    value={form.region}
                    onChange={(e) => set("region", e.target.value)}
                    placeholder="composition default"
                    className="font-mono text-xs"
                  />
                </Field>
              </div>
            )}

            <div className="flex items-center gap-2">
              <Badge variant="success">ducklake enabled</Badge>
              <span className="text-xs text-muted-foreground">
                Always on — the server rejects a warehouse without a catalog.
              </span>
            </div>

            {/* The literal request. Makes the "same call as the PostHog backend"
                claim inspectable, and doubles as a copy/paste curl body when an
                operator would rather run it by hand. */}
            <div className="space-y-1">
              <Label>Request preview</Label>
              <pre className="overflow-x-auto rounded-md border border-border bg-background/40 p-3 font-mono text-[11px] leading-relaxed">
                {`POST /api/v1/orgs/${orgId || ":id"}/provision\n${JSON.stringify(body, null, 2)}`}
              </pre>
            </div>

            {err && (
              <p className="flex items-start gap-2 text-xs text-destructive">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <span>{err}</span>
              </p>
            )}

            <div className="flex items-center gap-3 border-t border-border pt-3">
              <AdminGate>
                <Button
                  size="sm"
                  onClick={() => setConfirmOpen(true)}
                  disabled={!valid || provision.isPending}
                >
                  <Rocket className="h-4 w-4" /> Provision warehouse
                </Button>
              </AdminGate>
              {!valid && (
                <span className="text-xs text-muted-foreground">Fix the highlighted fields first.</span>
              )}
            </div>
          </CardContent>
        </Card>
      </PageBody>

      <Dialog open={confirmOpen} onOpenChange={(o) => (o ? setConfirmOpen(true) : setConfirmOpen(false))}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Provision a warehouse for "{orgId}"?</DialogTitle>
            <DialogDescription>
              This creates real infrastructure — a Duckling CR, an S3 bucket, a metadata database and an IAM
              role — and bills the org for it. The root password is returned once and cannot be read back
              afterwards.
            </DialogDescription>
          </DialogHeader>
          <Field id="confirm-org-id" label="Type the org id to confirm">
            <Input
              id="confirm-org-id"
              value={confirmText}
              onChange={(e) => setConfirmText(e.target.value)}
              placeholder={orgId}
              className="font-mono text-xs"
            />
          </Field>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setConfirmOpen(false)}>
              Cancel
            </Button>
            <Button size="sm" onClick={run} disabled={provision.isPending || confirmText.trim() !== orgId}>
              {provision.isPending ? "Provisioning…" : "Provision"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

// ProvisionStarted is the post-202 view: the once-only root password, the
// control-plane-owned bucket name, and the live lifecycle poll. The password is
// deliberately not persisted anywhere in the client — navigating away loses it,
// and the recovery path is the reset-password action on the org page.
function ProvisionStarted({
  org,
  result,
  onDone,
}: {
  org: string;
  result: ProvisionResult;
  onDone: () => void;
}) {
  const status = useWarehouseStatus(org);
  const state = status.data?.state;
  const done = state === "ready" || state === "failed";

  return (
    <>
      <PageHeader
        title="Provisioning started"
        description={`Warehouse for ${org} is being created.`}
        actions={
          <Button variant="outline" size="sm" onClick={onDone}>
            Open org
          </Button>
        }
      />
      <PageBody>
        <Card className="max-w-2xl">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              {done ? null : <Loader2 className="h-4 w-4 animate-spin" />}
              Root credentials
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Note kind="warn">
              The password below is shown <strong>once</strong>. Duckgres stores only its bcrypt hash, so it
              cannot be retrieved later — copy it now. If it is lost, rotate it with "Reset root password" on
              the org page once the warehouse is ready.
            </Note>
            <div className="grid grid-cols-2 gap-3">
              <Copyable label="Username" value={result.username} />
              <Copyable label="Password" value={result.password} />
              {result.bucket && <Copyable label="S3 bucket" value={result.bucket} />}
              <Copyable label="Org" value={result.org || org} />
            </div>

            <div className="space-y-2 border-t border-border pt-3">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Provisioning state
              </p>
              <div className="flex flex-wrap items-center gap-2">
                <StateBadge state={state ?? "pending"} />
                {status.data?.status_message && (
                  <span className="font-mono text-xs text-muted-foreground">
                    {status.data.status_message}
                  </span>
                )}
              </div>
              <p className="text-xs text-muted-foreground">
                Polled from <span className="font-mono">GET /orgs/{org}/warehouse/status</span> — the same
                lifecycle view the PostHog backend polls. A cold provision typically takes several minutes.
              </p>
            </div>

            <div className="border-t border-border pt-3">
              <Button size="sm" onClick={onDone}>
                Go to org
              </Button>
            </div>
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}

// Field binds its Label to the control via htmlFor/id — not decoration:
// screen readers (and the page tests) resolve an input by its label text.
function Field({
  id,
  label,
  error,
  children,
}: {
  id: string;
  label: string;
  error?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1">
      <Label htmlFor={id}>{label}</Label>
      {children}
      {error && <p className="text-xs text-destructive">{error}</p>}
    </div>
  );
}

function Note({ kind, children }: { kind: "info" | "warn"; children: React.ReactNode }) {
  return (
    <p
      className={
        kind === "warn"
          ? "flex items-start gap-2 rounded-md border border-warning/40 bg-warning/5 px-3 py-2 text-xs text-warning"
          : "rounded-md border border-border bg-background/40 px-3 py-2 text-xs text-muted-foreground"
      }
    >
      {kind === "warn" && <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />}
      <span>{children}</span>
    </p>
  );
}
