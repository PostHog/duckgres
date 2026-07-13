import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { AlertTriangle, ArrowLeft, ArrowRight, Play } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ShardBadge } from "@/components/ShardBadge";
import { AdminGate } from "@/components/AdminOnly";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ducklingEntryFor } from "@/lib/format";
import { classifySecretName } from "@/lib/reshard";
import { useDucklingsMetadata, useReshardTargets, useStartReshard, useWarehouse } from "@/hooks/useApi";
import type { StartReshardBody } from "@/types/api";

// Reshard start form: pick a target metadata store for the org, confirm, run.
// The operation itself is driven server-side; on success we navigate straight
// to the operation overview page with the live log.
export function ReshardForm() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const warehouse = useWarehouse(id);
  const metadata = useDucklingsMetadata();
  const start = useStartReshard(id);

  const [targetType, setTargetType] = useState<"cnpg-shard" | "external">("cnpg-shard");
  const [shard, setShard] = useState("");
  const [shardCustom, setShardCustom] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [user, setUser] = useState("postgres");
  const [database, setDatabase] = useState("postgres");
  const [passwordSecret, setPasswordSecret] = useState("");
  const [password, setPassword] = useState("");
  const [drainTimeoutMin, setDrainTimeoutMin] = useState("30");
  const [cutoverTimeoutMin, setCutoverTimeoutMin] = useState("15");
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [err, setErr] = useState<string | null>(null);

  const entry = ducklingEntryFor(metadata.data?.entries, id, warehouse.data?.duckling_name);
  const sourceKind = entry?.kind ?? warehouse.data?.metadata_store?.kind ?? "cnpg-shard";
  const currentShard = entry?.cnpg_shard ?? "";

  // Destination discovery: every cnpg shard in the cluster (including EMPTY
  // ones no tenant occupies yet) + known external stores, from
  // /reshards/targets. Unioned with the shards tenants occupy (duckling
  // metadata) as a belt-and-suspenders fallback; free-text still covers a
  // shard the server can't see (cluster_discovery=false on an RBAC degrade).
  const targets = useReshardTargets();
  const knownShards = useMemo(() => {
    const s = new Set<string>(targets.data?.shards ?? []);
    for (const e of Object.values(metadata.data?.entries ?? {})) {
      if (e.kind === "cnpg-shard" && e.cnpg_shard) s.add(e.cnpg_shard);
    }
    return [...s].sort();
  }, [targets.data, metadata.data]);
  const knownExternal = targets.data?.external_stores ?? [];

  const effectiveShard = shard === "__custom__" ? shardCustom.trim() : shard;
  const secretVerdict = classifySecretName(passwordSecret);
  const targetLabel =
    targetType === "cnpg-shard" ? `cnpg ${effectiveShard || "?"}` : `external ${endpoint || "?"}`;
  const sourceLabel =
    sourceKind === "cnpg-shard" ? `cnpg ${currentShard || "?"}` : `external ${entry?.endpoint ?? ""}`;
  const confirmToken = targetType === "cnpg-shard" ? effectiveShard : "reshard";

  const valid =
    (targetType === "cnpg-shard" &&
      effectiveShard !== "" &&
      /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/.test(effectiveShard) &&
      !(sourceKind === "cnpg-shard" && effectiveShard === currentShard)) ||
    (targetType === "external" &&
      sourceKind === "cnpg-shard" &&
      endpoint.trim() !== "" &&
      passwordSecret.trim() !== "" &&
      password !== "");

  const run = async () => {
    setErr(null);
    const body: StartReshardBody = {
      drain_timeout_seconds: Math.max(60, (Number(drainTimeoutMin) || 30) * 60),
      cutover_timeout_seconds: Math.max(60, (Number(cutoverTimeoutMin) || 15) * 60),
      target:
        targetType === "cnpg-shard"
          ? { type: "cnpg-shard", cnpg_shard: effectiveShard }
          : {
              type: "external",
              endpoint: endpoint.trim(),
              user: user.trim(),
              database: database.trim(),
              password_aws_secret: passwordSecret.trim(),
              password,
            },
    };
    try {
      const op = await start.mutateAsync(body);
      navigate(`/reshards/${op.id}`);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "starting the reshard failed");
      setConfirmOpen(false);
      setConfirmText("");
    }
  };

  return (
    <>
      <PageHeader
        title={`Reshard ${id}`}
        actions={
          <Button variant="outline" size="sm" asChild>
            <Link to={`/orgs/${encodeURIComponent(id)}`}>
              <ArrowLeft className="h-4 w-4" /> Back to org
            </Link>
          </Button>
        }
      />
      <PageBody>
        <Card className="max-w-2xl">
          <CardHeader>
            <CardTitle>Move the metadata store</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center gap-3 rounded-md border border-border bg-background/40 px-3 py-2">
              <div>
                <div className="text-[10px] uppercase text-muted-foreground">Current</div>
                <div className="flex items-center gap-2 text-sm">
                  <span className="font-mono text-xs">{sourceKind}</span>
                  <ShardBadge meta={entry} />
                </div>
              </div>
              <ArrowRight className="h-4 w-4 text-muted-foreground" />
              <div>
                <div className="text-[10px] uppercase text-muted-foreground">Target</div>
                <div className="font-mono text-xs">{targetLabel}</div>
              </div>
            </div>

            <div className="space-y-1">
              <Label>Target type</Label>
              <Select
                value={targetType}
                onValueChange={(v) => setTargetType(v as "cnpg-shard" | "external")}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="cnpg-shard">cnpg shard</SelectItem>
                  {sourceKind === "cnpg-shard" && (
                    <SelectItem value="external">external (RDS) — escape hatch</SelectItem>
                  )}
                </SelectContent>
              </Select>
            </div>

            {targetType === "cnpg-shard" ? (
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-1">
                  <Label>Target shard</Label>
                  <Select value={shard} onValueChange={setShard}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select shard…" />
                    </SelectTrigger>
                    <SelectContent>
                      {knownShards.map((s) => (
                        <SelectItem key={s} value={s} disabled={s === currentShard && sourceKind === "cnpg-shard"}>
                          {s}
                          {s === currentShard ? " (current)" : ""}
                        </SelectItem>
                      ))}
                      <SelectItem value="__custom__">other…</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {targets.data && !targets.data.cluster_discovery && (
                  <p className="col-span-2 text-xs text-muted-foreground">
                    Shard list limited to shards tenants already occupy (no cluster read access) —
                    use "other…" for a new empty shard.
                  </p>
                )}
                {shard === "__custom__" && (
                  <div className="space-y-1">
                    <Label>Shard name</Label>
                    <Input
                      value={shardCustom}
                      onChange={(e) => setShardCustom(e.target.value)}
                      placeholder="shard-004"
                      className="font-mono text-xs"
                    />
                  </div>
                )}
              </div>
            ) : (
              <div className="space-y-3">
                {knownExternal.length > 0 && (
                  <div className="space-y-1">
                    <Label>Known external stores</Label>
                    <Select
                      onValueChange={(v) => {
                        const st = knownExternal[Number(v)];
                        if (!st) return;
                        setEndpoint(st.endpoint);
                        setPasswordSecret(st.password_aws_secret);
                        setUser(st.user || "postgres");
                        setDatabase(st.database || "postgres");
                      }}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Prefill from a store already in use…" />
                      </SelectTrigger>
                      <SelectContent>
                        {knownExternal.map((st, i) => (
                          <SelectItem key={st.endpoint + st.database} value={String(i)}>
                            {st.endpoint}/{st.database || "postgres"}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-muted-foreground">
                      Prefills endpoint/secret/user/database from an external store another
                      warehouse already uses — the password below is still required.
                    </p>
                  </div>
                )}
                <div className="grid grid-cols-2 gap-3">
                  <div className="space-y-1">
                    <Label>Endpoint (RDS host)</Label>
                    <Input
                      value={endpoint}
                      onChange={(e) => setEndpoint(e.target.value)}
                      placeholder="db.xxxx.rds.amazonaws.com"
                      className="font-mono text-xs"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label>Password AWS secret (name)</Label>
                    <Input
                      value={passwordSecret}
                      onChange={(e) => setPasswordSecret(e.target.value)}
                      placeholder="duckling-<name>-…-rds-password"
                      className="font-mono text-xs"
                    />
                    {secretVerdict === "rds-managed" && (
                      <p className="flex items-start gap-1 text-xs text-destructive">
                        <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
                        This looks like the RDS-managed master secret (rds/…/master or rds!…) — the
                        ESO role can NOT read it, so the cutover would hang and roll back. Create a
                        secret named duckling-…-rds-password holding the same password and use that.
                      </p>
                    )}
                    {secretVerdict === "unknown-prefix" && (
                      <p className="flex items-start gap-1 text-xs text-warning">
                        <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
                        Name doesn't start with duckling- or posthog- — the ESO role can only read
                        secrets matching its allowed name prefixes. Double-check before running.
                      </p>
                    )}
                  </div>
                  <div className="space-y-1">
                    <Label>User</Label>
                    <Input value={user} onChange={(e) => setUser(e.target.value)} className="font-mono text-xs" />
                  </div>
                  <div className="space-y-1">
                    <Label>Database</Label>
                    <Input
                      value={database}
                      onChange={(e) => setDatabase(e.target.value)}
                      className="font-mono text-xs"
                    />
                  </div>
                </div>
                <p className="rounded-md border border-border bg-background/40 px-3 py-2 text-xs text-muted-foreground">
                  The secret must live in this account's Secrets Manager with a{" "}
                  <span className="font-mono">duckling-*</span> or{" "}
                  <span className="font-mono">posthog-*</span> name (the only prefixes the
                  external-secrets role may read — e.g.{" "}
                  <span className="font-mono">duckling-&lt;name&gt;-&lt;env&gt;-&lt;region&gt;-rds-password</span>
                  ), and its value must be the <strong>raw password string</strong> — ESO copies the
                  whole value verbatim, so JSON like{" "}
                  <span className="font-mono">{'{"password": …}'}</span> will not work. The
                  RDS-managed master secret (<span className="font-mono">rds/…/master</span>) will
                  NOT work — the ESO role can't read it.
                </p>
                <div className="space-y-1">
                  <Label>Password (sent once, never stored)</Label>
                  <Input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="font-mono text-xs"
                  />
                  <p className="text-xs text-muted-foreground">
                    Used directly for the catalog copy. The AWS secret above must contain exactly
                    this value as a plain string — the duckling reads it via ESO after the cutover
                    and the runner refuses to finish if they differ.
                  </p>
                </div>
              </div>
            )}

            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1">
                <Label>Drain timeout (minutes)</Label>
                <Input
                  type="number"
                  min={1}
                  value={drainTimeoutMin}
                  onChange={(e) => setDrainTimeoutMin(e.target.value)}
                  className="w-32"
                />
                <p className="text-xs text-muted-foreground">
                  New connections are blocked immediately; existing sessions get this long to
                  finish before the operation rolls back. In-flight queries are never killed.
                </p>
              </div>
              <div className="space-y-1">
                <Label>Cutover timeout (minutes)</Label>
                <Input
                  type="number"
                  min={1}
                  value={cutoverTimeoutMin}
                  onChange={(e) => setCutoverTimeoutMin(e.target.value)}
                  className="w-32"
                />
                <p className="text-xs text-muted-foreground">
                  How long to wait for the target store to come up before rolling back. Role/DB
                  creation on a cnpg shard can take several minutes.
                </p>
              </div>
            </div>

            <div className="flex items-center gap-3 border-t border-border pt-3">
              <AdminGate>
                <Button
                  variant="destructive"
                  size="sm"
                  disabled={!valid || start.isPending}
                  onClick={() => setConfirmOpen(true)}
                >
                  <Play className="h-4 w-4" /> Run reshard
                </Button>
              </AdminGate>
              {!valid && (
                <span className="flex items-center gap-1 text-xs text-muted-foreground">
                  <AlertTriangle className="h-3 w-3" />
                  {targetType === "cnpg-shard"
                    ? "pick a target shard different from the current one"
                    : "endpoint, secret name and password are required"}
                </span>
              )}
              {err && <span className="text-xs text-destructive">{err}</span>}
            </div>
          </CardContent>
        </Card>
      </PageBody>

      <Dialog open={confirmOpen} onOpenChange={(open) => (open ? setConfirmOpen(true) : (setConfirmOpen(false), setConfirmText("")))}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              Reshard "{id}": {sourceLabel} → {targetLabel}?
            </DialogTitle>
            <DialogDescription>
              The org goes into maintenance mode: new connections are refused, existing sessions
              drain (never killed), then the DuckLake catalog is copied and the duckling is
              re-pointed. S3 data never moves. You will be taken to the live operation log.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-1">
            <Label>Type "{confirmToken}" to confirm</Label>
            <Input
              value={confirmText}
              onChange={(e) => setConfirmText(e.target.value)}
              placeholder={confirmToken}
              className="font-mono text-xs"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => (setConfirmOpen(false), setConfirmText(""))}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              onClick={run}
              disabled={start.isPending || confirmText.trim() !== confirmToken}
            >
              {start.isPending ? "Starting…" : "Start reshard"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
