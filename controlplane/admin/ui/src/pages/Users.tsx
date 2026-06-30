import { useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { Ban, KeyRound, Pencil, Plus, Power, Search, Trash2, Users as UsersIcon, Zap } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { AdminGate } from "@/components/AdminOnly";
import { EmptyState, ErrorState, TableSkeleton } from "@/components/states";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  useCreateUser,
  useDeleteUser,
  useDeleteUserSecret,
  useDisableUser,
  useEnableUser,
  useKillUserSessions,
  useUpdateUser,
  useUserSecrets,
  useUsers,
} from "@/hooks/useApi";
import { fmtInt, fmtTime } from "@/lib/format";
import type { OrgUser } from "@/types/api";

export function UsersPage() {
  const users = useUsers();
  const [filter, setFilter] = useState("");
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<OrgUser | null>(null);
  const [deleting, setDeleting] = useState<OrgUser | null>(null);
  const [killing, setKilling] = useState<OrgUser | null>(null);
  const [disabling, setDisabling] = useState<OrgUser | null>(null);
  const [secretsFor, setSecretsFor] = useState<OrgUser | null>(null);

  const delUser = useDeleteUser();
  const killUser = useKillUserSessions();
  const disableUser = useDisableUser();
  const enableUser = useEnableUser();

  const columns = useMemo<ColumnDef<OrgUser, any>[]>(
    () => [
      {
        accessorKey: "org_id",
        header: "Org",
        cell: ({ getValue }) => <span className="font-mono text-xs">{String(getValue())}</span>,
      },
      {
        accessorKey: "username",
        header: "Username",
        cell: ({ getValue }) => <span className="font-mono text-xs font-medium">{String(getValue())}</span>,
      },
      {
        accessorKey: "disabled",
        header: "Status",
        cell: ({ getValue }) =>
          getValue() ? (
            <Badge variant="destructive">disabled</Badge>
          ) : (
            <Badge variant="secondary">active</Badge>
          ),
      },
      {
        accessorKey: "passthrough",
        header: "Passthrough",
        cell: ({ getValue }) =>
          getValue() ? <Badge variant="warning">passthrough</Badge> : <span className="text-muted-foreground">—</span>,
      },
      {
        accessorKey: "default_catalog",
        header: "Default catalog",
        cell: ({ getValue }) => {
          const v = getValue() as string | undefined;
          return v ? <Badge variant="secondary">{v}</Badge> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "max_vcpus",
        header: "Max vCPUs",
        cell: ({ getValue }) => {
          const v = getValue() as number;
          return <span className="tabular-nums">{v === 0 ? "∞" : fmtInt(v)}</span>;
        },
      },
      {
        accessorKey: "created_at",
        header: "Created",
        cell: ({ getValue }) => <span className="text-xs text-muted-foreground">{fmtTime(getValue() as string)}</span>,
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <div className="-my-1 flex justify-end gap-1">
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              title="Persistent secrets"
              onClick={() => setSecretsFor(row.original)}
            >
              <KeyRound className="h-3.5 w-3.5" />
            </Button>
            <AdminGate>
              <Button variant="ghost" size="icon" className="h-6 w-6" title="Edit" onClick={() => setEditing(row.original)}>
                <Pencil className="h-3.5 w-3.5" />
              </Button>
            </AdminGate>
            <AdminGate>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                title="Kill all sessions & queries"
                onClick={() => setKilling(row.original)}
              >
                <Zap className="h-3.5 w-3.5 text-destructive" />
              </Button>
            </AdminGate>
            <AdminGate>
              {row.original.disabled ? (
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  title="Enable (allow new connections)"
                  disabled={enableUser.isPending}
                  onClick={() => enableUser.mutate({ org: row.original.org_id, username: row.original.username })}
                >
                  <Power className="h-3.5 w-3.5 text-emerald-500" />
                </Button>
              ) : (
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  title="Disable (block new connections + kill live sessions)"
                  onClick={() => setDisabling(row.original)}
                >
                  <Ban className="h-3.5 w-3.5 text-destructive" />
                </Button>
              )}
            </AdminGate>
            <AdminGate>
              <Button variant="ghost" size="icon" className="h-6 w-6" title="Delete" onClick={() => setDeleting(row.original)}>
                <Trash2 className="h-3.5 w-3.5 text-destructive" />
              </Button>
            </AdminGate>
          </div>
        ),
      },
    ],
    [enableUser],
  );

  return (
    <>
      <PageHeader
        title="Users"
        description="Per-org login accounts. Manage credentials, passthrough mode, and persistent secrets."
        actions={
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter…" className="w-56 pl-8" />
            </div>
            <AdminGate>
              <Button size="sm" onClick={() => setCreating(true)}>
                <Plus className="h-4 w-4" /> New user
              </Button>
            </AdminGate>
          </div>
        }
      />
      <PageBody>
        <Card className="overflow-hidden">
          {users.isLoading ? (
            <TableSkeleton cols={8} />
          ) : users.isError ? (
            <ErrorState error={users.error} onRetry={() => users.refetch()} />
          ) : (
            <DataTable
              data={users.data ?? []}
              columns={columns}
              globalFilter={filter}
              onGlobalFilterChange={setFilter}
              initialSorting={[{ id: "org_id", desc: false }]}
              empty={
                <EmptyState icon={<UsersIcon className="h-6 w-6" />} title="No users" description="No org users are registered." />
              }
            />
          )}
        </Card>
      </PageBody>

      {creating && <CreateUserDialog onClose={() => setCreating(false)} />}
      {editing && <EditUserDialog user={editing} onClose={() => setEditing(null)} />}
      {secretsFor && <SecretsDialog user={secretsFor} onClose={() => setSecretsFor(null)} />}

      <Dialog open={!!killing} onOpenChange={(o) => !o && setKilling(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Kill all sessions for "{killing?.username}"?</DialogTitle>
            <DialogDescription>
              Immediately terminates every active session and in-flight query for{" "}
              <span className="font-mono">{killing?.username}</span> @{" "}
              <span className="font-mono">{killing?.org_id}</span> across all control-plane replicas. The
              user can reconnect right away — use Disable to also block new connections.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setKilling(null)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              disabled={killUser.isPending}
              onClick={async () => {
                if (!killing) return;
                await killUser.mutateAsync({ org: killing.org_id, username: killing.username });
                setKilling(null);
              }}
            >
              {killUser.isPending ? "Killing…" : "Kill sessions"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!disabling} onOpenChange={(o) => !o && setDisabling(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Disable "{disabling?.username}"?</DialogTitle>
            <DialogDescription>
              Blocks all new connections for <span className="font-mono">{disabling?.username}</span> @{" "}
              <span className="font-mono">{disabling?.org_id}</span> (PG wire + Flight SQL) and kills their
              live sessions now. Reverse it any time with Enable.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setDisabling(null)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              disabled={disableUser.isPending}
              onClick={async () => {
                if (!disabling) return;
                await disableUser.mutateAsync({ org: disabling.org_id, username: disabling.username });
                setDisabling(null);
              }}
            >
              {disableUser.isPending ? "Disabling…" : "Disable user"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={!!deleting} onOpenChange={(o) => !o && setDeleting(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete user "{deleting?.username}"?</DialogTitle>
            <DialogDescription>
              Removes <span className="font-mono">{deleting?.username}</span> from org{" "}
              <span className="font-mono">{deleting?.org_id}</span>. This cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setDeleting(null)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              disabled={delUser.isPending}
              onClick={async () => {
                if (!deleting) return;
                await delUser.mutateAsync({ org: deleting.org_id, username: deleting.username });
                setDeleting(null);
              }}
            >
              {delUser.isPending ? "Deleting…" : "Delete"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function CreateUserDialog({ onClose }: { onClose: () => void }) {
  const create = useCreateUser();
  const [org, setOrg] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [passthrough, setPassthrough] = useState(false);
  const [iceberg, setIceberg] = useState(false);
  const [maxVCPUs, setMaxVCPUs] = useState("0");
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    setErr(null);
    try {
      await create.mutateAsync({
        org_id: org,
        username,
        password,
        passthrough,
        default_catalog: iceberg ? "iceberg" : "",
        max_vcpus: Number(maxVCPUs) || 0,
      });
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Create failed");
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>New user</DialogTitle>
          <DialogDescription>Create a per-org login. The password is hashed server-side.</DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1">
              <Label>Org ID</Label>
              <Input value={org} onChange={(e) => setOrg(e.target.value)} className="font-mono" />
            </div>
            <div className="space-y-1">
              <Label>Username</Label>
              <Input value={username} onChange={(e) => setUsername(e.target.value)} className="font-mono" />
            </div>
          </div>
          <div className="space-y-1">
            <Label>Password</Label>
            <Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </div>
          <div className="space-y-1">
            <Label>Max vCPUs (0 = unbounded)</Label>
            <Input type="number" min={0} value={maxVCPUs} onChange={(e) => setMaxVCPUs(e.target.value)} />
          </div>
          <div className="flex items-center gap-6">
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={passthrough} onCheckedChange={setPassthrough} /> Passthrough
            </label>
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={iceberg} onCheckedChange={setIceberg} /> Default catalog: iceberg
            </label>
          </div>
          {err && <p className="text-xs text-destructive">{err}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button size="sm" onClick={submit} disabled={create.isPending || !org || !username || !password}>
            {create.isPending ? "Creating…" : "Create user"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EditUserDialog({ user, onClose }: { user: OrgUser; onClose: () => void }) {
  const update = useUpdateUser();
  const [password, setPassword] = useState("");
  const [passthrough, setPassthrough] = useState(user.passthrough);
  const [iceberg, setIceberg] = useState(user.default_catalog === "iceberg");
  const [maxVCPUs, setMaxVCPUs] = useState(String(user.max_vcpus));
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    setErr(null);
    try {
      await update.mutateAsync({
        org: user.org_id,
        username: user.username,
        body: {
          ...(password ? { password } : {}),
          passthrough,
          default_catalog: iceberg ? "iceberg" : "",
          max_vcpus: Number(maxVCPUs) || 0,
        },
      });
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Update failed");
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            Edit <span className="font-mono">{user.username}</span>
          </DialogTitle>
          <DialogDescription>
            Org <span className="font-mono">{user.org_id}</span>. Leave password blank to keep it unchanged.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1">
            <Label>New password</Label>
            <Input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="unchanged" />
          </div>
          <div className="space-y-1">
            <Label>Max vCPUs (0 = unbounded)</Label>
            <Input type="number" min={0} value={maxVCPUs} onChange={(e) => setMaxVCPUs(e.target.value)} />
          </div>
          <div className="flex items-center gap-6">
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={passthrough} onCheckedChange={setPassthrough} /> Passthrough
            </label>
            <label className="flex items-center gap-2 text-sm">
              <Switch checked={iceberg} onCheckedChange={setIceberg} /> Default catalog: iceberg
            </label>
          </div>
          {err && <p className="text-xs text-destructive">{err}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button size="sm" onClick={submit} disabled={update.isPending}>
            {update.isPending ? "Saving…" : "Save"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function SecretsDialog({ user, onClose }: { user: OrgUser; onClose: () => void }) {
  const secrets = useUserSecrets(user.org_id, user.username);
  const del = useDeleteUserSecret();

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <KeyRound className="h-4 w-4" /> Persistent secrets
          </DialogTitle>
          <DialogDescription>
            <span className="font-mono">{user.username}</span> @ <span className="font-mono">{user.org_id}</span>.
            Secret material is never exposed — only names and timestamps.
          </DialogDescription>
        </DialogHeader>
        <div className="max-h-80 overflow-y-auto">
          {secrets.isLoading ? (
            <p className="p-4 text-sm text-muted-foreground">Loading…</p>
          ) : (secrets.data?.length ?? 0) === 0 ? (
            <p className="p-4 text-sm text-muted-foreground">No persistent secrets.</p>
          ) : (
            <div className="space-y-1.5">
              {secrets.data!.map((s) => (
                <div
                  key={s.secret_name}
                  className="flex items-center justify-between rounded-md border border-border bg-background/40 px-3 py-2"
                >
                  <div>
                    <div className="font-mono text-sm">{s.secret_name}</div>
                    <div className="text-[11px] text-muted-foreground">updated {fmtTime(s.updated_at)}</div>
                  </div>
                  <AdminGate>
                    <Button
                      variant="ghost"
                      size="icon"
                      disabled={del.isPending}
                      onClick={() =>
                        del.mutate({ org: user.org_id, username: user.username, name: s.secret_name })
                      }
                    >
                      <Trash2 className="h-4 w-4 text-destructive" />
                    </Button>
                  </AdminGate>
                </div>
              ))}
            </div>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
