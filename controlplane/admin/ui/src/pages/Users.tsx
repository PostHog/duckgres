import { useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { KeyRound, Pencil, Plus, Search, Trash2, Users as UsersIcon } from "lucide-react";
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
  useUpdateUser,
  useUserSecrets,
  useUsers,
} from "@/hooks/useApi";
import { fmtTime } from "@/lib/format";
import type { OrgUser } from "@/types/api";

export function UsersPage() {
  const users = useUsers();
  const [filter, setFilter] = useState("");
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<OrgUser | null>(null);
  const [deleting, setDeleting] = useState<OrgUser | null>(null);
  const [secretsFor, setSecretsFor] = useState<OrgUser | null>(null);

  const delUser = useDeleteUser();

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
        accessorKey: "created_at",
        header: "Created",
        cell: ({ getValue }) => <span className="text-xs text-muted-foreground">{fmtTime(getValue() as string)}</span>,
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <div className="flex justify-end gap-1">
            <Button variant="ghost" size="icon" title="Persistent secrets" onClick={() => setSecretsFor(row.original)}>
              <KeyRound className="h-4 w-4" />
            </Button>
            <AdminGate>
              <Button variant="ghost" size="icon" title="Edit" onClick={() => setEditing(row.original)}>
                <Pencil className="h-4 w-4" />
              </Button>
            </AdminGate>
            <AdminGate>
              <Button variant="ghost" size="icon" title="Delete" onClick={() => setDeleting(row.original)}>
                <Trash2 className="h-4 w-4 text-destructive" />
              </Button>
            </AdminGate>
          </div>
        ),
      },
    ],
    [],
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
            <TableSkeleton cols={6} />
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
