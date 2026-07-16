import { useMemo, useState } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import { Info, Pencil, Plus, Search, ShieldAlert, ShieldCheck, Trash2 } from "lucide-react";
import { Link } from "react-router-dom";
import { PageBody, PageHeader } from "@/components/AppShell";
import { DataTable } from "@/components/DataTable";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
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
import { useDeleteOperator, useOperators, useUpsertOperator } from "@/hooks/useApi";
import { useIdentity } from "@/components/IdentityProvider";
import { fmtTime } from "@/lib/format";
import type { Operator, Role } from "@/types/api";

// RoleBadge renders an operator's console role with a consistent color: admin
// (full control) is highlighted, viewer (read-only) is muted.
function RoleBadge({ role }: { role: Role }) {
  return role === "admin" ? (
    <Badge variant="default">admin</Badge>
  ) : (
    <Badge variant="muted">viewer</Badge>
  );
}

// What each role can do — shown in the add/edit dialogs so the choice is
// unambiguous at the point of granting access.
const ROLE_HELP: Record<Role, string> = {
  admin: "Full control — manage orgs, users, operators, impersonate, and every other write action.",
  viewer: "Read-only — can view all console pages but cannot change anything.",
};

export function Operators() {
  const { isAdmin, me } = useIdentity();
  const operators = useOperators();
  const [filter, setFilter] = useState("");
  const [adding, setAdding] = useState(false);
  const [editing, setEditing] = useState<Operator | null>(null);
  const [deleting, setDeleting] = useState<Operator | null>(null);

  const myEmail = (me?.email ?? "").toLowerCase();

  const columns = useMemo<ColumnDef<Operator, any>[]>(
    () => [
      {
        accessorKey: "email",
        header: "Email",
        cell: ({ getValue }) => {
          const email = String(getValue());
          const isSelf = email.toLowerCase() === myEmail;
          return (
            <span className="flex items-center gap-2">
              <span className="font-mono text-xs font-medium">{email}</span>
              {isSelf && <Badge variant="outline">you</Badge>}
            </span>
          );
        },
      },
      {
        accessorKey: "role",
        header: "Console role",
        cell: ({ getValue }) => <RoleBadge role={getValue() as Role} />,
      },
      {
        accessorKey: "added_by",
        header: "Granted by",
        cell: ({ getValue }) => {
          const v = String(getValue() || "");
          return v ? <span className="font-mono text-xs text-muted-foreground">{v}</span> : <span className="text-muted-foreground">—</span>;
        },
      },
      {
        accessorKey: "updated_at",
        header: "Updated",
        cell: ({ getValue }) => <span className="text-xs text-muted-foreground">{fmtTime(getValue() as string)}</span>,
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <div className="-my-1 flex justify-end gap-1">
            <AdminGate>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                title="Change role"
                onClick={() => setEditing(row.original)}
              >
                <Pencil className="h-3.5 w-3.5" />
              </Button>
            </AdminGate>
            <AdminGate>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6"
                title="Revoke access"
                onClick={() => setDeleting(row.original)}
              >
                <Trash2 className="h-3.5 w-3.5 text-destructive" />
              </Button>
            </AdminGate>
          </div>
        ),
      },
    ],
    [myEmail],
  );

  // Console access is admin-only to view (the GET is RequireAdmin). Match the
  // other admin-only pages: show a clear notice instead of an error for viewers.
  if (!isAdmin) {
    return (
      <>
        <PageHeader title="Operators" description="Console access control." />
        <PageBody>
          <EmptyState
            icon={<ShieldAlert className="h-6 w-6 text-warning" />}
            title="Admin only"
            description="Managing console operators requires the admin role."
          />
        </PageBody>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title="Operators"
        description="Who can sign in to this admin console (the duckgres control plane) — PostHog staff, not customer database accounts."
        actions={
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter…" className="w-56 pl-8" />
            </div>
            <AdminGate>
              <Button size="sm" onClick={() => setAdding(true)}>
                <Plus className="h-4 w-4" /> Add operator
              </Button>
            </AdminGate>
          </div>
        }
      />
      <PageBody>
        <div className="mb-3 flex items-start gap-2 rounded-md border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            <span className="font-medium text-foreground">Operators</span> are internal staff granted access to
            this console via Google SSO. <span className="font-medium text-foreground">Admins</span> can change
            anything; <span className="font-medium text-foreground">viewers</span> are read-only. Looking for the
            per-org database logins your customers use to connect to their warehouse? Those live under{" "}
            <Link to="/users" className="text-primary hover:underline">
              Org Users
            </Link>
            .
          </span>
        </div>
        <Card className="overflow-hidden">
          {operators.isLoading ? (
            <TableSkeleton cols={5} />
          ) : operators.isError ? (
            <ErrorState error={operators.error} onRetry={() => operators.refetch()} />
          ) : (
            <DataTable
              data={operators.data ?? []}
              columns={columns}
              globalFilter={filter}
              onGlobalFilterChange={setFilter}
              initialSorting={[{ id: "role", desc: false }]}
              empty={
                <EmptyState
                  icon={<ShieldCheck className="h-6 w-6" />}
                  title="No operators"
                  description="No one has been granted console access yet."
                />
              }
            />
          )}
        </Card>
      </PageBody>

      {adding && <AddOperatorDialog onClose={() => setAdding(false)} />}
      {editing && <EditRoleDialog operator={editing} isSelf={editing.email.toLowerCase() === myEmail} onClose={() => setEditing(null)} />}
      {deleting && <DeleteOperatorDialog operator={deleting} isSelf={deleting.email.toLowerCase() === myEmail} onClose={() => setDeleting(null)} />}
    </>
  );
}

function AddOperatorDialog({ onClose }: { onClose: () => void }) {
  const upsert = useUpsertOperator();
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("viewer");
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    setErr(null);
    try {
      await upsert.mutateAsync({ email: email.trim(), role });
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Failed to add operator");
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add operator</DialogTitle>
          <DialogDescription>
            Grant a PostHog staff member access to this console. They sign in with their @posthog.com Google
            account; the role takes effect on their next request.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1">
            <Label>Email</Label>
            <Input
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="person@posthog.com"
              className="font-mono"
              autoFocus
            />
          </div>
          <div className="space-y-1">
            <Label>Console role</Label>
            <Select value={role} onValueChange={(v) => setRole(v as Role)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="viewer">viewer</SelectItem>
                <SelectItem value="admin">admin</SelectItem>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">{ROLE_HELP[role]}</p>
          </div>
          {err && <p className="text-xs text-destructive">{err}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button size="sm" onClick={submit} disabled={upsert.isPending || !email.trim()}>
            {upsert.isPending ? "Adding…" : "Add operator"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EditRoleDialog({ operator, isSelf, onClose }: { operator: Operator; isSelf: boolean; onClose: () => void }) {
  const upsert = useUpsertOperator();
  const [role, setRole] = useState<Role>(operator.role);
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    setErr(null);
    try {
      await upsert.mutateAsync({ email: operator.email, role });
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Failed to change role");
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            Change role for <span className="font-mono">{operator.email}</span>
          </DialogTitle>
          <DialogDescription>Sets this operator's access level for the admin console.</DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1">
            <Label>Console role</Label>
            <Select value={role} onValueChange={(v) => setRole(v as Role)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="viewer">viewer</SelectItem>
                <SelectItem value="admin">admin</SelectItem>
              </SelectContent>
            </Select>
            <p className="text-xs text-muted-foreground">{ROLE_HELP[role]}</p>
          </div>
          {isSelf && role === "viewer" && operator.role === "admin" && (
            <p className="text-xs text-warning">
              This is your own account — demoting yourself to viewer removes your ability to make further
              changes.
            </p>
          )}
          {err && <p className="text-xs text-destructive">{err}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button size="sm" onClick={submit} disabled={upsert.isPending || role === operator.role}>
            {upsert.isPending ? "Saving…" : "Save role"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function DeleteOperatorDialog({ operator, isSelf, onClose }: { operator: Operator; isSelf: boolean; onClose: () => void }) {
  const del = useDeleteOperator();
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    setErr(null);
    try {
      await del.mutateAsync(operator.email);
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Failed to revoke access");
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            Revoke access for <span className="font-mono">{operator.email}</span>?
          </DialogTitle>
          <DialogDescription>
            Removes <span className="font-mono">{operator.email}</span> from the console access list. They can
            no longer sign in (a fresh SSO login would re-provision them as a viewer). This does not affect any
            org database account.
          </DialogDescription>
        </DialogHeader>
        {isSelf && (
          <p className="text-xs text-warning">
            This is your own account — you will lose access to this console.
          </p>
        )}
        {err && <p className="text-xs text-destructive">{err}</p>}
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="destructive" size="sm" onClick={submit} disabled={del.isPending}>
            {del.isPending ? "Revoking…" : "Revoke access"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
