import { useEffect, useMemo, useState } from "react";
import { Database, Plus, Search, ShieldCheck, Trash2 } from "lucide-react";
import { PageHeader } from "@/components/AppShell";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { AdminGate } from "@/components/AdminOnly";
import { JsonValue, cellToString } from "@/components/JsonView";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  useDeleteOperator,
  useModel,
  useModels,
  useOperators,
  useUpsertOperator,
} from "@/hooks/useApi";
import { cn } from "@/lib/utils";
import { ApiError } from "@/lib/api";
import { fmtInt, fmtTime } from "@/lib/format";
import type { Operator, Role } from "@/types/api";

export function ConfigStore() {
  const models = useModels();
  const [selected, setSelected] = useState<string | undefined>();
  const [filter, setFilter] = useState("");
  const [detail, setDetail] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (!selected && models.data && models.data.length > 0) {
      setSelected(models.data[0].key);
    }
  }, [models.data, selected]);

  const grouped = useMemo(() => {
    const g = new Map<string, typeof models.data>();
    for (const m of models.data ?? []) {
      const arr = g.get(m.group) ?? [];
      arr.push(m);
      g.set(m.group, arr);
    }
    return [...g.entries()];
  }, [models.data]);

  return (
    <>
      <PageHeader title="Config Store" description="Read-only explorer over every config-store model." />
      <div className="flex h-[calc(100vh-3.5rem-4rem)] min-h-0">
        {/* sidebar */}
        <div className="w-60 shrink-0 overflow-y-auto border-r border-border p-2">
          {models.isLoading ? (
            <LoadingState />
          ) : models.isError ? (
            <ErrorState error={models.error} />
          ) : grouped.length === 0 ? (
            <p className="p-3 text-xs text-muted-foreground">No models.</p>
          ) : (
            grouped.map(([group, items]) => (
              <div key={group} className="mb-3">
                <p className="px-2 py-1 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                  {group}
                </p>
                {(items ?? []).map((m) => (
                  <button
                    key={m.key}
                    onClick={() => setSelected(m.key)}
                    className={cn(
                      "flex w-full items-center justify-between rounded-md px-2 py-1.5 text-sm transition-colors",
                      selected === m.key
                        ? "bg-primary/15 text-primary"
                        : "text-muted-foreground hover:bg-accent hover:text-foreground",
                    )}
                  >
                    <span>{m.label}</span>
                    <Badge variant="muted" className="tabular-nums">
                      {m.count < 0 ? "?" : fmtInt(m.count)}
                    </Badge>
                  </button>
                ))}
              </div>
            ))
          )}
        </div>

        {/* table */}
        <div className="flex min-w-0 flex-1 flex-col">
          {selected === "operators" ? (
            <OperatorsManager />
          ) : (
            <ModelTable model={selected} filter={filter} setFilter={setFilter} onRow={setDetail} />
          )}
        </div>
      </div>

      <Dialog open={!!detail} onOpenChange={(o) => !o && setDetail(null)}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Row detail</DialogTitle>
            <DialogDescription>Full record from the config store.</DialogDescription>
          </DialogHeader>
          <div className="max-h-[60vh] space-y-2 overflow-y-auto">
            {detail &&
              Object.entries(detail).map(([k, v]) => (
                <div key={k} className="grid grid-cols-[10rem_1fr] gap-3 border-b border-border/60 pb-2">
                  <span className="font-mono text-xs text-muted-foreground">{k}</span>
                  <JsonValue value={v} />
                </div>
              ))}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}

function ModelTable({
  model,
  filter,
  setFilter,
  onRow,
}: {
  model: string | undefined;
  filter: string;
  setFilter: (v: string) => void;
  onRow: (r: Record<string, unknown>) => void;
}) {
  const q = useModel(model);

  const filtered = useMemo(() => {
    const rows = q.data?.rows ?? [];
    if (!filter) return rows;
    const f = filter.toLowerCase();
    return rows.filter((r) =>
      Object.values(r).some((v) => cellToString(v).toLowerCase().includes(f)),
    );
  }, [q.data, filter]);

  if (!model) {
    return (
      <EmptyState icon={<Database className="h-6 w-6" />} title="Select a model" description="Pick a model from the sidebar." />
    );
  }

  const columns = q.data?.columns ?? [];

  return (
    <>
      <div className="flex items-center justify-between gap-2 border-b border-border px-4 py-2.5">
        <div className="flex items-center gap-2">
          <span className="font-mono text-sm">{q.data?.table ?? model}</span>
          {q.data && (
            <Badge variant="secondary">
              {fmtInt(q.data.count)} rows{q.data.truncated ? " (truncated)" : ""}
            </Badge>
          )}
        </div>
        <div className="relative">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter rows…" className="w-64 pl-8" />
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        {q.isLoading ? (
          <LoadingState />
        ) : q.isError ? (
          <ErrorState error={q.error} onRetry={() => q.refetch()} />
        ) : filtered.length === 0 ? (
          <EmptyState title="No rows" description={filter ? "No rows match the filter." : "This table is empty."} />
        ) : (
          <Table>
            <TableHeader className="sticky top-0 z-10 bg-card">
              <TableRow className="hover:bg-transparent">
                {columns.map((c) => (
                  <TableHead key={c} className="whitespace-nowrap">
                    {c}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((row, i) => (
                <TableRow key={i} className="cursor-pointer [&>td]:py-1.5" onClick={() => onRow(row)}>
                  {columns.map((c) => (
                    <TableCell key={c} className="max-w-xs truncate font-mono text-xs">
                      <CellValue value={row[c]} />
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </div>
    </>
  );
}

function CellValue({ value }: { value: unknown }) {
  if (value == null || value === "") return <span className="text-muted-foreground">—</span>;
  if (typeof value === "object") {
    return <span className="text-muted-foreground">{JSON.stringify(value).slice(0, 80)}</span>;
  }
  if (typeof value === "boolean") {
    return <span className={value ? "text-success" : "text-muted-foreground"}>{String(value)}</span>;
  }
  return <span title={String(value)}>{String(value)}</span>;
}

// Pull a readable message off any thrown error. The backend's last-admin guard
// returns 409 with a JSON `error` field, surfaced here verbatim.
function errMsg(e: unknown): string {
  if (e instanceof ApiError) return e.message;
  if (e instanceof Error) return e.message;
  return String(e);
}

function RoleBadge({ role }: { role: Role }) {
  return (
    <Badge variant={role === "admin" ? "default" : "muted"}>{role}</Badge>
  );
}

// Dedicated management view for the "operators" model. Replaces the generic
// read-only ModelTable so admins can add/remove operators and change roles.
// Renders under the "Admin" sidebar group of the config-store explorer.
function OperatorsManager() {
  const ops = useOperators();
  const upsert = useUpsertOperator();
  const del = useDeleteOperator();

  const [filter, setFilter] = useState("");
  const [adding, setAdding] = useState(false);
  const [deleting, setDeleting] = useState<Operator | null>(null);
  // Inline error surface for mutation failures (e.g. 409 last-admin guard).
  const [actionError, setActionError] = useState<string | null>(null);

  const rows = useMemo(() => ops.data ?? [], [ops.data]);
  const filtered = useMemo(() => {
    if (!filter) return rows;
    const f = filter.toLowerCase();
    return rows.filter(
      (o) =>
        o.email.toLowerCase().includes(f) ||
        o.role.toLowerCase().includes(f) ||
        o.added_by.toLowerCase().includes(f),
    );
  }, [rows, filter]);

  const changeRole = async (op: Operator, role: Role) => {
    if (role === op.role) return;
    setActionError(null);
    try {
      await upsert.mutateAsync({ email: op.email, role });
    } catch (e) {
      setActionError(errMsg(e));
    }
  };

  const confirmDelete = async () => {
    if (!deleting) return;
    setActionError(null);
    try {
      await del.mutateAsync(deleting.email);
      setDeleting(null);
    } catch (e) {
      setActionError(errMsg(e));
    }
  };

  return (
    <>
      <div className="flex items-center justify-between gap-2 border-b border-border px-4 py-2.5">
        <div className="flex items-center gap-2">
          <ShieldCheck className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Operators</span>
          <Badge variant="secondary">{fmtInt(rows.length)}</Badge>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Filter operators…"
              className="w-64 pl-8"
            />
          </div>
          <AdminGate>
            <Button size="sm" onClick={() => setAdding(true)}>
              <Plus className="h-4 w-4" /> Add operator
            </Button>
          </AdminGate>
        </div>
      </div>

      {actionError && (
        <div className="border-b border-destructive/30 bg-destructive/10 px-4 py-2 text-xs text-destructive">
          {actionError}
        </div>
      )}

      <div className="min-h-0 flex-1 overflow-auto">
        {ops.isLoading ? (
          <LoadingState />
        ) : ops.isError ? (
          <ErrorState error={ops.error} onRetry={() => ops.refetch()} />
        ) : filtered.length === 0 ? (
          <EmptyState
            icon={<ShieldCheck className="h-6 w-6" />}
            title="No operators"
            description={filter ? "No operators match the filter." : "No operators are configured."}
          />
        ) : (
          <Table>
            <TableHeader className="sticky top-0 z-10 bg-card">
              <TableRow className="hover:bg-transparent">
                <TableHead>Email</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>Added by</TableHead>
                <TableHead>Updated</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((op) => (
                <TableRow key={op.email} className="[&>td]:py-1.5">
                  <TableCell className="font-mono text-xs font-medium">{op.email}</TableCell>
                  <TableCell>
                    <RoleBadge role={op.role} />
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {op.added_by || "—"}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {fmtTime(op.updated_at)}
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center justify-end gap-2">
                      <AdminGate reason="Requires the admin role">
                        <Select
                          value={op.role}
                          onValueChange={(v) => changeRole(op, v as Role)}
                          disabled={upsert.isPending}
                        >
                          <SelectTrigger className="h-8 w-28">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="admin">admin</SelectItem>
                            <SelectItem value="viewer">viewer</SelectItem>
                          </SelectContent>
                        </Select>
                      </AdminGate>
                      <AdminGate>
                        <Button
                          variant="ghost"
                          size="icon"
                          title="Remove operator"
                          onClick={() => {
                            setActionError(null);
                            setDeleting(op);
                          }}
                        >
                          <Trash2 className="h-4 w-4 text-destructive" />
                        </Button>
                      </AdminGate>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </div>

      {adding && (
        <AddOperatorDialog
          existing={rows}
          onClose={() => setAdding(false)}
        />
      )}

      <Dialog open={!!deleting} onOpenChange={(o) => !o && setDeleting(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Remove operator?</DialogTitle>
            <DialogDescription>
              Revokes console access for{" "}
              <span className="font-mono">{deleting?.email}</span>. This cannot be undone.
            </DialogDescription>
          </DialogHeader>
          {actionError && <p className="text-xs text-destructive">{actionError}</p>}
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setDeleting(null)}>
              Cancel
            </Button>
            <Button variant="destructive" size="sm" disabled={del.isPending} onClick={confirmDelete}>
              {del.isPending ? "Removing…" : "Remove"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function AddOperatorDialog({
  existing,
  onClose,
}: {
  existing: Operator[];
  onClose: () => void;
}) {
  const upsert = useUpsertOperator();
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("viewer");
  const [err, setErr] = useState<string | null>(null);

  const trimmed = email.trim();
  const isUpdate = existing.some((o) => o.email.toLowerCase() === trimmed.toLowerCase());

  const submit = async () => {
    setErr(null);
    try {
      await upsert.mutateAsync({ email: trimmed, role });
      onClose();
    } catch (e) {
      setErr(errMsg(e));
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add operator</DialogTitle>
          <DialogDescription>
            Grant an SSO identity access to this console. Adding an existing email updates its role.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1">
            <Label>Email</Label>
            <Input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="someone@posthog.com"
              className="font-mono"
            />
          </div>
          <div className="space-y-1">
            <Label>Role</Label>
            <Select value={role} onValueChange={(v) => setRole(v as Role)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="admin">admin</SelectItem>
                <SelectItem value="viewer">viewer</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {isUpdate && (
            <p className="text-xs text-muted-foreground">
              This email already exists — its role will be updated.
            </p>
          )}
          {err && <p className="text-xs text-destructive">{err}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button size="sm" onClick={submit} disabled={upsert.isPending || !trimmed}>
            {upsert.isPending ? "Saving…" : isUpdate ? "Update role" : "Add operator"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
