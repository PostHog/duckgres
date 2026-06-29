import { useEffect, useMemo, useState } from "react";
import { Database, Search } from "lucide-react";
import { PageHeader } from "@/components/AppShell";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { JsonValue, cellToString } from "@/components/JsonView";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useModel, useModels } from "@/hooks/useApi";
import { cn } from "@/lib/utils";
import { fmtInt } from "@/lib/format";

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
          <ModelTable model={selected} filter={filter} setFilter={setFilter} onRow={setDetail} />
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
