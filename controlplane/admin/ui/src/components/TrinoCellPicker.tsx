import { useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import { useTrinoCells } from "@/hooks/useApi";

export function TrinoCellPicker() {
  const cells = useTrinoCells();
  const [params, setParams] = useSearchParams();
  const entries = cells.data?.cells ?? [];
  const selected = params.get("cell") ?? "";
  const fallback = entries[0]?.id ?? "";

  // Landing on a Trino page with no cell used to render an empty view until
  // the operator picked one. Default to the first registered cell instead.
  // This only fills an ABSENT parameter: an explicit cell in the URL, even
  // one the server no longer lists, is never replaced, so a shared link keeps
  // pointing where it was sent. replace keeps the defaulting out of history.
  useEffect(() => {
    if (selected || !fallback) return;
    setParams(
      (previous) => {
        if (previous.get("cell")) return previous;
        const next = new URLSearchParams(previous);
        next.set("cell", fallback);
        return next;
      },
      { replace: true },
    );
  }, [selected, fallback, setParams]);

  if (!entries.length) return null;
  return (
    <select
      aria-label="Trino cell"
      className="h-8 rounded border bg-background px-2 text-sm"
      value={selected || fallback}
      onChange={(event) => setParams((previous) => {
        const next = new URLSearchParams(previous);
        next.set("cell", event.target.value);
        return next;
      })}
    >
      {selected && !entries.some((cell) => cell.id === selected) && <option value={selected} disabled>Unknown cell: {selected}</option>}
      {entries.map((cell) => <option key={cell.id} value={cell.id}>{cell.id}</option>)}
    </select>
  );
}
