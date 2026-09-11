import { useSearchParams } from "react-router-dom";
import { useTrinoCells } from "@/hooks/useApi";

export function TrinoCellPicker() {
  const cells = useTrinoCells();
  const [params, setParams] = useSearchParams();
  const entries = cells.data?.cells ?? [];
  const selected = params.get("cell") ?? (entries.some((cell) => cell.id === "legacy") ? "legacy" : "");
  if (!entries.length) return null;
  return (
    <select
      aria-label="Trino cell"
      className="h-8 rounded border bg-background px-2 text-sm"
      value={selected}
      onChange={(event) => setParams((previous) => {
        const next = new URLSearchParams(previous);
        next.set("cell", event.target.value);
        return next;
      })}
    >
      <option value="" disabled>Choose a cell</option>
      {selected && !entries.some((cell) => cell.id === selected) && <option value={selected} disabled>Unknown cell: {selected}</option>}
      {entries.map((cell) => <option key={cell.id} value={cell.id}>{cell.id}</option>)}
    </select>
  );
}
