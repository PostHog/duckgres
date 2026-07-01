import { cn } from "@/lib/utils";

// Renders any value compactly for table cells / detail panels. Objects/arrays
// pretty-print as monospace JSON; scalars render inline.
export function cellToString(v: unknown): string {
  if (v == null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function JsonValue({ value, className }: { value: unknown; className?: string }) {
  if (value == null || value === "") {
    return <span className="text-muted-foreground">—</span>;
  }
  if (typeof value === "boolean") {
    return <span className={cn("font-mono", value ? "text-success" : "text-muted-foreground")}>{String(value)}</span>;
  }
  if (typeof value === "object") {
    return (
      <pre className={cn("max-h-64 overflow-auto whitespace-pre-wrap break-all rounded bg-muted/40 p-2 font-mono text-xs", className)}>
        {JSON.stringify(value, null, 2)}
      </pre>
    );
  }
  return <span className={cn("font-mono text-xs", className)}>{String(value)}</span>;
}
