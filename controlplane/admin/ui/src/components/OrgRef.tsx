import { CopyButton } from "@/components/CopyButton";

// OrgRef renders an org reference the way a human actually reads it: the
// database name (or alias) as the primary, the raw org id as a small subline.
// When only the id is known (org lookup unresolved) it degrades to just the
// id. Use inside table cells where a bare org-id string is all the row has.
//
// Deliberately single-column-friendly: the id subline is compact (11px,
// muted, truncated) so rows stay one-line-tall for scanning, with the full
// id reachable via title + copy button.
export function OrgRef({ id, label, copyable = true }: { id: string; label?: string; copyable?: boolean }) {
  const text = label && label !== "" ? label : id;
  const differ = text !== id;
  return (
    <span className="block min-w-0" title={differ ? `${text} • ${id}` : id}>
      <span className={`block truncate text-xs ${differ ? "font-medium text-foreground" : "font-mono"}`}>{text}</span>
      {differ && (
        <span className="flex items-center gap-1">
          <span className="block truncate font-mono text-[11px] text-muted-foreground">{id}</span>
          {copyable && <CopyButton value={id} />}
        </span>
      )}
    </span>
  );
}
