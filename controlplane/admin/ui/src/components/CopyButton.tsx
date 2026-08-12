import { useState } from "react";
import { Check, Copy } from "lucide-react";

// CopyButton is the small inline clipboard affordance used next to ids an
// operator wants to grab verbatim (org id, trace id). Shows a check briefly
// on success. Keeps the visual noise low — ghost icon button, mono-friendly.
export function CopyButton({ value, label }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  if (!value) return null;
  return (
    <button
      type="button"
      className="inline-flex shrink-0 items-center text-muted-foreground hover:text-foreground"
      title={label ?? `Copy ${value}`}
      aria-label={label ?? `Copy ${value}`}
      onClick={(e) => {
        e.stopPropagation();
        e.preventDefault();
        void navigator.clipboard?.writeText(value).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1200);
        });
      }}
    >
      {copied ? <Check className="h-3 w-3 text-success" /> : <Copy className="h-3 w-3" />}
    </button>
  );
}
