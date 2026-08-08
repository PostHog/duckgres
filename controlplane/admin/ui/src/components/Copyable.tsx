import { useState } from "react";
import { Check, Copy } from "lucide-react";

// A labelled value with a copy-to-clipboard button. Used for once-only
// credentials (the provision response, a password reset) where re-reading the
// value is impossible, so copying has to be one click and obviously available.
export function Copyable({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="flex items-center gap-1 break-all font-mono text-xs">
        {value}
        <button
          type="button"
          className="text-muted-foreground hover:text-foreground"
          title={`Copy ${label}`}
          aria-label={`Copy ${label}`}
          onClick={() => {
            void navigator.clipboard?.writeText(value).then(() => {
              setCopied(true);
              setTimeout(() => setCopied(false), 1200);
            });
          }}
        >
          {copied ? <Check className="h-3 w-3 text-success" /> : <Copy className="h-3 w-3" />}
        </button>
      </span>
    </div>
  );
}
