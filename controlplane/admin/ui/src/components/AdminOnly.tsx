import type { ReactElement } from "react";
import { cloneElement } from "react";
import { useIdentity } from "@/components/IdentityProvider";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

// Wrap an interactive control (button) that requires the admin role. Viewers
// see it disabled with an explanatory tooltip rather than a hidden affordance,
// so the UI is discoverable but the action is gated (the backend enforces 403).
export function AdminGate({
  children,
  reason = "Requires the admin role",
}: {
  children: ReactElement<{ disabled?: boolean }>;
  reason?: string;
}) {
  const { isAdmin } = useIdentity();
  if (isAdmin) return children;
  const disabled = cloneElement(children, { disabled: true });
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-not-allowed">{disabled}</span>
      </TooltipTrigger>
      <TooltipContent>{reason}</TooltipContent>
    </Tooltip>
  );
}
