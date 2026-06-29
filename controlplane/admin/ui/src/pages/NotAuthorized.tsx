import { ShieldX } from "lucide-react";

export function NotAuthorized() {
  return (
    <div className="flex h-screen flex-col items-center justify-center gap-4 bg-background text-center">
      <div className="rounded-full bg-destructive/15 p-4 text-destructive">
        <ShieldX className="h-8 w-8" />
      </div>
      <div>
        <h1 className="text-xl font-semibold">Not authorized</h1>
        <p className="mt-1 max-w-md text-sm text-muted-foreground">
          The upstream proxy did not provide an authenticated identity for this session.
          Sign in through your SSO provider and reload, or contact an administrator.
        </p>
      </div>
    </div>
  );
}
