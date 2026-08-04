import { createContext, useContext, type ReactNode } from "react";
import { useMe } from "@/hooks/useApi";
import { ApiError } from "@/lib/api";
import type { Me, Role } from "@/types/api";

interface IdentityState {
  me: Me | null;
  role: Role;
  isAdmin: boolean;
  loading: boolean;
  unauthorized: boolean; // 401 from /me
  error: Error | null;
}

const IdentityContext = createContext<IdentityState>({
  me: null,
  role: "viewer",
  isAdmin: false,
  loading: true,
  unauthorized: false,
  error: null,
});

export function IdentityProvider({ children }: { children: ReactNode }) {
  const q = useMe({ retry: false });
  const unauthorized = q.error instanceof ApiError && q.error.status === 401;
  // If /me isn't wired up (404) we fall back to viewer rather than locking the
  // operator out — the backend still enforces auth on every mutating call.
  const role: Role = q.data?.role ?? "viewer";

  const value: IdentityState = {
    me: q.data ?? null,
    role,
    isAdmin: role === "admin",
    loading: q.isLoading,
    unauthorized,
    error: q.error as Error | null,
  };

  return <IdentityContext.Provider value={value}>{children}</IdentityContext.Provider>;
}

// eslint-disable-next-line react-refresh/only-export-components
export function useIdentity() {
  return useContext(IdentityContext);
}
