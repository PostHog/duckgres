import { QueryClient } from "@tanstack/react-query";
import { ApiError } from "./api";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5_000,
      refetchOnWindowFocus: false,
      retry: (failureCount, error) => {
        // Never retry auth/permission/not-found — those are terminal answers,
        // not flakes. Retry transient errors a couple of times.
        if (error instanceof ApiError && [401, 403, 404].includes(error.status)) {
          return false;
        }
        return failureCount < 2;
      },
    },
  },
});

// Poll intervals (ms) for live data, centralized so they're easy to tune.
export const POLL = {
  fast: 3_000, // live queries / sessions
  normal: 5_000, // workers / status
  slow: 15_000, // fleet rollups
} as const;
