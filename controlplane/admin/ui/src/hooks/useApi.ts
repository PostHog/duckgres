// TanStack Query hooks wrapping the typed api client.
//
// Read hooks for endpoints that may not exist yet (`*Optional`) swallow 404 and
// return an empty value so pages render an empty state instead of crashing.

import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseQueryOptions,
} from "@tanstack/react-query";
import { api, ApiError } from "@/lib/api";
import { POLL } from "@/lib/query";
import { useIdentity } from "@/components/IdentityProvider";
import type {
  AuditEntry,
  ClusterStatus,
  ClusterSummary,
  CreateUserBody,
  DucklingDriftResponse,
  DucklingMetadataResponse,
  ErrorEntry,
  ErrorFilters,
  FleetStat,
  ImpersonateBody,
  ManagedWarehouse,
  Me,
  MetricsPanels,
  ModelListing,
  ModelSummary,
  Operator,
  Org,
  OrgUpdate,
  OrgUser,
  OrgUserSecret,
  PromRangeResponse,
  QueryDetail,
  RunningQuery,
  SessionStatus,
  UpdateUserBody,
  WorkerStatus,
} from "@/types/api";

// Treat 404 as "endpoint not wired yet → empty"; rethrow everything else.
function tolerate404<T>(fallback: T) {
  return tolerateStatus<T>(fallback, 404);
}

// Swallow the given HTTP status codes, returning a fallback; rethrow the rest.
// (Metrics return 503 when Prometheus is not configured — treat like empty.)
function tolerateStatus<T>(fallback: T, ...statuses: number[]) {
  return async (p: Promise<T>): Promise<T> => {
    try {
      return await p;
    } catch (e) {
      if (e instanceof ApiError && statuses.includes(e.status)) return fallback;
      throw e;
    }
  };
}

// ---- identity ----

export function useMe(opts?: Partial<UseQueryOptions<Me>>) {
  return useQuery({
    queryKey: ["me"],
    queryFn: () => api.me(),
    staleTime: 60_000,
    ...opts,
  });
}

// ---- overview ----

export function useClusterStatus() {
  return useQuery<ClusterStatus>({
    queryKey: ["status"],
    queryFn: () => tolerate404<ClusterStatus>({ total_orgs: 0, total_workers: 0, total_sessions: 0, orgs: [] })(api.status()),
    refetchInterval: POLL.normal,
  });
}

// Cluster totals for the admin nav (shown on every page). 404-tolerant: on a
// non-k8s backend / missing endpoint it resolves to zeros rather than erroring.
export function useClusterSummary() {
  return useQuery<ClusterSummary>({
    queryKey: ["cluster-summary"],
    queryFn: () =>
      tolerate404<ClusterSummary>({
        nodes: 0,
        workers: 0,
        worker_cpu_cores: 0,
        worker_mem_gib: 0,
        placeholders: 0,
        placeholder_cpu_cores: 0,
        placeholder_mem_gib: 0,
        pending: 0,
      })(api.clusterSummary()),
    refetchInterval: POLL.normal,
  });
}

// ---- orgs ----

export function useOrgs() {
  return useQuery<Org[]>({
    queryKey: ["orgs"],
    queryFn: () => tolerate404<Org[]>([])(api.listOrgs()),
  });
}

export function useOrg(id: string | undefined) {
  return useQuery<Org>({
    queryKey: ["orgs", id],
    queryFn: () => api.getOrg(id!),
    enabled: !!id,
  });
}

export function useUpdateOrg(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: OrgUpdate) => api.updateOrg(id, body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["orgs"] });
    },
  });
}

export function useDeleteOrg() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.deleteOrg(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["orgs"] }),
  });
}

export function useWarehouse(id: string | undefined) {
  return useQuery<ManagedWarehouse | null>({
    queryKey: ["orgs", id, "warehouse"],
    queryFn: () => tolerate404<ManagedWarehouse | null>(null)(api.getWarehouse(id!)),
    enabled: !!id,
  });
}

export function useUpdateWarehouse(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Partial<ManagedWarehouse>) => api.updateWarehouse(id, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["orgs", id, "warehouse"] }),
  });
}

// POST /orgs/:id/deprovision — asynchronous duckling teardown. Invalidate the
// warehouse (state flips to deleting) and the org queries (warehouse presence
// gates org deletion).
export function useDeprovisionWarehouse(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.deprovisionWarehouse(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["orgs", id, "warehouse"] });
      qc.invalidateQueries({ queryKey: ["orgs"] });
    },
  });
}

// ---- ducklings (admin-only) ----

const EMPTY_DRIFT: DucklingDriftResponse = { available: false, checked: 0, entries: [] };

// GET /ducklings/drift — the endpoint 403s for viewers, so we only fire it for
// admins. Any failure (403/404/503/network) degrades quietly to an empty,
// unavailable report rather than surfacing an error toast.
export function useDucklingDrift() {
  const { isAdmin } = useIdentity();
  return useQuery<DucklingDriftResponse>({
    queryKey: ["ducklings", "drift"],
    queryFn: () =>
      tolerateStatus<DucklingDriftResponse>(EMPTY_DRIFT, 403, 404, 503)(api.getDucklingDrift()).catch(
        () => EMPTY_DRIFT,
      ),
    enabled: isAdmin,
    refetchInterval: POLL.normal,
  });
}

const EMPTY_DUCKLING_METADATA: DucklingMetadataResponse = { available: false, entries: {} };

// Live per-Duckling metadata-store assignment (which cnpg shard each tenant is
// on). Viewer-accessible; a pre-rollout backend without the endpoint → empty.
export function useDucklingsMetadata() {
  return useQuery<DucklingMetadataResponse>({
    queryKey: ["ducklings", "metadata"],
    queryFn: () =>
      tolerateStatus<DucklingMetadataResponse>(EMPTY_DUCKLING_METADATA, 403, 404, 503)(
        api.getDucklingsMetadata(),
      ).catch(() => EMPTY_DUCKLING_METADATA),
    refetchInterval: POLL.slow,
  });
}

// ---- users ----

export function useUsers() {
  return useQuery<OrgUser[]>({
    queryKey: ["users"],
    queryFn: () => tolerate404<OrgUser[]>([])(api.listUsers()),
  });
}

export function useCreateUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateUserBody) => api.createUser(body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["users"] }),
  });
}

export function useUpdateUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; username: string; body: UpdateUserBody }) =>
      api.updateUser(v.org, v.username, v.body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["users"] }),
  });
}

export function useDeleteUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; username: string }) => api.deleteUser(v.org, v.username),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["users"] }),
  });
}

// Per-user kill switch. All three invalidate the live views (sessions/queries/
// workers) plus the user list (so the disabled badge refreshes).
function invalidateUserLive(qc: ReturnType<typeof useQueryClient>) {
  qc.invalidateQueries({ queryKey: ["users"] });
  qc.invalidateQueries({ queryKey: ["sessions"] });
  qc.invalidateQueries({ queryKey: ["queries"] });
  qc.invalidateQueries({ queryKey: ["workers"] });
}

export function useKillUserSessions() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; username: string }) => api.killUserSessions(v.org, v.username),
    onSuccess: () => invalidateUserLive(qc),
  });
}

export function useDisableUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; username: string }) => api.disableUser(v.org, v.username),
    onSuccess: () => invalidateUserLive(qc),
  });
}

export function useEnableUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; username: string }) => api.enableUser(v.org, v.username),
    onSuccess: () => invalidateUserLive(qc),
  });
}

export function useUserSecrets(org: string | undefined, username: string | undefined) {
  return useQuery<OrgUserSecret[]>({
    queryKey: ["users", org, username, "secrets"],
    queryFn: () => tolerate404<OrgUserSecret[]>([])(api.listUserSecrets(org!, username!)),
    enabled: !!org && !!username,
  });
}

export function useDeleteUserSecret() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; username: string; name: string }) =>
      api.deleteUserSecret(v.org, v.username, v.name),
    onSuccess: (_d, v) =>
      qc.invalidateQueries({ queryKey: ["users", v.org, v.username, "secrets"] }),
  });
}

// ---- live / workers ----

export function useQueries() {
  return useQuery<RunningQuery[]>({
    queryKey: ["queries"],
    queryFn: () => tolerate404<RunningQuery[]>([])(api.listQueries()),
    refetchInterval: POLL.fast,
  });
}

// Detail for one in-flight query, addressed by cluster-unique worker id and
// fetched on demand when a row is opened. Keeps refreshing while open so
// progress/elapsed stay live; workerId=null disables. Stops polling once the
// query is gone (404) — no point hammering a finished worker.
export function useQueryDetail(workerId: number | null) {
  return useQuery<QueryDetail>({
    queryKey: ["queryDetail", workerId],
    queryFn: () => api.queryDetail(workerId as number),
    enabled: workerId != null,
    retry: false,
    refetchInterval: (q) => (q.state.error ? false : POLL.fast),
  });
}

export function useSessions() {
  return useQuery<SessionStatus[]>({
    queryKey: ["sessions"],
    queryFn: () => tolerate404<SessionStatus[]>([])(api.listSessions()),
    refetchInterval: POLL.fast,
  });
}

// Recent redacted query errors for the Errors page. Filters are applied
// server-side (after the cross-CP merge) and are part of the query key so a
// filter change refetches. Polls at the normal cadence — errors accrete slower
// than live queries.
export function useErrors(filters: ErrorFilters) {
  return useQuery<ErrorEntry[]>({
    queryKey: ["errors", filters],
    queryFn: () => tolerate404<ErrorEntry[]>([])(api.listErrors(filters)),
    refetchInterval: POLL.normal,
  });
}

export function useCancelSession() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (workerId: number) => api.cancelSession(workerId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sessions"] });
      qc.invalidateQueries({ queryKey: ["queries"] });
      qc.invalidateQueries({ queryKey: ["workers"] });
    },
  });
}

// GET /workers — only session-holding workers (status active/idle).
export function useWorkers() {
  return useQuery<WorkerStatus[]>({
    queryKey: ["workers"],
    queryFn: () => tolerate404<WorkerStatus[]>([])(api.listWorkers()),
    refetchInterval: POLL.normal,
  });
}

// GET /workers/fleet — aggregated counts by lifecycle state.
export function useFleet() {
  return useQuery<FleetStat[]>({
    queryKey: ["fleet"],
    queryFn: () => tolerate404<FleetStat[]>([])(api.fleet()),
    refetchInterval: POLL.normal,
  });
}

// ---- metrics ----

export function useMetricsPanels() {
  return useQuery<MetricsPanels>({
    queryKey: ["metrics", "panels"],
    queryFn: () =>
      tolerateStatus<MetricsPanels>({ panels: [], configured: false }, 404, 503)(api.metricsPanels()),
    staleTime: 60_000,
  });
}

const EMPTY_RANGE: PromRangeResponse = {
  status: "success",
  data: { resultType: "matrix", result: [] },
};

export function useMetricRange(expr: string, org: string | undefined, window: string, enabled = true) {
  return useQuery<PromRangeResponse>({
    queryKey: ["metrics", expr, org ?? "", window],
    queryFn: () => tolerateStatus<PromRangeResponse>(EMPTY_RANGE, 404, 503)(api.queryRange(expr, org, window)),
    enabled,
    refetchInterval: POLL.slow,
  });
}

// ---- config-store models ----

export function useModels() {
  return useQuery<ModelSummary[]>({
    queryKey: ["models"],
    queryFn: async () => {
      const r = await tolerate404<{ models: ModelSummary[] }>({ models: [] })(api.listModels());
      return r.models;
    },
  });
}

export function useModel(model: string | undefined) {
  return useQuery<ModelListing>({
    queryKey: ["models", model],
    queryFn: () => api.getModel(model!),
    enabled: !!model,
  });
}

// ---- operators (admin allow-list) ----

export function useOperators() {
  return useQuery<Operator[]>({
    queryKey: ["operators"],
    queryFn: () => tolerate404<Operator[]>([])(api.listOperators()),
  });
}

// Invalidate both ["operators"] (the table) and ["models"] (the sidebar count
// shown under the Admin group in the config-store explorer).
export function useUpsertOperator() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { email: string; role: Operator["role"] }) => api.upsertOperator(body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["operators"] });
      qc.invalidateQueries({ queryKey: ["models"] });
    },
  });
}

export function useDeleteOperator() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (email: string) => api.deleteOperator(email),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["operators"] });
      qc.invalidateQueries({ queryKey: ["models"] });
    },
  });
}

// ---- impersonation ----

export function useImpersonateQuery() {
  return useMutation({
    mutationFn: (v: { org: string; body: ImpersonateBody }) =>
      api.impersonateQuery(v.org, v.body),
  });
}

// ---- audit ----

export function useAudit(params: { actor?: string; org?: string }) {
  return useQuery<AuditEntry[]>({
    queryKey: ["audit", params.actor ?? "", params.org ?? ""],
    queryFn: () =>
      tolerate404<AuditEntry[]>([])(api.audit({ actor: params.actor, org: params.org, limit: 500 })),
  });
}
