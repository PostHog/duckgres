// TanStack Query hooks wrapping the typed api client.
//
// Read hooks for endpoints that may not exist yet (`*Optional`) swallow 404 and
// return an empty value so pages render an empty state instead of crashing.

import { useEffect, useMemo, useState } from "react";
import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseQueryOptions,
} from "@tanstack/react-query";
import { api, ApiError } from "@/lib/api";
import { orgLabel } from "@/lib/format";
import { POLL } from "@/lib/query";
import { useRevealedCount } from "@/lib/logReplay";
import { warehouseNeedsPolling } from "@/lib/warehouseLifecycle";
import { useIdentity } from "@/components/IdentityProvider";
import type {
  AuditEntry,
  ClusterStatus,
  ClusterSummary,
  CreateUserBody,
  DatabaseNameCheck,
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
  MonthlyUsageResponse,
  Operator,
  Org,
  OrgTeam,
  OrgTeamCreateBody,
  OrgTeamUpdateBody,
  OrgUpdate,
  OrgUser,
  OrgUserSecret,
  PromRangeResponse,
  QueryDetail,
  ReshardLogEntry,
  ReshardOperation,
  ReshardTargetsResponse,
  RunningQuery,
  SessionStatus,
  StartReshardBody,
  UpdateUserBody,
  WarehouseStatusResult,
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
    refetchInterval: (q) =>
      q.state.data?.some((org) => warehouseNeedsPolling(org.warehouse?.state)) ? POLL.normal : false,
  });
}

export function useOrg(id: string | undefined) {
  return useQuery<Org>({
    queryKey: ["orgs", id],
    queryFn: () => api.getOrg(id!),
    enabled: !!id,
  });
}

// useOrgLabels resolves an org id (org.name / team.org_id / worker.org / …)
// to its human-readable label from orgLabel(): the database_name (preferred)
// or hostname_alias. Returns a Map keyed by org id so a detail page where the
// underlying list row only carries a bare org-id string can still render the
// readable name. Falls back gracefully (no entry) when the org lookup 404s —
// callers render the raw id in that case.
export function useOrgLabels(): Map<string, string> {
  const orgs = useOrgs();
  return useMemo(() => {
    const m = new Map<string, string>();
    for (const o of orgs.data ?? []) {
      m.set(o.name, orgLabel(o));
    }
    return m;
  }, [orgs.data]);
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
    refetchInterval: (q) => (warehouseNeedsPolling(q.state.data?.state) ? POLL.normal : false),
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

// No useProvisionWarehouse mutation hook: the AddOrgDialog calls
// api.provisionWarehouse directly so the 202 payload — which carries the
// one-time root password — never sits in the TanStack Query cache.

// GET /orgs/:id/warehouse/status — provisioning lifecycle watch. Pass a
// refetchInterval to follow an in-flight provision.
export function useWarehouseStatus(id: string | undefined, opts?: { refetchInterval?: number | false }) {
  return useQuery<WarehouseStatusResult>({
    queryKey: ["warehouse-status", id],
    queryFn: () => api.warehouseStatus(id!),
    enabled: !!id,
    refetchInterval: opts?.refetchInterval ?? false,
    retry: 1,
  });
}

// GET /database-name/check — availability probe for the provision form.
// Debounced/disabled by the caller via `enabled`.
export function useDatabaseNameAvailable(name: string, enabled: boolean) {
  return useQuery<DatabaseNameCheck>({
    queryKey: ["database-name-check", name],
    queryFn: () => api.checkDatabaseName(name),
    enabled: enabled && name.trim() !== "",
    retry: 1,
    staleTime: 30_000,
  });
}

// ---- org teams ----

// All teams across every org (the Org teams nav page). 404-tolerant so a
// pre-rollout backend renders an empty state instead of an error.
export function useAllOrgTeams() {
  return useQuery<OrgTeam[]>({
    queryKey: ["org-teams"],
    queryFn: () => tolerate404<OrgTeam[]>([])(api.listAllOrgTeams()),
  });
}

export function useOrgTeams(org: string | undefined) {
  return useQuery<OrgTeam[]>({
    queryKey: ["org-teams", org],
    queryFn: () => tolerate404<OrgTeam[]>([])(api.listOrgTeams(org!)),
    enabled: !!org,
  });
}

// Team mutations invalidate the global + per-org team lists, the org queries
// (teams are embedded on the org payload) and the models sidebar counts.
function invalidateOrgTeams(qc: ReturnType<typeof useQueryClient>) {
  qc.invalidateQueries({ queryKey: ["org-teams"] });
  qc.invalidateQueries({ queryKey: ["orgs"] });
  qc.invalidateQueries({ queryKey: ["models"] });
}

export function useCreateOrgTeam() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: OrgTeamCreateBody) => api.createOrgTeam(body),
    onSuccess: () => invalidateOrgTeams(qc),
  });
}

export function useUpdateOrgTeam() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; teamId: number; body: OrgTeamUpdateBody }) =>
      api.updateOrgTeam(v.org, v.teamId, v.body),
    onSuccess: () => invalidateOrgTeams(qc),
  });
}

export function useDeleteOrgTeam() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (v: { org: string; teamId: number }) => api.deleteOrgTeam(v.org, v.teamId),
    onSuccess: () => invalidateOrgTeams(qc),
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

// GET /operators is RequireAdmin, so it 403s for viewers — only fire it for
// admins (like useDucklingDrift) and tolerate 403/404 so a viewer who lands on
// the page sees the "admin only" notice rather than an error toast.
export function useOperators() {
  const { isAdmin } = useIdentity();
  return useQuery<Operator[]>({
    queryKey: ["operators"],
    queryFn: () => tolerateStatus<Operator[]>([], 403, 404)(api.listOperators()),
    enabled: isAdmin,
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

// ---- monthly usage (the Usage page) ----

// Monthly per-team usage over the retained billing buffer. The endpoint is
// RequireAdmin (per-team cost data), so the query only fires for admins —
// viewers never spend a 403 round-trip.
export function useMonthlyUsage(months: number) {
  const { isAdmin } = useIdentity();
  return useQuery<MonthlyUsageResponse>({
    queryKey: ["usage", "monthly", months],
    queryFn: () => api.monthlyUsage(months),
    enabled: isAdmin,
  });
}

// ---- reshard operations ----

const RESHARD_TERMINAL = new Set(["succeeded", "failed", "cancelled"]);

// Start a reshard; caller navigates to the returned operation's page.
export function useStartReshard(org: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: StartReshardBody) => api.startReshard(org, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["reshards", org] }),
  });
}

export function useOrgReshards(org: string | undefined) {
  return useQuery<ReshardOperation[]>({
    queryKey: ["reshards", org],
    queryFn: () => tolerateStatus<ReshardOperation[]>([], 403, 404, 503)(api.listReshards(org!)),
    enabled: !!org,
    refetchInterval: POLL.normal,
  });
}

// One operation, polled fast until it reaches a terminal state.
export function useReshard(opId: number | null) {
  return useQuery<ReshardOperation>({
    queryKey: ["reshard", opId],
    queryFn: () => api.getReshard(opId as number),
    enabled: opId != null,
    retry: false,
    refetchInterval: (q) => {
      if (q.state.error) return false;
      const op = q.state.data;
      return op && RESHARD_TERMINAL.has(op.state) ? false : POLL.fast;
    },
  });
}

// Catch-up page size: the server's hard cap (ListReshardLog clamps >2000 back
// to the 500 default), so a many-thousand-line backlog drains in a handful of
// round-trips.
export const RESHARD_LOG_CATCHUP_PAGE = 2000;

// Reshard op log accumulation, in two phases:
//
// 1. Catch-up (mount / opId change): drain the ENTIRE existing backlog in an
//    immediate tight loop of full pages — no poll-interval sleep between
//    pages — and land it in ONE state update, revealed instantly. Opening the
//    page of an op that already logged thousands of lines (a 20k-table copy
//    logs one line per table) jumps straight to the newest lines instead of
//    pumping batches at poll cadence.
// 2. Live tail: poll /log?after_id=<last seen> and append; new lines are
//    revealed progressively (useRevealedCount) like a live terminal. Keeps
//    polling (slower) even after the op is terminal so the final report lines
//    always land; the page unmount stops it.
export function useReshardLog(opId: number | null, opState: string | undefined) {
  const [entries, setEntries] = useState<ReshardLogEntry[]>([]);
  // Size of the instantly-revealed backlog; null until catch-up completes.
  const [backlog, setBacklog] = useState<number | null>(null);
  const lastID = entries.length > 0 ? entries[entries.length - 1].id : 0;
  const terminal = opState != null && RESHARD_TERMINAL.has(opState);

  useEffect(() => {
    if (opId == null) return;
    let cancelled = false;
    setEntries([]);
    setBacklog(null);
    (async () => {
      const acc: ReshardLogEntry[] = [];
      let after = 0;
      try {
        for (;;) {
          const page = await api.getReshardLog(opId, after, RESHARD_LOG_CATCHUP_PAGE);
          if (cancelled) return;
          acc.push(...page);
          if (page.length < RESHARD_LOG_CATCHUP_PAGE) break;
          after = page[page.length - 1].id;
        }
      } catch {
        // Partial backlog is fine — the incremental poll resumes from its tail.
      }
      if (!cancelled) {
        setEntries(acc);
        setBacklog(acc.length);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [opId]);

  const caughtUp = backlog != null;
  const poll = useQuery<ReshardLogEntry[]>({
    queryKey: ["reshardLog", opId, lastID],
    queryFn: () => api.getReshardLog(opId as number, lastID),
    enabled: opId != null && caughtUp,
    retry: false,
    // A full poll page changes lastID → new query key → immediate refetch, so
    // an over-500-line gap between polls also drains without interval sleeps.
    refetchInterval: terminal ? POLL.slow : POLL.fast,
  });

  useEffect(() => {
    const fresh = poll.data;
    if (caughtUp && fresh && fresh.length > 0) {
      setEntries((prev) => {
        const seen = prev.length > 0 ? prev[prev.length - 1].id : 0;
        const append = fresh.filter((e) => e.id > seen);
        return append.length > 0 ? [...prev, ...append] : prev;
      });
    }
  }, [poll.data, caughtUp]);

  const revealed = useRevealedCount(entries.length, backlog ?? 0);
  return useMemo(() => entries.slice(0, revealed), [entries, revealed]);
}

// Global operation list for the Reshards nav page. Polls at the normal
// cadence; running ops keep their live detail on the op page itself.
export function useAllReshards() {
  return useQuery<ReshardOperation[]>({
    queryKey: ["reshards", "all"],
    queryFn: () => tolerateStatus<ReshardOperation[]>([], 403, 404, 503)(api.listAllReshards()),
    refetchInterval: POLL.normal,
  });
}

const EMPTY_RESHARD_TARGETS: ReshardTargetsResponse = {
  shards: [],
  cluster_discovery: false,
  external_stores: [],
};

// Destination discovery for the reshard form. Pre-rollout backends without
// the endpoint degrade to empty (the form still allows manual entry).
export function useReshardTargets() {
  return useQuery<ReshardTargetsResponse>({
    queryKey: ["reshardTargets"],
    queryFn: () =>
      tolerateStatus<ReshardTargetsResponse>(EMPTY_RESHARD_TARGETS, 403, 404, 503)(
        api.getReshardTargets(),
      ).catch(() => EMPTY_RESHARD_TARGETS),
    refetchInterval: POLL.slow,
  });
}

export function useCancelReshard() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (opId: number) => api.cancelReshard(opId),
    onSuccess: (_data, opId) => qc.invalidateQueries({ queryKey: ["reshard", opId] }),
  });
}
