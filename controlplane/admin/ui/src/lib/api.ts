// Typed, same-origin API client for the admin console.
//
// Every request is relative to `/api/v1` — the browser is already authenticated
// upstream by the ALB (cookie/header), so we never attach credentials here. We
// surface HTTP status on failure so callers (and TanStack Query) can branch on
// 401 (not authorized page) vs 403 (disabled action) vs 404 (empty state).

import type {
  AuditEntry,
  ClusterStatus,
  CreateUserBody,
  CPInstance,
  FleetStat,
  ImpersonateBody,
  Me,
  ManagedWarehouse,
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
  QueryResult,
  RunningQuery,
  SessionStatus,
  UpdateUserBody,
  WorkerStatus,
} from "@/types/api";

export const API_BASE = "/api/v1";

export class ApiError extends Error {
  status: number;
  body: unknown;
  constructor(status: number, message: string, body?: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

async function request<T>(
  path: string,
  init?: RequestInit & { rawQuery?: Record<string, string | number | undefined> },
): Promise<T> {
  let url = `${API_BASE}${path}`;
  if (init?.rawQuery) {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(init.rawQuery)) {
      if (v !== undefined && v !== "") qs.set(k, String(v));
    }
    const s = qs.toString();
    if (s) url += `?${s}`;
  }

  let res: Response;
  try {
    res = await fetch(url, {
      ...init,
      headers: {
        Accept: "application/json",
        ...(init?.body ? { "Content-Type": "application/json" } : {}),
        ...init?.headers,
      },
    });
  } catch (e) {
    throw new ApiError(0, `network error: ${(e as Error).message}`);
  }

  const text = await res.text();
  let parsed: unknown = undefined;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = text;
    }
  }

  if (!res.ok) {
    const msg =
      (parsed && typeof parsed === "object" && "error" in parsed
        ? String((parsed as { error: unknown }).error)
        : undefined) ?? `${res.status} ${res.statusText}`;
    throw new ApiError(res.status, msg, parsed);
  }
  return parsed as T;
}

const get = <T>(path: string, rawQuery?: Record<string, string | number | undefined>) =>
  request<T>(path, { method: "GET", rawQuery });

const del = <T>(path: string) => request<T>(path, { method: "DELETE" });

const post = <T>(path: string, body: unknown) =>
  request<T>(path, { method: "POST", body: JSON.stringify(body) });

const put = <T>(path: string, body: unknown) =>
  request<T>(path, { method: "PUT", body: JSON.stringify(body) });

function enc(s: string): string {
  return encodeURIComponent(s);
}

export const api = {
  // identity
  me: () => get<Me>("/me"),

  // overview
  status: () => get<ClusterStatus>("/status"),

  // orgs
  listOrgs: () => get<Org[]>("/orgs"),
  getOrg: (id: string) => get<Org>(`/orgs/${enc(id)}`),
  updateOrg: (id: string, body: OrgUpdate) => put<Org>(`/orgs/${enc(id)}`, body),
  deleteOrg: (id: string) => del<{ deleted: string }>(`/orgs/${enc(id)}`),
  getWarehouse: (id: string) => get<ManagedWarehouse>(`/orgs/${enc(id)}/warehouse`),
  updateWarehouse: (id: string, body: Partial<ManagedWarehouse>) =>
    put<ManagedWarehouse>(`/orgs/${enc(id)}/warehouse`, body),

  // users
  listUsers: () => get<OrgUser[]>("/users"),
  createUser: (body: CreateUserBody) => post<OrgUser>("/users", body),
  updateUser: (org: string, username: string, body: UpdateUserBody) =>
    put<OrgUser>(`/orgs/${enc(org)}/users/${enc(username)}`, body),
  deleteUser: (org: string, username: string) =>
    del<{ deleted: string }>(`/orgs/${enc(org)}/users/${enc(username)}`),
  listUserSecrets: (org: string, username: string) =>
    get<{ secrets: OrgUserSecret[] }>(`/orgs/${enc(org)}/users/${enc(username)}/secrets`).then(
      (r) => r.secrets ?? [],
    ),
  deleteUserSecret: (org: string, username: string, name: string) =>
    del<{ deleted: string }>(`/orgs/${enc(org)}/users/${enc(username)}/secrets/${enc(name)}`),

  // workers / sessions / live
  listWorkers: () => get<WorkerStatus[]>("/workers"),
  fleet: () => get<{ fleet: FleetStat[] }>("/workers/fleet").then((r) => r.fleet ?? []),
  instances: () =>
    get<{ instances: CPInstance[] }>("/cluster/instances").then((r) => r.instances ?? []),
  listSessions: () => get<SessionStatus[]>("/sessions"),
  listQueries: () => get<{ queries: RunningQuery[] }>("/queries").then((r) => r.queries ?? []),
  queryDetail: (pid: number) => get<QueryDetail>(`/queries/${pid}`),
  cancelSession: (pid: number) => post<{ killed: number }>(`/sessions/${pid}/cancel`, {}),

  // metrics
  metricsPanels: () => get<MetricsPanels>("/metrics/panels"),
  queryRange: (
    expr: string,
    org: string | undefined,
    window: string,
    rateWindow?: string,
  ) =>
    get<PromRangeResponse>("/metrics/query_range", {
      expr,
      org,
      window,
      rate_window: rateWindow,
    }),

  // operators (admin allow-list)
  listOperators: () =>
    get<{ operators: Operator[] }>("/operators").then((r) => r.operators ?? []),
  upsertOperator: (body: { email: string; role: Operator["role"] }) =>
    post<Operator>("/operators", body),
  deleteOperator: (email: string): Promise<void> =>
    del<{ deleted: boolean }>(`/operators/${enc(email)}`).then(() => undefined),

  // config-store models explorer
  listModels: () => get<{ models: ModelSummary[] }>("/models"),
  getModel: (model: string) => get<ModelListing>(`/models/${enc(model)}`),

  // impersonation
  impersonateQuery: (org: string, body: ImpersonateBody) =>
    post<QueryResult>(`/orgs/${enc(org)}/impersonate/query`, body),

  // audit
  audit: (params?: { actor?: string; org?: string; limit?: number }) =>
    get<{ entries: AuditEntry[] }>("/audit", params).then((r) => r.entries ?? []),
};
