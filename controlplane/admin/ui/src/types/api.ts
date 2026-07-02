// Typed shapes for the admin REST API (relative `/api/v1/*`).
//
// Shapes confirmed against the Go handlers live in controlplane/admin/api.go,
// controlplane/admin/models_api.go and controlplane/configstore/models.go.
// Shapes marked "ASSUMED (backend must match)" describe endpoints the design
// calls for that are not yet served by the Go control plane — the UI degrades
// gracefully (empty state) when they 404. See the report / README for the list.

// ---- Identity ----

export type Role = "admin" | "viewer";

// GET /api/v1/me — identity resolved from the ALB OIDC header (or break-glass
// internal-secret). Backed by admin/authz.go Identity.
export interface Me {
  email: string;
  role: Role;
  source: string; // "sso" | "internal-secret"
}

// ---- Operators (admin allow-list) ----

// GET /api/v1/operators → { operators: Operator[] } (admin-only). An operator is
// an SSO identity granted admin/viewer access to this console. Mutations go
// through POST /api/v1/operators (upsert) and DELETE /api/v1/operators/:email;
// both 409 if the change would remove the last admin.
export interface Operator {
  email: string;
  role: Role;
  added_by: string;
  created_at: string;
  updated_at: string;
}

// ---- Orgs (confirmed) ----

export interface SecretRef {
  namespace: string;
  name: string;
  key: string;
}

export interface Org {
  name: string;
  database_name: string;
  hostname_alias: string | null;
  max_workers: number;
  max_vcpus: number;
  default_worker_cpu: string;
  default_worker_memory: string;
  default_worker_ttl: string;
  default_worker_min_hot_idle: number;
  users?: OrgUser[];
  warehouse?: ManagedWarehouse | null;
  created_at: string;
  updated_at: string;
}

// Editable subset of Org accepted by PUT /api/v1/orgs/:id.
export interface OrgUpdate {
  max_workers?: number;
  max_vcpus?: number;
  default_worker_cpu?: string;
  default_worker_memory?: string;
  default_worker_ttl?: string;
  default_worker_min_hot_idle?: number;
  hostname_alias?: string | null;
}

// ---- Users (confirmed) ----

export interface OrgUser {
  org_id: string;
  username: string;
  passthrough: boolean;
  // disabled is the per-user kill switch: when true the user is refused at
  // connect time. Toggled via disableUser/enableUser (which also kill live
  // sessions on disable). Optional for back-compat with older API responses.
  disabled?: boolean;
  default_catalog?: string;
  max_vcpus: number;
  created_at: string;
  updated_at: string;
}

// Envelope returned by the per-user kill switch endpoints (kill/disable/enable).
// killed is the cluster-wide count of sessions torn down; cp_responders/cp_total
// report how many control-plane replicas answered the fan-out.
export interface UserKillResult {
  killed?: number;
  disabled?: boolean;
  enabled?: boolean;
  cp_responders?: number;
  cp_total?: number;
}

export interface CreateUserBody {
  username: string;
  password: string;
  org_id: string;
  passthrough?: boolean;
  default_catalog?: string;
  max_vcpus?: number;
}

export interface UpdateUserBody {
  password?: string;
  passthrough?: boolean;
  default_catalog?: string;
  max_vcpus?: number;
}

// GET /api/v1/orgs/:id/users/:username/secrets → { secrets: OrgUserSecret[] }.
// Backed by configstore.OrgUserSecret (ciphertext is never exposed).
export interface OrgUserSecret {
  org_id: string;
  username: string;
  secret_name: string;
  created_at: string;
  updated_at: string;
}

// ---- Managed warehouse (confirmed) ----

export type WarehouseState =
  | "pending"
  | "provisioning"
  | "ready"
  | "failed"
  | "deleting"
  | "deleted"
  | string;

export interface ManagedWarehouse {
  org_id: string;
  // The explicit Duckling CR name (provisioner-owned). May be "" on legacy rows
  // that predate the field — callers fall back to ducklingName(org) in that case.
  duckling_name: string;
  image: string;
  ducklake_version: string;
  warehouse_database: { endpoint: string; port: number };
  metadata_store: {
    kind: string;
    endpoint: string;
    port: number;
    database_name: string;
    username: string;
    password_aws_secret?: string;
  };
  data_store?: { kind: string; bucket_name?: string; region?: string };
  pgbouncer: { enabled: boolean };
  s3: {
    provider: string;
    region: string;
    bucket: string;
    path_prefix: string;
    endpoint: string;
    use_ssl: boolean;
    url_style: string;
    delta_catalog_enabled: boolean;
    delta_catalog_path: string;
  };
  ducklake?: { enabled: boolean };
  iceberg: {
    enabled: boolean;
    backend: string;
    namespace: string;
    region: string;
    lakekeeper_endpoint?: string;
    lakekeeper_warehouse?: string;
    lakekeeper_client_id?: string;
    lakekeeper_oauth2_server_uri?: string;
    lakekeeper_client_credentials?: SecretRef;
  };
  worker_identity: { namespace: string; iam_role_arn: string };
  warehouse_database_credentials: SecretRef;
  metadata_store_credentials: SecretRef;
  s3_credentials: SecretRef;
  runtime_config: SecretRef;
  // *_state fields are READ-ONLY (provisioner-owned).
  state: WarehouseState;
  status_message: string;
  metadata_store_state: WarehouseState;
  s3_state: WarehouseState;
  iceberg_state: WarehouseState;
  identity_state: WarehouseState;
  secrets_state: WarehouseState;
  provisioning_started_at?: string | null;
  ready_at?: string | null;
  failed_at?: string | null;
  created_at: string;
  updated_at: string;
}

// ---- Duckling drift (admin-only) ----

// One mismatch between a managed warehouse row and its Duckling custom resource.
// For issue "orphan" (a CR with no warehouse row) `org` is "".
export type DucklingDriftIssue = "missing" | "not_ready" | "state_mismatch" | "orphan" | "check_error";

export interface DucklingDrift {
  org: string;
  duckling_name: string;
  warehouse_state: string;
  cr_present: boolean;
  cr_ready: boolean;
  issue: DucklingDriftIssue;
  message: string;
}

// GET /api/v1/ducklings/drift → drift report (admin-only; 403s for viewers).
// `available` is false when the check could not run.
export interface DucklingDriftResponse {
  available: boolean;
  checked: number;
  entries: DucklingDrift[];
}

// ---- Duckling metadata-store assignment ----

// The live metadata-store backend of one Duckling CR. `cnpg_shard` is the
// parsed shard name (e.g. "shard-001") for cnpg-shard tenants, absent for
// external.
export interface DucklingMetadataEntry {
  kind: string;
  endpoint: string;
  cnpg_shard?: string;
}

// GET /api/v1/ducklings/metadata → per-CR metadata-store assignment keyed by
// CR (duckling) name. `available` is false when the Duckling client is
// unavailable.
export interface DucklingMetadataResponse {
  available: boolean;
  entries: Record<string, DucklingMetadataEntry>;
}

// ---- Cluster status (confirmed) ----

export interface OrgStatus {
  name: string;
  workers: number;
  active_sessions: number;
  max_workers: number;
}

export interface ClusterStatus {
  total_orgs: number;
  total_workers: number;
  total_sessions: number;
  orgs: OrgStatus[];
}

// ---- Workers & sessions (confirmed) ----

// GET /api/v1/workers → WorkerStatus[] (only session-holding workers).
export interface WorkerStatus {
  id: number;
  org: string;
  active_sessions: number;
  status: string; // "active" | "idle"
  cpu: string; // e.g. "8"
  memory: string; // e.g. "16Gi"
  ttl_seconds: number; // 0 = default/unset
}

// GET /api/v1/sessions → SessionStatus[]. Note `user` (not `username`).
export interface SessionStatus {
  pid: number;
  worker_id: number;
  org: string;
  user: string;
  protocol: string;
}

export type WorkerLifecycleState =
  | "spawning"
  | "idle"
  | "reserved"
  | "activating"
  | "hot"
  | "hot_idle"
  | "draining"
  | "retired"
  | "lost"
  | string;

// GET /api/v1/workers/fleet → { fleet: FleetStat[] }. AGGREGATED worker counts
// grouped by image/lifecycle-state/binding from the durable runtime store — NOT
// per-worker rows (backed by admin/live.go FleetStat).
export interface FleetStat {
  image: string;
  state: WorkerLifecycleState;
  binding: string;
  count: number;
  cpu_cores: number;
  memory_bytes: number;
}

// GET /api/v1/cluster/instances → { instances: CPInstance[] }.
export interface CPInstance {
  id: string;
  self: boolean;
}

// ---- Live queries (confirmed) ----

// GET /api/v1/queries → { queries: RunningQuery[] }. Progress fields are FLAT
// (backed by admin/live.go QueryStatus). No SQL text is exposed.
export interface RunningQuery {
  org: string;
  user: string;
  pid: number;
  worker_id: number;
  protocol: string;
  percentage: number;
  rows: number;
  total_rows: number;
  stalled: boolean;
  started_at?: string; // RFC3339 session start (session age); may be absent/zero
  elapsed_ms: number; // how long the current statement has been running (0 = idle)
  state: string; // "active" | "idle" | "idle in transaction" | "idle in transaction (aborted)"
}

// GET /api/v1/queries/by-worker/:wid → expanded detail for one in-flight query,
// addressed by cluster-unique worker id. Fetched on demand when a query row is
// opened. `query` is redacted server-side (usersecrets.RedactForLog) — never raw
// SQL. Scatter-gathers across CP replicas; 404 if no replica owns the worker.
export interface QueryDetail {
  org: string;
  user: string;
  pid: number;
  worker_id: number;
  worker_pod: string;
  protocol: string;
  database: string;
  application_name: string;
  client_addr: string;
  client_port: number;
  state: string;
  query: string;
  backend_start: string; // RFC3339, "" if unknown
  query_start: string; // RFC3339, "" if idle
  elapsed_ms: number;
  percentage: number;
  rows: number;
  total_rows: number;
  stalled: boolean;
}

// ErrorEntry is one recent failed query for the Errors page. query + message are
// redacted server-side (a CREATE SECRET error never carries the credential).
export interface ErrorEntry {
  time: string; // RFC3339
  org: string;
  user: string;
  pid: number;
  worker_id: number;
  worker_pod: string;
  sqlstate: string; // Postgres SQLSTATE (e.g. 42P01)
  category: string; // "user" | "system" | "conflict" | "metadata_connection_lost"
  message: string; // redacted
  query: string; // redacted
  client_addr: string;
  trace_id: string;
}

// Optional slicing for the Errors list (all applied server-side after the
// cross-CP merge).
export interface ErrorFilters {
  org?: string;
  user?: string;
  sqlstate?: string;
  category?: string;
  limit?: number;
}

// ---- Metrics (raw Prometheus / VictoriaMetrics) ----

// GET /api/v1/metrics/panels → { panels: string[], configured: boolean }.
export interface MetricsPanels {
  panels: string[];
  configured: boolean;
}

// GET /api/v1/metrics/query_range?expr=<panelKey>&org=&window=&rate_window=
// proxies Prometheus verbatim: a `matrix` range result.
export interface PromRangeResult {
  metric: Record<string, string>;
  values: [number, string][]; // [unixSeconds, stringValue]
}

export interface PromRangeResponse {
  status: string; // "success"
  data: {
    resultType: string; // "matrix"
    result: PromRangeResult[];
  };
}

// Normalized series for charts (one named line of [t(ms), v] points).
export interface MetricSeries {
  name: string;
  labels?: Record<string, string>;
  points: { t: number; v: number }[];
}

// ---- Config-store models explorer (confirmed) ----

export interface ModelSummary {
  key: string;
  label: string;
  group: string;
  count: number;
}

export interface ModelListing {
  key: string;
  label: string;
  group: string;
  table: string;
  columns: string[];
  count: number;
  truncated: boolean;
  rows: Record<string, unknown>[];
}

// ---- Impersonation (confirmed) ----

// POST /api/v1/orgs/:id/impersonate/query — request body (snake_case
// `allow_write`; the write-confirm flow sends allow_write=true).
export interface ImpersonateBody {
  username: string;
  sql: string;
  allow_write: boolean;
}

// Response (backed by admin/impersonate.go QueryResult).
export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
}

// ---- Audit (confirmed) ----

// GET /api/v1/audit?org=&actor=&limit= → { entries: AuditEntry[] }.
// Backed by admin/audit.go AdminAuditEntry.
export interface AuditEntry {
  id: number;
  ts: string;
  actor: string;
  role: string;
  source: string;
  action: string;
  method: string;
  path: string;
  org?: string;
  target_user?: string;
  sql_redacted?: string;
  // Optional non-sensitive human context recorded by the handler, e.g.
  // "role viewer → admin" or "max_workers 4 → 10".
  detail?: string;
  status: number;
  remote_addr?: string;
}
