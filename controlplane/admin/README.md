# Control-plane admin console

The control plane (multi-tenant / `kubernetes` build tag) serves a React admin
console + REST API on `:8080`. It is the operate-everything surface: metrics,
live queries/sessions/connections, the worker fleet, the full config store, user
impersonation, and an audit log — sliceable by org and user.

Exposure is VPC-private: an internal-scheme ALB + Cognito (Google Workspace SSO),
reachable only over the Tailscale subnet router. See `docs/design/admin-ui.md`.

## Architecture

- **Frontend** (`ui/`): React + Vite + TypeScript + Tailwind + shadcn/ui,
  TanStack Query/Table, Recharts. Built to `ui/dist/` and embedded via
  `//go:embed all:ui/dist` (`embed_ui.go`), served by Gin with SPA fallback. The
  built bundle is a **gitignored build artifact** — only `ui/dist/.gitkeep` is
  tracked, so the embed has a target and `go build` compiles without node (the
  server then serves a "UI not built" notice). `just ui-build` produces it
  locally; both `Dockerfile` and `Dockerfile.controlplane` rebuild it in a node
  stage before `go build`, so a shipped image always has the fresh UI.
- **Backend**: Gin on `:8080`, all routes under `/api/v1` (the SPA owns `/`).

## Auth + RBAC

`AuthMiddleware` (`authz.go`) resolves every `/api/v1` request to an `Identity`
with a `Role`:

- A valid `TokenSet` token (`X-Duckgres-Internal-Secret` header or the
  `duckgres_admin_token` cookie) → **admin**. This is the service-to-service /
  break-glass path (`RegisterLogin` mints the cookie via `POST /login`).
- Otherwise the ALB-injected `X-Amzn-Oidc-Data` JWT (Cognito/Google) yields the
  caller's email (only `@posthog.com`, `email_verified != false`; otherwise
  treated as unauthenticated). The role is then resolved **per-request** from the
  `duckgres_operators` table in the config schema (goose migration
  `000006_create_operators.sql`): an `admin` row →
  **admin**, anything else (including no row) → **viewer**. Operators are managed
  by admins under **Admin → Operators** in the config-store explorer (and the
  `/api/v1/operators` API). The first SSO login auto-provisions a create-only
  **viewer** operator row; to mint the first admin, log in over the break-glass
  internal token and patch that row to `admin` under **Admin → Operators**.

`RoleGate` enforces the split: mutating verbs (POST/PUT/PATCH/DELETE) and the
audit-log GET require admin; other GETs allow viewer. `AuditMiddleware` records
every mutation. The ALB OIDC JWT signature is currently trusted-by-network (the
internal LB is the only ingress and strips client copies); verifying it by `kid`
is a hardening follow-up (see the design doc).

`?token=` URL auth is deliberately rejected (#721).

## API surface

Existing typed CRUD (`api.go`): orgs, users, managed warehouses (+ tenant
pinning). Generic read-only models explorer (`models_api.go`): `GET
/api/v1/models`, `GET /api/v1/models/:model` — secret columns (`json:"-"`)
dropped by the typed scan; **never swap in a raw map scan**.

Added for the console:

| Route | Role | Purpose |
|-------|------|---------|
| `GET /api/v1/me` | any | caller identity + role (SPA tailors its UI) |
| `GET /api/v1/queries` | viewer | running queries w/ progress, `?org=&user=` slicing |
| `GET /api/v1/queries/by-worker/:wid` | viewer | one query's detail: redacted SQL text + conn metadata + progress, addressed by cluster-unique worker id (pid is per-org, not unique). Scatter-gathers like `/queries` — checks locally, else fans out to peer CPs (`?scope=local` guard); 404 only if no replica owns the worker |
| `GET /api/v1/errors` | viewer | recent redacted query errors (live-triage ring, newest-first), `?org=&user=&sqlstate=&category=&limit=` slicing. Fans out + merges across CPs (each error belongs to one CP — disjoint, no dedup). `query`/`message` redacted server-side |
| `GET /api/v1/sessions`, `/workers` | viewer | live sessions / session-holding workers |
| `GET /api/v1/workers/fleet` | viewer | cluster worker counts by lifecycle state |
| `GET /api/v1/workers/hot-idle` | viewer | per-org hot-idle pool reporting (count, vCPU, memory, oldest park) + each org's configured `max_hot_idle_*` caps; backs the Workers page "Hot idle by org" card |
| `GET /api/v1/cluster/instances` | viewer | live CP replicas (self-flagged) |
| `POST /api/v1/sessions/:pid/cancel` | admin | tear down a session by pid — LOCAL only (pid is per-CP); prefer the worker-id form |
| `POST /api/v1/sessions/by-worker/:wid/cancel` | admin | tear down the session on a cluster-unique worker id; fans out to whichever CP owns it (pid can't be fanned out — it collides across CPs). Returns `{killed, cp_responders, cp_total}` |
| `POST /api/v1/orgs/:id/users/:username/kill` | admin | per-user kill switch (one-shot): tear down ALL of a user's sessions + in-flight queries cluster-wide. Returns `{killed, cp_responders, cp_total}`. Does NOT block reconnects |
| `POST /api/v1/orgs/:id/users/:username/disable` | admin | persist `disabled=true` (refused at pgwire connect), reload the snapshot cluster-wide so the block is immediate, AND kill the user's live sessions. Returns `{disabled, killed, …}` |
| `POST /api/v1/orgs/:id/users/:username/enable` | admin | persist `disabled=false` + reload cluster-wide so the user can reconnect at once |
| `GET /api/v1/metrics/panels`, `/metrics/query_range` | viewer | Prometheus proxy (allow-listed panels only) |
| `GET /api/v1/usage/monthly` | admin | cumulative per-team usage per UTC month (CPU-seconds, memory GiB-seconds, S3 GiB-seconds), backing the **Usage** page. Self-gates with `RequireAdmin` (not just RoleGate's method check) because per-team cost data across all orgs is as sensitive as the raw billing families. Reads the SAME billing buffer as `GET /billing/usage`, so retention is the buffer's: acked buckets are deleted, >30d buckets GC'd — `watermark_low` in the response marks where billed data was removed. `?months=N` (default 6, max 36) sets the window |
| `GET /api/v1/orgs/:id/usage/daily` | admin | one org's daily per-team usage series (same families), backing the org detail page's **Usage** charts. Same RequireAdmin gate and buffer-retention semantics; the org scope is the `:id` path segment flowing into the queries' WHERE clause. `?days=N` (default 14, max 31 — the buffer's 30d GC bounds useful range) |
| `GET /api/v1/orgs/:id/monitoring/snapshot` | internal secret | Customer-safe org warehouse state, resource limits, workers, sessions, queue depth, and CP coverage. Omits user, pod, image, SQL, client, trace, and control-plane identifiers |
| `GET /api/v1/orgs/:id/monitoring/series` | internal secret | Customer-safe, org-forced Prometheus range query. Requires an allow-listed `metric`; `window` is one of `1h`, `6h`, `24h` (default), `7d`, `30d` |
| `GET /api/v1/orgs/:id/users/:username/secrets`, `DELETE .../:name` | viewer/admin | list/delete stored persistent secrets (ciphertext never returned) |
| `POST /api/v1/orgs/:id/impersonate/query` | admin | run SQL as an org user on their worker |
| `GET /api/v1/trino/status` | viewer | Trino cell overview: cell id, coordinator `/v1/info` (version, environment, uptime, starting), query counts by state, blocked-query count, node/failed-node counts, and Trino-enabled orgs by provisioning state. `available:false` + `error` when the coordinator can't be read — the provisioning half still comes from the config store, so "the cell is down" and "these tenants never provisioned" stay distinguishable |
| `GET /api/v1/trino/queries` | viewer | live queries, `?org=&state=&active=1` slicing, longest-running first. SQL is redacted server-side; each row is stamped with the duckgres org resolved from the Trino principal |
| `GET /api/v1/trino/queries/:id` | viewer | one query; 404 when the coordinator has aged it out (410 Gone upstream) |
| `POST /api/v1/trino/queries/:id/kill` | admin | fail a query with a reason (`PUT /v1/query/{id}/killed`). The reason reaches the TENANT as the query's error message. Audited as `trino.query.kill` with the owning org |
| `GET /api/v1/trino/nodes` | viewer | the cell's fleet: `/v1/node` + `/v1/node/failed` where bound, else `/v1/announce` membership; the response names which (`source`) |
| `GET /api/v1/trino/orgs` | viewer | per-org Trino provisioning state + that org's live query counts |
| `GET /api/v1/orgs/:id/trino` | viewer | one org's Trino state; `enabled:false` for an org with no Trino row (not an error) |
| `GET /api/v1/audit` | admin | admin action log |
| `GET /api/v1/operators` | admin | list console operators (email → role) |
| `POST /api/v1/operators` | admin | add/update an operator (`{email, role}`; last-admin demotion → 409) |
| `DELETE /api/v1/operators/:email` | admin | remove an operator (removing the last admin → 409) |

### Cross-CP live-state aggregation (`live_aggregate.go` + `controlplane/live_aggregator.go`)

Live session/query state is **in-memory per CP** — each replica only knows the
sessions it owns. Behind the load-balancer that made the dashboard's numbers
flicker as polls landed on different pods. The session/query endpoints
(`/queries`, `/errors`, `/sessions`, `/workers`, `/status`) **fan out**: the serving CP
discovers its peer CP pods (K8s pod list, name-prefix match), GETs each peer's
`?scope=local` view (the recursion guard — a peer returns only its own slice)
with the internal secret, and concatenates (a session is owned by exactly one
CP, so the union is disjoint — no dedup). Peers are fetched concurrently with a
short per-peer timeout; a slow/down peer is omitted, and `/queries` reports
`cp_responders`/`cp_total` for coverage. `PeerFetcher` is nil in single-CP /
test setups (local-only). `/workers/fleet` is already cluster-wide (config
store) and is not fanned out.

The same fan-out also powers the per-user **kill switch** as a mutation:
`PeerFetcher.PostPeers` POSTs `…/kill` (or `…/disable`) `?scope=local` to every
peer so the user's sessions are torn down on whichever replica owns them, and the
per-CP `killed` counts are summed. The `disable`/`enable` handlers additionally
call `ConfigStore.ReloadSnapshot()` on every replica so the connect-time block
(the `duckgres_org_users.disabled` column, goose migration
`000011_add_org_user_disabled.sql`) takes effect cluster-wide immediately rather
than one config-poll later. The disabled flag is enforced at auth in
`control.go` (pgwire → distinct `28000` "account is disabled" error, only after
the password checks out so it never leaks account existence).

### Impersonation (`impersonate.go` + `controlplane/admin_providers.go`)

`POST /api/v1/orgs/:id/impersonate/query` `{username, sql, allow_write}` opens a
**real** session as the target org+user (workers trust the CP — no password),
runs the SQL via the returned `FlightExecutor`, streams rows back, and **always**
destroys the session. It is admin-only, every statement is audited with the admin
actor + redacted SQL, and a write statement requires `allow_write=true` (the SQL
classifier is conservative — WITH/CTEs and anything non-obviously-read-only count
as writes). Caveat: the session consumes a worker exclusively
(one-session-per-worker), counts against the org's connection limits, and appears
in the org's session accounting. Rows capped at `maxImpersonationRows`.

### Metrics proxy (`metrics_proxy.go`)

Not an open PromQL relay: the client passes a panel KEY (+ optional org/window);
the PromQL is built server-side from the allow-list (`rangePanels`). Forwards to
`DUCKGRES_PROMETHEUS_URL` (the in-cluster VictoriaMetrics vmselect, Prometheus-
compatible). Org-labelled panels (`duckgres_query_total{org,status,reason}` etc.) keep
slicing enforced. Unset URL → 503 so the UI shows "metrics not configured".

### Product monitoring API (`monitoring.go`)

The PostHog backend reads `GET /api/v1/orgs/:id/monitoring/snapshot` and
`/series` with the internal secret. These routes are not operator-dashboard
shortcuts: they are a deliberately smaller customer-safe contract. The org is
fixed by the path, every config-store query uses that org, and every Prometheus
query includes an exact `org` label selector. SSO identities, including admin
operators, receive 403.

The snapshot combines durable worker records and connection leases/queue rows
with the cross-CP live-session fan-out. It reports `cp_responders`, `cp_total`,
and `partial` so a missing control plane never looks like zero activity. Worker
limits use the current org defaults with deployment fallbacks. Empty worker
profile and zero-TTL sentinels use deployment defaults because org-shaped
workers persist explicit profiles. Query progress is `null` when DuckDB does not
provide a percentage. It intentionally omits usernames, PIDs, pod/image names,
control-plane ownership, SQL, client metadata, trace identifiers, and secrets.

The series endpoint accepts only `query_rate`, `error_ratio`, `duration_p50`,
`duration_p95`, `sessions_active`, `acquire_p95`,
`acquire_by_source`, `storage_bytes`, and `worker_crash_rate`. It normalizes the
Prometheus response and retains only the `status`, `reason`, or `source` labels needed by
the corresponding chart. Unknown metrics and windows return 400; unknown orgs
return 404 with `code: "managed_warehouse_not_found"` before Prometheus is called.

### Trino cell views (`trino.go` + `trino_client.go`)

The console observes the shared Trino cell through the coordinator's REST
API, as a dedicated **observer principal** (`opa.ObserverPrincipal` =
`__duckgres_observer`) that the provisioner mints alongside the admin pair
and projects into `password.db` / `group.db`.

- **Why a second principal.** Trino routes operator reads through the same
  access-control SPI as everything else — `GET /v1/query` filters through
  `FilterViewQueryOwnedBy`, `/v1/query/{id}` is gated on `ViewQueryOwnedBy`,
  kill on `KillQueryOwnedBy`, and `/v1/node` + `/v1/resourceGroupState` are
  `MANAGEMENT_READ` (`checkCanReadSystemInformation`). A console credential
  with no grant sees an empty cluster. Note that `ReadSystemInformation` is
  a single operation gating *every* `MANAGEMENT_READ` resource — also
  `/v1/thread`, `/v1/announce` (GET), `/v1/maxActiveSplits` and
  `/v1/integrations/gateway`. All are GETs of cluster-operational state and
  none reads a catalog, a table or SQL text; the node-registering POST on
  `/v1/announce` is `INTERNAL_ONLY`. The enumeration lives in policy.rego so
  the real blast radius is visible where the grant is written. The grant is deliberately NOT added
  to `__admin_provisioner`: that credential can CREATE/DROP catalogs and by
  policy sees only its own queries, while the observer sees every tenant's
  query metadata and holds **no catalog at all** (no entry in
  `data.group_catalogs`; the observer group is excluded from
  `tenant_owns_catalog` and from the same-org query match). One leaked
  credential yields one half of that authority, never both.
- **SQL text is redacted at decode** (`usersecrets.RedactForLog`, in
  `toTrinoQuery`), not per-caller, so no handler can leak it by forgetting.
  Query text is tenant data: table names, filter literals, customer
  identifiers, and a failed `CREATE SECRET` carries a credential.
- **Airlift units are STRINGS on the wire.** `io.airlift.units.Duration`
  serializes as `"%.2f<unit>"` (`"12.34ms"`, `"1.50s"`) and `DataSize` as an
  exact byte count with a `B` suffix (`"1234B"`). Decoding either into a
  numeric Go field silently yields a page of zeroes — hence
  `parseAirliftDurationMS` / `parseAirliftDataSizeBytes`, both of which
  return 0 rather than failing so a unit-spelling change costs one column,
  not the operator's whole view.
- **Reads are cached** (2s for queries, 15s for nodes/info) because the
  console polls from every open tab and `/v1/query` walks every query the
  coordinator holds. A refresh in flight does not block readers — they get
  the previous value — so a slow coordinator can't turn N polling tabs into
  N stuck requests during the incident the console exists for.
- **Both sides of the console tag `X-Trino-Source`**
  (`duckgres-admin` here, `duckgres-provisioner` in the reconcile loop), so
  control-plane traffic is distinguishable from tenant SQL in
  `system.runtime.queries` and in the console's own live view.
- **Trino binds exactly one node-listing route, chosen by `discovery.type`.**
  `/v1/node` (heartbeat health, plus `/v1/node/failed`) exists only under
  `AIRLIFT_DISCOVERY`; `/v1/announce` (the set of announced node URIs, no
  health) is bound under `ANNOUNCE` — Trino's default, and what these cells
  run — and under `DNS`. The client tries `/v1/node` and falls back, and the
  payload carries `source` so the SPA renders membership-only rows instead of
  zero-filled health columns. Both routes are `@ResourceSecurity(MANAGEMENT_READ)`,
  so the observer's existing `ReadSystemInformation` grant covers both.
- **`system.runtime.nodes` is preferred over `/v1/announce`** on a cell that
  binds no `/v1/node`: it is served regardless of `discovery.type` and is the
  only source carrying `node_version`, so it is where worker version skew
  becomes visible. It is the observer's one data grant — `AccessCatalog` on
  `system` plus `SelectFromColumns` pinned to that single table — and it
  needs its own resource-group lane, because the catch-all selector would
  otherwise file the console's query as a tenant. Order is `/v1/node` →
  `system.runtime.nodes` → `/v1/announce`, so a cell where the grant has not
  rolled out yet still lists its fleet.
- Neither route carries a node id or version, so worker version skew is not
  observable there; the Nodes page's pod projection (running images) is where
  that lives.
- No cell configured (`DUCKGRES_TRINO_COORDINATOR_URL` unset) leaves every
  route unregistered, and the SPA renders a "no cell" state off the 404.

Touching this → update `trino_test.go`, `trino_client_test.go`,
`ui/src/lib/trino.test.ts`, `ui/src/pages/TrinoQueries.test.tsx`, and
`ui/src/pages/OrgTrinoCard.test.tsx`.

## Local UI development

Two ways to iterate without redeploying:

1. **Vite dev server** (live React/HMR): `cd controlplane/admin/ui && npm run dev`,
   with `VITE_PROXY_TARGET` pointing at a port-forwarded CP (or the devserver).
2. **Go devserver** (`devserver/`): serves the built UI off disk and proxies
   `/api`, `/login`, `/health` to a deployed CP, injecting the internal secret
   server-side. One `--context` drives secret fetch + port-forward, with a RED
   banner when the context name contains `prod`.

```sh
just ui-dev mw-dev-admin       # → http://127.0.0.1:5173 (dev banner)
just ui-dev mw-prod-us-admin   # → RED prod banner
```

The SPA uses relative `/api/v1` paths, so the same bundle runs identically
embedded, under Vite, or under the Go devserver.

## Tests

**Backend:** `authz_test.go` (SSO role mapping, RoleGate, SQL classifier),
`dashboard_test.go` (TokenSet / break-glass login / cookie), `api_test.go` +
`api_postgres_test.go` (CRUD), `models_api_test.go` (redaction). e2e: the
`admin_*` / `impersonation_*` / `models_explorer_api` assertions in
`tests/e2e-mw-dev/harness.sh`.

**Frontend** (`ui/`, Vitest + Testing Library — `just ui-test`, CI job
`ui-tests`): the dashboard's data-derivation logic has shipped wrong more than
once (worker hot/idle counts; a leak warning firing while every worker was
busy), so that math lives in pure, unit-tested modules (`src/lib/*.ts`) instead
of inline JSX. `src/lib/fleet.test.ts` pins the worker-fleet/load math
(busy=`hot` vs idle=`hot_idle`, the leak threshold, per-org load %);
`src/pages/Overview.test.tsx` renders the page with mocked hooks and asserts the
Workers card + leak warning. New derivation/display logic on a page **must** get
a `*.test.ts(x)` here — keep computed values out of the JSX so they're testable.
