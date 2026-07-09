# Duckgres Admin UI — Design

Status: **in progress** (branch `admin-ui`). A VPC-private web console for operating and
observing the multi-tenant (remote/k8s) control plane. "Manage everything": metrics,
live queries/sessions/connections, worker fleet + hot-idle, error/success rates, all
sliced by org and user; view+edit the entire config store; and impersonate a user to run
SQL against their worker.

This UI only exists in the **`control-plane` remote backend** (build tag `kubernetes`).
It extends the existing `controlplane/admin/` server — it is **not** a new service.

## Decisions (locked with the requester)

| Area | Decision |
|---|---|
| Frontend | React + Vite + TypeScript + Tailwind + shadcn/ui; TanStack Query/Table; Recharts. Built to `dist/`, embedded via `go:embed`, served by the existing Gin admin server on `:8080`. |
| Impersonation | Full read-write. Every statement audited with the admin's SSO identity. Mutating statements require an explicit client-side confirm. |
| AuthZ | Two tiers — **viewer** (read/monitor) and **admin** (edit config store + impersonate). The ALB Cognito identity yields the caller's `@posthog.com` email; the role is resolved per-request from the `duckgres_operators` config-schema table (goose migration `000006_create_operators.sql`; admins manage it under **Admin → Operators**; the first SSO login auto-provisions a create-only viewer row, and the first admin is minted by logging in over break-glass and patching that row to admin). The existing `TokenSet` (internal secret) remains the service-to-service / break-glass admin path. |
| Metrics | Live "now" view from in-memory CP state; time-series trends (error/success/duration per org) from the in-cluster Prometheus via a backend proxy. Native Recharts, no Grafana embed. |
| Exposure | Internal-scheme AWS ALB Ingress + Cognito (the Grafana pattern already in `posthog-mw-prod-us`), behind the Tailscale subnet router. Host under `*.mw-prod-us.posthog.dev`. Dev (`mw-dev`) first, then prod-us. |

## Architecture

```
Operator laptop (tailnet, group:managed-warehouse/engineering)
  → Tailscale subnet router (10.62.0.0/16, scheme=internal)
  → internal ALB  ── Cognito (Google Workspace SSO) ──┐
                                                       │ injects x-amzn-oidc-data (signed JWT, email)
  → duckgres control-plane pod :8080 (Gin)
       middleware: ssoEmail → operators-table role(viewer|admin)  (TokenSet bypass = admin)
       /                     → React SPA (go:embed dist/, SPA fallback)
       /api/v1/...           → existing CRUD + new endpoints below
       /api/v1/metrics/*     → Prometheus proxy (PROMETHEUS_URL)
       /api/v1/orgs/:id/impersonate/query → StackForOrg → CreateSession → FlightExecutor
       audit middleware: append-only record of every mutation + impersonation
```

Ports (unchanged): PG 5432, Flight 8815, **admin/api 8080**, Prometheus `/metrics` 9090.

## Backend changes (`controlplane/admin/` + `controlplane/multitenant.go`)

### 1. Live-state interface widening
The admin package currently sees only the narrow `OrgStackInfo`
(`AllOrgStats/AllWorkerStatuses/AllSessionStatuses`). We widen it (new interface, same
`orgRouterAdapter` already satisfies most of it) to surface, per org and globally:

- **Running queries**: `SessionProgress` (`session_mgr.go` — `Percentage/Rows/TotalRows/Stalled`)
  per PID, joined with `ManagedSession{PID,WorkerID,Protocol}` and the live query text
  (redacted via `usersecrets.RedactForLog`). New endpoint `GET /api/v1/queries`.
- **Connections per org + limits**: `SessionManager.SessionCount()` / `maxConnections`.
- **Worker fleet lifecycle**: hot-idle / spawning / activating / draining counts and
  queue depth. These live in the durable configstore runtime store + pool internals, not
  the in-mem session map, so the adapter reads `K8sWorkerPool` lifecycle + `worker_records`.
  New endpoint `GET /api/v1/workers/fleet` (per-org and cluster rollup).
- **CP replicas**: `ControlPlaneRuntimeTracker` (id, podName, draining). `GET /api/v1/cluster/instances`.

### 2. Prometheus proxy — `metrics_proxy.go`
`GET /api/v1/metrics/query` and `/api/v1/metrics/query_range` forward to
`PROMETHEUS_URL` (`DUCKGRES_PROMETHEUS_URL` env). Allow-list the metric names we chart so
the proxy is not an open PromQL relay. Org-labelled metrics we expose:
`duckgres_query_total{org,outcome}` (error/success rate — the gold metric),
`duckgres_query_duration_seconds{org}`, `duckgres_org_sessions_active{org}`,
`duckgres_org_worker_crashes_total{org}`, `duckgres_s3_bytes_read_total{org}`,
`duckgres_scan_*{org}`. Fleet (no org label): worker lifecycle/spawn/reap/queue/cap-drift.

### 3. RBAC + audit — `authz.go`, `audit.go`
- `AuthMiddleware`: decode `x-amzn-oidc-data` (ALB-signed JWT; verify via the ALB public
  key endpoint, cache keys — hardening follow-up) → email. Only `@posthog.com` +
  `email_verified != false` is accepted, else 401. The role is resolved per-request from
  the `duckgres_operators` config-schema table (goose migration
  `000006_create_operators.sql`): an `admin` row → `admin`, else
  (including no row) → `viewer`. Admins manage operators under **Admin → Operators**
  (`/api/v1/operators`, admin-only, with a last-admin guard); the first SSO login
  auto-provisions a create-only viewer row, and the first admin is minted by logging in
  over break-glass and patching that row to admin. A valid `TokenSet` token (header/cookie)
  short-circuits to `admin` (service / break-glass). Sets the `Identity` (email, role,
  source) in `gin.Context`.
- `requireAdmin`: per-route gate on all mutating verbs (POST/PUT/PATCH/DELETE), the
  configstore write endpoints, and impersonation.
- `audit`: append-only table `duckgres_admin_audit` (actor, role, action, method, path,
  org, target_user, sql_redacted, status, ts) in the configstore. Every mutation +
  every impersonation statement writes a row. `GET /api/v1/audit` (admin only) to read it.
  SQL text is stored redacted via `usersecrets.RedactForLog` — never raw secrets.

### 4. Impersonation — `impersonate.go`
`POST /api/v1/orgs/:id/impersonate/query` `{username, sql, allowWrite}` (admin only):
resolve `StackForOrg(org)` → `Sessions.CreateSessionWithProtocol(ctx, username, ...)` →
run `sql` via the returned `FlightExecutor.QueryContext` → stream rows back as JSON →
**always** `DestroySession` in a `defer`. A short worker TTL is forced. `allowWrite=false`
rejects non-read statements (defense in depth; UI also confirms). Risks documented inline:
it is a *real* session (consumes a worker under one-session-per-worker, counts against the
org's connection limits, appears in the customer's session accounting) — audited loudly.

### 5. Config-store write coverage — extend `api.go`
Existing typed CRUD covers orgs/users/warehouse/pinning. Add: `org_user_secrets`
list+delete (configstore `DeleteOrgUserSecret` already exists, currently unrouted). Keep
runtime tables (`worker_records`/`flight_session_records`/leases/queue) **read-only** in
the models explorer; any force-action (force-retire a wedged worker) must route through the
existing epoch/CAS-fenced methods, never raw edits. Render warehouse `*_state` columns
read-only (hand-editing desyncs the provisioner state machine).

## Frontend (`controlplane/admin/ui/`)

Vite + React + TS. Tailwind + shadcn/ui, dark-first. TanStack Query (server cache + polling
for live views), TanStack Table (dense sortable tables), Recharts (trends). Pages:

1. **Overview** — fleet vCPU/mem, worker counts by lifecycle state, hot-idle + queue depth,
   leader/replica health, cluster-wide query rate + error %.
2. **Organizations** — list + detail; edit org config (max_workers, connections, default
   worker cpu/mem/ttl, hot-idle floor, hostname alias); managed-warehouse view/edit.
3. **Users** — per-org users CRUD; persistent secrets list/delete.
4. **Live** — running queries / sessions / connections, sliced + filterable by org & user,
   with progress bars (SessionProgress), a running-query **Duration** column
   (`QueryStatus.ElapsedMS`, computed on the owning CP from the connection's query-start),
   a **State** column (`QueryStatus.State`, the pg_stat_activity-style state) that flags
   sessions holding a worker with **no in-flight query** — `idle` / `idle in transaction` —
   a smell when persistent, and a cancel affordance. Opening a running-query row fetches
   `GET /api/v1/queries/by-worker/:wid` on demand — addressed by the **cluster-unique worker
   id**, not pid, because the CP allocates backend pids per-org (every stack starts at 1000)
   so two orgs can share a pid. The detail dialog shows the redacted SQL text
   (`server.ConnDetailByWorkerID` → `usersecrets.RedactForLog`), connection metadata (worker
   pod, client addr, application, elapsed) and live progress. The SQL text lives only on the
   CP replica that owns the connection, so this scatter-gathers like `/queries`: it checks
   locally, then fans out to peer CPs via the `PeerFetcher` (`?scope=local` recursion guard)
   and returns the owning replica's result — a 404 means no replica owns the worker (the
   query ended), not "wrong replica answered".
5. **Workers** — fleet table by lifecycle state, per-org rollup, spawn/reap/drain activity.
6. **Metrics** — error/success/duration trends per org (Prometheus), selectable org & window.
7. **Config store** — generic explorer over every model (read), with edit where safe.
8. **Impersonate** — pick org+user, SQL console, results grid, write-confirm modal.
9. **Audit** — searchable admin action log.

### Build + embed
`npm run build` → `controlplane/admin/ui/dist/`. `embed_ui.go` in the admin package:
`//go:embed all:ui/dist` served by Gin with SPA fallback to `index.html`; the API/login/health
routes keep precedence. The built bundle is a **gitignored build artifact** — only
`ui/dist/.gitkeep` is tracked so the embed has a target and `go build` compiles without node
(the server serves a "UI not built" notice in that case). `just ui-build` produces it locally;
both `Dockerfile` and `Dockerfile.controlplane` (the prod CP image) have a `node` builder
stage that rebuilds `dist/` and copies it into the Go build context before `go build`, so a
shipped image always has the fresh UI. The React app calls relative `/api/v1` paths, so a Vite
dev proxy (`just ui-dev-vite`) and the `devserver` both work for local iteration.

## Exposure (charts + cloud-infra)

Reuse the Grafana pattern in the same cluster: internal-scheme ALB Ingress on the admin
`:8080` port with `alb.ingress.kubernetes.io/auth-type: cognito` pointing at the existing
mw-prod-us Cognito pool (Google IdP), host under the `*.mw-prod-us.posthog.dev`
external-dns/ACM wildcard, layered behind the Tailscale subnet router (ACLs already scope
`10.62.0.0/16` to infra/managed-warehouse/engineering). The CP gets `DUCKGRES_PROMETHEUS_URL`
and the admin Google-group name via values. Ship to `mw-dev` first, then `mw-prod-us`.

## Testing

Unit (`-tags kubernetes`): RBAC gate (viewer blocked from writes/impersonation; TokenSet
bypass), Prometheus proxy allow-list, impersonation round-trip with a fake stack +
session-destroy-on-defer, audit row written per mutation, new configstore writes.
e2e (`tests/mw-dev/e2e/harness.sh`): admin UI reachable in-cluster, RBAC enforced,
impersonation SQL round-trip against a real org worker is audited, a config-store edit via
the API is observed by a subsequent connection. Update CLAUDE.md + admin/README.md.
