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
| `GET /api/v1/sessions`, `/workers` | viewer | live sessions / session-holding workers |
| `GET /api/v1/workers/fleet` | viewer | cluster worker counts by lifecycle state |
| `GET /api/v1/cluster/instances` | viewer | live CP replicas (self-flagged) |
| `POST /api/v1/sessions/:pid/cancel` | admin | tear down a session + its worker |
| `GET /api/v1/metrics/panels`, `/metrics/query_range` | viewer | Prometheus proxy (allow-listed panels only) |
| `GET /api/v1/orgs/:id/users/:username/secrets`, `DELETE .../:name` | viewer/admin | list/delete stored persistent secrets (ciphertext never returned) |
| `POST /api/v1/orgs/:id/impersonate/query` | admin | run SQL as an org user on their worker |
| `GET /api/v1/audit` | admin | admin action log |
| `GET /api/v1/operators` | admin | list console operators (email → role) |
| `POST /api/v1/operators` | admin | add/update an operator (`{email, role}`; last-admin demotion → 409) |
| `DELETE /api/v1/operators/:email` | admin | remove an operator (removing the last admin → 409) |

### Cross-CP live-state aggregation (`live_aggregate.go` + `controlplane/live_aggregator.go`)

Live session/query state is **in-memory per CP** — each replica only knows the
sessions it owns. Behind the load-balancer that made the dashboard's numbers
flicker as polls landed on different pods. The session/query endpoints
(`/queries`, `/sessions`, `/workers`, `/status`) **fan out**: the serving CP
discovers its peer CP pods (K8s pod list, name-prefix match), GETs each peer's
`?scope=local` view (the recursion guard — a peer returns only its own slice)
with the internal secret, and concatenates (a session is owned by exactly one
CP, so the union is disjoint — no dedup). Peers are fetched concurrently with a
short per-peer timeout; a slow/down peer is omitted, and `/queries` reports
`cp_responders`/`cp_total` for coverage. `PeerFetcher` is nil in single-CP /
test setups (local-only). `/workers/fleet` is already cluster-wide (config
store) and is not fanned out.

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
compatible). Org-labelled panels (`duckgres_query_total{org,outcome}` etc.) keep
slicing enforced. Unset URL → 503 so the UI shows "metrics not configured".

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
