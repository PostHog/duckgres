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
  `//go:embed all:ui/dist` (`embed_ui.go`), served by Gin with SPA fallback. A
  committed placeholder `ui/dist/index.html` keeps `go build` working without a
  node toolchain; CI/Docker run `npm run build` to produce the real bundle
  before `go build`.
- **Backend**: Gin on `:8080`, all routes under `/api/v1` (the SPA owns `/`).

## Auth + RBAC

`AuthMiddleware` (`authz.go`) resolves every `/api/v1` request to an `Identity`
with a `Role`:

- A valid `TokenSet` token (`X-Duckgres-Internal-Secret` header or the
  `duckgres_admin_token` cookie) → **admin**. This is the service-to-service /
  break-glass path (`RegisterLogin` mints the cookie via `POST /login`).
- Otherwise the ALB-injected `X-Amzn-Oidc-Data` JWT (Cognito/Google) is decoded
  to email + groups. Membership in `DUCKGRES_ADMIN_SSO_GROUP` → **admin**, else
  **viewer**. (Unset group → admin for any SSO user, logged — set it in prod.)

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

`authz_test.go` (SSO role mapping, RoleGate, SQL classifier), `dashboard_test.go`
(TokenSet / break-glass login / cookie), `api_test.go` + `api_postgres_test.go`
(CRUD), `models_api_test.go` (redaction). e2e: the `admin_*` /
`impersonation_*` / `models_explorer_api` assertions in
`tests/e2e-mw-dev/harness.sh`.
