# Control-plane admin dashboard

The control plane (multi-tenant / `kubernetes` build tag) serves a small admin
dashboard on `:8080`. It is gated behind the internal secret — there is no
public exposure; reach it via `kubectl port-forward`.

## Pages

| Route | Page | Purpose |
|-------|------|---------|
| `/`, `/models` | `static/models.html` | **Models explorer** — the primary surface. Sidebar of every config-store model grouped Tenants / Config / Runtime, a table of the selected model's rows, and a click-through detail panel. Nested warehouse sub-configs render as expandable sections. |
| `/orgs` | `static/orgs.html` | Org list + create/delete. |
| `/workers` | `static/workers.html` | Live worker/duckling status. |
| `/sessions` | `static/sessions.html` | Active sessions. |
| `/settings` | `static/settings.html` | Singleton config editing. |
| `POST /login` | `static/login.html` | Token login (sets the `duckgres_admin_token` cookie). |

The pages are plain HTML + vanilla JS served via `//go:embed static/*` and
`html/template`. The models explorer and login page follow the dark "mission
control" design language (Chakra Petch + IBM Plex Mono, cyan/amber accents).
Because the templates are parsed with `html/template`, **do not write `{{` in
their inline JS** — only `login.html` uses template directives (`{{.Next}}` /
`{{.Error}}`).

## Auth

- Service-to-service: `X-Duckgres-Internal-Secret` header.
- Dashboard UI: the `POST /login` form mints an `HttpOnly` cookie.
- The internal secret plus rotation fallbacks are accepted (see `TokenSet`).
- `?token=` URL auth is deliberately rejected (#721).

## Models explorer API (read-only)

Backed by `models_api.go`. The registry in `modelDescriptors()` is the single
source of truth — add a config-store model there and it appears in the sidebar,
listing, and column derivation.

- `GET /api/v1/models` → `{ "models": [ { key, label, group, count } ] }` — sidebar with live row counts.
- `GET /api/v1/models/:model` → `{ key, label, group, table, columns, count, truncated, rows }` — one model's rows.

Rows are scanned into the typed model and marshaled via its `json` tags, so
columns tagged `json:"-"` (`OrgUser.Password`, `OrgUserSecret.Ciphertext`,
`DuckLakeConfig.S3SecretKey`, the singleton IDs) are dropped from the response —
**never swap the typed scan for a raw `map` scan**, that would leak them.
Runtime-schema tables (`worker_records`, `cp_instances`, …) are schema-qualified
with `ConfigStore.RuntimeSchema()`. Listings are capped at `modelsRowLimit`
(`truncated: true` when hit).

Covered by `models_api_test.go` (redaction + real query path) and the
`models_explorer_api` assertion in `tests/e2e-mw-dev/harness.sh`.

## Local UI development (no redeploy)

Iterate on the UI against a *deployed* control plane without rebuilding the
image. A tiny dev proxy (`devserver/`) serves `static/` off disk and proxies
`/api`, `/login`, `/health` to the port-forwarded CP, injecting the internal
secret server-side. The browser sees one origin → no CORS, and the secret never
reaches page JS. The embedded UI uses relative `/api/v1` paths, so the same HTML
runs byte-for-byte embedded or under the dev proxy.

```sh
# 1. port-forward the deployed control plane's admin API (own terminal):
just ui-port-forward                       # defaults: posthog-mw-dev / duckgres / duckgres-control-plane
#   or: kubectl --context <ctx> -n <ns> port-forward deploy/<cp> 8080:8080

# 2. serve the UI locally, pointed at the port-forward:
DUCKGRES_INTERNAL_SECRET=<cp-secret> just ui-dev
#   → http://127.0.0.1:5173

# 3. edit controlplane/admin/static/*.html and refresh the browser. No rebuild.
```

Once the markup is right, it is already what `//go:embed` bakes into the CP — no
separate build step.
