# Control-plane admin dashboard

The control plane (multi-tenant / `kubernetes` build tag) serves a small admin
dashboard on `:8080`. It is gated behind the internal secret — there is no
public exposure; reach it via `kubectl port-forward`.

## Pages

| Route | Page | Purpose |
|-------|------|---------|
| `/`, `/models` | `static/models.html` | **Models explorer** — the only dashboard surface. Sidebar of every config-store model grouped Tenants / Runtime, a table of the selected model's rows, and a click-through detail panel. Columns are sortable with per-column tooltips; the orgs table cross-links to org-users + managed-warehouses; the orgs detail panel supports inline edit + delete. Nested warehouse sub-configs render as expandable sections. |
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
image. The dev proxy (`devserver/`) serves `static/` off disk and proxies
`/api`, `/login`, `/health` to the CP, injecting the internal secret
server-side. The browser sees one origin → no CORS, and the secret never reaches
page JS. The embedded UI uses relative `/api/v1` paths, so the same HTML runs
byte-for-byte embedded or under the dev proxy.

One `--context` drives everything: the devserver fetches the internal secret
(`kubectl get secret duckgres-tokens`), port-forwards that context's control
plane, and serves the explorer with a **context banner — RED when the context
name contains `prod`** (a dev-only cue; the deployed CP has no such endpoint).

```sh
# one environment:
just ui-dev mw-dev-admin                 # → http://127.0.0.1:5173 (dev banner)
just ui-dev mw-prod-us-admin             # → http://127.0.0.1:5173 (RED prod banner)

# both side by side (prod :5173, dev :5174):
just ui-dev-all

# edit controlplane/admin/static/*.html and refresh — no rebuild.
```

Once the markup is right, it is already what `//go:embed` bakes into the CP — no
separate build step. (Without `--context`, the devserver falls back to an
external `--target` + `--secret`/`$DUCKGRES_INTERNAL_SECRET`.)
