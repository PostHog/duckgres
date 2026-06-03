# Kubernetes Deployment

This directory contains **development/reference manifests** for running duckgres in Kubernetes with the multitenant control plane spawning DuckDB worker pods on demand. The `remote` worker backend now requires a config store. These manifests are intended for local development and testing; production deployments should adapt them to your cluster's requirements (resource limits, persistent storage, ingress, monitoring, etc.).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                  │
│                                                      │
│  ┌────────────────────────────────┐                  │
│  │ Control Plane Pod              │                  │
│  │  duckgres --mode control-plane │                  │
│  │  --worker-backend remote       │                  │
│  │  --config-store ...            │                  │
│  │                                │                  │
│  │  Creates worker pods via K8s   │                  │
│  │  API, routes queries via gRPC  │                  │
│  └──────────┬─────────────────────┘                  │
│             │ Arrow Flight SQL (TCP :8816)            │
│             ├──────────────┬──────────────┐           │
│             ▼              ▼              ▼           │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │
│  │ Worker Pod 0 │ │ Worker Pod 1 │ │ Worker Pod N │  │
│  │  DuckDB svc  │ │  DuckDB svc  │ │  DuckDB svc  │  │
│  │  Bearer auth │ │  Bearer auth │ │  Bearer auth │  │
│  └──────────────┘ └──────────────┘ └──────────────┘  │
│                                                      │
│  Runtime coordination lives in config-store Postgres │
│  (`cp_instances`, `worker_records`, Flight sessions) │
│                                                      │
│  Worker pods have:                                   │
│  - SecurityContext: non-root (UID 1000)              │
│  - Bearer token from K8s Secret                      │
│  - ConfigMap mount for shared config                 │
└─────────────────────────────────────────────────────┘
```

The control plane handles TLS, authentication, PostgreSQL wire protocol, and SQL transpilation. Workers are thin DuckDB execution engines exposed via Arrow Flight SQL. The shared warm pool keeps neutral workers ready when `--k8s-shared-warm-target` is greater than zero, activates workers per org on demand, and moves them to `hot_idle` after their last session. Hot-idle workers keep their org assignment and DuckLake attachment for same-org reuse, then are retired by the janitor after 5 minutes if not reclaimed. Planned rolling replacements mark old replicas draining and fail readiness before termination; unplanned control-plane failure still drops existing pgwire connections.

## Manifests

| File | Description |
|------|-------------|
| `namespace.yaml` | `duckgres` namespace |
| `rbac.yaml` | Control-plane and neutral worker ServiceAccounts, Role (pods + secrets), RoleBinding |
| `configmap.yaml` | Shared duckgres config (users, extensions, data dir) |
| `secret.yaml` | Bearer token secret (auto-populated by CP if empty) |
| `managed-warehouse-secrets.yaml` | Local secret payloads referenced by the seeded managed-warehouse contract |
| `worker-identity.yaml` | Local worker ServiceAccount referenced by the seeded managed-warehouse contract |
| `networkpolicy.yaml` | Restricts worker ingress to CP pods only |
| `control-plane-multitenant-local.yaml` | Optional OrbStack-oriented shared warm-worker control-plane manifest |
| `kind/config-store.overlay.yaml` | Compose overlay that attaches local dependency containers to the external Docker `kind` network |
| `kind/config-store.seed.sql` | Kind-oriented managed-warehouse seed for the shared warm-worker flow |
| `kind/control-plane.yaml` | Kind-first shared warm-worker control-plane manifest used by local dev and CI |
| `orbstack/dependency-ports.overlay.yaml` | Optional OrbStack overlay that publishes local DuckLake and MinIO dependency ports on the host |

## Configuration

Key flags for Kubernetes multitenant mode:

| Flag | Env Var | Description |
|------|---------|-------------|
| `--worker-backend remote` | - | Use K8s remote workers in config-store-backed multitenant mode |
| `--config-store` | `DUCKGRES_CONFIG_STORE` | PostgreSQL config-store connection string required for remote mode |
| `--session-init-timeout` | `DUCKGRES_SESSION_INIT_TIMEOUT` | Session startup metadata initialization and catalog probe timeout (`10s` default) |
| `--handover-drain-timeout` | `DUCKGRES_HANDOVER_DRAIN_TIMEOUT` | Max time to drain planned shutdowns/upgrades before forced exit (`15m` default in remote mode) |
| `--sni-routing-mode` | `DUCKGRES_SNI_ROUTING_MODE` | Managed-hostname routing: `off`, `passthrough`, or `enforce`. Postgres uses requested dbname first; managed SNI must resolve to the same org, and SNI supplies the database only when dbname is empty |
| `--managed-hostname-suffixes` | `DUCKGRES_MANAGED_HOSTNAME_SUFFIXES` | Comma-separated managed hostname suffixes such as `.dw.test.local` |
| `--k8s-worker-image` | `DUCKGRES_K8S_WORKER_IMAGE` | Docker image for worker pods |
| `--k8s-worker-image-pull-policy` | `DUCKGRES_K8S_WORKER_IMAGE_PULL_POLICY` | Image pull policy (`Never`, `IfNotPresent`, `Always`) |
| `--k8s-worker-service-account` | `DUCKGRES_K8S_WORKER_SERVICE_ACCOUNT` | Neutral ServiceAccount name for worker pods (`duckgres-worker` default) |
| `--k8s-worker-secret` | `DUCKGRES_K8S_WORKER_SECRET` | K8s Secret name for bearer token |
| `--k8s-worker-configmap` | `DUCKGRES_K8S_WORKER_CONFIGMAP` | ConfigMap name for worker config |
| `--k8s-shared-warm-target` | `DUCKGRES_K8S_SHARED_WARM_TARGET` | Global neutral shared warm-worker target for multi-tenant K8s mode (`0` disables prewarm; subject to `--k8s-max-workers`) |

The worker Secret setting is a base name for per-worker RPC Secrets. Each worker pod gets its own derived Secret containing its RPC bearer token and TLS material. If the derived Secret does not exist, the control plane creates it before spawning the pod.

Shared warm workers should use the neutral `duckgres-worker` ServiceAccount with `automountServiceAccountToken: false`. Tenant authority must arrive only through activation-time scoped credentials.

For managed-hostname routing, `passthrough` logs legacy/non-managed callers while allowing them to route by requested dbname. `enforce` rejects Postgres clients whose TLS SNI does not match a configured managed suffix. In both managed modes, when SNI does match a suffix, the hostname prefix and requested Postgres database must resolve to the same org; if the client omits the startup database, the SNI prefix is used as the fallback database source. Unknown routing-mode values behave like `off`.

For seamless planned deployments, use a rolling strategy with overlap and enough termination grace period for drain completion. The provided control-plane manifests now set:

- `strategy.rollingUpdate.maxUnavailable: 0`
- `strategy.rollingUpdate.maxSurge: 1`
- `terminationGracePeriodSeconds: 900`
- `--handover-drain-timeout 15m`

That gives the old replica time to fail readiness, stop taking new pgwire sessions, keep existing pgwire and Flight sessions alive during the drain window, and then force shutdown at the timeout boundary if sessions remain.

## Local Development with kind

The primary shared warm-worker workflow now uses [`kind`](https://kind.sigs.k8s.io/). Prerequisites: Docker, `kubectl`, `kind`, and `just`.

```bash
just run-multitenant-kind
just multitenant-port-forward-pg
just multitenant-port-forward-api
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres dbname=duckgres sslmode=require"
```

`just multitenant-port-forward-pg` forwards both pgwire on `5432` and Flight SQL on `8815`.

`just run-multitenant-kind` recreates a local kind cluster, starts the config store plus the local warehouse DB, DuckLake metadata DB, and MinIO backing the seeded managed-warehouse contract, attaches those dependency containers to the Docker `kind` network, loads the locally built image into kind, and deploys the shared warm-worker control plane.

Default login: `postgres / postgres`

## Optional OrbStack Workflow

[OrbStack](https://orbstack.dev/) remains available as an optional local workflow on macOS. Prerequisites: OrbStack Kubernetes enabled, Docker, `kubectl`, and `just`. This flow uses `host.docker.internal` to reach the local dependency containers from the cluster.

```bash
orb start k8s
just run-multitenant-local
just multitenant-port-forward-pg
just multitenant-port-forward-api
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres sslmode=require"
```

`just multitenant-port-forward-pg` forwards both pgwire on `5432` and Flight SQL on `8815`.

The admin dashboard requires the admin token printed in the control-plane logs. Fetch it with:

```bash
kubectl -n duckgres logs deployment/duckgres-control-plane | rg "Generated admin API token"
```

The local seed populates one managed-warehouse contract for the `local` team. That row includes separate `warehouse_database` and `metadata_store` sections plus secret references only, not secret values. Every non-empty managed-warehouse secret ref stores an explicit namespace, and that namespace must match `worker_identity.namespace`.

Seeded warehouse contract notes:

- The config store keeps at most one managed-warehouse row per team.
- `GET /api/v1/teams/local/warehouse` reads that row.
- `PUT /api/v1/teams/local/warehouse` replaces that row for the team.
- `just multitenant-seed-local` is idempotent and updates the same `local` warehouse row rather than creating duplicates.
- Destructive production teardown is covered by the [Managed Warehouse Deprovision runbook](../docs/runbooks/managed-warehouse-deprovision.md).

Tear down the local environments:

```bash
just kind-cluster-down
just multitenant-config-store-down-kind
# or, for the optional OrbStack path:
just multitenant-config-store-down
```

After code changes, rerun `just run-multitenant-kind` to rebuild, redeploy, and reseed the default local environment.

## Iceberg (Lakekeeper) Local Dev

The local multitenant flows (both kind and OrbStack) now bring up a [Lakekeeper](https://lakekeeper.io/) Iceberg REST catalog alongside the existing MinIO + Postgres dependencies, and seed the `local` tenant as a **"both" warehouse** — DuckLake *and* Iceberg enabled on the same row. No extra commands are needed: `just run-multitenant-local` / `just run-multitenant-kind` start and bootstrap it automatically.

What runs (added to `k8s/local-config-store.compose.yaml`):

- `lakekeeper-catalog` — Postgres backing Lakekeeper's own catalog metadata (tmpfs).
- `lakekeeper-migrate` — one-shot schema migration; `serve` waits for it.
- `lakekeeper` — the REST catalog in **allow-all (no-auth)** mode, pinned to `quay.io/lakekeeper/catalog:v0.12.3`, exposed on host port **38181** in both flows.

After the catalog is healthy, `k8s/lakekeeper/bootstrap.sh` (run by the `…-config-store-up` recipes) bootstraps the server and creates the warehouse `local` against the MinIO bucket `duckgres-local`. It is idempotent — the catalog Postgres is tmpfs, so bootstrap re-runs cleanly on every `up`.

Connect to the Iceberg catalog by selecting `dbname=iceberg`:

```bash
just multitenant-port-forward-pg   # in another shell
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres dbname=iceberg sslmode=require"

# e.g.
CREATE SCHEMA demo;
CREATE TABLE demo.t (id int, name text);
INSERT INTO demo.t VALUES (1, 'a');
DROP SCHEMA demo CASCADE;
```

How the wiring works:

- The worker reaches Lakekeeper at `host.docker.internal:38181/catalog` (OrbStack) or `duckgres-lakekeeper:8181/catalog` (kind) — see the `iceberg_*` columns in the seed files.
- ATTACH uses `AUTHORIZATION_TYPE 'none'` (allow-all), so no OAuth2 secret is needed.
- **Iceberg data S3 access reuses the DuckLake `ducklake_s3` secret.** The iceberg-only S3 secret has no MinIO endpoint knobs, so an Iceberg session only gets working MinIO reads/writes when a DuckLake catalog is attached first in the same session — hence the "both" tenant. Lakekeeper itself reaches MinIO via the compose service name (`http://minio:9000`); both point at the same bucket, and Iceberg metadata stores absolute `s3://` paths, so the differing endpoints are consistent.
- NetworkPolicy is **not enforced** on OrbStack/kind (default CNIs don't enforce it), so the worker can reach Lakekeeper on a non-standard port without policy changes. The policies in this directory still document the intended production boundaries.

### Validating the Iceberg DROP SCHEMA CASCADE path

`just qa-iceberg-drop-schema` runs `tests/clusterqa/iceberg_drop_schema_test.go` against the running cluster (needs an active `just multitenant-port-forward-pg`). It connects with `dbname=iceberg`, creates a populated schema, and runs `DROP SCHEMA … CASCADE` — exercising the `server/iceberg_drop_schema.go` fallback whose internal list-tables query hit the Flight `?`-placeholder bug. It fails on an unfixed image and passes once the fix is in.

### Running Integration Tests

```bash
# Against an existing deployment (skip build/deploy, just run tests)
DUCKGRES_K8S_TEST_SKIP_SETUP=true go test -v -count=1 -tags k8s_integration -timeout 600s ./tests/k8s/...

# Full default setup (kind)
just test-k8s-integration

# Full setup against the optional OrbStack/local mode
DUCKGRES_K8S_TEST_SETUP=local go test -v -count=1 -tags k8s_integration -timeout 600s ./tests/k8s/...
```

Test environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKGRES_K8S_TEST_SKIP_SETUP` | - | Set to `true` to skip build/deploy/teardown |
| `DUCKGRES_K8S_TEST_SETUP` | `kind` | Managed setup mode: `kind` for the default local/CI path, `local` for the optional OrbStack path |
| `DUCKGRES_K8S_TEST_NAMESPACE` | `duckgres` | K8s namespace for test resources |
