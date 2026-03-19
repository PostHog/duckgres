# Kubernetes Deployment

This directory contains **development/reference manifests** for running duckgres in Kubernetes with the control plane spawning DuckDB worker pods on demand. These are intended for local development and testing — production deployments should adapt these to your cluster's requirements (resource limits, persistent storage, ingress, monitoring, etc.).

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                  │
│                                                      │
│  ┌────────────────────────────────┐                  │
│  │ Control Plane Pod              │                  │
│  │  duckgres --mode control-plane │                  │
│  │  --worker-backend remote       │                  │
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
│  Worker pods have:                                   │
│  - Team-specific runtime Secret mount                │
│  - SecurityContext: non-root (UID 1000)              │
│  - Bearer token from K8s Secret                      │
│  - Team-scoped labels and identity                   │
└─────────────────────────────────────────────────────┘
```

The control plane handles TLS, authentication, PostgreSQL wire protocol, and SQL transpilation. Workers are thin DuckDB execution engines exposed via Arrow Flight SQL. Workers are spawned on demand (one per connection) and reaped when idle.

## Manifests

| File | Description |
|------|-------------|
| `shared/namespace.yaml` | `duckgres` namespace |
| `shared/rbac.yaml` | ServiceAccount, Role (pods + secrets), RoleBinding |
| `shared/configmap.yaml` | Shared control-plane config for the local manifests |
| `shared/secret.yaml` | Bearer token secret (auto-populated by CP if empty) |
| `shared/networkpolicy.yaml` | Restricts worker ingress to CP pods only |
| `shared/config-store.compose.yaml` | Shared local config-store PostgreSQL plus the warehouse-db, DuckLake metadata DB, and MinIO runtime dependencies |
| `shared/worker-identity.yaml` | Shared multitenant worker ServiceAccount; the runtime Secret is generated at deploy time |
| `kind/config-store.overlay.yaml` | Kind-specific compose overlay that joins the external Docker `kind` network |
| `kind/config-store-seed.sql` | Managed-warehouse seed data for the default `kind` workflow |
| `kind/control-plane.yaml` | Default multitenant control-plane deployment for local `kind` and CI runs |
| `orbstack/config-store-seed.sql` | Managed-warehouse seed data for the optional OrbStack workflow |
| `orbstack/control-plane.yaml` | Optional OrbStack-specific multitenant control-plane deployment |

## Configuration

Key flags for Kubernetes mode:

| Flag | Env Var | Description |
|------|---------|-------------|
| `--worker-backend remote` | - | Use remote workers (K8s pods) instead of local processes |
| `--k8s-worker-image` | `DUCKGRES_K8S_WORKER_IMAGE` | Docker image for worker pods |
| `--k8s-worker-image-pull-policy` | `DUCKGRES_K8S_WORKER_IMAGE_PULL_POLICY` | Image pull policy (`Never`, `IfNotPresent`, `Always`) |
| `--k8s-worker-secret` | `DUCKGRES_K8S_WORKER_SECRET` | K8s Secret name for bearer token |

The bearer token secret is used to authenticate gRPC connections between the control plane and workers. If the secret exists but is empty, the CP auto-generates a random token and populates it. On the multitenant managed-warehouse path, team workers no longer fall back to a shared worker ConfigMap; they require a per-team `runtime_config` Secret.

Kubernetes support in this repo is now centered on the multi-tenant control-plane path. The supported local and CI workflow is `kind` via `just run-multitenant-kind`, with OrbStack retained only as an alternate local workflow.

## Local Development with kind

The default local and CI-backed Kubernetes workflow uses [`kind`](https://kind.sigs.k8s.io/). Prerequisites: Docker, `kubectl`, and `kind`.

```bash
just run-multitenant-kind
just multitenant-port-forward-pg
just multitenant-port-forward-admin
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres sslmode=require"
open http://127.0.0.1:9090/
```

Default login: `postgres / postgres`

The admin dashboard requires the admin token printed in the control-plane logs. Fetch it with:

```bash
kubectl -n duckgres logs deployment/duckgres-control-plane | rg "Generated admin API token"
```

`just run-multitenant-kind` recreates a local `kind` cluster, starts the config-store PostgreSQL plus the local warehouse database, DuckLake metadata PostgreSQL, and MinIO bucket backing the managed-warehouse contract, attaches those dependency containers to the `kind` Docker network, and deploys the multi-tenant control plane. The config-store seed points the `local` team at those container-reachable endpoints, and `go run ./cmd/render-multitenant-local-runtime` renders the `duckgres-local-runtime` Secret from the seeded contract during `deploy-multitenant-kind`.

## Optional OrbStack Workflow

[OrbStack](https://orbstack.dev/) remains available as an alternate macOS-local workflow. Prerequisites: OrbStack Kubernetes enabled, Docker, and `kubectl`. This flow assumes `host.docker.internal` is reachable from the cluster.

```bash
orb start k8s
just run-multitenant-local
```

Use this only when you specifically want the OrbStack-backed local cluster flow. The repo's standard K8s test path uses `kind`.

The local seed populates one managed-warehouse contract for the `local` team. That row includes separate `warehouse_database` and `metadata_store` sections plus secret references only, not secret values.

Seeded warehouse contract notes:

- The config store keeps at most one managed-warehouse row per team.
- `GET /api/v1/teams/local/warehouse` reads that row.
- `PUT /api/v1/teams/local/warehouse` replaces that row for the team.
- `just multitenant-seed-kind` and `just multitenant-seed-local` are idempotent and update the same `local` warehouse row rather than creating duplicates.
- The worker runtime Secret is generated locally from the seeded contract and resolved secret material instead of being checked into git.

Tear down the local config store:

```bash
just multitenant-config-store-down
```

After code changes, rerun `just run-multitenant-kind` to rebuild, redeploy, reseed, and refresh the local runtime artifacts. Use `just run-multitenant-local` only for the optional OrbStack path.

### Running Integration Tests

```bash
# Against an existing multi-tenant deployment (skip setup, just run tests)
DUCKGRES_K8S_TEST_SKIP_SETUP=true go test -v -count=1 -tags k8s_integration -timeout 600s ./tests/k8s/...

# Full local setup against the default kind environment
go test -v -count=1 -tags k8s_integration -timeout 600s ./tests/k8s/...

# Full setup against the optional OrbStack environment
DUCKGRES_K8S_TEST_SETUP=local go test -v -count=1 -tags k8s_integration -timeout 600s ./tests/k8s/...
```

Test environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKGRES_K8S_TEST_SKIP_SETUP` | - | Set to `true` to skip build/deploy/teardown |
| `DUCKGRES_K8S_TEST_SETUP` | `kind` | Managed setup mode: `kind` for the standard local/CI path, `local` for the optional OrbStack-style cluster flow |
| `DUCKGRES_K8S_TEST_NAMESPACE` | `duckgres` | K8s namespace for externally managed deployments; managed setup uses `duckgres` |

### Cleanup

```bash
kubectl delete namespace duckgres
```
