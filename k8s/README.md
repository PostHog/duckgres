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
│  Worker pods have:                                   │
│  - Owner references → CP pod (GC on CP deletion)    │
│  - SecurityContext: non-root (UID 1000)              │
│  - Bearer token from K8s Secret                      │
│  - ConfigMap mount for shared config                 │
└─────────────────────────────────────────────────────┘
```

The control plane handles TLS, authentication, PostgreSQL wire protocol, and SQL transpilation. Workers are thin DuckDB execution engines exposed via Arrow Flight SQL. Workers are spawned on demand and reaped when idle.

## Manifests

| File | Description |
|------|-------------|
| `namespace.yaml` | `duckgres` namespace |
| `rbac.yaml` | ServiceAccount, Role (pods + secrets), RoleBinding |
| `configmap.yaml` | Shared duckgres config (users, extensions, data dir) |
| `secret.yaml` | Bearer token secret (auto-populated by CP if empty) |
| `managed-warehouse-secrets.yaml` | Local secret payloads referenced by the seeded managed-warehouse contract |
| `worker-identity.yaml` | Local worker ServiceAccount referenced by the seeded managed-warehouse contract |
| `networkpolicy.yaml` | Restricts worker ingress to CP pods only |
| `control-plane-deployment.yaml` | Local multitenant CP Deployment + ClusterIP Service |
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
| `--k8s-worker-image` | `DUCKGRES_K8S_WORKER_IMAGE` | Docker image for worker pods |
| `--k8s-worker-image-pull-policy` | `DUCKGRES_K8S_WORKER_IMAGE_PULL_POLICY` | Image pull policy (`Never`, `IfNotPresent`, `Always`) |
| `--k8s-worker-secret` | `DUCKGRES_K8S_WORKER_SECRET` | K8s Secret name for bearer token |
| `--k8s-worker-configmap` | `DUCKGRES_K8S_WORKER_CONFIGMAP` | ConfigMap name for worker config |
| `--k8s-shared-warm-target` | `DUCKGRES_K8S_SHARED_WARM_TARGET` | Neutral shared warm-worker target for multi-tenant K8s mode (`0` disables prewarm) |

The bearer token secret is used to authenticate gRPC connections between the control plane and workers. If the secret exists but is empty, the CP auto-generates a random token and populates it.

## Local Development with kind

The primary shared warm-worker workflow now uses [`kind`](https://kind.sigs.k8s.io/). Prerequisites: Docker, `kubectl`, `kind`, and `just`.

```bash
just run-multitenant-kind
just multitenant-port-forward-pg
just multitenant-port-forward-admin
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres sslmode=require"
```

`just run-multitenant-kind` recreates a local kind cluster, starts the config store plus the local warehouse DB, DuckLake metadata DB, and MinIO backing the seeded managed-warehouse contract, attaches those dependency containers to the Docker `kind` network, loads the locally built image into kind, and deploys the shared warm-worker control plane.

Default login: `postgres / postgres`

## Optional OrbStack Workflow

[OrbStack](https://orbstack.dev/) remains available as an optional local workflow on macOS. Prerequisites: OrbStack Kubernetes enabled, Docker, `kubectl`, and `just`. This flow uses `host.docker.internal` to reach the local dependency containers from the cluster.

```bash
orb start k8s
just run-multitenant-local
just multitenant-port-forward-pg
just multitenant-port-forward-admin
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres sslmode=require"
```

The admin dashboard requires the admin token printed in the control-plane logs. Fetch it with:

```bash
kubectl -n duckgres logs deployment/duckgres-control-plane | rg "Generated admin API token"
```

The local seed populates one managed-warehouse contract for the `local` team. That row includes separate `warehouse_database` and `metadata_store` sections plus secret references only, not secret values.

Seeded warehouse contract notes:

- The config store keeps at most one managed-warehouse row per team.
- `GET /api/v1/teams/local/warehouse` reads that row.
- `PUT /api/v1/teams/local/warehouse` replaces that row for the team.
- `just multitenant-seed-local` is idempotent and updates the same `local` warehouse row rather than creating duplicates.

Tear down the local environments:

```bash
just kind-cluster-down
just multitenant-config-store-down-kind
# or, for the optional OrbStack path:
just multitenant-config-store-down
```

After code changes, rerun `just run-multitenant-kind` to rebuild, redeploy, and reseed the default local environment.

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
