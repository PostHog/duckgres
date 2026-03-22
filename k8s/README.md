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
| `networkpolicy.yaml` | Restricts worker ingress to CP pods only |
| `control-plane-deployment.yaml` | Local multitenant CP Deployment + ClusterIP Service |
| `control-plane-multitenant-local.yaml` | Alternate local multitenant CP manifest used by helper recipes |

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

## Deploy (Dev)

These manifests use permissive defaults suitable for local development (no resource limits, emptyDir volumes, self-signed TLS, config store on `host.docker.internal:5434`). For production, you should customize resource requests/limits, storage, TLS certificates, network policies, and config-store connectivity for your environment.

```bash
# Build with Kubernetes support
docker build --build-arg BUILD_TAGS=kubernetes -t duckgres:latest .

# Start the local config store required by remote mode
docker compose -f k8s/local-config-store.compose.yaml up -d

# Apply all manifests
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/networkpolicy.yaml
kubectl apply -f k8s/control-plane-deployment.yaml

# Wait for readiness
kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s

# Seed one local team/user so logins work
docker exec -i duckgres-config-store psql -U duckgres -d duckgres_config < k8s/local-config-store.seed.sql
```

## Local Development with OrbStack

[OrbStack](https://orbstack.dev/) provides a lightweight Kubernetes cluster for macOS that shares Docker's image store, so locally built images are immediately available to pods. Local development now defaults to the multi-tenant config-store workflow. Prerequisites: OrbStack Kubernetes enabled, Docker, and `kubectl`. This flow assumes `host.docker.internal` is reachable from the cluster.

```bash
orb start k8s
just run-multitenant-local
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

The local seed populates one managed-warehouse contract for the `local` team. That row includes separate `warehouse_database` and `metadata_store` sections plus secret references only, not secret values.

Seeded warehouse contract notes:

- The config store keeps at most one managed-warehouse row per team.
- `GET /api/v1/teams/local/warehouse` reads that row.
- `PUT /api/v1/teams/local/warehouse` replaces that row for the team.
- `just multitenant-seed-local` is idempotent and updates the same `local` warehouse row rather than creating duplicates.

Tear down the local config store:

```bash
just multitenant-config-store-down
```

After code changes, rerun `just run-multitenant-local` to rebuild, redeploy, and reseed the local environment.

### Running Integration Tests

```bash
# Against an existing deployment (skip build/deploy, just run tests)
DUCKGRES_K8S_TEST_SKIP_SETUP=true go test -v -tags k8s_integration -timeout 600s ./tests/k8s/...

# Full setup (builds image, deploys, runs tests, cleans up)
go test -v -tags k8s_integration -timeout 600s ./tests/k8s/...
```

Test environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKGRES_K8S_TEST_SKIP_SETUP` | - | Set to `true` to skip build/deploy/teardown |
| `DUCKGRES_K8S_TEST_NAMESPACE` | `duckgres` | K8s namespace for test resources |

### Cleanup

```bash
kubectl delete namespace duckgres
docker compose -f k8s/local-config-store.compose.yaml down -v
```
