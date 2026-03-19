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
│  - Owner references → CP pod (GC on CP deletion)    │
│  - SecurityContext: non-root (UID 1000)              │
│  - Bearer token from K8s Secret                      │
│  - ConfigMap mount for shared config                 │
└─────────────────────────────────────────────────────┘
```

The control plane handles TLS, authentication, PostgreSQL wire protocol, and SQL transpilation. Workers are thin DuckDB execution engines exposed via Arrow Flight SQL. Workers are spawned on demand (one per connection) and reaped when idle.

## Manifests

| File | Description |
|------|-------------|
| `namespace.yaml` | `duckgres` namespace |
| `rbac.yaml` | ServiceAccount, Role (pods + secrets), RoleBinding |
| `configmap.yaml` | Shared duckgres config (users, extensions, data dir) |
| `secret.yaml` | Bearer token secret (auto-populated by CP if empty) |
| `local-config-store.compose.yaml` | Local config-store PostgreSQL plus the warehouse-db, DuckLake metadata DB, and MinIO runtime dependencies |
| `multitenant-local-runtime.yaml` | Local multi-tenant worker ServiceAccount; the runtime Secret is generated at deploy time |
| `networkpolicy.yaml` | Restricts worker ingress to CP pods only |
| `control-plane-deployment.yaml` | CP Deployment + ClusterIP Service |

## Configuration

Key flags for Kubernetes mode:

| Flag | Env Var | Description |
|------|---------|-------------|
| `--worker-backend remote` | - | Use remote workers (K8s pods) instead of local processes |
| `--k8s-worker-image` | `DUCKGRES_K8S_WORKER_IMAGE` | Docker image for worker pods |
| `--k8s-worker-image-pull-policy` | `DUCKGRES_K8S_WORKER_IMAGE_PULL_POLICY` | Image pull policy (`Never`, `IfNotPresent`, `Always`) |
| `--k8s-worker-secret` | `DUCKGRES_K8S_WORKER_SECRET` | K8s Secret name for bearer token |
| `--k8s-worker-configmap` | `DUCKGRES_K8S_WORKER_CONFIGMAP` | ConfigMap name for worker config |

The bearer token secret is used to authenticate gRPC connections between the control plane and workers. If the secret exists but is empty, the CP auto-generates a random token and populates it.

## Deploy (Dev)

These manifests use permissive defaults suitable for local development (no resource limits, emptyDir volumes, self-signed TLS). For production, you should customize resource requests/limits, storage, TLS certificates, and network policies for your environment.

```bash
# Build with Kubernetes support
docker build --build-arg BUILD_TAGS=kubernetes -t duckgres:latest .

# Apply all manifests
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/networkpolicy.yaml
kubectl apply -f k8s/control-plane-deployment.yaml

# Wait for readiness
kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s
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

`just run-multitenant-local` starts the config-store PostgreSQL plus the local warehouse database, DuckLake metadata PostgreSQL, and MinIO bucket backing the managed-warehouse contract. The config-store seed points the `local` team at those host-reachable endpoints, and `go run ./cmd/render-multitenant-local-runtime` renders the `duckgres-local-runtime` Secret from the seeded contract during `deploy-multitenant-local`.

The local seed populates one managed-warehouse contract for the `local` team. That row includes separate `warehouse_database` and `metadata_store` sections plus secret references only, not secret values.

Seeded warehouse contract notes:

- The config store keeps at most one managed-warehouse row per team.
- `GET /api/v1/teams/local/warehouse` reads that row.
- `PUT /api/v1/teams/local/warehouse` replaces that row for the team.
- `just multitenant-seed-local` is idempotent and updates the same `local` warehouse row rather than creating duplicates.
- The worker runtime Secret is generated locally from the seeded contract and resolved secret material instead of being checked into git.

Tear down the local config store:

```bash
just multitenant-config-store-down
```

After code changes, rerun `just run-multitenant-local` to rebuild, redeploy, reseed, and refresh the local runtime artifacts.

### Running Integration Tests

```bash
# Against an existing multi-tenant deployment (skip setup, just run tests)
DUCKGRES_K8S_TEST_SKIP_SETUP=true go test -v -tags k8s_integration -timeout 600s ./tests/k8s/...

# Full setup (builds image, starts the config store and local warehouse/metadata/S3 deps, deploys the multi-tenant control plane, seeds config store, renders runtime artifacts, runs tests, cleans up)
go test -v -tags k8s_integration -timeout 600s ./tests/k8s/...
```

Test environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKGRES_K8S_TEST_SKIP_SETUP` | - | Set to `true` to skip build/deploy/teardown |
| `DUCKGRES_K8S_TEST_NAMESPACE` | `duckgres` | K8s namespace for externally managed deployments; managed setup uses `duckgres` |

### Cleanup

```bash
kubectl delete namespace duckgres
```
