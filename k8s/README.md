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

[OrbStack](https://orbstack.dev/) provides a lightweight Kubernetes cluster for macOS that shares Docker's image store — images built locally are immediately available to K8s pods without a registry or load step.

```bash
# Start OrbStack Kubernetes
orb start k8s

# Build the image (automatically available to K8s)
docker build --build-arg BUILD_TAGS=kubernetes -t duckgres:test .

# Deploy to OrbStack (patch image to use :test tag)
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/networkpolicy.yaml
sed 's|duckgres:latest|duckgres:test|g' k8s/control-plane-deployment.yaml | kubectl apply -f -

# Wait for the control plane
kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s

# Connect via port-forward
kubectl -n duckgres port-forward svc/duckgres 5432:5432 &
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres sslmode=require"
```

### Iterating

After code changes, rebuild and restart the deployment:

```bash
docker build --build-arg BUILD_TAGS=kubernetes -t duckgres:test .
kubectl -n duckgres rollout restart deployment/duckgres-control-plane
kubectl -n duckgres rollout status deployment/duckgres-control-plane
```

## Local Multi-Tenant Dev

The repo now includes a concrete local workflow for the multi-tenant control-plane path added behind `--config-store`.

Prerequisites:

- macOS with [OrbStack](https://orbstack.dev/) Kubernetes enabled
- Docker and `kubectl`
- `host.docker.internal` reachable from the cluster (OrbStack supports this)

The workflow uses:

- `k8s/local-config-store.compose.yaml` for the PostgreSQL config store on `localhost:5434`
- `k8s/local-config-store.seed.sql` for the default local tenant/user bootstrap
- `k8s/control-plane-multitenant-local.yaml` for a local K8s deployment that enables `--config-store`
- `duckgres:test` as both the control-plane and worker image tag

Bring everything up:

```bash
orb start k8s
just run-multitenant-local
```

Port-forward PostgreSQL and the admin UI in separate terminals:

```bash
just multitenant-port-forward-pg
just multitenant-port-forward-admin
```

Then connect and inspect:

```bash
PGPASSWORD=postgres psql "host=127.0.0.1 port=5432 user=postgres sslmode=require"
open http://127.0.0.1:9090/
```

The local workflow seeds a default login:

```text
username: postgres
password: postgres
```

Tear down the local config store when finished:

```bash
just multitenant-config-store-down
```

Notes:

- The committed local manifest hardcodes the config-store DSN as `postgres://duckgres:duckgres@host.docker.internal:5434/duckgres_config?sslmode=disable`.
- The control plane polls the config store every `2s` in this local-only manifest so seeded users become available quickly after startup.
- If you are not using OrbStack, update `k8s/control-plane-multitenant-local.yaml` to point at a reachable Postgres hostname before deploying.
- The admin API and dashboard are exposed on `:9090` without authentication in the current implementation. Treat this workflow as local-only.

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
```
