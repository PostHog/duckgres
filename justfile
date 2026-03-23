# Duckgres — PostgreSQL wire protocol server backed by DuckDB

# Default recipe: list all available recipes
default:
    @just --list --list-submodules

# === Build ===

# Build the duckgres binary
[group('build')]
build:
    go build -o duckgres .

# Build with version info baked in
[group('build')]
build-release version="dev":
    go build -ldflags "-X main.version={{version}} -X main.commit=$(git rev-parse --short HEAD) -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o duckgres .

# Build Docker image
[group('build')]
docker tag="duckgres:dev":
    docker build -t {{tag}} .

# Clean build artifacts
[group('build')]
clean:
    go clean
    rm -f duckgres

# === Dev ===

num_cores := `sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4`

# Run in standalone mode
[group('dev')]
run: build
    ./duckgres

# Run in control-plane mode
[group('dev')]
run-control-plane: build
    ./duckgres --mode control-plane --process-min-workers {{num_cores}} --socket-dir ./sockets

# Build a Kubernetes-enabled image for local cluster work
[group('dev')]
build-k8s-image tag="duckgres:test":
    docker build --build-arg BUILD_TAGS=kubernetes -t {{tag}} .

# Recreate the local kind cluster used by the portable K8s integration flow
[group('dev')]
kind-cluster-reset:
    kind delete cluster --name "${DUCKGRES_KIND_CLUSTER_NAME:-duckgres}" || true
    kind create cluster --name "${DUCKGRES_KIND_CLUSTER_NAME:-duckgres}" --wait 120s
    kind export kubeconfig --name "${DUCKGRES_KIND_CLUSTER_NAME:-duckgres}" --kubeconfig "${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}"

# Delete the local kind cluster used by the portable K8s integration flow
[group('dev')]
kind-cluster-down:
    kind delete cluster --name "${DUCKGRES_KIND_CLUSTER_NAME:-duckgres}" || true

# Load the local Kubernetes-enabled image into the kind cluster
[group('dev')]
kind-load-k8s-image tag="duckgres:test":
    kind load docker-image {{tag}} --name "${DUCKGRES_KIND_CLUSTER_NAME:-duckgres}"

# Verify that the fixed local dependency ports needed by the optional OrbStack workflow are available
[group('dev')]
check-multitenant-local-ports:
    @set -eu; \
    failed=0; \
    for spec in \
      "5434 config-store-postgres" \
      "35434 warehouse-postgres" \
      "35433 ducklake-metadata" \
      "39000 minio-api" \
      "39001 minio-console"; do \
      port="${spec%% *}"; \
      name="${spec#* }"; \
      if nc -z 127.0.0.1 "${port}" >/dev/null 2>&1; then \
        echo "Required local dev port ${port} is already in use (${name})."; \
        if command -v lsof >/dev/null 2>&1; then \
          lsof -nP -iTCP:"${port}" -sTCP:LISTEN || true; \
        elif command -v ss >/dev/null 2>&1; then \
          ss -ltn "( sport = :${port} )" || true; \
        fi; \
        failed=1; \
      fi; \
    done; \
    if [ "${failed}" -ne 0 ]; then \
      echo "Free the occupied local dev ports before running the duckgres multitenant K8s setup."; \
      exit 1; \
    fi

# Verify that the config-store port needed by the kind-backed multitenant K8s flow is available
[group('dev')]
check-multitenant-kind-ports:
    @set -eu; \
    if nc -z 127.0.0.1 5434 >/dev/null 2>&1; then \
      echo "Required local dev port 5434 is already in use (config-store-postgres)."; \
      if command -v lsof >/dev/null 2>&1; then \
        lsof -nP -iTCP:5434 -sTCP:LISTEN || true; \
      elif command -v ss >/dev/null 2>&1; then \
        ss -ltn "( sport = :5434 )" || true; \
      fi; \
      echo "Free the occupied local dev port before running the duckgres multitenant kind setup."; \
      exit 1; \
    fi

# Start the local PostgreSQL config store used by the multi-tenant K8s flow
[group('dev')]
multitenant-config-store-up: check-multitenant-local-ports
    docker compose -f k8s/local-config-store.compose.yaml -f k8s/orbstack/dependency-ports.overlay.yaml up -d --wait
    docker exec duckgres-local-minio mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
    docker exec duckgres-local-minio mc mb local/duckgres-local --ignore-existing

# Start the local PostgreSQL config store used by the kind-backed multi-tenant K8s flow
[group('dev')]
multitenant-config-store-up-kind: check-multitenant-kind-ports
    docker compose -f k8s/local-config-store.compose.yaml -f k8s/kind/config-store.overlay.yaml up -d --wait
    docker exec duckgres-local-minio mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
    docker exec duckgres-local-minio mc mb local/duckgres-local --ignore-existing

# Stop the local PostgreSQL config store used by the multi-tenant K8s flow
[group('dev')]
multitenant-config-store-down:
    docker compose -f k8s/local-config-store.compose.yaml -f k8s/orbstack/dependency-ports.overlay.yaml down -v

# Stop the local PostgreSQL config store used by the kind-backed multi-tenant K8s flow
[group('dev')]
multitenant-config-store-down-kind:
    docker compose -f k8s/local-config-store.compose.yaml -f k8s/kind/config-store.overlay.yaml down -v

# Seed a default local tenant/user into the config store for psql access
[group('dev')]
multitenant-seed-local:
    docker exec -i duckgres-config-store psql -v ON_ERROR_STOP=1 -U duckgres -d duckgres_config < k8s/local-config-store.seed.sql

# Seed a default local tenant/user into the config store for the kind-backed K8s flow
[group('dev')]
multitenant-seed-kind:
    docker exec -i duckgres-config-store psql -v ON_ERROR_STOP=1 -U duckgres -d duckgres_config < k8s/kind/config-store.seed.sql

# Deploy the local multi-tenant control plane to the optional OrbStack Kubernetes workflow
[group('dev')]
deploy-multitenant-local:
    kubectl apply -f k8s/namespace.yaml
    kubectl apply -f k8s/rbac.yaml
    kubectl apply -f k8s/configmap.yaml
    kubectl apply -f k8s/secret.yaml
    kubectl apply -f k8s/worker-identity.yaml
    kubectl apply -f k8s/managed-warehouse-secrets.yaml
    kubectl apply -f k8s/networkpolicy.yaml
    kubectl apply -f k8s/control-plane-multitenant-local.yaml
    kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s

# Deploy the local multi-tenant control plane to kind Kubernetes
[group('dev')]
deploy-multitenant-kind:
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/namespace.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/rbac.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/configmap.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/secret.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/worker-identity.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/managed-warehouse-secrets.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/networkpolicy.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl apply -f k8s/kind/control-plane.yaml
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s

# End-to-end local multi-tenant setup: optional OrbStack K8s + config store + control plane
[group('dev')]
run-multitenant-local: multitenant-config-store-up build-k8s-image deploy-multitenant-local multitenant-seed-local
    kubectl -n duckgres rollout restart deployment/duckgres-control-plane
    kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s
    @echo "Multi-tenant control plane ready."
    @echo "Default login: postgres / postgres"
    @echo "Fetch admin token with: kubectl -n duckgres logs deployment/duckgres-control-plane | rg 'Generated admin API token'"
    @echo "Run 'just multitenant-port-forward-pg', 'just multitenant-port-forward-admin', and 'just multitenant-port-forward-provisioning' in separate terminals."

# End-to-end local multi-tenant setup: kind K8s + config store + control plane
[group('dev')]
run-multitenant-kind: kind-cluster-reset multitenant-config-store-up-kind build-k8s-image kind-load-k8s-image deploy-multitenant-kind multitenant-seed-kind
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl -n duckgres rollout restart deployment/duckgres-control-plane
    KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s
    @echo "Multi-tenant control plane ready on kind."
    @echo "Default login: postgres / postgres"
    @echo "Fetch admin token with: kubectl -n duckgres logs deployment/duckgres-control-plane | rg 'Generated admin API token'"
    @echo "Run 'just multitenant-port-forward-pg' in another terminal if you want direct psql access."

# Tear down the optional OrbStack/local multitenant environment
[group('dev')]
cleanup-multitenant-local:
    kubectl delete namespace duckgres --ignore-not-found --wait=true || true
    just multitenant-config-store-down

# Tear down the kind-backed multitenant environment
[group('dev')]
cleanup-multitenant-kind:
    kubectl delete namespace duckgres --ignore-not-found --wait=true || true
    just multitenant-config-store-down-kind
    just kind-cluster-down

# Port-forward PostgreSQL traffic from the local control plane
[group('dev')]
multitenant-port-forward-pg:
    kubectl -n duckgres port-forward svc/duckgres 5432:5432

# Port-forward the admin dashboard and API from the local control plane
[group('dev')]
multitenant-port-forward-admin:
    kubectl -n duckgres port-forward deployment/duckgres-control-plane 9090:9090

# Port-forward the provisioning API from the local control plane
[group('dev')]
multitenant-port-forward-provisioning:
    kubectl -n duckgres port-forward deployment/duckgres-control-plane 9091:9091

# Run with DuckLake config
[group('dev')]
run-ducklake: build
    ./duckgres --config duckgres_local_ducklake.yaml

# Connect via psql (standalone default port)
[group('dev')]
psql port="5432":
    PGPASSWORD=postgres psql "host=127.0.0.1 port={{port}} user=postgres sslmode=require"

# Watch for changes and rebuild
[group('dev')]
watch:
    watchexec -e go -- go build -o duckgres .

# Format Go source files
[group('dev')]
format:
    gofmt -w .

# === Test ===

# Run all tests
[group('test')]
test:
    go test ./...

# Run unit tests only (server + transpiler)
[group('test')]
test-unit:
    go test -v ./server/... ./transpiler/...

# Run integration tests
[group('test')]
test-integration:
    go test -v ./tests/integration/...

# Run control plane tests
[group('test')]
test-controlplane:
    go test -v -timeout 300s ./tests/controlplane/...

# Run Kubernetes integration tests against the default kind-backed multitenant setup
[group('test')]
test-k8s-integration:
    DUCKGRES_K8S_TEST_SETUP="${DUCKGRES_K8S_TEST_SETUP:-kind}" go test -count=1 -tags kubernetes ./controlplane
    DUCKGRES_K8S_TEST_SETUP="${DUCKGRES_K8S_TEST_SETUP:-kind}" DUCKGRES_K8S_TEST_KUBECONFIG="${DUCKGRES_KIND_KUBECONFIG:-/tmp/duckgres-kind-kubeconfig}" go test -v -count=1 -tags k8s_integration -timeout 600s ./tests/k8s/...

# Run perf tests
[group('test')]
test-perf:
    go test -v ./tests/perf/...

# Run DuckLake-specific tests
[group('test')]
test-ducklake:
    ./scripts/test_ducklake.sh

# Run extension loading tests
[group('test')]
test-extensions:
    ./scripts/test_extensions.sh

# Run rate limiting tests
[group('test')]
test-ratelimit:
    ./scripts/test_ratelimit.sh

# Run Prometheus metrics tests
[group('test')]
test-metrics:
    ./scripts/test_metrics.sh

# Quick perf smoke test
[group('test')]
perf-smoke:
    ./scripts/perf_smoke.sh

# Full perf nightly suite
[group('test')]
perf-nightly:
    ./scripts/perf_nightly.sh

# Lint (matches CI — uses golangci-lint, not go vet)
[group('test')]
lint:
    golangci-lint run

# Run what CI runs: lint + unit + integration + controlplane tests
[group('test')]
ci: lint test-unit test-integration test-controlplane

# === Metrics ===

# Start Prometheus only
[group('metrics')]
prometheus:
    docker compose -f metrics-compose.yml up -d prometheus
    @echo "Prometheus ready at http://localhost:9091"

# Start Prometheus + Grafana, open dashboard
[group('metrics')]
grafana: prometheus
    docker compose -f metrics-compose.yml up -d grafana
    @echo "Waiting for Grafana to start..."
    @until curl -sf http://localhost:3000/api/health > /dev/null 2>&1; do sleep 1; done
    @echo "Grafana ready at http://localhost:3000/d/duckgres-overview/duckgres"
    open http://localhost:3000/d/duckgres-overview/duckgres

# Stop Prometheus + Grafana
[group('metrics')]
metrics-stop:
    docker compose -f metrics-compose.yml down

# Client compatibility tests (just client-compat --list to see recipes)
mod client-compat 'scripts/client-compat/justfile'

# === Scripts ===

# Generate self-signed TLS certs
[group('scripts')]
gen-certs:
    ./scripts/gen-certs.sh

# Seed DuckLake test data
[group('scripts')]
seed-ducklake:
    ./scripts/seed_ducklake.sh

# Bootstrap DuckLake frozen dataset
[group('scripts')]
bootstrap-ducklake:
    ./scripts/bootstrap_ducklake_frozen_dataset.sh
