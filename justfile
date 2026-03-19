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

# Start the local PostgreSQL config store used by the multi-tenant K8s flow
[group('dev')]
multitenant-config-store-up:
    docker compose -f k8s/local-config-store.compose.yaml up -d

# Stop the local PostgreSQL config store used by the multi-tenant K8s flow
[group('dev')]
multitenant-config-store-down:
    docker compose -f k8s/local-config-store.compose.yaml down -v

# Seed a default local tenant/user into the config store for psql access
[group('dev')]
multitenant-seed-local:
    docker exec -i duckgres-config-store psql -U duckgres -d duckgres_config < k8s/local-config-store.seed.sql

# Deploy the local multi-tenant control plane to OrbStack Kubernetes
[group('dev')]
deploy-multitenant-local:
    kubectl apply -f k8s/namespace.yaml
    kubectl apply -f k8s/rbac.yaml
    kubectl apply -f k8s/configmap.yaml
    kubectl apply -f k8s/secret.yaml
    kubectl apply -f k8s/networkpolicy.yaml
    kubectl apply -f k8s/control-plane-multitenant-local.yaml
    kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s

# End-to-end local multi-tenant setup: OrbStack K8s + config store + control plane
[group('dev')]
run-multitenant-local: multitenant-config-store-up build-k8s-image deploy-multitenant-local multitenant-seed-local
    kubectl -n duckgres rollout restart deployment/duckgres-control-plane
    kubectl -n duckgres wait deployment/duckgres-control-plane --for=condition=available --timeout=120s
    @echo "Multi-tenant control plane ready."
    @echo "Default login: postgres / postgres"
    @echo "Fetch admin token with: kubectl -n duckgres logs deployment/duckgres-control-plane | rg 'Generated admin API token'"
    @echo "Run 'just multitenant-port-forward-pg', 'just multitenant-port-forward-admin', and 'just multitenant-port-forward-provisioning' in separate terminals."

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
