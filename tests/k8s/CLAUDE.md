# Claude Code Context for tests/k8s/

The end-to-end Kubernetes integration suite. Build-tagged `k8s_integration`,
runs against a real kind cluster, real config-store Postgres, real control
plane pod, real worker pods. The iceberg test additionally hits real AWS S3
Tables via GitHub OIDC against the mw-dev sandbox.

## ⚠ Destructive `TestMain` — read before running anything

`TestMain` in `k8s_test.go` unconditionally calls `setupMultiTenant()` which
begins with `kubectl delete namespace duckgres --ignore-not-found --wait=true`
against **whatever kubeconfig is active**. The safety guard
(`requireLocalKindCluster`) runs *after* the destructive call, so a
mis-pointed kubeconfig will delete a non-kind namespace before the check
fires.

The 2026-05-19 incident: `go test -run TestNonExistent ./tests/k8s/` was run
expecting `-run` to filter out everything. It does for `func Test*` — but
`TestMain` always runs. It deleted the `duckgres` namespace on the
operator's default kubeconfig (mw-dev).

**Rules** for any agent or contributor touching this package:

- Never invoke the test binary except for a real end-to-end run with
  `DUCKGRES_K8S_TEST_KUBECONFIG` explicitly pointed at a kind kubeconfig.
- For compile / vet / lint checks, use **build-only**:
  ```
  go test -c -o /tmp/x -tags 'k8s_integration kubernetes' ./tests/k8s/
  go vet -tags 'k8s_integration kubernetes' ./tests/k8s/
  ```
  `-c` builds the binary without running TestMain. `-run TestNonExistent`
  is **not** safe — TestMain still runs.
- The right way to run the suite: `just test-k8s-integration`. The recipe
  sets `DUCKGRES_K8S_TEST_SETUP=kind` and the kubeconfig path for you.

## What's tested here

- `k8s_test.go` — Core control-plane / worker / kind integration. SetupMultiTenant, tenant-isolation harness, basic queries, worker pod creation, crash recovery, version-mismatch reaper, port-forward state machine. Defines `setupMultiTenant`, the suite-wide `TestMain`, and the shared helpers (`openDBConnAs`, `retryDBOperationWithReconnectAs`, `queryIntWithReconnectAs`, `waitForTenantDBReady`).
- `tenant_isolation_test.go` / `tenant_isolation_helper_test.go` — Per-tenant catalog separation, object-store prefix isolation, worker-pod token mount restrictions. Source of the `kubectlCommandOutput` / `queryRuntimeStoreText` diagnostic helpers reused elsewhere.
- `ducklake_test.go` — DuckLake round-trip on the local MinIO + Postgres metadata path: write-read parity, durability across worker restart, concurrent-writer coordination. Uses the `local` tenant fixture.
- `iceberg_test.go` — Iceberg-on-S3-Tables activation against **real AWS** (mw-dev sandbox) via GitHub OIDC. **Activation-level coverage only** — `USE iceberg.<ns>`, `CREATE TABLE iceberg.ns.t`, `SELECT FROM iceberg.ns.t` are blocked upstream (DuckDB iceberg ext bug on the s3_tables endpoint: schema-by-name lookup disagrees with schema enumeration). See the SCOPE block in the file.
- The remaining files (network policy, SNI, RBAC, port-forward state) are individual-feature regression tests.

## When code changes obligate test changes

This suite is the load-bearing coverage for everything that has to work
together end-to-end in the multi-tenant remote-worker topology. Treat
updating it as part of the change, not a follow-up, when touching:

- **Activation pipeline** — `controlplane/shared_worker_activator.go`,
  `controlplane/sts_broker.go`, `duckdbservice/activation.go`,
  `server/worker_activation.go`. The activator builds the per-tenant
  payload and dispatches secret + ATTACH SQL; a wiring regression here
  only surfaces with a real worker pod talking to real cloud storage.
- **Catalog attach paths** — `server.AttachDeltaCatalog`,
  `server.AttachIcebergCatalog`, `server.attachDuckLake*`,
  `server.refresh*Secret`. The `ducklake_test.go` / `iceberg_test.go`
  pollers (`pollIcebergAttached` and the DuckLake count checks) are the
  only end-to-end check that ATTACH actually executed in a worker session.
- **Iceberg backend split** — `server/iceberg/` (config, dispatcher,
  backend implementations). A new backend or a default change requires
  updating the `iceberg_backend` column in
  `iceberg_test.go::buildIcebergConfigStoreSeed`. The default is
  `'lakekeeper'`; omitting the column silently routes activation to the
  wrong path and the test still passes startup but the catalog never
  attaches.
- **ManagedWarehouse / ManagedWarehouseIceberg / ManagedWarehouseS3
  models** — `controlplane/configstore/models.go`. Any new GORM-tagged
  column on these structs needs to be set explicitly in the test seed,
  otherwise the DB default takes over silently and the activator sees a
  value the test author didn't intend.
- **AWS credential plumbing** —
  `controlplane/shared_worker_activator.go::readS3Credentials`,
  `server.IcebergConfig.SessionToken`, anything that flows
  AccessKey/SecretKey/SessionToken into a DuckDB `CREATE SECRET`. STS
  expiry / ASIA-vs-AKIA / session_token-omitted regressions all manifest
  here and only here.

## Iceberg-specific CI requirements

`iceberg_test.go` does **not** silently skip when env vars are missing —
it fails the job openly. The required env vars are documented in the
`TestK8sIcebergRoundTrip` godoc; the CI workflow sets them via
`aws-actions/configure-aws-credentials` (OIDC → STS AssumeRole on
`github-duckgres-iceberg-ci-testing-role` in mw-dev) plus three
`DUCKGRES_K8S_ICEBERG_*` env vars naming the sandbox bucket. The IAM
trust policy + the bucket ARNs are provisioned in
`PostHog/posthog-cloud-infra` (units under
`terraform/environments/aws-accnt-managed-warehouse-dev/us-east-1/`).
If the test starts failing with a "required env vars unset" message,
something upstream of duckgres rotated.

## On-failure diagnostics

The iceberg test calls `captureIcebergActivationDiagnostics()` on a
`pollIcebergAttached` failure, which dumps the control-plane pod logs
(`--tail=200`), the worker pod logs (`-l app=duckgres-worker
--tail=200 --prefix=true`), and the live row from
`duckgres_managed_warehouses` including `iceberg_backend`. The kind
cluster is torn down right after the test exits, so anything not in
that diagnostic dump is unrecoverable. If you add a new failure mode
to activation that needs different diagnostics, extend the function;
don't add ad-hoc print statements.
