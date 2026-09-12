# Local and CI object storage with PGSTY Silo

Duckgres uses [PGSTY Silo](https://github.com/pgsty/silo), a maintained MinIO
fork, for local development and test object storage. Duckgres continues to use
the generic S3 API. This migration does not change production storage or the
real-S3 performance workflows.

## Release pins

Server and initialization containers use the same immutable multi-platform image:

```text
docker.io/pgsty/silo:RELEASE.2026-09-03T13-18-01Z@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3
```

The image supports `linux/amd64` and `linux/arm64` and bundles `mcli` version
`RELEASE.2026-09-03T07-13-05Z`, pinning both server and admin client together.
The executables are `silo` and `mcli`. Existing `MINIO_*` variables, `/minio/*`
routes, Compose service/container/volume names, and S3 endpoints are retained.

## Defaults and local development

The root Compose stack still exposes S3 at `localhost:9000` and its console at
`http://localhost:9001`, with local credentials `minioadmin` / `minioadmin`.
It initializes the `ducklake` bucket with the existing anonymous-download
policy. Integration and local Kubernetes fixtures retain their existing port
mapping, buckets, credentials, reader policy, and metadata configuration; refer
to their Compose files for the separate fixture defaults.

Start and initialize root Compose storage:

```bash
docker compose up -d --wait minio
docker compose run --rm --no-deps minio-init
docker compose ps -a
docker compose logs minio minio-init
```

Duckgres application configuration and TLS setup are separate from this storage
setup. For integration storage, reconcile the pinned server and initialize it:

```bash
docker compose -f tests/integration/docker-compose.yml up -d --wait minio
docker compose -f tests/integration/docker-compose.yml run --rm --no-deps minio-init
just test-silo
just test-integration
```

`just test-silo` checks S3 access, reader policy, and credential revocation.
`SILO_TEST_CONTAINER` defaults to `duckgres-test-minio`; override it for an
isolated fixture using the same credentials. `just test-integration` alone
does not upgrade an already-running MinIO server. Trino checks use
`just trino-ducklake-smoke`, `just perf-trino-ducklake`, and
`just perf-trino-ducklake-realistic`. Kind and OrbStack dependencies use
`just run-multitenant-kind` and `just run-multitenant-local`.

## Migration and recovery

1. Record the previous checkout and actual server/client image digests; mutable
   tags alone cannot reproduce the old stack. Stop writers, export any needed
   data from tmpfs fixtures, then stop the affected stack.
2. Back up the full object volume (including hidden metadata) and PostgreSQL
   data together while stopped. Do not use `down -v`. Integration and local
   Kubernetes object-storage fixtures use tmpfs rather than persistent volumes.
3. Switch checkout, pull the pinned image, and run the setup commands above for
   the intended stack, preserving its Compose project name and volumes.
4. Confirm healthy storage and successful initialization, then verify bucket
   access and affected tests before resuming writers. On failure inspect
   `docker compose ps -a` and `docker compose logs minio minio-init` (add the
   integration Compose `-f` option for that stack); check ports and credentials.

To roll back, stop writers and the stack, preserve the failed state, and restore
the paired object/metadata backups, previous checkout, and recorded image
digests. Verify access before resuming work; do not assume an older MinIO image
can safely use data written by Silo. Disposable fixtures can instead be explicitly
discarded and reseeded, limited to that fixture's data.
