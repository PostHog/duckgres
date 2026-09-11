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

The [server release](https://github.com/pgsty/silo/releases/tag/RELEASE.2026-09-03T13-18-01Z)
bundles `mcli` version `RELEASE.2026-09-03T07-13-05Z`. Using the bundled client
also pins the admin executable by the image digest; no runtime download of an
unversioned client is needed.

| Platform | Image manifest digest |
| --- | --- |
| `linux/amd64` | `sha256:885275e0f42acfdf80304c577d2e46c2c3978a276619d60b9eedd7486f104b30` |
| `linux/arm64` | `sha256:c35123a06f2147372523ffc5ce42a0efc4a239405481cbc2a3534bd883257b06` |

Release provenance was checked with `gh attestation verify` against `pgsty/silo`
and `pgsty/mc` for their respective release checksum manifests. Downloaded Linux
archives matched those manifests, and the extracted server and client binary
hashes matched `/usr/bin/silo` and `/usr/bin/mcli` in the container on both
architectures. The [client release](https://github.com/pgsty/mc/releases/tag/RELEASE.2026-09-03T07-13-05Z)
and server release archives have these SHA256 hashes:

| Archive | SHA256 |
| --- | --- |
| Silo Linux amd64 | `cbe5c01eac0a97ccb22fa252eafa432e8608bfde7e3ea27a324cb5ed625a1e96` |
| Silo Linux arm64 | `311846ca9387de36f8e34daa8bf1a130684cc7b8c61aa39581264249eb8df0cf` |
| mcli Linux amd64 | `cd7fcd449bb6b52e2eb727431ba6975b1e5d90df011a75869020ea9ac9e2b2a8` |
| mcli Linux arm64 | `7962afc37c3e60e5758b19e819cb62d2f340ee655fad7b067e2ac9bc5716c2e8` |

The image shell, entrypoint, and executable versions were verified. The isolated
S3/admin contract passed on both architectures, with amd64 run under emulation.

The server executable is `silo`; initialization and administrative operations
use `mcli` instead of `mc`. Silo preserves `MINIO_*` environment variables,
`/minio/*` routes, and the on-disk format according to upstream. The Compose
service names `minio` and `minio-init`, container names, volume names, and S3
endpoints are deliberately retained so existing local configurations and
container-to-container connections continue to work.

## Defaults and local development

The root Compose stack still exposes S3 at `localhost:9000` and its console at
`http://localhost:9001`, with local credentials `minioadmin` / `minioadmin`.
It initializes the `ducklake` bucket with the existing anonymous-download
policy. Integration and local Kubernetes fixtures retain their existing port
mapping, buckets, credentials, reader policy, and metadata configuration; refer
to their Compose files for the separate fixture defaults.

For a new root Compose environment:

```bash
docker compose up -d
docker compose ps -a
docker compose logs minio minio-init
just build
./duckgres --config duckgres.yaml
# In a separate terminal once Duckgres is ready:
just seed-ducklake
```

Before running the integration storage contract, explicitly reconcile the
server with the pinned Compose image, wait for health, and initialize it:

```bash
docker compose -f tests/integration/docker-compose.yml up -d --wait minio
docker compose -f tests/integration/docker-compose.yml run --rm --no-deps minio-init
just test-silo
just test-integration
```

`just test-silo` checks the initialized fixture's S3 and admin behavior. CI uses
the same Compose server and initializer before running this recipe. The
integration harness checks TCP reachability when reusing an existing fixture;
`just test-integration` alone does not upgrade an already-running MinIO server.
The contract script defaults `SILO_TEST_CONTAINER` to `duckgres-test-minio`.
For an isolated fixture, override that container name. Its credential defaults
are `SILO_TEST_ROOT_USER=minioadmin`, `SILO_TEST_ROOT_PASSWORD=minioadmin`,
`SILO_TEST_READER_USER=trino-reader`, and `SILO_TEST_READER_PASSWORD=trino-reader`.
Use `just trino-ducklake-smoke`, `just perf-trino-ducklake`, and
`just perf-trino-ducklake-realistic` for the opt-in Trino paths.
Kind and OrbStack setup remain `just run-multitenant-kind` and
`just run-multitenant-local`, respectively.

## Migrate an existing local environment

1. Record the current checkout and the actual image digests of existing server
   and client containers before replacing them. Older configuration used mutable
   tags, so a tag alone is not a reproducible rollback target.
2. Stop Duckgres, Trino, and other writers, then stop the affected Compose stack.
   Back up its existing object-storage data and metadata together while stopped.
   Preserve the entire object-store volume, including hidden server metadata,
   and the PostgreSQL data or a verified database backup. Do not use `down -v`.
   Local Kubernetes dependencies use tmpfs for object and metadata storage;
   export any fixture data you need before stopping those containers.
3. Switch to the migration checkout and pull the pinned image. Start only the
   intended local stack with its existing Compose project name and volumes.
   For root Compose, run `docker compose up -d`; for test and Kubernetes
   fixtures, use the explicit integration setup commands or Kubernetes recipes
   above.
4. Check that the object store becomes healthy and initialization exits with
   status zero. Verify bucket access and run the affected test recipes before
   resuming local work. Credential rotation tests must still reject disabled
   credentials and accept the replacement credentials; S3 compatibility alone
   does not establish admin API compatibility.

## Failure recovery and rollback

Inspect `docker compose ps -a` and `docker compose logs minio minio-init` for
the root stack; add `-f tests/integration/docker-compose.yml` for the integration
stack. A failing healthcheck or nonzero initialization exit should be fixed
before starting clients. Confirm the pinned image was pulled for the intended
architecture, the configured credentials agree, and the configured ports are
available. Keep `MINIO_*` settings and `/minio/*` routes unchanged.

To roll back, stop writers and the affected stack, preserve the failed state
for diagnosis, restore the pre-migration object-store and metadata backups as
a consistent pair, and restore the previous checkout and recorded image
digests. Then start the previous stack and verify access before resuming work.
Do not assume that starting an older MinIO image against data already written
by Silo is a safe rollback. For disposable test fixtures, explicitly discard
and reseed only that fixture's data instead of restoring a backup.
