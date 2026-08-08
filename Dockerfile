# Build the admin console SPA (controlplane/admin/ui) so the kubernetes build
# embeds a FRESH dist/ via //go:embed all:ui/dist, overwriting the committed
# bundle. Runs before the Go build.
FROM node:20-bookworm-slim AS uibuilder
WORKDIR /ui
COPY controlplane/admin/ui/package.json controlplane/admin/ui/package-lock.json ./
RUN npm ci
COPY controlplane/admin/ui/ ./
RUN npm run build

FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ libc6-dev curl gzip && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

# Bundled DuckDB extensions. Downloaded BEFORE `COPY . .` so this layer
# depends only on the extension version args, not on source — a source-only
# PR keeps the GHA layer-cache hit and skips the 5 downloads entirely. (They
# previously ran after the source COPY + build, so they re-fetched on every
# edit.)
ARG TARGETARCH
ARG DUCKDB_EXTENSION_VERSION=1.5.5
ARG HTTPFS_EXTENSION_TAG=v1.5.5-cred-refresh-write-retry
ARG DUCKLAKE_EXTENSION_TAG=v1.0-posthog.7
ARG DUCKDB_EXTENSION_REPOSITORY=https://extensions.duckdb.org
# Repository for postgres_scanner specifically. The checksums content-pin the
# DuckDB 1.5.5 nightly artifact built from duckdb-postgres ab217c6; CI overrides
# all three values together for rollback rows.
ARG POSTGRES_SCANNER_REPOSITORY=https://nightly-extensions.duckdb.org
ARG POSTGRES_SCANNER_SHA256_AMD64=7ff4913fab203f7895eaa6a9a87a14a7ad659d400deff667f8bf16e58a28937f
ARG POSTGRES_SCANNER_SHA256_ARM64=9ea7d5a3610f2b460bd2a5075684c5f6dad55fa19745a377b5471f4994a8b460
# `: ${VAR:?msg}` asserts every required input is non-empty — catches a
# CI matrix row that forgets to pass a build-arg and would otherwise
# silently fall back to the ARG default, producing a cross-version
# bundle (the failure class the binding-pin check in Dockerfile.worker
# exists to prevent). The per-file `[ -s ... ]` size check below catches
# the curl|gunzip failure modes — a curl -fsSL 404 writes nothing, gunzip
# on empty input exits non-zero, the && chain breaks. (`set -o pipefail`
# would be cleaner but /bin/sh here is dash, which rejects -o pipefail.)
RUN : "${DUCKDB_EXTENSION_VERSION:?must be set}" \
    && : "${HTTPFS_EXTENSION_TAG:?must be set}" \
    && : "${DUCKLAKE_EXTENSION_TAG:?must be set}" \
    && : "${DUCKDB_EXTENSION_REPOSITORY:?must be set}" \
    && : "${POSTGRES_SCANNER_REPOSITORY:?must be set}" \
    && case "${TARGETARCH}" in \
         amd64) postgres_scanner_sha256="${POSTGRES_SCANNER_SHA256_AMD64}" ;; \
         arm64) postgres_scanner_sha256="${POSTGRES_SCANNER_SHA256_ARM64}" ;; \
         *) echo "ERROR: unsupported TARGETARCH for postgres_scanner: ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && : "${postgres_scanner_sha256:?postgres_scanner checksum must be set}" \
    && mkdir -p "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}" \
    && curl -fsSL "https://github.com/PostHog/duckdb-httpfs/releases/download/${HTTPFS_EXTENSION_TAG}/httpfs-linux-${TARGETARCH}.duckdb_extension" \
      -o "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/httpfs.duckdb_extension" \
    && curl -fsSL "https://github.com/PostHog/ducklake/releases/download/${DUCKLAKE_EXTENSION_TAG}/ducklake-linux-${TARGETARCH}.duckdb_extension" \
      -o "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/ducklake.duckdb_extension" \
    && curl -fsSL "${DUCKDB_EXTENSION_REPOSITORY}/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/json.duckdb_extension.gz" \
      | gunzip > "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/json.duckdb_extension" \
    && curl -fsSL "${POSTGRES_SCANNER_REPOSITORY}/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/postgres_scanner.duckdb_extension.gz" \
      -o /tmp/postgres_scanner.duckdb_extension.gz \
    && echo "${postgres_scanner_sha256}  /tmp/postgres_scanner.duckdb_extension.gz" | sha256sum -c - \
    && gunzip -c /tmp/postgres_scanner.duckdb_extension.gz \
      > "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/postgres_scanner.duckdb_extension" \
    && rm /tmp/postgres_scanner.duckdb_extension.gz \
    && for f in httpfs ducklake json postgres_scanner; do \
         [ -s "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/$f.duckdb_extension" ] \
           || { echo "ERROR: $f.duckdb_extension is empty after fetch" >&2; exit 1; }; \
       done

COPY . .
# Overwrite the committed placeholder with the freshly built SPA so the
# kubernetes build embeds the real bundle.
COPY --from=uibuilder /ui/dist ./controlplane/admin/ui/dist
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TAGS=""
RUN CGO_ENABLED=1 \
    DUCKGRES_TEST_DUCKDB_EXTENSION_DIRECTORY=/build/duckdb-extensions \
    go test -count=1 -tags "${BUILD_TAGS}" \
      -run '^TestDoCopyFromStdinIngestsPostgresBinaryWithBundledScanner$' \
      ./duckdbservice
RUN CGO_ENABLED=1 go build -tags "${BUILD_TAGS}" -ldflags "-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o duckgres .

FROM chainguard/wolfi-base:latest

# postgresql-18-client provides pg_dump/pg_restore, used by the control-plane
# reshard runner's pre-flip catalog backup (docs/design/resharding.md). Pinned
# to PG 18 to match the cnpg shard major so it can dump PG-18 catalogs. mw-dev
# runs this single all-in-one image as BOTH control plane and workers, so the
# client must live here (not only in Dockerfile.controlplane). libstdc++ is
# the C++ runtime the CGO-linked DuckDB engine needs — present implicitly on
# debian-slim, but not in wolfi-base.
RUN apk add --no-cache ca-certificates-bundle libstdc++ postgresql-18-client \
    && addgroup -S duckgres && adduser -S -G duckgres -h /app duckgres

WORKDIR /app
COPY --from=builder /build/duckgres .
COPY --from=builder /build/duckdb-extensions ./extensions
RUN mkdir -p data certs && chown -R duckgres:duckgres /app

USER duckgres

EXPOSE 5432 8816 9090

ENTRYPOINT ["/app/duckgres"]
