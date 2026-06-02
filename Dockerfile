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
ARG DUCKDB_EXTENSION_VERSION=1.5.3
ARG HTTPFS_EXTENSION_TAG=v1.5.3-stoi-fix
ARG DUCKLAKE_EXTENSION_TAG=v1.0-posthog.4
ARG DUCKDB_EXTENSION_REPOSITORY=https://extensions.duckdb.org
# Repository for postgres_scanner specifically. Defaults to the stable
# extensions repo, overridable per-row in CI (e.g. legacy DuckDB versions
# may need the nightly repo to match what was previously published).
ARG POSTGRES_SCANNER_REPOSITORY=https://extensions.duckdb.org
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
    && mkdir -p "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}" \
    && curl -fsSL "https://github.com/PostHog/duckdb-httpfs/releases/download/${HTTPFS_EXTENSION_TAG}/httpfs-linux-${TARGETARCH}.duckdb_extension" \
      -o "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/httpfs.duckdb_extension" \
    && curl -fsSL "https://github.com/PostHog/ducklake/releases/download/${DUCKLAKE_EXTENSION_TAG}/ducklake-linux-${TARGETARCH}.duckdb_extension" \
      -o "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/ducklake.duckdb_extension" \
    && curl -fsSL "${DUCKDB_EXTENSION_REPOSITORY}/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/json.duckdb_extension.gz" \
      | gunzip > "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/json.duckdb_extension" \
    && curl -fsSL "${POSTGRES_SCANNER_REPOSITORY}/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/postgres_scanner.duckdb_extension.gz" \
      | gunzip > "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/postgres_scanner.duckdb_extension" \
    && curl -fsSL "${DUCKDB_EXTENSION_REPOSITORY}/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/iceberg.duckdb_extension.gz" \
      | gunzip > "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/iceberg.duckdb_extension" \
    && for f in httpfs ducklake json postgres_scanner iceberg; do \
         [ -s "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/$f.duckdb_extension" ] \
           || { echo "ERROR: $f.duckdb_extension is empty after fetch" >&2; exit 1; }; \
       done

COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TAGS=""
RUN CGO_ENABLED=1 go build -tags "${BUILD_TAGS}" -ldflags "-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o duckgres .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN groupadd -r duckgres && useradd -r -g duckgres -d /app duckgres

WORKDIR /app
COPY --from=builder /build/duckgres .
COPY --from=builder /build/duckdb-extensions ./extensions
RUN mkdir -p data certs && chown -R duckgres:duckgres /app

USER duckgres

EXPOSE 5432 8816 9090

ENTRYPOINT ["/app/duckgres"]
