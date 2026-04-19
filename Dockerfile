FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ libc6-dev curl gzip && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TAGS=""
ARG TARGETARCH
ARG DUCKDB_EXTENSION_VERSION=1.5.2
RUN CGO_ENABLED=1 go build -tags "${BUILD_TAGS}" -ldflags "-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o duckgres .
RUN mkdir -p "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}" \
    && curl -fsSL "https://extensions.duckdb.org/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/httpfs.duckdb_extension.gz" \
      | gzip -d > "/build/duckdb-extensions/v${DUCKDB_EXTENSION_VERSION}/linux_${TARGETARCH}/httpfs.duckdb_extension"

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
