FROM golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o cache-proxy ./cmd/cache-proxy/

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN groupadd -r cacheproxy && useradd -r -g cacheproxy -d /app cacheproxy
WORKDIR /app
COPY --from=builder /build/cache-proxy .
RUN mkdir -p /cache && chown -R cacheproxy:cacheproxy /app /cache
USER cacheproxy
EXPOSE 8080 8081 8082
ENTRYPOINT ["/app/cache-proxy"]
