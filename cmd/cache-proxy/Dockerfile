FROM golang:1.27-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o cache-proxy ./cmd/cache-proxy/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN groupadd -r cacheproxy && useradd -r -g cacheproxy -d /app cacheproxy
WORKDIR /app
COPY --from=builder /build/cache-proxy .
RUN mkdir -p /cache && chown -R cacheproxy:cacheproxy /app /cache
USER cacheproxy
EXPOSE 8080 8081 8082
ENTRYPOINT ["/app/cache-proxy"]
