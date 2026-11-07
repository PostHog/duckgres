FROM golang:1.27-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0 AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags "-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o cache-proxy ./cmd/cache-proxy/

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN groupadd -r cacheproxy && useradd -r -g cacheproxy -d /app cacheproxy
WORKDIR /app
COPY --from=builder /build/cache-proxy .
RUN mkdir -p /cache && chown -R cacheproxy:cacheproxy /app /cache
USER cacheproxy
EXPOSE 8080 8081 8082
ENTRYPOINT ["/app/cache-proxy"]
