FROM golang:1.25-bookworm AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o cache-proxy ./cmd/cache-proxy/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN groupadd -r cacheproxy && useradd -r -g cacheproxy -d /app cacheproxy
WORKDIR /app
COPY --from=builder /build/cache-proxy .
RUN mkdir -p /cache && chown -R cacheproxy:cacheproxy /app /cache
USER cacheproxy
EXPOSE 8080 8081 8082
ENTRYPOINT ["/app/cache-proxy"]
