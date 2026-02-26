FROM golang:1.24-bookworm AS builder

WORKDIR /app
COPY pgx/go.mod pgx/go.sum ./
RUN go mod download
COPY pgx/main.go .
RUN CGO_ENABLED=0 go build -o /test_pgx .

FROM debian:bookworm-slim

COPY --from=builder /test_pgx /test_pgx
COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml

ENTRYPOINT ["/entrypoint.sh", "/test_pgx"]
