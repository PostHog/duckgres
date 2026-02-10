FROM golang:1.25-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o duckgres .

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /build/duckgres .
RUN mkdir -p data certs

EXPOSE 5432 9090

ENTRYPOINT ["/app/duckgres"]
