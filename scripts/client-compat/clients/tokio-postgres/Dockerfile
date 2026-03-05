FROM rust:1.84-bookworm AS builder

WORKDIR /app
COPY clients/tokio-postgres/Cargo.toml ./
# Create dummy main to cache dependency builds
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY clients/tokio-postgres/src ./src
# Touch main.rs so cargo rebuilds it (not just the deps)
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rust-compat-test /test_rust
COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml

ENTRYPOINT ["/entrypoint.sh", "/test_rust"]
