FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends gdb procps ca-certificates && rm -rf /var/lib/apt/lists/*
CMD ["sleep", "1800"]
