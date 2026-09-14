FROM python:3.12-slim-bookworm
ARG TARGETARCH
RUN pip install --no-cache-dir duckdb==1.5.5 pyarrow==21.0.0
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/duckdb-extensions \
    && curl -fsSL "https://github.com/PostHog/duckdb-httpfs/releases/download/v1.5.5-cred-refresh-write-retry/httpfs-linux-${TARGETARCH}.duckdb_extension" -o /opt/duckdb-extensions/httpfs.duckdb_extension \
    && curl -fsSL "https://github.com/PostHog/ducklake/releases/download/v1.0-posthog.7/ducklake-linux-${TARGETARCH}.duckdb_extension" -o /opt/duckdb-extensions/ducklake.duckdb_extension
COPY experiments/properties-memory/probe.py /experiment/probe.py
ENTRYPOINT ["python", "/experiment/probe.py"]
