FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml
COPY clients/psql/test_psql.sh /test_psql.sh

ENTRYPOINT ["/entrypoint.sh", "/test_psql.sh"]
