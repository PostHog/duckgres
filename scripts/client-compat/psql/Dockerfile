FROM postgres:17-bookworm

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml
COPY psql/test_psql.sh /test_psql.sh

ENTRYPOINT ["/entrypoint.sh", "/test_psql.sh"]
