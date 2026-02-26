FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml
COPY report_client.py /report_client.py
COPY clients/harlequin/test_harlequin.py /test_harlequin.py

ENTRYPOINT ["/entrypoint.sh", "uv", "run", "--with", "harlequin[postgres]", "--with", "psycopg2-binary", "--with", "pyyaml", "python", "/test_harlequin.py"]
