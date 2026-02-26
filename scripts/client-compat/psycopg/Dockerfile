FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir psycopg2-binary pyyaml

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml
COPY report_client.py /report_client.py
COPY psycopg/test_psycopg.py /test_psycopg.py

ENTRYPOINT ["/entrypoint.sh", "python", "/test_psycopg.py"]
