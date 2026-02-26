FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir psycopg2-binary sqlalchemy pyyaml

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml
COPY report_client.py /report_client.py
COPY sqlalchemy/test_sqlalchemy.py /test_sqlalchemy.py

ENTRYPOINT ["/entrypoint.sh", "python", "/test_sqlalchemy.py"]
