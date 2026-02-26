FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir dbt-postgres pyyaml

COPY entrypoint.sh /entrypoint.sh
COPY queries.yaml /queries.yaml
COPY report_client.py /report_client.py
COPY clients/dbt/test_dbt.py /test_dbt.py
COPY clients/dbt/project /dbt_project

ENTRYPOINT ["/entrypoint.sh", "python", "/test_dbt.py"]
