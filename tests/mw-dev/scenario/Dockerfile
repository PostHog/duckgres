FROM public.ecr.aws/docker/library/golang:1.25-bookworm

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m pip install --break-system-packages --no-cache-dir dbt-postgres

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go test -run '^$' ./tests/mw-dev/scenario

ENV DUCKGRES_SCENARIO_DBT_BIN=dbt

ENTRYPOINT ["bash", "scripts/scenario_run.sh"]
