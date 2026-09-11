FROM public.ecr.aws/docker/library/golang:1.25-bookworm

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m pip install --break-system-packages --no-cache-dir dbt-postgres boto3 pyarrow

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go test -run '^$' ./tests/mw-dev/scenario
RUN python3 -m unittest discover -s tests/mw-dev/scenario/perf -p test_setup_hoglake.py

ENV DUCKGRES_SCENARIO_DBT_BIN=dbt

ENTRYPOINT ["bash", "scripts/scenario_run.sh"]
