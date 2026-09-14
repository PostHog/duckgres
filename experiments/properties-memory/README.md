# Synthetic properties memory experiment

This diagnostic runs DuckDB 1.5.5 directly, avoiding Flight schema discovery's duplicate execution. The image bundles the perf worker's HTTPFS and DuckLake extension versions, but queries read local synthetic Parquet and do not attach a DuckLake catalog. This isolates Parquet scan and expression memory; it does not measure remote I/O or Duckgres overhead.

Run one job per JSON width (4096, 16384, 65536 bytes), each with 131072 rows and 8192 rows per Parquet row group. Each job sequentially tests threads 1, 4, 8 and three queries: JSON browser aggregation, SUM(length(properties)), and equivalent STRUCT aggregation. Every query uses a fresh process. Defaults: 48GiB DuckDB memory limit and 180 seconds per query, with killed queries reported before continuing. Orchestration should request 8 CPU and 64Gi memory per job and impose a 40-minute job deadline and a 41-minute orchestration deadline, including scheduling and fixture generation. Run jobs on separate nodes to prevent resource contention.

JSON documents have exact specified byte widths, four browser values and deterministic per-row hexadecimal padding. Dictionary encoding is disabled. Zstd compression and row-group geometry are fixed. Fixtures are generated once per width; generation memory is excluded from child peak RSS. Files and spill stay in the job's temporary volume. Fresh processes reset engine state, but the OS page cache remains warm after generation, so this is a memory experiment rather than a cold-cache latency benchmark.

Each query emits JSONL with wall time, process peak RSS, DuckDB peak buffer and temporary directory usage, engine version, configuration, and synthetic result validation. Missing profiles after errors have null memory metrics rather than misleading zeros. Child baseline peak RSS includes Python/DuckDB import; peak RSS is the whole process and cannot be interpreted as DuckDB buffers alone. Profiles are retained in the work directory. Generation emits its own timing and file-size record.

Build from the repository root with `docker build -f experiments/properties-memory/Dockerfile -t properties-memory .`. Container arguments example:

```sh
--width 4096 --rows 131072 --row-group-size 8192 --threads 1,4,8 --memory-limit 48GiB --query-timeout 180 --work-dir /work --output /work/results.jsonl
```

For a local smoke run, install the pinned Python packages in a temporary virtualenv, then run:

```sh
python experiments/properties-memory/probe.py --width 128 --rows 2048 --row-group-size 512 --threads 1 --memory-limit 512MiB --work-dir /tmp/properties-memory-smoke --output /tmp/properties-memory-smoke/results.jsonl
```

Use a new work directory for each attempt; existing result files are rejected. On failure, collect emitted JSONL and profiles before deleting temporary resources. Never upload fixtures: only synthetic measurement summaries are needed. Remove the temporary namespace/volume after collection; no AWS credentials or S3 fixture paths are required by this probe.
