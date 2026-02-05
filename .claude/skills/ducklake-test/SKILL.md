---
name: ducklake-test
description: Start duckgres with DuckLake (S3-backed storage with PostgreSQL metadata) for local testing. Use when testing duckgres against DuckLake or reproducing Fivetran sync issues.
disable-model-invocation: true
---

Start duckgres with DuckLake configuration:

1. Start dependencies (PostgreSQL metadata store + MinIO object storage):
   ```bash
   docker-compose up -d
   ```

2. Kill any existing duckgres process:
   ```bash
   pkill -f duckgres || true
   ```

3. Ensure config file exists at `duckgres_local_test.yaml`:
   ```yaml
   host: "0.0.0.0"
   port: 35437
   data_dir: "./data"
   users:
     postgres: "postgres"
   extensions:
     - ducklake
   ducklake:
     metadata_store: "postgres:host=localhost port=5433 user=ducklake password=ducklake dbname=ducklake"
     object_store: "s3://ducklake/data/"
     s3_provider: "config"
     s3_endpoint: "localhost:9000"
     s3_access_key: "minioadmin"
     s3_secret_key: "minioadmin"
     s3_region: "us-east-1"
     s3_use_ssl: false
     s3_url_style: "path"
   ```

4. Build and run duckgres:
   ```bash
   go build -o duckgres . && ./duckgres --config duckgres_local_test.yaml
   ```

5. Connect with psql (in another terminal):
   ```bash
   PGPASSWORD=postgres psql "host=127.0.0.1 port=35437 user=postgres sslmode=require"
   ```

To cleanup: `pkill -f duckgres && docker-compose down`
