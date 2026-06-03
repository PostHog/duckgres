#!/usr/bin/env bash
#
# Bootstrap the local Lakekeeper Iceberg REST catalog and create the "local"
# warehouse backed by the local MinIO bucket. Idempotent: safe to run on every
# `just multitenant-config-store-up[-kind]`.
#
# The Lakekeeper catalog Postgres uses tmpfs (see local-config-store.compose.yaml),
# so its state is wiped whenever the container restarts — which is exactly why
# this runs unconditionally and tolerates "already done" responses.
#
# Both the OrbStack and kind flows expose Lakekeeper on host port 38181, so the
# default URL works for both. Override with LAKEKEEPER_URL if needed.
set -euo pipefail

LAKEKEEPER_URL="${LAKEKEEPER_URL:-http://127.0.0.1:38181}"
WAREHOUSE_NAME="${WAREHOUSE_NAME:-local}"

# Endpoint Lakekeeper itself uses to reach MinIO (compose service DNS). This is
# distinct from the endpoint DuckDB uses (host.docker.internal:39000 on OrbStack,
# duckgres-local-minio:9000 on kind) — both point at the same bucket, and Iceberg
# metadata stores absolute s3:// paths, so the differing endpoints are consistent.
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
S3_BUCKET="${S3_BUCKET:-duckgres-local}"
S3_KEY_PREFIX="${S3_KEY_PREFIX:-iceberg/local}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"

log() { printf '[lakekeeper-bootstrap] %s\n' "$*" >&2; }

# Wait for Lakekeeper to report healthy.
log "waiting for Lakekeeper at ${LAKEKEEPER_URL} ..."
for i in $(seq 1 60); do
  if curl -fsS -o /dev/null "${LAKEKEEPER_URL}/health"; then
    log "Lakekeeper is healthy"
    break
  fi
  if [ "$i" -eq 60 ]; then
    log "ERROR: Lakekeeper did not become healthy in time"
    exit 1
  fi
  sleep 1
done

# POST helper: prints the HTTP status, treats the listed extra codes as success.
# Usage: post_json <path> <json> <ok-extra-codes...>
post_json() {
  local path="$1"; shift
  local body="$1"; shift
  local resp status
  resp="$(curl -sS -w $'\n%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    --data "${body}" \
    "${LAKEKEEPER_URL}${path}")"
  status="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  for code in 2 "$@"; do
    case "${status}" in
      "${code}"*) return 0 ;;
    esac
  done
  log "ERROR: POST ${path} -> HTTP ${status}"
  log "${body}"
  return 1
}

# Bootstrap the server (creates the default project). Already-bootstrapped
# servers return a 4xx we tolerate.
log "bootstrapping server ..."
post_json /management/v1/bootstrap '{"accept-terms-of-use": true}' 400 409 \
  || { log "bootstrap reported an error; assuming already bootstrapped"; }

# Create the warehouse — but only if it doesn't already exist. Lakekeeper
# rejects a duplicate with HTTP 400 (storage-profile overlap), not a clean
# 409, so we check first rather than tolerate an ambiguous error code. This
# keeps the script idempotent across repeated `up` runs.
if curl -fsS "${LAKEKEEPER_URL}/management/v1/warehouse" | tr -d ' \n' \
    | grep -q "\"name\":\"${WAREHOUSE_NAME}\""; then
  log "warehouse '${WAREHOUSE_NAME}' already exists; skipping creation"
  log "done: warehouse '${WAREHOUSE_NAME}' is ready (REST catalog at ${LAKEKEEPER_URL}/catalog)"
  exit 0
fi

log "creating warehouse '${WAREHOUSE_NAME}' on bucket '${S3_BUCKET}' ..."
read -r -d '' WAREHOUSE_JSON <<JSON || true
{
  "warehouse-name": "${WAREHOUSE_NAME}",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "${S3_BUCKET}",
    "key-prefix": "${S3_KEY_PREFIX}",
    "endpoint": "${MINIO_ENDPOINT}",
    "region": "${S3_REGION}",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "access-key-id": "${S3_ACCESS_KEY}",
    "secret-access-key": "${S3_SECRET_KEY}"
  }
}
JSON
post_json /management/v1/warehouse "${WAREHOUSE_JSON}" 409

log "done: warehouse '${WAREHOUSE_NAME}' is ready (REST catalog at ${LAKEKEEPER_URL}/catalog)"
