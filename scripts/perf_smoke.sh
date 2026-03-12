#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATASET_VERSION="${DUCKGRES_PERF_DATASET_VERSION:-}"
MANIFEST_TABLE="${DUCKGRES_PERF_DATASET_MANIFEST_TABLE:-ducklake.main.dataset_manifest}"
PGWIRE_DSN="${DUCKGRES_PERF_PGWIRE_DSN:-}"
FLIGHT_ADDR="${DUCKGRES_PERF_FLIGHT_ADDR:-}"
PERF_USERNAME="${DUCKGRES_PERF_USERNAME:-perfuser}"
PERF_PASSWORD="${DUCKGRES_PERF_PASSWORD:-perfpass}"
if [[ -n "$DATASET_VERSION" ]]; then
  DEFAULT_CATALOG="$ROOT_DIR/tests/perf/queries/ducklake_frozen.yaml"
else
  DEFAULT_CATALOG="$ROOT_DIR/tests/perf/queries/smoke.yaml"
fi
CATALOG_PATH="${DUCKGRES_PERF_CATALOG:-$DEFAULT_CATALOG}"
OUTPUT_BASE="${DUCKGRES_PERF_OUTPUT_BASE:-$ROOT_DIR/artifacts/perf}"
RUN_ID="${DUCKGRES_PERF_RUN_ID:-smoke-$(date -u +%Y%m%dT%H%M%SZ)}"

require_frozen_manifest() {
  if [[ -z "$DATASET_VERSION" ]]; then
    return 0
  fi
  if [[ -z "$PGWIRE_DSN" ]]; then
    echo "DUCKGRES_PERF_PGWIRE_DSN is required when DUCKGRES_PERF_DATASET_VERSION is set"
    exit 1
  fi
  if [[ -z "$FLIGHT_ADDR" ]]; then
    echo "DUCKGRES_PERF_FLIGHT_ADDR is required when DUCKGRES_PERF_DATASET_VERSION is set"
    exit 1
  fi
  if ! command -v psql >/dev/null 2>&1; then
    echo "psql is required to validate dataset manifest expectations"
    exit 1
  fi

  local escaped_version
  escaped_version="${DATASET_VERSION//\'/\'\'}"
  local count_sql
  count_sql="SELECT COUNT(*) FROM ${MANIFEST_TABLE} WHERE dataset_version = '${escaped_version}';"
  local manifest_count
  if ! manifest_count="$(PGPASSWORD="$PERF_PASSWORD" psql "$PGWIRE_DSN" -Atqc "$count_sql" | tr -d '[:space:]')"; then
    echo "Failed to read manifest table ${MANIFEST_TABLE}; dataset_version=${DATASET_VERSION}"
    exit 1
  fi
  if [[ "$manifest_count" =~ ^[0-9]+$ ]] && [[ "$manifest_count" -gt 0 ]]; then
    return 0
  fi
  echo "Manifest check failed for dataset_version=${DATASET_VERSION} in ${MANIFEST_TABLE}"
  exit 1
}

echo "Running perf smoke harness"
echo "  catalog: $CATALOG_PATH"
echo "  output:  $OUTPUT_BASE/$RUN_ID"
if [[ -n "$DATASET_VERSION" ]]; then
  echo "  dataset_version: $DATASET_VERSION"
  echo "  manifest_table:  $MANIFEST_TABLE"
fi

mkdir -p "$OUTPUT_BASE"
require_frozen_manifest

go_test_args=(
  go test ./tests/perf
  -run TestGoldenQueryPerformanceHarness
  -perf-run
  -perf-catalog "$CATALOG_PATH"
  -perf-output-base "$OUTPUT_BASE"
  -perf-run-id "$RUN_ID"
  -perf-username "$PERF_USERNAME"
  -perf-password "$PERF_PASSWORD"
)
if [[ -n "$PGWIRE_DSN" ]]; then
  go_test_args+=(-perf-pgwire-dsn "$PGWIRE_DSN")
fi
if [[ -n "$FLIGHT_ADDR" ]]; then
  go_test_args+=(-perf-flight-addr "$FLIGHT_ADDR")
fi
if [[ -n "$PGWIRE_DSN" ]]; then
  PGPASSWORD="$PERF_PASSWORD" "${go_test_args[@]}"
else
  "${go_test_args[@]}"
fi

echo "Perf smoke artifacts written to $OUTPUT_BASE/$RUN_ID"
